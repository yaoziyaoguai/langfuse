import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

const OWNED_DIRECTORY = /^parquet-(aie_[a-f0-9]{28})-/;
const RESERVATION_FILE = ".reservation";

type ScratchAllocation = {
  readonly executionId: string;
  readonly directory: string;
  readonly relativeDirectory: string;
  readonly filePath: string;
  readonly reservedBytes: number;
  cleanup(): Promise<void>;
};

export class ParquetScratchManager {
  private mutex: Promise<void> = Promise.resolve();
  private initializedRoot: string | null = null;

  constructor(
    private readonly options: {
      readonly root?: string;
      readonly maxBytes?: number;
    } = {},
  ) {}

  private async initialize(): Promise<string> {
    if (this.initializedRoot) return this.initializedRoot;
    // macOS exposes the system temp directory through `/var`, whose trusted
    // OS-level alias resolves to `/private/var`. Canonicalize only the default
    // base; operator-configured roots retain the strict no-alias policy below.
    const configuredRoot =
      this.options.root ??
      path.join(await fs.realpath(tmpdir()), "langfuse-doris-parquet");
    if (!path.isAbsolute(configuredRoot)) {
      throw new Error("Parquet scratch root must be absolute");
    }
    await fs.mkdir(configuredRoot, { recursive: true, mode: 0o700 });
    const stat = await fs.lstat(configuredRoot);
    if (!stat.isDirectory() || stat.isSymbolicLink()) {
      throw new Error("Parquet scratch root is not a real directory");
    }
    await fs.chmod(configuredRoot, 0o700);
    const resolved = await fs.realpath(configuredRoot);
    if (resolved !== path.resolve(configuredRoot)) {
      throw new Error(
        "Parquet scratch root resolves outside its configured path",
      );
    }
    this.initializedRoot = resolved;
    return resolved;
  }

  private async withLock<T>(operation: () => Promise<T>): Promise<T> {
    const previous = this.mutex;
    let release!: () => void;
    this.mutex = new Promise<void>((resolve) => {
      release = resolve;
    });
    await previous;
    try {
      return await operation();
    } finally {
      release();
    }
  }

  private async withHostLock<T>(
    root: string,
    operation: () => Promise<T>,
  ): Promise<T> {
    const lockPath = path.join(root, ".quota.lock");
    for (let attempt = 0; attempt < 200; attempt += 1) {
      try {
        const handle = await fs.open(lockPath, "wx", 0o600);
        try {
          return await operation();
        } finally {
          await handle.close();
          await fs.unlink(lockPath).catch(() => undefined);
        }
      } catch (error) {
        if (
          !(error instanceof Error) ||
          !("code" in error) ||
          error.code !== "EEXIST"
        ) {
          throw error;
        }
        const stat = await fs.stat(lockPath).catch(() => null);
        if (stat && Date.now() - stat.mtimeMs > 60_000) {
          await fs.unlink(lockPath).catch(() => undefined);
          continue;
        }
        await new Promise((resolve) => setTimeout(resolve, 25));
      }
    }
    throw new Error("Parquet scratch quota lock is busy");
  }

  private async ownedBytes(root: string): Promise<number> {
    let bytes = 0;
    for (const entry of await fs.readdir(root, { withFileTypes: true })) {
      if (!entry.isDirectory() || !OWNED_DIRECTORY.test(entry.name)) continue;
      const directory = path.join(root, entry.name);
      const resolved = await fs.realpath(directory);
      if (!resolved.startsWith(`${root}${path.sep}`)) {
        throw new Error("Parquet scratch directory escaped its root");
      }
      let actualBytes = 0;
      let reservedBytes = 0;
      for (const file of await fs.readdir(directory, { withFileTypes: true })) {
        if (!file.isFile() || file.isSymbolicLink()) continue;
        if (file.name === RESERVATION_FILE) {
          const reservation = await fs.readFile(
            path.join(directory, file.name),
            "utf8",
          );
          const parsed = Number(reservation);
          if (!Number.isSafeInteger(parsed) || parsed < 1) {
            throw new Error("Parquet scratch reservation is invalid");
          }
          reservedBytes = parsed;
          continue;
        }
        actualBytes += (await fs.stat(path.join(directory, file.name))).size;
      }
      bytes += Math.max(actualBytes, reservedBytes);
    }
    return bytes;
  }

  async allocate(input: {
    readonly executionId: string;
    readonly reservedBytes: number;
  }): Promise<ScratchAllocation> {
    if (
      !/^aie_[a-f0-9]{28}$/.test(input.executionId) ||
      !Number.isSafeInteger(input.reservedBytes) ||
      input.reservedBytes < 1
    ) {
      throw new TypeError("Invalid Parquet scratch allocation");
    }
    return this.withLock(async () => {
      const root = await this.initialize();
      return this.withHostLock(root, async () => {
        const maxBytes = this.options.maxBytes ?? 2 * 1024 * 1024 * 1024;
        const usedBytes = await this.ownedBytes(root);
        if (usedBytes + input.reservedBytes > maxBytes) {
          throw new Error("Parquet scratch capacity is exhausted");
        }
        const directory = await fs.mkdtemp(
          path.join(root, `parquet-${input.executionId}-`),
        );
        await fs.chmod(directory, 0o700);
        const resolvedDirectory = await fs.realpath(directory);
        if (!resolvedDirectory.startsWith(`${root}${path.sep}`)) {
          throw new Error("Parquet scratch allocation escaped its root");
        }
        const reservationPath = path.join(resolvedDirectory, RESERVATION_FILE);
        await fs.writeFile(reservationPath, String(input.reservedBytes), {
          flag: "wx",
          mode: 0o600,
        });
        const filePath = path.join(resolvedDirectory, "data.parquet");
        const handle = await fs.open(filePath, "wx", 0o600);
        await handle.close();
        await fs.chmod(filePath, 0o600);
        return {
          executionId: input.executionId,
          directory: resolvedDirectory,
          relativeDirectory: path.relative(root, resolvedDirectory),
          filePath,
          reservedBytes: input.reservedBytes,
          cleanup: () => fs.rm(resolvedDirectory, { recursive: true }),
        };
      });
    });
  }

  async cleanupOrphans(input: {
    readonly isLive: (
      executionId: string,
      relativeDirectory: string,
    ) => Promise<boolean>;
    readonly onRemove?: (
      executionId: string,
      relativeDirectory: string,
    ) => Promise<void>;
  }): Promise<number> {
    return this.withLock(async () => {
      const root = await this.initialize();
      return this.withHostLock(root, async () => {
        let removed = 0;
        for (const entry of await fs.readdir(root, { withFileTypes: true })) {
          const match = OWNED_DIRECTORY.exec(entry.name);
          if (!entry.isDirectory() || !match?.[1]) continue;
          const directory = path.join(root, entry.name);
          const resolved = await fs.realpath(directory);
          if (!resolved.startsWith(`${root}${path.sep}`)) continue;
          if (await input.isLive(match[1], entry.name)) continue;
          await fs.rm(resolved, { recursive: true });
          await input.onRemove?.(match[1], entry.name);
          removed += 1;
        }
        return removed;
      });
    });
  }
}
