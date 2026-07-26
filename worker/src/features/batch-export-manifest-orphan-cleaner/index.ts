import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import type {
  StorageListFilesPage,
  StorageService,
} from "@langfuse/shared/src/server";

import { PeriodicRunner } from "../../utils/PeriodicRunner";

type ManifestStorage = Pick<StorageService, "listFilesPage" | "deleteFiles">;

function isManifestAttemptKey(key: string, prefix: string): boolean {
  if (!key.startsWith(prefix)) return false;
  const relative = key.slice(prefix.length);
  return /^[A-Za-z0-9_-]+\/[1-9][0-9]*\/[A-Za-z0-9_-]+\.ndjson\.gz\.b64$/.test(
    relative,
  );
}

export async function cleanupBatchExportManifestPage(input: {
  readonly client?: PrismaClient;
  readonly storage: ManifestStorage;
  readonly prefix: string;
  readonly cursor?: string;
  readonly now: Date;
  readonly minAgeMs: number;
  readonly limit: number;
}): Promise<{ readonly deleted: number; readonly nextCursor?: string }> {
  if (
    !input.prefix.endsWith("batch-export-manifests/") ||
    input.prefix.includes("..") ||
    Number.isNaN(input.now.getTime()) ||
    !Number.isSafeInteger(input.minAgeMs) ||
    input.minAgeMs < 10 * 60_000 ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 1_000
  ) {
    throw new TypeError("Invalid batch export manifest cleanup request");
  }
  const client = input.client ?? prisma;
  const page: StorageListFilesPage = await input.storage.listFilesPage(
    input.prefix,
    {
      ...(input.cursor ? { cursor: input.cursor } : {}),
      limit: input.limit,
    },
  );
  const cutoff = new Date(input.now.getTime() - input.minAgeMs);
  const candidates = page.files
    .filter(
      ({ file, createdAt }) =>
        createdAt <= cutoff && isManifestAttemptKey(file, input.prefix),
    )
    .map(({ file }) => file);
  if (candidates.length === 0) {
    return {
      deleted: 0,
      ...(page.nextCursor ? { nextCursor: page.nextCursor } : {}),
    };
  }

  const rows = await client.batchExport.findMany({
    where: { manifestObjectKey: { in: candidates } },
    select: {
      id: true,
      manifestObjectKey: true,
      executionState: true,
      expiresAt: true,
      finishedAt: true,
      updatedAt: true,
    },
  });
  const referenced = new Set<string>();
  for (const row of rows) {
    if (!row.manifestObjectKey) continue;
    const terminal =
      row.executionState === "COMPLETED" ||
      row.executionState === "CANCELLED" ||
      row.executionState === "FAILED" ||
      row.executionState === "QUARANTINED";
    const terminalAt = row.expiresAt ?? row.finishedAt ?? row.updatedAt;
    if (terminal && terminalAt <= cutoff) {
      continue;
    }
    referenced.add(row.manifestObjectKey);
  }
  const orphaned = candidates.filter((key) => !referenced.has(key));
  if (orphaned.length > 0) await input.storage.deleteFiles(orphaned);
  return {
    deleted: orphaned.length,
    ...(page.nextCursor ? { nextCursor: page.nextCursor } : {}),
  };
}

export class BatchExportManifestOrphanCleaner extends PeriodicRunner {
  protected readonly name = "BatchExportManifestOrphanCleaner";
  private cursor: string | undefined;

  constructor(
    private readonly dependencies: {
      readonly storage: ManifestStorage;
      readonly prefix: string;
      readonly intervalMs: number;
      readonly minAgeMs: number;
      readonly batchSize: number;
      readonly cleanupPage?: typeof cleanupBatchExportManifestPage;
    },
  ) {
    super();
  }

  protected get defaultIntervalMs(): number {
    return this.dependencies.intervalMs;
  }

  public processPage(): Promise<number | void> {
    return this.execute();
  }

  protected async execute(): Promise<number | void> {
    const result = await (
      this.dependencies.cleanupPage ?? cleanupBatchExportManifestPage
    )({
      storage: this.dependencies.storage,
      prefix: this.dependencies.prefix,
      ...(this.cursor ? { cursor: this.cursor } : {}),
      now: new Date(),
      minAgeMs: this.dependencies.minAgeMs,
      limit: this.dependencies.batchSize,
    });
    this.cursor = result.nextCursor;
    return result.nextCursor ? 0 : undefined;
  }
}
