import { promises as fs } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { afterEach, describe, expect, it } from "vitest";

import { ParquetScratchManager } from "./ParquetScratchManager";

const roots: string[] = [];
const executionId = (suffix: string) =>
  `aie_${suffix.padEnd(28, "0").slice(0, 28)}`;

async function makeRoot(): Promise<string> {
  const created = await fs.mkdtemp(
    path.join(tmpdir(), "langfuse-scratch-test-"),
  );
  const root = await fs.realpath(created);
  roots.push(root);
  return root;
}

afterEach(async () => {
  for (const root of roots.splice(0)) {
    await fs.rm(root, { recursive: true, force: true });
  }
});

describe("ParquetScratchManager", () => {
  it("canonicalizes the trusted system temp base for default allocations", async () => {
    const manager = new ParquetScratchManager();
    const allocation = await manager.allocate({
      executionId: executionId("defa"),
      reservedBytes: 10,
    });

    await expect(fs.stat(allocation.filePath)).resolves.toBeDefined();
    await allocation.cleanup();
  });

  it("creates private owned paths and accounts concurrent reservations", async () => {
    const root = await makeRoot();
    const firstManager = new ParquetScratchManager({
      root,
      maxBytes: 100,
    });
    const secondManager = new ParquetScratchManager({
      root,
      maxBytes: 100,
    });
    const first = await firstManager.allocate({
      executionId: executionId("1"),
      reservedBytes: 60,
    });

    await expect(
      secondManager.allocate({
        executionId: executionId("2"),
        reservedBytes: 60,
      }),
    ).rejects.toThrow(/capacity is exhausted/i);
    expect((await fs.stat(root)).mode & 0o777).toBe(0o700);
    expect((await fs.stat(first.directory)).mode & 0o777).toBe(0o700);
    expect((await fs.stat(first.filePath)).mode & 0o777).toBe(0o600);

    await first.cleanup();
    await expect(
      secondManager.allocate({
        executionId: executionId("2"),
        reservedBytes: 60,
      }),
    ).resolves.toMatchObject({ reservedBytes: 60 });
  });

  it("removes only expired owned directories and preserves unknown paths", async () => {
    const root = await makeRoot();
    const manager = new ParquetScratchManager({ root, maxBytes: 1_000 });
    const live = await manager.allocate({
      executionId: executionId("3"),
      reservedBytes: 10,
    });
    const expired = await manager.allocate({
      executionId: executionId("4"),
      reservedBytes: 10,
    });
    const unknown = path.join(root, "operator-owned");
    await fs.mkdir(unknown);
    const removed: string[] = [];

    await expect(
      manager.cleanupOrphans({
        isLive: async (id) => id === live.executionId,
        onRemove: async (id) => {
          removed.push(id);
        },
      }),
    ).resolves.toBe(1);
    await expect(fs.stat(live.directory)).resolves.toBeDefined();
    await expect(fs.stat(expired.directory)).rejects.toThrow();
    await expect(fs.stat(unknown)).resolves.toBeDefined();
    expect(removed).toEqual([expired.executionId]);

    await expect(
      manager.cleanupOrphans({
        isLive: async () => false,
      }),
    ).resolves.toBe(1);
    await expect(
      manager.cleanupOrphans({
        isLive: async () => false,
      }),
    ).resolves.toBe(0);
  });

  it("rejects a symlinked configured root", async () => {
    const parent = await makeRoot();
    const target = path.join(parent, "target");
    const linked = path.join(parent, "linked");
    await fs.mkdir(target);
    await fs.symlink(target, linked);
    const manager = new ParquetScratchManager({ root: linked });

    await expect(
      manager.allocate({
        executionId: executionId("5"),
        reservedBytes: 10,
      }),
    ).rejects.toThrow(/not a real directory/i);
  });
});
