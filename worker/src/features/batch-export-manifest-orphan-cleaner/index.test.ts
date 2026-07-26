import { describe, expect, it, vi } from "vitest";

vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));
vi.mock("@langfuse/shared/src/server", () => ({
  logger: { error: vi.fn() },
  instrumentAsync: vi.fn(async (_options, operation) =>
    operation({ setAttribute: vi.fn() }),
  ),
}));

import { cleanupBatchExportManifestPage } from ".";

describe("batch export manifest orphan cleanup", () => {
  it("deletes only old, well-formed, non-authoritative attempt objects", async () => {
    const prefix = "tenant/batch-export-manifests/";
    const orphan = `${prefix}export-1/1/claim-a.ndjson.gz.b64`;
    const authoritative = `${prefix}export-2/1/claim-b.ndjson.gz.b64`;
    const young = `${prefix}export-3/1/claim-c.ndjson.gz.b64`;
    const malformed = `${prefix}unexpected.txt`;
    const storage = {
      listFilesPage: vi.fn().mockResolvedValue({
        files: [
          { file: orphan, createdAt: new Date("2026-07-20T00:00:00Z") },
          {
            file: authoritative,
            createdAt: new Date("2026-07-20T00:00:00Z"),
          },
          { file: young, createdAt: new Date("2026-07-23T11:30:00Z") },
          {
            file: malformed,
            createdAt: new Date("2026-07-20T00:00:00Z"),
          },
        ],
        nextCursor: "next-page",
      }),
      deleteFiles: vi.fn().mockResolvedValue(undefined),
    };
    const client = {
      batchExport: {
        findMany: vi.fn().mockResolvedValue([
          {
            id: "row-2",
            manifestObjectKey: authoritative,
            executionState: "EXPORTING",
            expiresAt: null,
            finishedAt: null,
            updatedAt: new Date("2026-07-20T00:00:00Z"),
          },
        ]),
      },
    };

    await expect(
      cleanupBatchExportManifestPage({
        client: client as never,
        storage,
        prefix,
        now: new Date("2026-07-23T12:00:00Z"),
        minAgeMs: 24 * 60 * 60_000,
        limit: 100,
      }),
    ).resolves.toEqual({ deleted: 1, nextCursor: "next-page" });
    expect(storage.deleteFiles).toHaveBeenCalledWith([orphan]);
  });

  it("deletes an expired terminal manifest without mutating its audit descriptor", async () => {
    const prefix = "batch-export-manifests/";
    const objectKey = `${prefix}export-1/2/claim-a.ndjson.gz.b64`;
    const storage = {
      listFilesPage: vi.fn().mockResolvedValue({
        files: [
          { file: objectKey, createdAt: new Date("2026-07-20T00:00:00Z") },
        ],
      }),
      deleteFiles: vi.fn().mockResolvedValue(undefined),
    };

    await expect(
      cleanupBatchExportManifestPage({
        client: {
          batchExport: {
            findMany: vi.fn().mockResolvedValue([
              {
                id: "row-1",
                manifestObjectKey: objectKey,
                executionState: "COMPLETED",
                expiresAt: new Date("2026-07-21T00:00:00Z"),
                finishedAt: new Date("2026-07-20T00:00:00Z"),
                updatedAt: new Date("2026-07-20T00:00:00Z"),
              },
            ]),
          },
        } as never,
        storage,
        prefix,
        now: new Date("2026-07-23T12:00:00Z"),
        minAgeMs: 24 * 60 * 60_000,
        limit: 100,
      }),
    ).resolves.toMatchObject({ deleted: 1 });
    expect(storage.deleteFiles).toHaveBeenCalledWith([objectKey]);
  });
});
