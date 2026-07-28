import type { AnalyticsEntityHead } from "@prisma/client";
import { encodeFileReferenceIdentity } from "@langfuse/shared/src/server";
import { describe, expect, it, vi } from "vitest";

import { deleteDorisProjectRetentionHeads } from "./deleteDorisProjectRetentionHeads";

function head(
  input: Partial<AnalyticsEntityHead> & Pick<AnalyticsEntityHead, "entityType">,
): AnalyticsEntityHead {
  return {
    id: "head-1",
    projectId: "project-1",
    entityType: input.entityType,
    entityKey:
      input.entityKey ??
      encodeFileReferenceIdentity({
        projectId: "project-1",
        entityType: "EVENT",
        entityId: "trace-1",
        fileId: "raw-file-1",
      }),
    lookupId: input.lookupId ?? "raw-file-1",
    owningTraceId: input.owningTraceId ?? "trace-1",
    owningDatasetId: null,
    owningDatasetRunId: null,
    sourceVersion: 1n,
    canonicalPayloadHash: "hash",
    partitionDate: new Date("2026-06-01T00:00:00.000Z"),
    canonicalizerVersion: "1",
    fenceGeneration: 1n,
    traceDeletionGeneration: 0n,
    projectDeletionGeneration: 0n,
    datasetDeletionGeneration: 0n,
    runDeletionGeneration: 0n,
    operationId: "ingestion-1",
    createdAt: new Date("2026-06-01T00:00:00.000Z"),
    updatedAt: new Date("2026-06-01T00:00:00.000Z"),
    ...input,
  };
}

describe("deleteDorisProjectRetentionHeads", () => {
  it("deletes retained raw objects before publishing Doris tombstones", async () => {
    const order: string[] = [];
    const query = vi
      .fn()
      .mockResolvedValueOnce([
        { bucket_path: "project-1/raw-1.json" },
        { bucket_path: "project-1/raw-1.json" },
        { bucket_path: null },
      ])
      .mockResolvedValueOnce([]);
    const deleteFiles = vi.fn(async () => {
      order.push("s3");
    });
    const deleteHeads = vi.fn(async () => {
      order.push("doris");
    });
    const heads = [
      head({ entityType: "FILE_REFERENCE" }),
      head({
        id: "head-2",
        entityType: "EVENT",
        entityKey: '["project-1","trace-1","span-1"]',
      }),
    ];

    await deleteDorisProjectRetentionHeads(
      "run-1",
      heads,
      {
        projectId: "project-1",
        cutoffDate: new Date("2026-07-01T00:00:00.000Z"),
      },
      {
        query,
        bucketName: "events",
        deleteFiles,
        deleteHeads,
      },
    );

    expect(query).toHaveBeenNthCalledWith(
      1,
      expect.stringContaining("FROM blob_storage_file_log"),
      ["project-1", "events", "2026-06-01", "EVENT", "trace-1", "raw-file-1"],
    );
    expect(query).toHaveBeenNthCalledWith(
      2,
      expect.stringContaining("SELECT DISTINCT bucket_path"),
      ["project-1", "events", "project-1/raw-1.json", "2026-07-01"],
    );
    expect(deleteFiles).toHaveBeenCalledWith(["project-1/raw-1.json"]);
    expect(deleteHeads).toHaveBeenCalledWith("run-1", heads);
    expect(order).toEqual(["s3", "doris"]);
  });

  it("does not query object storage metadata for non-file heads", async () => {
    const query = vi.fn();
    const deleteFiles = vi.fn();
    const deleteHeads = vi.fn(async () => undefined);
    const heads = [
      head({
        entityType: "EVENT",
        entityKey: '["project-1","trace-1","span-1"]',
      }),
    ];

    await deleteDorisProjectRetentionHeads(
      "run-1",
      heads,
      {
        projectId: "project-1",
        cutoffDate: new Date("2026-07-01T00:00:00.000Z"),
      },
      {
        query,
        bucketName: "events",
        deleteFiles,
        deleteHeads,
      },
    );

    expect(query).not.toHaveBeenCalled();
    expect(deleteFiles).not.toHaveBeenCalled();
    expect(deleteHeads).toHaveBeenCalledWith("run-1", heads);
  });

  it("keeps a raw object that is still referenced by a current head", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([{ bucket_path: "project-1/shared.json" }])
      .mockResolvedValueOnce([{ bucket_path: "project-1/shared.json" }]);
    const deleteFiles = vi.fn();
    const deleteHeads = vi.fn(async () => undefined);
    const heads = [head({ entityType: "FILE_REFERENCE" })];

    await deleteDorisProjectRetentionHeads(
      "run-1",
      heads,
      {
        projectId: "project-1",
        cutoffDate: new Date("2026-07-01T00:00:00.000Z"),
      },
      {
        query,
        bucketName: "events",
        deleteFiles,
        deleteHeads,
      },
    );

    expect(deleteFiles).not.toHaveBeenCalled();
    expect(deleteHeads).toHaveBeenCalledWith("run-1", heads);
  });

  it("rejects a head outside the project scope before any deletion", async () => {
    const query = vi.fn();
    const deleteFiles = vi.fn();
    const deleteHeads = vi.fn();

    await expect(
      deleteDorisProjectRetentionHeads(
        "run-1",
        [
          head({
            projectId: "project-2",
            entityType: "EVENT",
            entityKey: '["project-2","trace-1","span-1"]',
          }),
        ],
        {
          projectId: "project-1",
          cutoffDate: new Date("2026-07-01T00:00:00.000Z"),
        },
        {
          query,
          bucketName: "events",
          deleteFiles,
          deleteHeads,
        },
      ),
    ).rejects.toThrow("scope mismatch");
    expect(query).not.toHaveBeenCalled();
    expect(deleteFiles).not.toHaveBeenCalled();
    expect(deleteHeads).not.toHaveBeenCalled();
  });
});
