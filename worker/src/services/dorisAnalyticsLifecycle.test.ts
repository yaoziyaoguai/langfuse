import type { AnalyticsEntityHead } from "@prisma/client";
import {
  encodeEventIdentity,
  encodeFileReferenceIdentity,
  encodeScoreIdentity,
} from "@langfuse/shared/analytics-persistence";
import { describe, expect, it, vi } from "vitest";

import { DorisMaterializedDeletionWriter } from "./dorisAnalyticsLifecycle";

function head(
  entityType: AnalyticsEntityHead["entityType"],
  entityKey: string,
): AnalyticsEntityHead {
  return {
    id: `${entityType}-head`,
    projectId: "project-1",
    entityType,
    entityKey,
    lookupId: "lookup-1",
    owningTraceId: "trace-1",
    sourceVersion: 1n,
    canonicalPayloadHash: "hash",
    partitionDate: new Date("2026-07-18T00:00:00.000Z"),
    canonicalizerVersion: "r1a-v1",
    fenceGeneration: 1n,
    traceDeletionGeneration: 0n,
    projectDeletionGeneration: 0n,
    operationId: "ingestion-1",
    createdAt: new Date("2026-07-18T00:00:00.000Z"),
    updatedAt: new Date("2026-07-18T00:00:00.000Z"),
  };
}

describe("DorisMaterializedDeletionWriter", () => {
  it("loads terminal delete keys for every R1A materialized table", async () => {
    const load = vi.fn().mockResolvedValue({
      committed: true,
      requiresReconciliation: false,
      numberTotalRows: 1,
      numberFilteredRows: 0,
    });
    const writer = new DorisMaterializedDeletionWriter({
      load,
      reconcile: vi.fn(),
    });

    await writer.deleteHeads("deletion-1", [
      head(
        "EVENT",
        encodeEventIdentity({
          projectId: "project-1",
          traceId: "trace-1",
          spanId: "span-1",
        }),
      ),
      head(
        "SCORE",
        encodeScoreIdentity({ projectId: "project-1", scoreId: "score-1" }),
      ),
      head(
        "FILE_REFERENCE",
        encodeFileReferenceIdentity({
          projectId: "project-1",
          entityType: "EVENT",
          entityId: "span-1",
          fileId: "file-1",
        }),
      ),
    ]);

    expect(load).toHaveBeenCalledTimes(3);
    expect(load).toHaveBeenCalledWith(
      expect.objectContaining({
        table: "events_current",
        mergeType: "DELETE",
        ndjsonBody: expect.stringContaining(
          '"version_token":"9223372036854775807"',
        ),
      }),
    );
    expect(load).toHaveBeenCalledWith(
      expect.objectContaining({ table: "scores_current" }),
    );
    expect(load).toHaveBeenCalledWith(
      expect.objectContaining({ table: "blob_storage_file_log" }),
    );
    expect(
      load.mock.calls.every(([request]) => request.mergeType === "DELETE"),
    ).toBe(true);
    expect(
      load.mock.calls.every(
        ([request]) =>
          JSON.parse(request.ndjsonBody).project_id === "project-1",
      ),
    ).toBe(true);
  });

  it("rejects a head whose encoded identity belongs to another project", async () => {
    const writer = new DorisMaterializedDeletionWriter({
      load: vi.fn(),
      reconcile: vi.fn(),
    });

    await expect(
      writer.deleteHeads("deletion-1", [
        head(
          "SCORE",
          encodeScoreIdentity({ projectId: "project-2", scoreId: "score-1" }),
        ),
      ]),
    ).rejects.toThrow("Analytics entity head project mismatch");
  });

  it("accepts a deterministic delete label already proven visible", async () => {
    const reconcile = vi.fn().mockResolvedValue({
      status: "VISIBLE",
      visible: true,
    });
    const writer = new DorisMaterializedDeletionWriter({
      load: vi.fn().mockResolvedValue({
        status: "Label Already Exists",
        committed: false,
        requiresReconciliation: true,
        numberTotalRows: 0,
        numberFilteredRows: 0,
      }),
      reconcile,
    });

    await expect(
      writer.deleteHeads("deletion-1", [
        head(
          "SCORE",
          encodeScoreIdentity({ projectId: "project-1", scoreId: "score-1" }),
        ),
      ]),
    ).resolves.toBeUndefined();
    expect(reconcile).toHaveBeenCalledOnce();
  });

  it("uses stable delete batches and labels regardless of database row order", async () => {
    const requests: unknown[] = [];
    const load = vi.fn().mockImplementation(async (request) => {
      requests.push(request);
      return {
        committed: true,
        requiresReconciliation: false,
        numberTotalRows: request.ndjsonBody.trim().split("\n").length,
        numberFilteredRows: 0,
      };
    });
    const writer = new DorisMaterializedDeletionWriter({
      load,
      reconcile: vi.fn(),
    });
    const heads = [
      head(
        "SCORE",
        encodeScoreIdentity({ projectId: "project-1", scoreId: "score-2" }),
      ),
      head(
        "EVENT",
        encodeEventIdentity({
          projectId: "project-1",
          traceId: "trace-1",
          spanId: "span-1",
        }),
      ),
      head(
        "SCORE",
        encodeScoreIdentity({ projectId: "project-1", scoreId: "score-1" }),
      ),
    ];

    await writer.deleteHeads("deletion-1", heads);
    const firstRun = structuredClone(requests);
    requests.length = 0;
    await writer.deleteHeads("deletion-1", [...heads].reverse());

    expect(requests).toEqual(firstRun);
  });
});
