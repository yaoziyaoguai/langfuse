import { describe, expect, it, vi } from "vitest";

import { publishAnalyticsDatasetDeletionOutboxBatch } from ".";

function operation() {
  return {
    id: "operation-1",
    scope: "DATASET_RUNS" as const,
    projectId: "project-1",
    datasetId: "dataset-1",
    datasetRunIds: ["run-1"],
    datasetGeneration: null,
    runGenerations: { "run-1": "2" },
    status: "SCHEDULED" as const,
    phase: "visibility_barrier",
    workerFence: 0n,
    leaseOwner: null,
    leaseExpiresAt: null,
    attempts: 0,
    lastErrorCode: null,
    logicallyInvisible: false,
    completedAt: null,
    createdAt: new Date("2026-07-23T09:00:00.000Z"),
    updatedAt: new Date("2026-07-23T09:00:00.000Z"),
  };
}

function clientHarness() {
  const findMany = vi.fn(async () => [
    {
      id: "outbox-1",
      operationId: "operation-1",
      status: "PENDING",
      attempts: 0,
      nextAttemptAt: new Date("2026-07-23T09:00:00.000Z"),
      lockedBy: null,
      lockedUntil: null,
      publishedAt: null,
      createdAt: new Date("2026-07-23T09:00:00.000Z"),
      updatedAt: new Date("2026-07-23T09:00:00.000Z"),
      operation: operation(),
    },
  ]);
  const findUnique = vi.fn(async () => ({ attempts: 0 }));
  const updateMany = vi.fn(async () => ({ count: 1 }));
  return {
    client: {
      analyticsDatasetDeletionOutbox: {
        findMany,
        findUnique,
        updateMany,
      },
    },
    findMany,
    findUnique,
    updateMany,
  };
}

describe("publishAnalyticsDatasetDeletionOutboxBatch", () => {
  it("reconstructs and publishes the durable queue reference", async () => {
    const { client, updateMany } = clientHarness();
    const publish = vi.fn(async () => true);

    await expect(
      publishAnalyticsDatasetDeletionOutboxBatch({
        client: client as never,
        publish,
        now: new Date("2026-07-23T09:00:01.000Z"),
        limit: 10,
      }),
    ).resolves.toBe(1);
    expect(publish).toHaveBeenCalledWith({
      deletionType: "dataset-runs",
      projectId: "project-1",
      datasetId: "dataset-1",
      datasetRunIds: ["run-1"],
      analyticsDeletion: {
        operationId: "operation-1",
        datasetGeneration: null,
        runGenerations: { "run-1": "2" },
      },
    });
    expect(updateMany).not.toHaveBeenCalled();
  });

  it("defers an unavailable queue with bounded backoff", async () => {
    const { client, updateMany } = clientHarness();

    await expect(
      publishAnalyticsDatasetDeletionOutboxBatch({
        client: client as never,
        publish: vi.fn(async () => false),
        now: new Date("2026-07-23T09:00:01.000Z"),
        limit: 10,
      }),
    ).resolves.toBe(0);
    expect(updateMany).toHaveBeenCalledWith({
      where: { operationId: "operation-1", status: "PENDING" },
      data: expect.objectContaining({
        attempts: { increment: 1 },
        nextAttemptAt: new Date("2026-07-23T09:00:02.000Z"),
      }),
    });
  });
});
