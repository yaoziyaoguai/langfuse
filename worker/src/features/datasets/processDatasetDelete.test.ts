import { describe, expect, it, vi } from "vitest";

import { processDatasetDelete } from "./processDatasetDelete";

const operation = {
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

describe("processDatasetDelete", () => {
  it("rejects unmanaged legacy jobs instead of silently dropping Doris work", async () => {
    await expect(
      processDatasetDelete({
        deletionType: "dataset",
        projectId: "project-1",
        datasetId: "dataset-1",
      }),
    ).rejects.toMatchObject({ code: "ANALYTICS_UNSUPPORTED_FEATURE" });
  });

  it("makes the durable barrier visible before cleanup and completion", async () => {
    const calls: string[] = [];
    const writeBarrier = vi.fn(async () => {
      calls.push("barrier");
    });
    const markBarrierVisible = vi.fn(async () => {
      calls.push("mark");
      return true;
    });
    const cleanupRunItems = vi.fn(async () => {
      calls.push("cleanup");
    });
    const completeOperation = vi.fn(async () => {
      calls.push("complete");
      return true;
    });

    await processDatasetDelete(
      {
        deletionType: "dataset-runs",
        projectId: "project-1",
        datasetId: "dataset-1",
        datasetRunIds: ["run-1"],
        analyticsDeletion: {
          operationId: "operation-1",
          datasetGeneration: null,
          runGenerations: { "run-1": "2" },
        },
      },
      {
        workerId: "worker-1",
        findOperation: vi.fn(async () => operation),
        claimOperation: vi.fn(async () => ({
          ...operation,
          workerFence: 1n,
          leaseOwner: "worker-1",
        })),
        writeBarrier,
        markBarrierVisible,
        cleanupRunItems,
        completeOperation,
      },
    );

    expect(calls).toEqual(["barrier", "mark", "cleanup", "complete"]);
    expect(writeBarrier).toHaveBeenCalledWith(
      expect.objectContaining({
        datasetGeneration: null,
        runGenerations: { "run-1": 2n },
        lease: { owner: "worker-1", fence: 1n },
      }),
    );
  });

  it("resumes cleanup without republishing a visible barrier", async () => {
    const writeBarrier = vi.fn();
    const cleanupRunItems = vi.fn(async () => undefined);
    const completeOperation = vi.fn(async () => true);
    await processDatasetDelete(
      {
        deletionType: "dataset-runs",
        projectId: "project-1",
        datasetId: "dataset-1",
        datasetRunIds: ["run-1"],
        analyticsDeletion: {
          operationId: "operation-1",
          datasetGeneration: null,
          runGenerations: { "run-1": "2" },
        },
      },
      {
        workerId: "worker-2",
        findOperation: vi.fn(async () => ({
          ...operation,
          logicallyInvisible: true,
          status: "RETRYING",
        })),
        claimOperation: vi.fn(async () => ({
          ...operation,
          logicallyInvisible: true,
          status: "RETRYING",
          workerFence: 2n,
          leaseOwner: "worker-2",
        })),
        writeBarrier,
        cleanupRunItems,
        completeOperation,
      },
    );

    expect(writeBarrier).not.toHaveBeenCalled();
    expect(cleanupRunItems).toHaveBeenCalledOnce();
    expect(completeOperation).toHaveBeenCalledOnce();
  });

  it("accepts the same persisted run-generation map regardless of JSON key order", async () => {
    const twoRunOperation = {
      ...operation,
      datasetRunIds: ["run-1", "run-2"],
      runGenerations: { "run-1": "2", "run-2": "3" },
    };

    await expect(
      processDatasetDelete(
        {
          deletionType: "dataset-runs",
          projectId: "project-1",
          datasetId: "dataset-1",
          datasetRunIds: ["run-2", "run-1"],
          analyticsDeletion: {
            operationId: "operation-1",
            datasetGeneration: null,
            runGenerations: { "run-2": "3", "run-1": "2" },
          },
        },
        {
          workerId: "worker-3",
          findOperation: vi.fn(async () => twoRunOperation),
          claimOperation: vi.fn(async () => ({
            ...twoRunOperation,
            workerFence: 3n,
            leaseOwner: "worker-3",
          })),
          writeBarrier: vi.fn(async () => undefined),
          markBarrierVisible: vi.fn(async () => true),
          cleanupRunItems: vi.fn(async () => undefined),
          completeOperation: vi.fn(async () => true),
        },
      ),
    ).resolves.toBeUndefined();
  });
});
