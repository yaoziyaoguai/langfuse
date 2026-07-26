import { describe, expect, it, vi } from "vitest";

const { TestBatchExportProvenanceError } = vi.hoisted(() => ({
  TestBatchExportProvenanceError: class extends Error {},
}));

vi.mock("@langfuse/shared/src/server", () => ({
  BatchExportQueue: { getInstance: vi.fn() },
  BatchExportProvenanceError: TestBatchExportProvenanceError,
  deferBatchExportDispatch: vi.fn(),
  findPendingBatchExportDispatchIds: vi.fn(),
  publishBatchExportDispatch: vi.fn(),
  quarantineBatchExportDispatch: vi.fn(),
  recordBatchExportDispatchFailure: vi.fn(),
  logger: { debug: vi.fn(), error: vi.fn() },
  QueueJobs: { BatchExportJob: "batch-export-job" },
  QueueName: { BatchExport: "BatchExport" },
}));
vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));

import { publishBatchExportDispatchBatch } from ".";

describe("batch export dispatch recovery", () => {
  it("publishes the durable payload with a generation-stable BullMQ identity", async () => {
    const now = new Date("2026-07-23T04:00:00.000Z");
    const delivery = {
      getState: vi.fn().mockResolvedValue("waiting"),
      retry: vi.fn(),
    };
    const queue = { add: vi.fn().mockResolvedValue(delivery) };
    const durablePayload = {
      projectId: "project-1",
      batchExportId: "export-1",
      dispatchGeneration: 3,
      analyticsBackend: "DORIS" as const,
      deploymentGeneration: "7",
      workloadEpochFingerprint: "e".repeat(64),
      runtimeContractVersion: 1,
      capabilityActivationGeneration: "4",
      capabilityContractVersion: 1,
    };
    const publishDispatch = vi.fn(async ({ publish }) => {
      await publish(durablePayload);
      return true;
    });

    await expect(
      publishBatchExportDispatchBatch({
        client: {} as never,
        queue: queue as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 7n,
        },
        now,
        limit: 25,
        findPending: vi
          .fn()
          .mockResolvedValue([{ batchExportId: "export-1", generation: 3 }]),
        publishDispatch: publishDispatch as never,
      }),
    ).resolves.toBe(1);

    expect(queue.add).toHaveBeenCalledWith(
      "batch-export-job",
      {
        id: "export-1-g3",
        name: "batch-export-job",
        timestamp: now,
        payload: durablePayload,
      },
      { jobId: "export-1-g3" },
    );
    expect(delivery.retry).not.toHaveBeenCalled();
  });

  it("retries a retained failed BullMQ job and counts only newly published rows", async () => {
    const delivery = {
      getState: vi.fn().mockResolvedValue("failed"),
      retry: vi.fn().mockResolvedValue(undefined),
    };
    const publishDispatch = vi
      .fn()
      .mockImplementationOnce(async ({ publish }) => {
        await publish({
          projectId: "project-1",
          batchExportId: "export-1",
          dispatchGeneration: 1,
          analyticsBackend: "DORIS",
          deploymentGeneration: "1",
          workloadEpochFingerprint: "e".repeat(64),
          runtimeContractVersion: 1,
          capabilityActivationGeneration: "1",
          capabilityContractVersion: 1,
        });
        return true;
      })
      .mockResolvedValueOnce(false);

    await expect(
      publishBatchExportDispatchBatch({
        client: {} as never,
        queue: { add: vi.fn().mockResolvedValue(delivery) } as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 1n,
        },
        now: new Date(),
        limit: 2,
        findPending: vi.fn().mockResolvedValue([
          { batchExportId: "export-1", generation: 1 },
          { batchExportId: "export-2", generation: 1 },
        ]),
        publishDispatch: publishDispatch as never,
      }),
    ).resolves.toBe(1);
    expect(delivery.retry).toHaveBeenCalledWith("failed");
  });

  it("quarantines a tampered durable row and continues the recovery page", async () => {
    const quarantineDispatch = vi.fn().mockResolvedValue(undefined);
    const publishDispatch = vi
      .fn()
      .mockRejectedValueOnce(new TestBatchExportProvenanceError("tampered"))
      .mockResolvedValueOnce(false);

    await expect(
      publishBatchExportDispatchBatch({
        client: {} as never,
        queue: { add: vi.fn() } as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 1n,
        },
        findPending: vi.fn().mockResolvedValue([
          { batchExportId: "tampered", generation: 1 },
          { batchExportId: "healthy", generation: 1 },
        ]),
        publishDispatch: publishDispatch as never,
        quarantineDispatch: quarantineDispatch as never,
      }),
    ).resolves.toBe(0);
    expect(publishDispatch).toHaveBeenCalledTimes(2);
    expect(quarantineDispatch).toHaveBeenCalledWith(
      expect.objectContaining({
        batchExportId: "tampered",
        failureCode: "BATCH_EXPORT_DISPATCH_PROVENANCE_MISMATCH",
      }),
    );
  });

  it("records a transient failure and continues publishing the recovery page", async () => {
    const recordFailure = vi.fn().mockResolvedValue(true);
    const publishDispatch = vi
      .fn()
      .mockRejectedValueOnce(new Error("redis unavailable"))
      .mockResolvedValueOnce(true);

    await expect(
      publishBatchExportDispatchBatch({
        client: {} as never,
        queue: { add: vi.fn() } as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 1n,
        },
        now: new Date("2026-07-23T04:00:00.000Z"),
        findPending: vi.fn().mockResolvedValue([
          { batchExportId: "temporarily-failed", generation: 2 },
          { batchExportId: "healthy", generation: 1 },
        ]),
        publishDispatch: publishDispatch as never,
        recordFailure: recordFailure as never,
      }),
    ).resolves.toBe(1);
    expect(publishDispatch).toHaveBeenCalledTimes(2);
    expect(recordFailure).toHaveBeenCalledWith({
      client: expect.any(Object),
      batchExportId: "temporarily-failed",
      expectedGeneration: 2,
    });
  });
});
