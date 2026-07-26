import { describe, expect, it, vi } from "vitest";

const { TestProvenanceError } = vi.hoisted(() => ({
  TestProvenanceError: class extends Error {},
}));

vi.mock("@langfuse/shared/src/server", () => ({
  AnalyticsEvaluationDispatchQueue: { getInstance: vi.fn() },
  AnalyticsEvaluationDispatchProvenanceError: TestProvenanceError,
  deferAnalyticsEvaluationDispatch: vi.fn(),
  findPendingAnalyticsEvaluationDispatches: vi.fn(),
  publishAnalyticsEvaluationDispatch: vi.fn(),
  quarantineAnalyticsEvaluationDispatch: vi.fn(),
  logger: { debug: vi.fn(), error: vi.fn() },
  QueueJobs: {
    AnalyticsEvaluationDispatch: "analytics-evaluation-dispatch-job",
  },
}));
vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));

import { logger } from "@langfuse/shared/src/server";
import { publishAnalyticsEvaluationDispatchBatch } from ".";

describe("analytics evaluation dispatch recovery", () => {
  it("publishes a generation-stable BullMQ job from the durable row", async () => {
    const now = new Date("2026-07-23T05:00:00.000Z");
    const delivery = {
      getState: vi.fn().mockResolvedValue("waiting"),
      retry: vi.fn(),
    };
    const queue = { add: vi.fn().mockResolvedValue(delivery) };
    const envelope = {
      dispatchId: "dispatch-1",
      dispatchGeneration: 3,
      projectId: "project-1",
      operationId: "operation-1",
      targetType: "TRACE_UPSERT" as const,
      targetId: "trace-1",
      analyticsBackend: "DORIS" as const,
      deploymentGeneration: "7",
      workloadEpochFingerprint: "e".repeat(64),
      runtimeContractVersion: 1,
      capabilityActivationGeneration: "4",
      capabilityContractVersion: 1,
    };
    const publishDispatch = vi.fn(async ({ publish }) => {
      await publish(envelope);
      return true;
    });

    await expect(
      publishAnalyticsEvaluationDispatchBatch({
        client: {} as never,
        queue: queue as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 7n,
        },
        now,
        findPending: vi
          .fn()
          .mockResolvedValue([{ id: "dispatch-1", dispatchGeneration: 3 }]),
        publishDispatch: publishDispatch as never,
      }),
    ).resolves.toBe(1);
    expect(queue.add).toHaveBeenCalledWith(
      "analytics-evaluation-dispatch-job",
      {
        id: "dispatch-1-g3",
        name: "analytics-evaluation-dispatch-job",
        timestamp: now,
        payload: envelope,
      },
      { jobId: "dispatch-1-g3" },
    );
  });

  it("quarantines provenance failures without blocking the remaining page", async () => {
    const quarantineDispatch = vi.fn().mockResolvedValue(true);
    const publishDispatch = vi
      .fn()
      .mockRejectedValueOnce(new TestProvenanceError("tampered"))
      .mockResolvedValueOnce(false);

    await expect(
      publishAnalyticsEvaluationDispatchBatch({
        client: {} as never,
        queue: { add: vi.fn() } as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 1n,
        },
        findPending: vi.fn().mockResolvedValue([
          { id: "tampered", dispatchGeneration: 1 },
          { id: "healthy", dispatchGeneration: 1 },
        ]),
        publishDispatch: publishDispatch as never,
        quarantineDispatch: quarantineDispatch as never,
      }),
    ).resolves.toBe(0);
    expect(quarantineDispatch).toHaveBeenCalledWith(
      expect.objectContaining({
        dispatchId: "tampered",
        expectedGeneration: 1,
        failureCode: "EVALUATION_DISPATCH_PROVENANCE_MISMATCH",
      }),
    );
    expect(publishDispatch).toHaveBeenCalledTimes(2);
  });

  it("defers transient publication failures", async () => {
    const deferDispatch = vi.fn().mockResolvedValue(true);
    const secretError = new Error(
      "redis://admin:password@cache Authorization=Bearer token prompt=secret",
    );
    await expect(
      publishAnalyticsEvaluationDispatchBatch({
        client: {} as never,
        queue: { add: vi.fn() } as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 1n,
        },
        findPending: vi
          .fn()
          .mockResolvedValue([{ id: "dispatch-1", dispatchGeneration: 2 }]),
        publishDispatch: vi.fn().mockRejectedValue(secretError),
        deferDispatch: deferDispatch as never,
      }),
    ).resolves.toBe(0);
    expect(deferDispatch).toHaveBeenCalledWith({
      client: expect.any(Object),
      dispatchId: "dispatch-1",
      expectedGeneration: 2,
    });
    const logs = JSON.stringify(vi.mocked(logger.error).mock.calls);
    expect(logs).not.toContain("password");
    expect(logs).not.toContain("Bearer token");
    expect(logs).not.toContain("prompt=secret");
  });
});
