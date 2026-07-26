import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  recover: vi.fn(),
  find: vi.fn(),
  publish: vi.fn(),
  defer: vi.fn(),
  quarantine: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));
vi.mock("@langfuse/shared/src/server", () => {
  class AnalyticsIntegrationExecutionProvenanceError extends Error {}
  return {
    AnalyticsIntegrationExecutionProvenanceError,
    BlobStorageIntegrationProcessingQueue: { getInstance: vi.fn() },
    MixpanelIntegrationProcessingQueue: { getInstance: vi.fn() },
    PostHogIntegrationProcessingQueue: { getInstance: vi.fn() },
    deferAnalyticsIntegrationExecutionPublication: mocks.defer,
    findPublishableAnalyticsIntegrationExecutions: mocks.find,
    logger: { error: vi.fn() },
    publishAnalyticsIntegrationExecution: mocks.publish,
    quarantineAnalyticsIntegrationExecution: mocks.quarantine,
    QueueJobs: {
      PostHogIntegrationProcessingJob: "posthog",
      MixpanelIntegrationProcessingJob: "mixpanel",
      BlobStorageIntegrationProcessingJob: "blob",
    },
    recoverExpiredAnalyticsIntegrationExecutions: mocks.recover,
  };
});

import { AnalyticsIntegrationExecutionProvenanceError } from "@langfuse/shared/src/server";
import { publishAnalyticsIntegrationExecutionBatch } from ".";

const envelope = {
  executionId: "aie_1234567890abcdef1234567890ab",
  projectId: "project-1",
  integrationType: "POSTHOG" as const,
  integrationGeneration: "2",
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "1",
  workloadEpochFingerprint: "e".repeat(64),
  runtimeContractVersion: 1,
  capabilityActivationGeneration: "3",
  capabilityContractVersion: 1,
  manifestChecksum: "c".repeat(64),
};
const admissionContext = {
  runtimeLeaseId: "worker-1",
  backend: "doris" as const,
  deploymentGeneration: 1n,
};

describe("publishAnalyticsIntegrationExecutionBatch", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.recover.mockResolvedValue(0);
    mocks.find.mockResolvedValue([envelope]);
  });

  it("recovers expired claims and publishes only the authoritative envelope", async () => {
    const add = vi.fn().mockResolvedValue({
      getState: vi.fn().mockResolvedValue("waiting"),
    });
    mocks.publish.mockImplementation(async ({ publish }) => {
      await publish(envelope);
      return true;
    });

    await expect(
      publishAnalyticsIntegrationExecutionBatch({
        client: {} as never,
        admissionContext,
        now: new Date("2026-07-25T00:00:00.000Z"),
        queueFor: () => ({ queue: { add } as never, jobName: "posthog" }),
      }),
    ).resolves.toBe(1);
    expect(mocks.recover).toHaveBeenCalledOnce();
    expect(add).toHaveBeenCalledWith(
      "posthog",
      expect.objectContaining({ payload: envelope }),
      expect.objectContaining({ jobId: envelope.executionId }),
    );
  });

  it("quarantines provenance failures without queue publication", async () => {
    const add = vi.fn();
    mocks.publish.mockRejectedValue(
      new AnalyticsIntegrationExecutionProvenanceError("tampered"),
    );

    await expect(
      publishAnalyticsIntegrationExecutionBatch({
        client: {} as never,
        admissionContext,
        queueFor: () => ({ queue: { add } as never, jobName: "posthog" }),
      }),
    ).resolves.toBe(0);
    expect(add).not.toHaveBeenCalled();
    expect(mocks.quarantine).toHaveBeenCalledWith(
      expect.objectContaining({
        executionId: envelope.executionId,
        failureCode: "INTEGRATION_EXECUTION_PROVENANCE_MISMATCH",
      }),
    );
    expect(mocks.defer).not.toHaveBeenCalled();
  });

  it("defers transient queue failures for recovery", async () => {
    mocks.publish.mockRejectedValue(new Error("redis unavailable"));

    await expect(
      publishAnalyticsIntegrationExecutionBatch({
        client: {} as never,
        admissionContext,
        queueFor: () => ({ queue: null, jobName: "posthog" }),
      }),
    ).resolves.toBe(0);
    expect(mocks.defer).toHaveBeenCalledWith(
      expect.objectContaining({
        executionId: envelope.executionId,
        failureCode: "INTEGRATION_QUEUE_PUBLICATION_FAILED",
      }),
    );
    expect(mocks.quarantine).not.toHaveBeenCalled();
  });
});
