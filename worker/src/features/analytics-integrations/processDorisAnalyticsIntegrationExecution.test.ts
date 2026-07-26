import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  claim: vi.fn(),
  complete: vi.fn(),
  defer: vi.fn(),
  renew: vi.fn(),
  logError: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", () => ({
  claimAnalyticsIntegrationExecution: mocks.claim,
  completeAnalyticsIntegrationExecution: mocks.complete,
  deferAnalyticsIntegrationExecution: mocks.defer,
  DorisAnalyticsIntegrationExportSource: class {},
  logger: { error: mocks.logError },
  renewAnalyticsIntegrationExecutionClaim: mocks.renew,
}));

vi.mock("../../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(() => ({
    runtimeLeaseId: "worker-1",
    backend: "doris",
    deploymentGeneration: 1n,
  })),
  isWorkerAnalyticsRuntimeFenced: vi.fn(() => false),
}));

vi.mock("../../utils/hostId", () => ({
  WORKER_HOST_ID: "worker-1",
}));

import { processDorisAnalyticsIntegrationExecution } from "./processDorisAnalyticsIntegrationExecution";

const payload = {
  executionId: "aie_1234567890abcdef1234567890ab",
  projectId: "project-1",
  integrationType: "MIXPANEL" as const,
  integrationGeneration: "2",
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "1",
  workloadEpochFingerprint: "e".repeat(64),
  runtimeContractVersion: 1,
  capabilityActivationGeneration: "3",
  capabilityContractVersion: 1,
  manifestChecksum: "c".repeat(64),
};

const records = [
  {
    deliveryKind: "TRACE" as const,
    entityKey: "trace-1",
    event: {
      langfuse_id: "trace-1",
      timestamp: new Date("2026-07-25T00:00:00.000Z"),
    },
  },
];

describe("processDorisAnalyticsIntegrationExecution", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.claim.mockResolvedValue({
      execution: {
        id: payload.executionId,
        projectId: payload.projectId,
      },
      manifest: {
        items: [
          {
            deliveryKind: "TRACE",
            entityKey: "trace-1",
            deliveryIds: ["delivery-1"],
          },
        ],
      },
    });
    mocks.renew.mockResolvedValue(true);
    mocks.complete.mockResolvedValue(undefined);
    mocks.defer.mockResolvedValue(undefined);
  });

  it("terminalizes the sealed manifest only after the remote send succeeds", async () => {
    const send = vi.fn().mockResolvedValue(undefined);
    const source = {
      readExact: vi.fn().mockResolvedValue({ records, missing: [] }),
    };

    await expect(
      processDorisAnalyticsIntegrationExecution({
        payload,
        expectedIntegrationType: "MIXPANEL",
        projectName: "Project One",
        source: source as never,
        send,
      }),
    ).resolves.toEqual({ sent: 1, sourceDeleted: 0 });

    expect(send).toHaveBeenCalledWith(records);
    expect(mocks.complete).toHaveBeenCalledOnce();
    expect(mocks.defer).not.toHaveBeenCalled();
  });

  it("defers the whole manifest when a remote batch reports partial failure", async () => {
    const secret = "Authorization=Bearer integration-secret";
    const send = vi
      .fn()
      .mockRejectedValue(
        new Error(`remote accepted only part of the batch: ${secret}`),
      );
    const source = {
      readExact: vi.fn().mockResolvedValue({ records, missing: [] }),
    };

    await expect(
      processDorisAnalyticsIntegrationExecution({
        payload,
        expectedIntegrationType: "MIXPANEL",
        projectName: "Project One",
        source: source as never,
        send,
      }),
    ).rejects.toThrow(
      "Analytics integration delivery failed (INTEGRATION_DELIVERY_FAILED)",
    );

    expect(mocks.complete).not.toHaveBeenCalled();
    expect(mocks.defer).toHaveBeenCalledWith(
      expect.objectContaining({
        executionId: payload.executionId,
        failureCode: "INTEGRATION_DELIVERY_FAILED",
      }),
    );
    expect(JSON.stringify(mocks.logError.mock.calls)).not.toContain(secret);
  });
});
