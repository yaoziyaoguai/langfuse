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
  DorisBlobAnalyticsExportSource: class {},
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

import { processDorisBlobIntegrationExecution } from "./processDorisBlobIntegrationExecution";

const payload = {
  executionId: "aie_1234567890abcdef1234567890ab",
  projectId: "project-1",
  integrationType: "BLOB_STORAGE" as const,
  integrationGeneration: "2",
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "1",
  workloadEpochFingerprint: "e".repeat(64),
  runtimeContractVersion: 1,
  capabilityActivationGeneration: "3",
  capabilityContractVersion: 1,
  manifestChecksum: "c".repeat(64),
};

describe("processDorisBlobIntegrationExecution", () => {
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

  it("redacts a storage failure and leaves the sealed manifest retryable", async () => {
    const secret = "password=s3-secret telemetry_payload=private";
    const upload = vi.fn().mockRejectedValue(new Error(secret));
    const source = {
      readExact: vi.fn().mockResolvedValue({
        records: [
          {
            item: {
              deliveryKind: "TRACE",
              entityKey: "trace-1",
              deliveryIds: ["delivery-1"],
            },
            table: "traces",
            row: { id: "trace-1" },
          },
        ],
        missing: [],
      }),
    };

    await expect(
      processDorisBlobIntegrationExecution({
        payload,
        observationTable: "observations",
        observationFieldGroups: [],
        source: source as never,
        upload,
      }),
    ).rejects.toThrow(
      "Analytics integration delivery failed (BLOB_DELIVERY_FAILED)",
    );

    expect(mocks.complete).not.toHaveBeenCalled();
    expect(mocks.defer).toHaveBeenCalledWith(
      expect.objectContaining({
        executionId: payload.executionId,
        failureCode: "BLOB_DELIVERY_FAILED",
      }),
    );
    expect(JSON.stringify(mocks.logError.mock.calls)).not.toContain(secret);
  });
});
