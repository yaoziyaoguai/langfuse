import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  transfer: vi.fn(),
  replayOperation: vi.fn(),
  verifyBootstrap: vi.fn(),
  dispatchIdentity: vi.fn(
    ({ requestId, targetType, targetId }) =>
      `${requestId}:${targetType}:${targetId}`,
  ),
  targetsFromArtifact: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", () => ({
  analyticsEvaluationDispatchIdentity: mocks.dispatchIdentity,
  parseAnalyticsEvaluationReplayCutoff: vi.fn((value) => value),
  replayVisibleAnalyticsEvaluationOperation: mocks.replayOperation,
  transferSuspendedAnalyticsEvaluationDispatches: mocks.transfer,
  verifyDurableAnalyticsEvaluationBootstrap: mocks.verifyBootstrap,
}));
vi.mock("../../services/AnalyticsWriter", () => ({
  analyticsEvaluationTargetsFromCanonicalBatch: mocks.targetsFromArtifact,
}));

import { replayAnalyticsEvaluationCutoff } from "./analyticsEvaluationReplay";

const cutoff = {
  kind: "evaluation_operation_cutoff",
  version: 1,
  deploymentGeneration: "1",
  sourceActivationGeneration: "7",
  configurationDigest: "a".repeat(64),
  configurationCount: 1,
  lowerAcceptanceSequence: "10",
  captureHandoffAcceptanceSequence: "20",
  sealedAt: "2026-07-24T00:00:00.000Z",
  captureHandoffAt: "2026-07-24T00:01:00.000Z",
};

function clientFixture(dispatchCount = 1) {
  const operation = {
    id: "operation-1",
    projectId: "project-1",
    acceptanceSequence: 11n,
    canonicalObjectKey: "canonical/operation-1.json",
    canonicalArtifactChecksum: "b".repeat(64),
    candidates: [
      {
        candidateKey: "event-1",
        disposition: "LOAD_REQUIRED",
        loadBatchId: "load-1",
      },
    ],
    loadBatches: [
      {
        id: "load-1",
        status: "VISIBLE",
        filteredRows: 0,
      },
    ],
  };
  return {
    analyticsCapabilityActivation: {
      findUniqueOrThrow: vi.fn().mockResolvedValue({
        status: "DARK",
        captureEnabled: true,
        captureRequired: true,
        rescanRequired: true,
        deploymentGeneration: 1n,
        generation: 8n,
        cutoffDigest: "c".repeat(64),
        cutoffState: cutoff,
      }),
    },
    jobConfiguration: {
      findMany: vi.fn().mockResolvedValue([{ projectId: "project-1" }]),
    },
    analyticsIngestionOperation: {
      findMany: vi
        .fn()
        .mockResolvedValueOnce([operation])
        .mockResolvedValueOnce([]),
    },
    analyticsEvaluationDispatch: {
      findMany: vi.fn().mockResolvedValue(
        dispatchCount === 0
          ? []
          : [
              {
                id: "operation-1:TRACE_UPSERT:trace-1",
                operationId: "operation-1",
                projectId: "project-1",
                status: "SUSPENDED",
                deploymentGeneration: 1n,
                capabilityActivationGeneration: 8n,
              },
            ],
      ),
    },
    $transaction: vi.fn((run) => run({ transaction: true })),
  };
}

describe("replayAnalyticsEvaluationCutoff", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.transfer.mockResolvedValue(1);
    mocks.replayOperation.mockResolvedValue(1);
    mocks.verifyBootstrap.mockResolvedValue({
      bootstrapEvidenceDigest: "d".repeat(64),
    });
    mocks.targetsFromArtifact.mockReturnValue([
      {
        candidateKey: "event-1",
        targetType: "TRACE_UPSERT",
        targetId: "trace-1",
        traceId: "trace-1",
        observationId: null,
        datasetItemId: null,
        targetTimestamp: new Date("2026-07-24T00:00:00.000Z"),
        traceEnvironment: "production",
      },
    ]);
  });

  it("transfers old suspended work, replays the bounded operation range, and seals evidence", async () => {
    const client = clientFixture();
    const artifactStore = { get: vi.fn().mockResolvedValue({ children: [] }) };

    await expect(
      replayAnalyticsEvaluationCutoff({
        client: client as never,
        artifactStore: artifactStore as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 1n,
        },
        expectedCutoffDigest: "c".repeat(64),
      }),
    ).resolves.toEqual({
      operationsScanned: 1,
      targetsVerified: 1,
      bootstrapEvidenceDigest: "d".repeat(64),
    });

    expect(mocks.transfer).toHaveBeenCalledOnce();
    expect(artifactStore.get).toHaveBeenCalledWith(
      "canonical/operation-1.json",
      "b".repeat(64),
    );
    expect(mocks.replayOperation).toHaveBeenCalledWith(
      expect.objectContaining({
        operationId: "operation-1",
        projectId: "project-1",
      }),
    );
    expect(mocks.verifyBootstrap).toHaveBeenCalledWith(
      { transaction: true },
      {
        deploymentGeneration: 1n,
        activationGeneration: 8n,
        expectedCutoffDigest: "c".repeat(64),
      },
    );
  });

  it("fails closed when a replayed target has no authoritative dispatch", async () => {
    const client = clientFixture(0);

    await expect(
      replayAnalyticsEvaluationCutoff({
        client: client as never,
        artifactStore: { get: vi.fn().mockResolvedValue({}) } as never,
        admissionContext: {
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 1n,
        },
        expectedCutoffDigest: "c".repeat(64),
      }),
    ).rejects.toThrow("coverage is incomplete");

    expect(mocks.verifyBootstrap).not.toHaveBeenCalled();
  });
});
