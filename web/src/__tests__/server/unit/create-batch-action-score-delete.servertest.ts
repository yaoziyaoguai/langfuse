import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  backend: "doris" as "clickhouse" | "doris",
  mode: "MANAGED" as "MANAGED" | "LEGACY_COMPATIBILITY" | "UNAVAILABLE",
  provenance: {
    analyticsBackend: "DORIS" as const,
    deploymentGeneration: 7n,
    workloadEpochFingerprint: "a".repeat(64),
    runtimeContractVersion: 3,
    producerRuntimeLeaseId: "runtime-producer",
  } as {
    analyticsBackend: "CLICKHOUSE" | "DORIS";
    deploymentGeneration: bigint;
    workloadEpochFingerprint: string;
    runtimeContractVersion: number;
    producerRuntimeLeaseId: string;
  } | null,
}));

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  auditLog: vi.fn(),
  assertLegacySearch: vi.fn(),
  createClaim: vi.fn(),
  getQueue: vi.fn(),
  lockClaimForIo: vi.fn(),
  lockLegacyAdmission: vi.fn(),
  renewClaim: vi.fn(),
  releaseClaim: vi.fn(),
}));

vi.mock("@langfuse/shared", () => ({
  ActionId: {
    ScoreDelete: "score-delete",
    TraceDelete: "trace-delete",
  },
  BatchActionStatus: { Queued: "queued", Processing: "processing" },
  BatchActionQuerySchema: { parse: vi.fn((value) => value) },
  createTraceDeleteBatchActionConfig: vi.fn(),
  InternalServerError: class InternalServerError extends Error {
    readonly httpCode = 500;
  },
  NotImplementedError: class NotImplementedError extends Error {
    readonly httpCode = 501;
  },
}));

vi.mock("@langfuse/shared/src/server", () => ({
  BatchActionQueue: {
    getInstance: mocks.getQueue,
  },
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
  createAnalyticsBackendClaimLease: mocks.createClaim,
  lockAnalyticsBackendClaimLeaseForIo: mocks.lockClaimForIo,
  logger: { debug: vi.fn(), warn: vi.fn() },
  lockLegacyAnalyticsAdmission: mocks.lockLegacyAdmission,
  QueueJobs: { BatchActionProcessingJob: "batch-action-processing-job" },
  redis: null,
  renewAnalyticsBackendClaimLease: mocks.renewClaim,
  releaseAnalyticsBackendClaimLease: mocks.releaseClaim,
  serializeAnalyticsDurableProvenance: (provenance: {
    deploymentGeneration: bigint;
  }) => ({
    ...provenance,
    deploymentGeneration: provenance.deploymentGeneration.toString(),
  }),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    $transaction: vi.fn(
      async (run: (transaction: object) => Promise<unknown>) => run({}),
    ),
  },
}));
vi.mock("@/src/features/audit-logs/auditLog", () => ({
  auditLog: mocks.auditLog,
}));
vi.mock("@/src/features/table/server/helpers", () => ({
  generateBatchActionId: vi.fn(() => "batch-score-delete-1"),
}));
vi.mock("@/src/features/traces/server/legacyIoSearch", () => ({
  assertLegacyTracingIoSearchCanCreateBatchJob: mocks.assertLegacySearch,
}));
vi.mock("@/src/server/analyticsRuntime", () => ({
  getWebAnalyticsDurableWorkState: () =>
    state.mode === "MANAGED"
      ? { mode: "MANAGED", provenance: state.provenance }
      : state.mode === "LEGACY_COMPATIBILITY"
        ? { mode: "LEGACY_COMPATIBILITY", backend: state.backend }
        : { mode: "UNAVAILABLE" },
}));

import { createBatchActionJob } from "@/src/features/table/server/createBatchActionJob";

const createScoreDelete = () =>
  createBatchActionJob({
    projectId: "project-1",
    actionId: "score-delete" as never,
    tableName: "scores" as never,
    actionType: "delete" as never,
    session: {
      user: { id: "user-1" },
      orgId: "org-1",
      orgRole: "OWNER",
    } as never,
    query: {
      filter: [],
      orderBy: { column: "timestamp", order: "DESC" },
    },
  });

describe("score-delete batch action provenance", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    state.backend = "doris";
    state.mode = "MANAGED";
    mocks.getQueue.mockReturnValue({ add: mocks.add });
    mocks.createClaim.mockResolvedValue({ id: "score-delete-claim" });
    mocks.renewClaim.mockResolvedValue({ id: "score-delete-claim" });
    mocks.releaseClaim.mockResolvedValue(true);
    state.provenance = {
      analyticsBackend: "DORIS",
      deploymentGeneration: 7n,
      workloadEpochFingerprint: "a".repeat(64),
      runtimeContractVersion: 3,
      producerRuntimeLeaseId: "runtime-producer",
    };
  });

  it("publishes a managed Doris batch deletion with immutable provenance", async () => {
    await createScoreDelete();

    const event = mocks.add.mock.calls[0]?.[1];
    expect(event.payload).toMatchObject({
      actionId: "score-delete",
      deletionOperationId: "batch-score-delete-1",
      deletionGeneration: "7",
      analyticsProvenance: {
        analyticsBackend: "DORIS",
        deploymentGeneration: "7",
        workloadEpochFingerprint: "a".repeat(64),
        runtimeContractVersion: 3,
        producerRuntimeLeaseId: "runtime-producer",
      },
    });
    expect(mocks.auditLog).toHaveBeenCalledOnce();
    expect(mocks.getQueue).toHaveBeenCalledOnce();
    expect(mocks.add).toHaveBeenCalledOnce();
  });

  it("stamps managed ClickHouse batch jobs with their backend generation", async () => {
    state.backend = "clickhouse";
    state.mode = "MANAGED";
    state.provenance = {
      ...state.provenance!,
      analyticsBackend: "CLICKHOUSE",
    };

    await createScoreDelete();

    const payload = mocks.add.mock.calls[0]?.[1].payload;
    expect(payload).toMatchObject({
      actionId: "score-delete",
      deletionOperationId: "batch-score-delete-1",
      deletionGeneration: "7",
      analyticsProvenance: {
        analyticsBackend: "CLICKHOUSE",
        deploymentGeneration: "7",
      },
    });
    expect(mocks.auditLog.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.add.mock.invocationCallOrder[0]!,
    );
    expect(mocks.createClaim).toHaveBeenCalledOnce();
    expect(mocks.releaseClaim).toHaveBeenCalledOnce();
  });

  it("does not enqueue from a fenced managed ClickHouse runtime", async () => {
    state.backend = "clickhouse";
    state.mode = "UNAVAILABLE";
    state.provenance = null;

    await expect(createScoreDelete()).rejects.toThrow(
      "requires runtime admission",
    );
    expect(mocks.auditLog).not.toHaveBeenCalled();
    expect(mocks.getQueue).not.toHaveBeenCalled();
    expect(mocks.add).not.toHaveBeenCalled();
  });

  it("surfaces managed ClickHouse queue publication failures", async () => {
    state.backend = "clickhouse";
    state.provenance = {
      ...state.provenance!,
      analyticsBackend: "CLICKHOUSE",
    };
    mocks.add.mockRejectedValueOnce(new Error("redis unavailable"));

    await expect(createScoreDelete()).rejects.toThrow("redis unavailable");

    expect(mocks.add).toHaveBeenCalledOnce();
    expect(mocks.releaseClaim).not.toHaveBeenCalled();
  });
});
