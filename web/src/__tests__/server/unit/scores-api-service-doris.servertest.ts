import type * as SharedServer from "@langfuse/shared/src/server";

const runtimeState = vi.hoisted(() => ({
  backend: "doris" as "clickhouse" | "doris",
  mode: "MANAGED" as "MANAGED" | "LEGACY_COMPATIBILITY" | "UNAVAILABLE",
}));

const {
  mockClickhouseCount,
  mockClickhouseGet,
  mockClickhouseList,
  mockDorisGet,
  mockDorisRead,
  mockAcceptAnalytics,
  mockAddScoreDelete,
  mockGetScoreDeleteQueue,
  mockGetWebAnalyticsDurableWorkState,
  mockAuditLog,
  mockCreateClaim,
  mockLockClaimForIo,
  mockLockLegacyAdmission,
  mockRenewClaim,
  mockReleaseClaim,
  mockTransaction,
  mockDatasetRunIngestionActive,
} = vi.hoisted(() => ({
  mockClickhouseCount: vi.fn(),
  mockClickhouseGet: vi.fn(),
  mockClickhouseList: vi.fn(),
  mockDorisGet: vi.fn(),
  mockDorisRead: vi.fn(),
  mockAcceptAnalytics: vi.fn(),
  mockAddScoreDelete: vi.fn(),
  mockGetScoreDeleteQueue: vi.fn(),
  mockAuditLog: vi.fn(),
  mockCreateClaim: vi.fn(),
  mockLockClaimForIo: vi.fn(),
  mockLockLegacyAdmission: vi.fn(),
  mockRenewClaim: vi.fn(),
  mockReleaseClaim: vi.fn(),
  mockDatasetRunIngestionActive: vi.fn().mockResolvedValue(true),
  mockTransaction: vi.fn(async (operation: (transaction: object) => unknown) =>
    operation({}),
  ),
  mockGetWebAnalyticsDurableWorkState: vi.fn(() =>
    runtimeState.mode === "MANAGED"
      ? {
          mode: "MANAGED" as const,
          provenance: {
            analyticsBackend:
              runtimeState.backend === "doris"
                ? ("DORIS" as const)
                : ("CLICKHOUSE" as const),
            deploymentGeneration: 7n,
            workloadEpochFingerprint: "a".repeat(64),
            runtimeContractVersion: 3,
            producerRuntimeLeaseId: "runtime-producer",
          },
        }
      : runtimeState.mode === "LEGACY_COMPATIBILITY"
        ? {
            mode: "LEGACY_COMPATIBILITY" as const,
            backend: runtimeState.backend,
          }
        : { mode: "UNAVAILABLE" as const },
  ),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: { $transaction: mockTransaction },
}));

vi.mock("@/src/server/analyticsRuntime", () => ({
  getWebAnalyticsAdmissionContext: vi.fn(() => ({
    runtimeLeaseId: "runtime-producer",
    backend: runtimeState.backend,
    deploymentGeneration: 7n,
  })),
  getWebAnalyticsDurableWorkState: mockGetWebAnalyticsDurableWorkState,
}));

vi.mock("@/src/server/communityCapabilityRuntime", () => ({
  isInternalDorisCapabilityActive: mockDatasetRunIngestionActive,
}));

vi.mock("@/src/features/public-api/server/scores", () => ({
  _handleGenerateScoresForPublicApi: mockClickhouseList,
  _handleGetScoresCountForPublicApi: mockClickhouseCount,
  convertScoreToPublicApi: vi.fn((score) => score),
}));

vi.mock("@/src/features/audit-logs/auditLog", () => ({
  auditLog: mockAuditLog,
}));

vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const original = await importOriginal<typeof SharedServer>();
  return {
    ...original,
    _handleGetScoreById: mockClickhouseGet,
    getDorisTelemetryRepositories: () => ({
      scores: { get: mockDorisGet },
    }),
    isDorisAnalyticsBackend: () => runtimeState.backend === "doris",
    readDorisScoresForPublicApi: mockDorisRead,
    acceptAnalyticsIngestion: mockAcceptAnalytics,
    createAnalyticsBackendClaimLease: mockCreateClaim,
    getS3EventStorageClient: vi.fn(() => ({})),
    lockLegacyAnalyticsAdmission: mockLockLegacyAdmission,
    lockAnalyticsBackendClaimLeaseForIo: mockLockClaimForIo,
    renewAnalyticsBackendClaimLease: mockRenewClaim,
    releaseAnalyticsBackendClaimLease: mockReleaseClaim,
    ScoreDeleteQueue: {
      getInstance: mockGetScoreDeleteQueue,
    },
  };
});

import { ScoresApiService } from "@/src/features/public-api/server/scores-api-service";
import type { ScoreDomain } from "@langfuse/shared";
import type { ScoreQueryType } from "@langfuse/shared/src/server";

const score = {
  id: "score-1",
  projectId: "project-1",
  environment: "production",
  name: "quality",
  value: 0.9,
  source: "API",
  authorUserId: null,
  comment: null,
  metadata: {},
  configId: null,
  queueId: null,
  executionTraceId: null,
  createdAt: new Date("2026-07-17T10:00:00.000Z"),
  updatedAt: new Date("2026-07-17T10:00:00.000Z"),
  timestamp: new Date("2026-07-17T10:00:00.000Z"),
  traceId: "trace-1",
  sessionId: null,
  datasetRunId: null,
  observationId: null,
  longStringValue: "",
  dataType: "NUMERIC",
  stringValue: null,
} satisfies ScoreDomain;

const props: ScoreQueryType = {
  projectId: "project-1",
  page: 1,
  limit: 10,
};

describe("ScoresApiService Doris routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    runtimeState.backend = "doris";
    runtimeState.mode = "MANAGED";
    mockDorisRead.mockResolvedValue({
      items: [{ ...score, trace: { userId: "user-1" } }],
      count: 1,
    });
    mockDorisGet.mockResolvedValue(score);
    mockAcceptAnalytics.mockResolvedValue({
      operationId: "operation-1",
      status: "ACCEPTED",
    });
    mockGetScoreDeleteQueue.mockReturnValue({ add: mockAddScoreDelete });
    mockCreateClaim.mockResolvedValue({ id: "score-delete-claim" });
    mockLockClaimForIo.mockResolvedValue(undefined);
    mockRenewClaim.mockResolvedValue(true);
    mockReleaseClaim.mockResolvedValue(true);
  });

  it("shares the Doris list/count read and never calls ClickHouse handlers", async () => {
    const service = new ScoresApiService("v2");

    await expect(service.generateScoresForPublicApi(props)).resolves.toEqual([
      expect.objectContaining({ id: "score-1", trace: { userId: "user-1" } }),
    ]);
    await expect(service.getScoresCountForPublicApi(props)).resolves.toBe(1);

    expect(mockDorisRead).toHaveBeenCalledTimes(1);
    expect(mockDorisRead).toHaveBeenCalledWith(props, "v2");
    expect(mockClickhouseList).not.toHaveBeenCalled();
    expect(mockClickhouseCount).not.toHaveBeenCalled();
  });

  it("routes point reads to the immutable Doris score locator", async () => {
    await expect(
      new ScoresApiService("v2").getScoreById({
        projectId: "project-1",
        scoreId: "score-1",
      }),
    ).resolves.toEqual(score);

    expect(mockDorisGet).toHaveBeenCalledWith({
      projectId: "project-1",
      scoreId: "score-1",
    });
    expect(mockClickhouseGet).not.toHaveBeenCalled();
  });

  it("durably accepts score writes without invoking the legacy event batch", async () => {
    await expect(
      new ScoresApiService("v2").createScore({
        body: {
          id: "score-2",
          name: "quality",
          value: 1,
          dataType: "NUMERIC",
          environment: "default",
          source: "API",
          traceId: "trace-1",
        },
        auth: { scope: { projectId: "project-1" } } as never,
        attribution: {
          ingestionApiKey: "pk-test",
          ingestionSdkName: "python",
          ingestionSdkVersion: "4.0.0",
        },
      }),
    ).resolves.toMatchObject({
      id: "score-2",
      result: { errors: [], successes: [{ status: 201 }] },
    });
    expect(mockAcceptAnalytics).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        canonicalizerVersion: "1",
        schemaVersion: 1,
        envelope: expect.objectContaining({ source: "score" }),
      }),
    );
  });

  it("uses the activated schema-2 capability contract for dataset-run scores", async () => {
    await expect(
      new ScoresApiService("v2").createScore({
        body: {
          id: "score-2",
          name: "quality",
          value: 1,
          dataType: "NUMERIC",
          environment: "default",
          source: "API",
          datasetRunId: "run-1",
        },
        auth: { scope: { projectId: "project-1" } } as never,
        attribution: {
          ingestionApiKey: "pk-test",
          ingestionSdkName: "python",
          ingestionSdkVersion: "4.0.0",
        },
      }),
    ).resolves.toMatchObject({
      id: "score-2",
      result: { errors: [], successes: [{ status: 201 }] },
    });
    expect(mockAcceptAnalytics).toHaveBeenCalledWith(
      expect.objectContaining({
        schemaVersion: 2,
        capability: "datasetRunIngestion",
      }),
    );
  });

  it("rejects a dataset-run score before durable acceptance when its capability is inactive", async () => {
    mockDatasetRunIngestionActive.mockResolvedValueOnce(false);

    await expect(
      new ScoresApiService("v2").createScore({
        body: {
          id: "score-2",
          name: "quality",
          value: 1,
          dataType: "NUMERIC",
          environment: "default",
          source: "API",
          datasetRunId: "run-1",
        },
        auth: { scope: { projectId: "project-1" } } as never,
        attribution: {
          ingestionApiKey: "pk-test",
          ingestionSdkName: "python",
          ingestionSdkVersion: "4.0.0",
        },
      }),
    ).resolves.toMatchObject({
      id: "score-2",
      result: {
        successes: [],
        errors: [
          {
            status: 501,
            code: "R1B_EXPERIMENTS_UNAVAILABLE",
          },
        ],
      },
    });
    expect(mockAcceptAnalytics).not.toHaveBeenCalled();
  });

  it("queues managed Doris score deletion with immutable provenance", async () => {
    await expect(
      new ScoresApiService("v2").deleteScore({
        projectId: "project-1",
        orgId: "org-1",
        apiKeyId: "api-key-1",
        scoreId: "score-1",
      }),
    ).resolves.toEqual({ message: "Score deletion queued successfully" });
    const event = mockAddScoreDelete.mock.calls[0]?.[1];
    expect(event.payload).toMatchObject({
      projectId: "project-1",
      scoreIds: ["score-1"],
      deletionOperationId: event.id,
      deletionGeneration: "7",
      analyticsProvenance: {
        analyticsBackend: "DORIS",
        deploymentGeneration: "7",
      },
    });
    expect(mockGetScoreDeleteQueue).toHaveBeenCalledOnce();
    expect(mockAuditLog).toHaveBeenCalledOnce();
    expect(mockAddScoreDelete).toHaveBeenCalledOnce();
  });

  it("stamps managed ClickHouse score deletion with its backend generation", async () => {
    runtimeState.backend = "clickhouse";

    await expect(
      new ScoresApiService("v2").deleteScore({
        projectId: "project-1",
        orgId: "org-1",
        apiKeyId: "api-key-1",
        scoreId: "score-1",
      }),
    ).resolves.toEqual({ message: "Score deletion queued successfully" });

    expect(mockAddScoreDelete).toHaveBeenCalledWith(
      "score-delete",
      expect.objectContaining({
        payload: expect.objectContaining({
          projectId: "project-1",
          scoreIds: ["score-1"],
          deletionGeneration: "7",
          analyticsProvenance: expect.objectContaining({
            analyticsBackend: "CLICKHOUSE",
          }),
        }),
      }),
      expect.objectContaining({ jobId: expect.any(String) }),
    );
    const [, event, options] = mockAddScoreDelete.mock.calls[0]!;
    expect(options).toEqual({ jobId: event.id });
    expect(mockAuditLog).toHaveBeenCalledOnce();
    expect(mockCreateClaim).toHaveBeenCalledOnce();
    expect(mockReleaseClaim).toHaveBeenCalledOnce();
  });

  it("does not accept a score delete from a fenced managed ClickHouse runtime", async () => {
    runtimeState.backend = "clickhouse";
    runtimeState.mode = "UNAVAILABLE";

    await expect(
      new ScoresApiService("v2").deleteScore({
        projectId: "project-1",
        orgId: "org-1",
        apiKeyId: "api-key-1",
        scoreId: "score-1",
      }),
    ).rejects.toThrow("Analytics queue publication requires runtime admission");
    expect(mockGetScoreDeleteQueue).not.toHaveBeenCalled();
    expect(mockAuditLog).not.toHaveBeenCalled();
    expect(mockAddScoreDelete).not.toHaveBeenCalled();
  });
});
