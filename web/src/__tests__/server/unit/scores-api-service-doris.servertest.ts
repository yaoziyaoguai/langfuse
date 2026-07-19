import type * as SharedServer from "@langfuse/shared/src/server";

const {
  mockClickhouseCount,
  mockClickhouseGet,
  mockClickhouseList,
  mockDorisGet,
  mockDorisRead,
} = vi.hoisted(() => ({
  mockClickhouseCount: vi.fn(),
  mockClickhouseGet: vi.fn(),
  mockClickhouseList: vi.fn(),
  mockDorisGet: vi.fn(),
  mockDorisRead: vi.fn(),
}));

vi.mock("@/src/features/public-api/server/scores", () => ({
  _handleGenerateScoresForPublicApi: mockClickhouseList,
  _handleGetScoresCountForPublicApi: mockClickhouseCount,
  convertScoreToPublicApi: vi.fn((score) => score),
}));

vi.mock("@/src/features/audit-logs/auditLog", () => ({
  auditLog: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const original = await importOriginal<typeof SharedServer>();
  return {
    ...original,
    _handleGetScoreById: mockClickhouseGet,
    getDorisTelemetryRepositories: () => ({
      scores: { get: mockDorisGet },
    }),
    isDorisAnalyticsBackend: () => true,
    readDorisScoresForPublicApi: mockDorisRead,
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
    mockDorisRead.mockResolvedValue({
      items: [{ ...score, trace: { userId: "user-1" } }],
      count: 1,
    });
    mockDorisGet.mockResolvedValue(score);
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
});
