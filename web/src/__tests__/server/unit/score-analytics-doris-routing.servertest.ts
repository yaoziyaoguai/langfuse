import type * as SharedServer from "@langfuse/shared/src/server";
import type { Session } from "next-auth";

const mocks = vi.hoisted(() => ({
  estimateDorisScoreComparison: vi.fn(),
  getDorisScoreComparisonAnalytics: vi.fn(),
  queryClickhouse: vi.fn(),
  buildEstimateQuery: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", async () => ({
  ...(await vi.importActual<typeof SharedServer>(
    "@langfuse/shared/src/server",
  )),
  isDorisAnalyticsBackend: () => true,
  queryClickhouse: mocks.queryClickhouse,
}));

vi.mock("@/src/features/score-analytics/server/dorisScoreAnalytics", () => ({
  estimateDorisScoreComparison: mocks.estimateDorisScoreComparison,
  getDorisScoreComparisonAnalytics: mocks.getDorisScoreComparisonAnalytics,
}));

vi.mock("@/src/features/score-analytics/server/buildEstimateQuery", () => ({
  buildEstimateQuery: mocks.buildEstimateQuery,
}));

import { prisma } from "@langfuse/shared/src/db";
import { appRouter } from "@/src/server/api/root";
import { createInnerTRPCContext } from "@/src/server/api/trpc";

const projectId = "project-1";
const session: Session = {
  user: {
    id: "user-1",
    canCreateOrganizations: true,
    name: "Doris User",
    organizations: [
      {
        id: "org-1",
        name: "Doris Org",
        role: "OWNER",
        plan: "oss",
        cloudConfig: undefined,
        metadata: {},
        aiFeaturesEnabled: false,
        aiTelemetryEnabled: false,
        projects: [
          {
            id: projectId,
            role: "ADMIN",
            retentionDays: 30,
            deletedAt: null,
            name: "Doris Project",
            hasTraces: true,
            metadata: {},
            createdAt: new Date().toISOString(),
          },
        ],
      },
    ],
    featureFlags: {
      excludeClickhouseRead: false,
      templateFlag: false,
      searchBar: false,
      v4BetaToggleVisible: false,
      observationEvals: false,
      experimentsV4Enabled: false,
    },
    admin: false,
  },
  environment: {} as never,
  expires: new Date().toISOString(),
};

const score1 = {
  name: "quality",
  dataType: "NUMERIC",
  source: "API",
};
const score2 = {
  name: "correctness",
  dataType: "NUMERIC",
  source: "ANNOTATION",
};
const fromTimestamp = new Date("2026-07-17T00:00:00.000Z");
const toTimestamp = new Date("2026-07-18T00:00:00.000Z");

describe("Doris score analytics routing", () => {
  const caller = appRouter.createCaller({
    ...createInnerTRPCContext({ session, headers: {} }),
    prisma,
  });

  beforeEach(() => {
    vi.clearAllMocks();
    mocks.queryClickhouse.mockRejectedValue(
      new Error("ClickHouse must not be used in Doris mode"),
    );
    mocks.buildEstimateQuery.mockRejectedValue(
      new Error("ClickHouse estimate must not be used in Doris mode"),
    );
  });

  it("routes dataset-run estimates to the Doris repository", async () => {
    mocks.estimateDorisScoreComparison.mockResolvedValue({
      score1Count: 2,
      score2Count: 3,
      matchedCount: 1,
    });

    await expect(
      caller.scoreAnalytics.estimateScoreComparisonSize({
        projectId,
        score1,
        score2,
        fromTimestamp,
        toTimestamp,
        objectType: "dataset_run",
      }),
    ).resolves.toMatchObject({
      score1Count: 2,
      score2Count: 3,
      estimatedMatchedCount: 1,
    });

    expect(mocks.estimateDorisScoreComparison).toHaveBeenCalledWith({
      projectId,
      score1,
      score2,
      fromTimestamp,
      toTimestamp,
      objectType: "dataset_run",
    });
    expect(mocks.buildEstimateQuery).not.toHaveBeenCalled();
    expect(mocks.queryClickhouse).not.toHaveBeenCalled();
  });

  it("routes dataset-run analytics to Doris before ClickHouse planning", async () => {
    const response = {
      counts: { score1Total: 2, score2Total: 3, matchedCount: 1 },
    };
    mocks.getDorisScoreComparisonAnalytics.mockResolvedValue(response);

    await expect(
      caller.scoreAnalytics.getScoreComparisonAnalytics({
        projectId,
        score1,
        score2,
        fromTimestamp,
        toTimestamp,
        interval: { count: 1, unit: "day" },
        nBins: 10,
        objectType: "dataset_run",
      }),
    ).resolves.toBe(response);

    expect(mocks.getDorisScoreComparisonAnalytics).toHaveBeenCalledWith({
      projectId,
      score1,
      score2,
      fromTimestamp,
      toTimestamp,
      interval: { count: 1, unit: "day" },
      nBins: 10,
      objectType: "dataset_run",
      mode: "two",
    });
    expect(mocks.buildEstimateQuery).not.toHaveBeenCalled();
    expect(mocks.queryClickhouse).not.toHaveBeenCalled();
  });
});
