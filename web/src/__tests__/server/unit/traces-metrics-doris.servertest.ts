import type { Session } from "next-auth";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  applyCommentFilters: vi.fn(),
  getScoresForTraces: vi.fn(),
  getTracesTableMetrics: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {},
  Role: { OWNER: "OWNER" },
}));

vi.mock("@langfuse/shared/src/server", async () => {
  const { ROOT_CONTEXT } = await import("@opentelemetry/api");
  return {
    applyCommentFilters: mocks.applyCommentFilters,
    getScoresForTraces: mocks.getScoresForTraces,
    getTracesTableMetrics: mocks.getTracesTableMetrics,
    traceException: vi.fn(),
    getTraceById: vi.fn(),
    getTraceByIdFromEventsTable: vi.fn(),
    addUserToSpan: vi.fn(),
    contextWithLangfuseProps: () => ROOT_CONTEXT,
    logger: {
      debug: vi.fn(),
      info: vi.fn(),
      warn: vi.fn(),
      error: vi.fn(),
    },
    redis: {
      status: "end",
      disconnect: vi.fn(),
    },
    ClickHouseClientManager: {
      getInstance: () => ({ closeAllConnections: vi.fn() }),
    },
    ClickHouseResourceError: class ClickHouseResourceError extends Error {
      static ERROR_ADVICE_MESSAGE = "ClickHouse resource limit exceeded.";
      errorType = "unknown";
      tags = {};
    },
    DorisError: class DorisError extends Error {},
  };
});

import { createInnerTRPCContext } from "@/src/server/api/trpc";
import { traceRouter } from "@/src/server/api/routers/traces";

const projectId = "project-1";
const session: Session = {
  expires: "1",
  user: {
    id: "user-1",
    name: "Doris user",
    email: "doris@example.com",
    canCreateOrganizations: true,
    organizations: [
      {
        id: "org-1",
        name: "Doris org",
        role: "OWNER",
        plan: "cloud:hobby",
        cloudConfig: undefined,
        metadata: {},
        aiFeaturesEnabled: false,
        aiTelemetryEnabled: false,
        projects: [
          {
            id: projectId,
            name: "Doris project",
            role: "ADMIN",
            deletedAt: null,
            retentionDays: null,
            hasTraces: true,
            metadata: {},
            createdAt: new Date(0).toISOString(),
          },
        ],
      },
    ],
    featureFlags: {
      excludeClickhouseRead: false,
      observationEvals: false,
      templateFlag: false,
      searchBar: false,
      v4BetaToggleVisible: false,
      experimentsV4Enabled: false,
    },
    admin: false,
  },
  environment: {
    enableExperimentalFeatures: false,
    selfHostedInstancePlan: "cloud:hobby",
  },
};

describe("traces.metrics Doris route", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.applyCommentFilters.mockResolvedValue({
      filterState: [],
      hasNoMatches: false,
      matchingIds: null,
    });
    mocks.getScoresForTraces.mockResolvedValue([]);
    mocks.getTracesTableMetrics.mockResolvedValue([
      {
        id: "trace-old",
        projectId,
        promptTokens: 10n,
        completionTokens: 5n,
        totalTokens: 15n,
        latency: 1,
        level: "DEFAULT",
        observationCount: 1n,
        calculatedTotalCost: null,
        calculatedInputCost: null,
        calculatedOutputCost: null,
        usageDetails: { input: 10, output: 5, total: 15 },
        costDetails: {},
        errorCount: 0n,
        warningCount: 0n,
        defaultCount: 1n,
        debugCount: 0n,
      },
    ]);
  });

  it("passes exact trace IDs to the backend-neutral metrics service", async () => {
    const caller = traceRouter.createCaller(
      createInnerTRPCContext({ session, headers: {} }),
    );

    await expect(
      caller.metrics({
        projectId,
        traceIds: ["trace-old"],
        filter: [],
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: "trace-old",
        promptTokens: 10n,
        scores: {},
      }),
    ]);
    expect(mocks.getTracesTableMetrics).toHaveBeenCalledWith({
      projectId,
      filter: [
        {
          type: "stringOptions",
          operator: "any of",
          column: "ID",
          value: ["trace-old"],
        },
      ],
      orderBy: { column: "timestamp", order: "DESC" },
    });
  });
});
