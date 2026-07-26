import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  filterOptionValues: vi.fn(),
  traceList: vi.fn(),
  traceGetMany: vi.fn(),
  queryClickhouse: vi.fn(),
  userCount: vi.fn(),
  userList: vi.fn(),
  traceControlFindMany: vi.fn(),
  observationListForTrace: vi.fn(),
}));

vi.mock("../../db", () => ({
  prisma: {
    traceControlState: { findMany: mocks.traceControlFindMany },
  },
}));

vi.mock("./telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => ({
    traces: {
      filterOptionValues: mocks.filterOptionValues,
      list: mocks.traceList,
      getMany: mocks.traceGetMany,
    },
    observations: { listForTrace: mocks.observationListForTrace },
    users: { count: mocks.userCount, list: mocks.userList },
  }),
}));

vi.mock("./clickhouse", () => ({
  BLOB_EXPORT_PARQUET_CLICKHOUSE_SETTINGS: {},
  commandClickhouse: vi.fn(),
  parseClickhouseUTCDateTimeFormat: vi.fn(),
  queryClickhouse: mocks.queryClickhouse,
  queryClickhouseExecRaw: vi.fn(),
  queryClickhouseStream: vi.fn(),
  upsertClickhouse: vi.fn(),
}));

import {
  getTracesGroupedByName,
  getTracesGroupedBySessionId,
  getTracesGroupedByTags,
  getTracesGroupedByUsers,
  getTotalUserCount,
  getTracesBySessionId,
  getTracesByIds,
  getAgentGraphData,
  getUserMetrics,
  hasAnyUser,
} from "./traces";

const timestampFilter = [
  {
    type: "datetime" as const,
    column: "timestamp",
    operator: ">=" as const,
    value: new Date("2026-07-17T00:00:00.000Z"),
  },
  {
    type: "datetime" as const,
    column: "timestamp",
    operator: "<" as const,
    value: new Date("2026-07-18T00:00:00.000Z"),
  },
];

describe("trace repository Doris routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.filterOptionValues.mockImplementation(
      async ({ column }: { column: string }) => [
        { value: `${column}-value`, count: 2 },
      ],
    );
    mocks.traceControlFindMany.mockResolvedValue([]);
  });

  it("routes trace name and tag facets through the bounded Doris repository", async () => {
    await expect(
      getTracesGroupedByName("project-1", undefined, timestampFilter),
    ).resolves.toEqual([{ name: "name-value", count: "2" }]);
    await expect(
      getTracesGroupedByTags({
        projectId: "project-1",
        filter: timestampFilter,
      }),
    ).resolves.toEqual([{ value: "tags-value" }]);

    expect(mocks.filterOptionValues).toHaveBeenCalledWith(
      expect.objectContaining({
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: new Date("2026-07-18T00:00:00.000Z"),
        },
        filters: expect.arrayContaining([
          expect.objectContaining({ column: "startTime" }),
        ]),
      }),
    );
  });

  it("preserves user and session facet paging/search inputs", async () => {
    await expect(
      getTracesGroupedByUsers("project-1", timestampFilter, "alice", 20, 40),
    ).resolves.toEqual([{ user: "userId-value", count: "2" }]);
    await expect(
      getTracesGroupedBySessionId(
        "project-1",
        timestampFilter,
        "session",
        10,
        5,
      ),
    ).resolves.toEqual([{ session_id: "sessionId-value", count: "2" }]);

    expect(mocks.filterOptionValues).toHaveBeenCalledWith(
      expect.objectContaining({
        column: "userId",
        valueQuery: "alice",
        limit: 20,
        offset: 40,
      }),
    );
    expect(mocks.filterOptionValues).toHaveBeenCalledWith(
      expect.objectContaining({
        column: "sessionId",
        valueQuery: "session",
        limit: 10,
        offset: 5,
      }),
    );
  });

  it("routes legacy user existence/count/metrics through event-derived Doris users", async () => {
    mocks.userCount.mockResolvedValue(1);
    mocks.userList.mockResolvedValue({
      items: [
        {
          id: "user-1",
          projectId: "project-1",
          minTimestamp: new Date("2026-07-17T10:00:00.000Z"),
          maxTimestamp: new Date("2026-07-17T10:00:03.000Z"),
          sessionIds: ["session-1"],
          environments: ["production"],
          traceCount: 1,
          sessionCount: 1,
          observationCount: 2,
          totalInputTokens: 10,
          totalOutputTokens: 5,
          totalUsage: 15,
          totalCost: 0.1,
        },
      ],
      nextCursor: null,
    });

    await expect(hasAnyUser("project-1")).resolves.toBe(true);
    await expect(
      getTotalUserCount("project-1", timestampFilter),
    ).resolves.toEqual([{ totalCount: 1n }]);
    await expect(
      getUserMetrics("project-1", ["user-1"], timestampFilter),
    ).resolves.toEqual([
      expect.objectContaining({ userId: "user-1", totalUsage: 15 }),
    ]);
  });

  it("routes legacy session trace reads through the bounded Doris repository", async () => {
    mocks.traceList.mockResolvedValue({
      items: [
        {
          id: "trace-1",
          projectId: "project-1",
          timestamp: new Date("2026-07-17T10:00:00.000Z"),
          endTime: new Date("2026-07-17T10:00:03.000Z"),
          name: "trace",
          environment: "production",
          userId: "user-1",
          sessionId: "session-1",
          release: null,
          version: null,
          tags: [],
          inputPreview: null,
          outputPreview: null,
          rootObservationId: "span-1",
          fallbackObservationId: "span-1",
          incomplete: false,
          observationCount: 1,
          totalInputTokens: 1,
          totalOutputTokens: 1,
          totalUsage: 2,
          totalCost: 0.1,
          latency: 3,
        },
      ],
      nextCursor: null,
    });

    await expect(
      getTracesBySessionId(
        "project-1",
        ["session-1"],
        new Date("2026-07-17T00:00:00.000Z"),
      ),
    ).resolves.toEqual([
      expect.objectContaining({ id: "trace-1", sessionId: "session-1" }),
    ]);

    expect(mocks.traceList).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: expect.any(Date),
        },
        filters: expect.arrayContaining([
          expect.objectContaining({ column: "sessionId" }),
        ]),
      }),
    );
  });

  it("loads exact trace IDs from Doris without a ClickHouse fallback", async () => {
    mocks.traceGetMany.mockResolvedValue([
      {
        id: "trace-1",
        projectId: "project-1",
        timestamp: new Date("2026-01-01T10:00:00.000Z"),
        endTime: new Date("2026-01-01T10:00:01.000Z"),
        name: "old trace",
        environment: "production",
        userId: null,
        sessionId: null,
        release: null,
        version: null,
        tags: [],
        inputPreview: null,
        outputPreview: null,
        input: { question: "old" },
        output: { answer: "found" },
        metadata: {},
        rootObservationId: "span-1",
        fallbackObservationId: "span-1",
        incomplete: false,
        observationCount: 1,
        totalInputTokens: 1,
        totalOutputTokens: 1,
        totalUsage: 2,
        totalCost: 0.01,
        latency: 1,
      },
    ]);
    mocks.traceControlFindMany.mockResolvedValue([
      { traceId: "trace-1", bookmarked: true, public: false },
    ]);

    await expect(
      getTracesByIds(
        ["trace-1", "missing"],
        "project-1",
        new Date("2026-07-01T00:00:00.000Z"),
      ),
    ).resolves.toEqual([
      expect.objectContaining({
        id: "trace-1",
        bookmarked: true,
        input: { question: "old" },
      }),
    ]);
    expect(mocks.traceGetMany).toHaveBeenCalledWith({
      projectId: "project-1",
      traceIds: ["trace-1", "missing"],
    });
    expect(mocks.queryClickhouse).not.toHaveBeenCalled();
  });

  it("routes agent graph reads through Doris", async () => {
    mocks.observationListForTrace.mockResolvedValue({
      items: [
        {
          id: "observation-1",
          parentObservationId: null,
          type: "SPAN",
          name: "agent",
          startTime: new Date("2026-07-17T10:00:00.000Z"),
          endTime: new Date("2026-07-17T10:00:01.000Z"),
          metadata: { langgraph_node: "planner", langgraph_step: 1 },
        },
      ],
    });

    await expect(
      getAgentGraphData({
        projectId: "project-1",
        traceId: "trace-1",
        chMinStartTime: "2026-07-17 00:00:00.000",
        chMaxStartTime: "2026-07-18 00:00:00.000",
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: "observation-1",
        node: "planner",
        step: 1,
      }),
    ]);

    expect(mocks.observationListForTrace).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        traceId: "trace-1",
        includeFullContent: true,
        filters: [
          expect.objectContaining({ column: "startTime", operator: ">=" }),
          expect.objectContaining({ column: "startTime", operator: "<=" }),
        ],
      }),
    );
  });
});
