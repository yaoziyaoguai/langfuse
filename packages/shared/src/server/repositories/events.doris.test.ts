import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  users: {
    list: vi.fn(),
    count: vi.fn(),
  },
  sessions: {
    list: vi.fn(),
    count: vi.fn(),
  },
  traces: {
    list: vi.fn(),
    get: vi.fn(),
  },
  observations: {
    list: vi.fn(),
    scan: vi.fn(),
    listForTrace: vi.fn(),
    count: vi.fn(),
    counts: vi.fn(),
    countForTrace: vi.fn(),
    get: vi.fn(),
    filterOptionValues: vi.fn(),
    numericStats: vi.fn(),
    latestSdkMetadata: vi.fn(),
  },
  modelFindMany: vi.fn(),
  traceControlFindMany: vi.fn(),
  traceControlFindUnique: vi.fn(),
  publicObservations: vi.fn(),
  publicObservationCount: vi.fn(),
  publicTraces: vi.fn(),
  publicTraceCount: vi.fn(),
}));

vi.mock("../../db", () => ({
  prisma: {
    model: { findMany: mocks.modelFindMany },
    traceControlState: {
      findMany: mocks.traceControlFindMany,
      findUnique: mocks.traceControlFindUnique,
    },
  },
}));

vi.mock("./telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => mocks,
}));

vi.mock("./telemetry/doris/publicApi", () => ({
  getDorisObservationsForPublicApi: mocks.publicObservations,
  getDorisObservationsCountForPublicApi: mocks.publicObservationCount,
}));

vi.mock("./telemetry/doris/publicTraces", () => ({
  getDorisTracesForPublicApi: mocks.publicTraces,
  getDorisTracesCountForPublicApi: mocks.publicTraceCount,
}));

import {
  getSessionMetricsFromEvents,
  getTraceDeleteCursorPageFromEvents,
  getObservationByIdFromEventsTable,
  getAgentGraphDataFromEventsTable,
  getEventsFilterOptionsForColumns,
  getEventsFilterOptionValuesPage,
  getEventsGroupedByTraceName,
  getEventsGroupedByTraceTags,
  getEventsGroupedByUserId,
  getEventsNumericStatsByFilterColumn,
  getLatestSdkVersionInfoFromEvents,
  getObservationsCountsFromEventsTable,
  getObservationsBatchIOFromEventsTable,
  getObservationsForTraceFromEventsTable,
  getObservationsTraceIdsFromEventsTable,
  getObservationsWithModelDataFromEventsTable,
  getObservationFullIOForSessionFromEventsTable,
  getObservationsCountFromEventsTableForPublicApi,
  getObservationsV2FromEventsTableForPublicApi,
  getTracesCountFromEventsTableForPublicApi,
  getTraceByIdFromEventsTable,
  getTracesFromEventsTableForPublicApi,
  getTracesIdentifierForSessionFromEvents,
  getUserMetricsFromEventsTable,
  getUsersCountFromEventsTable,
  getUsersFromEventsTable,
  hasAnySessionFromEventsTable,
  hasAnyTraceFromEventsTable,
  hasAnyUserFromEventsTable,
} from "./events";

const user = (id: string, traceCount: number) => ({
  id,
  projectId: "project-1",
  minTimestamp: new Date("2026-07-17T10:00:00.000Z"),
  maxTimestamp: new Date("2026-07-17T10:00:03.000Z"),
  sessionIds: ["session-1"],
  environments: ["production"],
  traceCount,
  sessionCount: 1,
  observationCount: 3,
  totalInputTokens: 12,
  totalOutputTokens: 6,
  totalUsage: 18,
  totalCost: 0.5,
});

const session = {
  id: "session-1",
  projectId: "project-1",
  minTimestamp: new Date("2026-07-17T10:00:00.000Z"),
  maxTimestamp: new Date("2026-07-17T10:00:03.000Z"),
  traceIds: ["trace-1"],
  userIds: ["user-1"],
  environments: ["production"],
  tags: ["api"],
  traceCount: 1,
  observationCount: 2,
  totalInputTokens: 12,
  totalOutputTokens: 6,
  totalUsage: 18,
  totalCost: 0.5,
  duration: 3,
};

const observation = {
  id: "span-1",
  traceId: "trace-1",
  projectId: "project-1",
  partitionDate: "2026-07-17",
  parentObservationId: null,
  type: "GENERATION",
  name: "generation",
  environment: "production",
  userId: "user-1",
  sessionId: "session-1",
  traceName: "trace",
  release: null,
  version: null,
  level: "DEFAULT",
  statusMessage: null,
  isAppRoot: true,
  bookmarked: false,
  public: false,
  startTime: new Date("2026-07-17T10:00:00.000Z"),
  endTime: new Date("2026-07-17T10:00:02.000Z"),
  completionStartTime: null,
  createdAt: new Date("2026-07-17T10:00:00.000Z"),
  updatedAt: new Date("2026-07-17T10:00:02.000Z"),
  providedModelName: "gpt-test",
  internalModelId: null,
  promptId: null,
  promptName: null,
  promptVersion: null,
  totalInputTokens: 10,
  totalOutputTokens: 5,
  totalUsage: 15,
  totalCost: 0.1,
  latency: 2,
  timeToFirstToken: null,
  tags: ["prod"],
  usageDetails: { input: 10, output: 5, total: 15 },
  costDetails: { total: 0.1 },
  providedUsageDetails: {},
  providedCostDetails: {},
  toolDefinitionsCount: 0,
  toolCallsCount: 0,
  inputPreview: "preview",
  outputPreview: "preview",
};

describe("events repository Doris routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.modelFindMany.mockResolvedValue([]);
    mocks.traceControlFindMany.mockResolvedValue([]);
    mocks.traceControlFindUnique.mockResolvedValue(null);
  });

  it("routes the events table list and stable order through Doris", async () => {
    mocks.observations.list.mockResolvedValue({
      items: [observation],
      nextCursor: null,
    });

    await expect(
      getObservationsWithModelDataFromEventsTable({
        projectId: "project-1",
        filter: [
          {
            type: "datetime",
            column: "startTime",
            operator: ">=",
            value: new Date("2026-07-17T00:00:00.000Z"),
          },
          {
            type: "datetime",
            column: "startTime",
            operator: "<",
            value: new Date("2026-07-18T00:00:00.000Z"),
          },
        ],
        orderBy: { column: "totalCost", order: "ASC" },
        limit: 50,
        offset: 100,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: "span-1",
        traceTags: ["prod"],
        modelId: null,
      }),
    ]);
    expect(mocks.observations.list).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: new Date("2026-07-18T00:00:00.000Z"),
        },
        orderBy: { column: "totalCost", order: "ASC" },
        offset: 100,
        limit: 50,
      }),
    );
  });

  it("routes direct events point readers through locator-backed Doris detail", async () => {
    mocks.observations.get.mockResolvedValue({
      ...observation,
      input: { question: "full" },
      output: { answer: "full" },
      metadata: { region: "eu" },
    });
    mocks.traces.get.mockResolvedValue({
      id: "trace-1",
      projectId: "project-1",
      timestamp: observation.startTime,
      endTime: observation.endTime,
      name: "trace",
      environment: "production",
      userId: "user-1",
      sessionId: "session-1",
      release: null,
      version: null,
      tags: ["prod"],
      inputPreview: "preview",
      outputPreview: "preview",
      input: { question: "full" },
      output: { answer: "full" },
      metadata: { region: "eu" },
      rootObservationId: "span-1",
      fallbackObservationId: "span-1",
      incomplete: false,
      observationCount: 1,
      totalInputTokens: 10,
      totalOutputTokens: 5,
      totalUsage: 15,
      totalCost: 0.1,
      latency: 2,
    });

    await expect(
      getObservationByIdFromEventsTable({
        projectId: "project-1",
        id: "span-1",
        fetchWithInputOutput: true,
      }),
    ).resolves.toEqual(
      expect.objectContaining({ id: "span-1", input: { question: "full" } }),
    );
    await expect(
      getTraceByIdFromEventsTable({
        projectId: "project-1",
        traceId: "trace-1",
      }),
    ).resolves.toEqual(
      expect.objectContaining({ id: "trace-1", input: { question: "full" } }),
    );
  });

  it("routes event counts and trace-scoped detail through Doris", async () => {
    mocks.observations.counts.mockResolvedValue({
      totalCount: 3,
      uniqueTraceCount: 2,
    });
    mocks.observations.listForTrace.mockResolvedValue({
      items: [observation],
      nextCursor: null,
    });

    await expect(
      getObservationsCountsFromEventsTable({
        projectId: "project-1",
        filter: [
          {
            type: "datetime",
            column: "startTime",
            operator: ">=",
            value: new Date("2026-07-17T00:00:00.000Z"),
          },
          {
            type: "datetime",
            column: "startTime",
            operator: "<",
            value: new Date("2026-07-18T00:00:00.000Z"),
          },
        ],
      }),
    ).resolves.toEqual({ totalCount: 3, uniqueTraceCount: 2 });
    await expect(
      getObservationsForTraceFromEventsTable({
        projectId: "project-1",
        traceId: "trace-1",
        selectIOAndMetadata: true,
      }),
    ).resolves.toEqual({
      observations: [expect.objectContaining({ id: "span-1" })],
      totalCount: 1,
    });
    expect(mocks.observations.listForTrace).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        traceId: "trace-1",
        includeFullContent: true,
      }),
    );
  });

  it("routes the bounded trace existence probe through Doris", async () => {
    mocks.observations.count.mockResolvedValue(1);

    await expect(hasAnyTraceFromEventsTable("project-1")).resolves.toBe(true);
    expect(mocks.observations.count).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        range: { from: expect.any(Date), to: expect.any(Date) },
        filters: [],
      }),
    );
  });

  it("routes batch and session-scoped full content through Doris", async () => {
    const fullObservation = {
      ...observation,
      input: { question: "full" },
      output: { answer: "full" },
      metadata: { region: "eu" },
      toolCalls: ["search"],
      toolCallNames: ["search"],
      experimentItemExpectedOutput: '{"answer":"expected"}',
      experimentItemMetadata: { dataset: "golden" },
    };
    mocks.observations.list.mockResolvedValue({
      items: [fullObservation],
      nextCursor: null,
    });
    mocks.observations.get.mockResolvedValue(fullObservation);

    await expect(
      getObservationsBatchIOFromEventsTable({
        projectId: "project-1",
        observations: [{ id: "span-1", traceId: "trace-1" }],
        minStartTime: new Date("2026-07-17T09:59:59.000Z"),
        maxStartTime: new Date("2026-07-17T10:00:01.000Z"),
        truncated: false,
        includeExperimentFields: true,
        includeToolCallFields: true,
      }),
    ).resolves.toEqual([
      {
        id: "span-1",
        input: '{"question":"full"}',
        output: '{"answer":"full"}',
        metadata: { region: "eu" },
        experimentItemExpectedOutput: '{"answer":"expected"}',
        experimentItemMetadata: { dataset: "golden" },
        toolCalls: ["search"],
        toolCallNames: ["search"],
      },
    ]);
    await expect(
      getObservationFullIOForSessionFromEventsTable({
        projectId: "project-1",
        sessionId: "session-1",
        traceId: "trace-1",
        observationId: "span-1",
        startTime: observation.startTime,
      }),
    ).resolves.toEqual({
      id: "span-1",
      input: '{"question":"full"}',
      output: '{"answer":"full"}',
      metadata: { region: "eu" },
    });

    await expect(
      getObservationsTraceIdsFromEventsTable({
        projectId: "project-1",
        observationIds: ["span-1"],
      }),
    ).resolves.toEqual([{ id: "span-1", traceId: "trace-1" }]);
  });

  it("routes the durable event trace-delete cursor through Doris", async () => {
    mocks.observations.scan.mockResolvedValue({
      items: [observation],
      nextCursor: null,
    });

    await expect(
      getTraceDeleteCursorPageFromEvents({
        projectId: "project-1",
        filter: [],
        cutoffCreatedAt: new Date("2026-07-18T00:00:00.000Z"),
        cursor: {
          id: "span-cursor",
          traceId: "trace-cursor",
          timestamp: "2026-07-17T12:00:00.000Z",
        },
        limit: 50,
      }),
    ).resolves.toEqual([
      {
        id: "span-1",
        traceId: "trace-1",
        timestamp: "2026-07-17T10:00:00.000Z",
      },
    ]);

    const cursor = mocks.observations.scan.mock.calls[0]?.[0]?.cursor as string;
    expect(
      JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")),
    ).toEqual({
      version: 1,
      startTime: "2026-07-17T12:00:00.000Z",
      traceId: "trace-cursor",
      spanId: "span-cursor",
    });
  });

  it("routes trace graph rows through the bounded Doris detail reader", async () => {
    mocks.observations.listForTrace.mockResolvedValue({
      items: [
        {
          ...observation,
          metadata: { langgraph_node: "agent", langgraph_step: 2 },
        },
      ],
      nextCursor: null,
    });

    await expect(
      getAgentGraphDataFromEventsTable({
        projectId: "project-1",
        traceId: "trace-1",
        chMinStartTime: "2026-07-17 09:59:00.000",
        chMaxStartTime: "2026-07-17 10:01:00.000",
      }),
    ).resolves.toEqual([
      {
        id: "span-1",
        parent_observation_id: null,
        type: "GENERATION",
        name: "generation",
        start_time: "2026-07-17T10:00:00.000Z",
        end_time: "2026-07-17T10:00:02.000Z",
        node: "agent",
        step: 2,
      },
    ]);
  });

  it("routes event filter options and numeric ranges through bounded Doris scans", async () => {
    mocks.observations.filterOptionValues.mockImplementation(
      async ({ column }: { column: string }) => [
        { column, value: `${column}-value`, count: 2 },
      ],
    );
    mocks.observations.numericStats.mockResolvedValue({
      min: 0.25,
      max: 2,
      avg: 1.125,
      count: 4,
    });
    const filter = [
      {
        type: "datetime" as const,
        column: "startTime",
        operator: ">=" as const,
        value: new Date("2026-07-17T00:00:00.000Z"),
      },
      {
        type: "datetime" as const,
        column: "startTime",
        operator: "<" as const,
        value: new Date("2026-07-18T00:00:00.000Z"),
      },
    ];

    await expect(
      getEventsFilterOptionsForColumns({
        projectId: "project-1",
        filter,
        columns: ["name", "traceTags", "experimentId"],
      }),
    ).resolves.toEqual([
      { column: "name", value: "name-value", count: 2 },
      { column: "traceTags", value: "traceTags-value", count: 2 },
      { column: "experimentId", value: "experimentId-value", count: 2 },
    ]);
    await expect(
      getEventsFilterOptionValuesPage({
        projectId: "project-1",
        filter,
        column: "userId",
        limit: 20,
        offset: 40,
      }),
    ).resolves.toEqual([{ column: "userId", value: "userId-value", count: 2 }]);
    await expect(
      getEventsNumericStatsByFilterColumn("project-1", filter, "latency"),
    ).resolves.toEqual({ min: 0.25, max: 2, avg: 1.125, count: 4 });
    await expect(
      getEventsGroupedByTraceName("project-1", filter),
    ).resolves.toEqual([{ traceName: "traceName-value", count: 2 }]);
    await expect(
      getEventsGroupedByTraceTags("project-1", filter),
    ).resolves.toEqual([{ tag: "traceTags-value" }]);
    await expect(
      getEventsGroupedByUserId("project-1", filter),
    ).resolves.toEqual([{ userId: "userId-value", count: 2 }]);

    expect(mocks.observations.filterOptionValues).toHaveBeenCalledWith(
      expect.objectContaining({
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: new Date("2026-07-18T00:00:00.000Z"),
        },
      }),
    );
    expect(mocks.observations.filterOptionValues).toHaveBeenCalledWith(
      expect.objectContaining({ column: "experimentId" }),
    );

    await getEventsGroupedByTraceName("project-1", filter, {
      scope: "scoredTraces",
    });
    expect(mocks.observations.filterOptionValues).toHaveBeenLastCalledWith(
      expect.objectContaining({ requireScore: {} }),
    );
  });

  it("routes recent SDK attribution lookup through Doris", async () => {
    mocks.observations.latestSdkMetadata.mockResolvedValue({
      isOtel: true,
      name: "js",
      version: "4.1.0",
      language: "javascript",
    });

    await expect(
      getLatestSdkVersionInfoFromEvents({ projectId: "project-1" }),
    ).resolves.toEqual({
      isOtel: true,
      name: "js",
      version: "4.1.0",
      language: "javascript",
    });
  });

  it("paginates user rows and preserves their existing shape", async () => {
    mocks.users.list
      .mockResolvedValueOnce({
        items: [user("user-3", 3), user("user-2", 2)],
        nextCursor: "next",
      })
      .mockResolvedValueOnce({
        items: [user("user-1", 1)],
        nextCursor: null,
      });

    await expect(
      getUsersFromEventsTable("project-1", [], undefined, 2, 1),
    ).resolves.toEqual([
      { user: "user-2", count: "2" },
      { user: "user-1", count: "1" },
    ]);
  });

  it("routes user counts, existence, and metrics to Doris", async () => {
    mocks.users.count.mockResolvedValue(2);
    mocks.users.list.mockResolvedValue({
      items: [user("user-1", 2)],
      nextCursor: null,
    });

    await expect(
      getUsersCountFromEventsTable("project-1", []),
    ).resolves.toEqual([{ totalCount: "2" }]);
    await expect(hasAnyUserFromEventsTable("project-1")).resolves.toBe(true);
    await expect(
      getUserMetricsFromEventsTable("project-1", ["user-1"], []),
    ).resolves.toEqual([
      expect.objectContaining({
        userId: "user-1",
        traceCount: 2,
        totalUsage: 18,
      }),
    ]);
  });

  it("routes session existence and metrics to Doris", async () => {
    mocks.sessions.count.mockResolvedValue(1);
    mocks.sessions.list.mockResolvedValue({
      items: [session],
      nextCursor: null,
    });

    await expect(hasAnySessionFromEventsTable("project-1")).resolves.toBe(true);
    await expect(
      getSessionMetricsFromEvents({
        projectId: "project-1",
        sessionIds: ["session-1"],
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        session_id: "session-1",
        total_observations: 2,
      }),
    ]);
  });

  it("routes session trace identifiers to Doris with stable ordering", async () => {
    mocks.traces.list.mockResolvedValue({
      items: [
        {
          id: "trace-2",
          userId: "user-1",
          name: "second",
          timestamp: new Date("2026-07-17T10:00:02.000Z"),
          environment: "production",
        },
        {
          id: "trace-1",
          userId: "user-1",
          name: "first",
          timestamp: new Date("2026-07-17T10:00:01.000Z"),
          environment: "production",
        },
      ],
      nextCursor: null,
    });

    await expect(
      getTracesIdentifierForSessionFromEvents("project-1", "session-1"),
    ).resolves.toEqual([
      expect.objectContaining({ id: "trace-1" }),
      expect.objectContaining({ id: "trace-2" }),
    ]);
  });

  it("loads the Doris public services without introducing a module cycle", async () => {
    mocks.publicObservations.mockResolvedValue([{ id: "observation-1" }]);
    mocks.publicObservationCount.mockResolvedValue(1);
    mocks.publicTraces.mockResolvedValue([{ id: "trace-1" }]);
    mocks.publicTraceCount.mockResolvedValue(1);

    await expect(
      getObservationsV2FromEventsTableForPublicApi({
        projectId: "project-1",
        page: 1,
        limit: 50,
        fields: ["core"],
      }),
    ).resolves.toEqual([{ id: "observation-1" }]);
    await expect(
      getObservationsCountFromEventsTableForPublicApi({
        projectId: "project-1",
        page: 1,
        limit: 50,
      }),
    ).resolves.toBe(1);
    await expect(
      getTracesFromEventsTableForPublicApi({
        projectId: "project-1",
        page: 1,
        limit: 50,
      }),
    ).resolves.toEqual([{ id: "trace-1" }]);
    await expect(
      getTracesCountFromEventsTableForPublicApi({
        projectId: "project-1",
        page: 1,
        limit: 50,
      }),
    ).resolves.toBe(1);
  });
});
