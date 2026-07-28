import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  list: vi.fn(),
  count: vi.fn(),
  metrics: vi.fn(),
  traceControlFindMany: vi.fn(),
}));

vi.mock("../../db", () => ({
  prisma: {
    traceControlState: { findMany: mocks.traceControlFindMany },
  },
}));

vi.mock("../repositories/telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => ({
    traces: { list: mocks.list, count: mocks.count, metrics: mocks.metrics },
  }),
}));

import {
  getTraceDeleteCursorPageFromTraces,
  getTracesTable,
  getTracesTableCount,
  getTracesTableMetrics,
} from "./traces-ui-table-service";

const trace = {
  id: "trace-1",
  projectId: "project-1",
  timestamp: new Date("2026-07-17T10:00:00.000Z"),
  endTime: new Date("2026-07-17T10:00:02.000Z"),
  name: "trace",
  environment: "production",
  userId: "user-1",
  sessionId: "session-1",
  release: null,
  version: null,
  tags: ["prod"],
  inputPreview: null,
  outputPreview: null,
  rootObservationId: "span-1",
  fallbackObservationId: "span-1",
  incomplete: false,
  observationCount: 1,
  totalInputTokens: 10,
  totalOutputTokens: 5,
  totalUsage: 15,
  totalCost: 0.1,
  latency: 2,
};

const dateFilters = [
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

describe("Doris traces UI service", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.traceControlFindMany.mockResolvedValue([
      { traceId: "trace-1", bookmarked: true, public: false },
    ]);
  });

  it("routes bounded list, bookmark control, order, and offset to Doris", async () => {
    mocks.list.mockResolvedValue({ items: [trace], nextCursor: null });

    await expect(
      getTracesTable({
        projectId: "project-1",
        filter: [
          ...dateFilters,
          {
            type: "boolean",
            column: "bookmarked",
            operator: "=",
            value: true,
          },
        ],
        orderBy: { column: "traceName", order: "ASC" },
        page: 2,
        limit: 10,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: "trace-1",
        timestamp: new Date("2026-07-17T10:00:00.000Z"),
        bookmarked: true,
        public: false,
      }),
    ]);
    expect(mocks.list).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: new Date("2026-07-18T00:00:00.000Z"),
        },
        filters: expect.arrayContaining([
          expect.objectContaining({
            column: "traceId",
            operator: "any of",
            value: ["trace-1"],
          }),
        ]),
        orderBy: { column: "name", order: "ASC" },
        offset: 20,
        limit: 10,
      }),
    );
  });

  it("routes count through the same bounded Doris filters", async () => {
    mocks.count.mockResolvedValue(3);

    await expect(
      getTracesTableCount({
        projectId: "project-1",
        filter: dateFilters,
        searchType: ["id"],
      }),
    ).resolves.toBe(3);
    expect(mocks.count).toHaveBeenCalledWith(
      expect.objectContaining({ projectId: "project-1" }),
    );
  });

  it("routes trace metrics and the export ID alias through Doris", async () => {
    mocks.metrics.mockResolvedValue([
      {
        id: "trace-1",
        projectId: "project-1",
        timestamp: new Date("2026-07-17T10:00:00.000Z"),
        latency: 2,
        level: "ERROR",
        observationCount: 2,
        usageDetails: {
          input: 10,
          input_cached: 2,
          output: 5,
          total: 17,
        },
        costDetails: { input: 0.1, output: 0.2, total: 0.3 },
        errorCount: 1,
        warningCount: 0,
        defaultCount: 1,
        debugCount: 0,
      },
    ]);

    await expect(
      getTracesTableMetrics({
        projectId: "project-1",
        filter: [
          ...dateFilters,
          {
            type: "stringOptions",
            column: "ID",
            operator: "any of",
            value: ["trace-1"],
          },
        ],
        orderBy: { column: "timestamp", order: "DESC" },
        limit: 50,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: "trace-1",
        promptTokens: 12n,
        completionTokens: 5n,
        totalTokens: 17n,
        observationCount: 2n,
        level: "ERROR",
        errorCount: 1n,
      }),
    ]);
    expect(mocks.metrics).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        filters: expect.arrayContaining([
          expect.objectContaining({
            column: "traceId",
            operator: "any of",
            value: ["trace-1"],
          }),
        ]),
        orderBy: { column: "timestamp", order: "DESC" },
        offset: 0,
        limit: 50,
      }),
    );
  });

  it("preserves null latency and empty usage/cost details", async () => {
    mocks.metrics.mockResolvedValue([
      {
        id: "trace-1",
        projectId: "project-1",
        timestamp: new Date("2026-07-17T10:00:00.000Z"),
        latency: null,
        level: "DEBUG",
        observationCount: 1,
        usageDetails: {},
        costDetails: {},
        errorCount: 0,
        warningCount: 0,
        defaultCount: 0,
        debugCount: 1,
      },
    ]);

    await expect(
      getTracesTableMetrics({
        projectId: "project-1",
        filter: dateFilters,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: "trace-1",
        latency: null,
        promptTokens: 0n,
        completionTokens: 0n,
        totalTokens: 0n,
        calculatedTotalCost: null,
        calculatedInputCost: null,
        calculatedOutputCost: null,
        usageDetails: {},
        costDetails: {},
      }),
    ]);
  });

  it("routes the durable trace-delete cursor through Doris", async () => {
    mocks.list.mockResolvedValue({ items: [trace], nextCursor: null });

    await expect(
      getTraceDeleteCursorPageFromTraces({
        projectId: "project-1",
        filter: [],
        cutoffCreatedAt: new Date("2026-07-18T00:00:00.000Z"),
        cursor: {
          traceId: "trace-cursor",
          timestamp: "2026-07-17T12:00:00.000Z",
        },
        limit: 50,
      }),
    ).resolves.toEqual([
      {
        traceId: "trace-1",
        timestamp: "2026-07-17T10:00:00.000Z",
      },
    ]);

    const cursor = mocks.list.mock.calls[0]?.[0]?.cursor as string;
    expect(
      JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")),
    ).toEqual({
      version: 1,
      timestamp: "2026-07-17T12:00:00.000Z",
      traceId: "trace-cursor",
    });
    expect(mocks.list).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        cursor,
        limit: 50,
        offset: 0,
        orderBy: undefined,
      }),
    );
  });
});
