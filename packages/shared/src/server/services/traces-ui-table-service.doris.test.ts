import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  list: vi.fn(),
  count: vi.fn(),
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
    traces: { list: mocks.list, count: mocks.count },
  }),
}));

import { getTracesTable, getTracesTableCount } from "./traces-ui-table-service";

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
});
