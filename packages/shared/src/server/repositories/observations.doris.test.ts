import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  list: vi.fn(),
  listForTrace: vi.fn(),
  count: vi.fn(),
  countForTrace: vi.fn(),
  filterOptionValues: vi.fn(),
  get: vi.fn(),
  traceGet: vi.fn(),
  modelFindMany: vi.fn(),
  traceControlStateFindMany: vi.fn(),
}));

vi.mock("../../db", () => ({
  prisma: {
    model: { findMany: mocks.modelFindMany },
    traceControlState: { findMany: mocks.traceControlStateFindMany },
  },
}));

vi.mock("./telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => ({
    observations: {
      list: mocks.list,
      listForTrace: mocks.listForTrace,
      count: mocks.count,
      countForTrace: mocks.countForTrace,
      filterOptionValues: mocks.filterOptionValues,
      get: mocks.get,
    },
    traces: { get: mocks.traceGet },
  }),
}));

import {
  getObservationsGroupedByModel,
  getObservationsGroupedByName,
  getObservationsGroupedByPromptName,
  getObservationsGroupedByTraceId,
  getObservationsForTrace,
  getCostForTraces,
  getLatencyAndTotalCostForObservationsByTraces,
  getObservationsTableCount,
  getObservationsTableWithModelData,
  getTraceIdsForObservations,
} from "./observations";

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
  promptName: "support",
  promptVersion: 1,
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

describe("legacy observation repository Doris routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.modelFindMany.mockResolvedValue([]);
    mocks.traceControlStateFindMany.mockResolvedValue([]);
  });

  it("routes generation list/count contracts through Doris", async () => {
    mocks.list.mockResolvedValue({ items: [observation], nextCursor: null });
    mocks.count.mockResolvedValue(1);

    await expect(
      getObservationsTableWithModelData({
        projectId: "project-1",
        filter,
        orderBy: { column: "startTime", order: "DESC" },
        limit: 50,
        offset: 100,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        id: "span-1",
        traceName: "trace",
        traceTags: ["prod"],
        toolDefinitionsCount: 0,
      }),
    ]);
    await expect(
      getObservationsTableCount({ projectId: "project-1", filter }),
    ).resolves.toBe(1);

    expect(mocks.list).toHaveBeenCalledWith(
      expect.objectContaining({
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: new Date("2026-07-18T00:00:00.000Z"),
        },
        orderBy: { column: "startTime", order: "DESC" },
        limit: 50,
        offset: 100,
      }),
    );
  });

  it("routes trace observation expansion through locator-bounded Doris pages", async () => {
    mocks.listForTrace.mockResolvedValue({
      items: [{ ...observation, input: { question: "full" }, metadata: {} }],
      nextCursor: null,
    });

    await expect(
      getObservationsForTrace({
        projectId: "project-1",
        traceId: "trace-1",
        includeIO: true,
      }),
    ).resolves.toEqual([
      expect.objectContaining({ id: "span-1", input: { question: "full" } }),
    ]);
    expect(mocks.listForTrace).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        traceId: "trace-1",
        includeFullContent: true,
      }),
    );
  });

  it("resolves annotation queue parent traces through Doris locators", async () => {
    mocks.get.mockResolvedValue(observation);

    await expect(
      getTraceIdsForObservations("project-1", ["span-1"]),
    ).resolves.toEqual([{ id: "span-1", traceId: "trace-1" }]);
  });

  it("routes generation filter options through Doris facets", async () => {
    mocks.filterOptionValues.mockImplementation(
      async ({ column }: { column: string }) => [
        { column, value: `${column}-value`, count: 1 },
      ],
    );

    await expect(
      getObservationsGroupedByModel("project-1", filter),
    ).resolves.toEqual([{ model: "providedModelName-value" }]);
    await expect(
      getObservationsGroupedByName("project-1", filter),
    ).resolves.toEqual([{ name: "name-value" }]);
    await expect(
      getObservationsGroupedByPromptName("project-1", filter),
    ).resolves.toEqual([{ promptName: "promptName-value" }]);
  });

  it("derives dataset/session trace aggregates from locator-bounded Doris reads", async () => {
    mocks.listForTrace.mockResolvedValue({
      items: [observation],
      nextCursor: null,
    });
    mocks.traceGet.mockResolvedValue({
      id: "trace-1",
      totalCost: 0.1,
      latency: 2,
    });

    await expect(
      getObservationsGroupedByTraceId("project-1", ["trace-1"]),
    ).resolves.toEqual(
      new Map([["trace-1", [["span-1", null, "0.1", "0", "0", 2_000]]]]),
    );
    await expect(
      getLatencyAndTotalCostForObservationsByTraces("project-1", ["trace-1"]),
    ).resolves.toEqual([{ traceId: "trace-1", totalCost: 0.1, latency: 2 }]);
    await expect(
      getCostForTraces("project-1", observation.startTime, ["trace-1"]),
    ).resolves.toBe(0.1);
  });
});
