import { describe, expect, it, vi } from "vitest";

import {
  getDorisObservationsCountForPublicApi,
  getDorisObservationsForPublicApi,
} from "./publicApi";

const observation = {
  id: "span-1",
  traceId: "trace-1",
  projectId: "project-1",
  partitionDate: "2026-07-17",
  parentObservationId: null,
  type: "GENERATION",
  name: "generation",
  environment: "production",
  userId: null,
  sessionId: null,
  traceName: null,
  release: null,
  version: null,
  level: "DEFAULT",
  statusMessage: null,
  isAppRoot: false,
  bookmarked: false,
  public: false,
  startTime: new Date("2026-07-17T10:00:00.000Z"),
  endTime: null,
  completionStartTime: null,
  createdAt: new Date("2026-07-17T10:00:00.000Z"),
  updatedAt: new Date("2026-07-17T10:00:00.000Z"),
  providedModelName: null,
  internalModelId: null,
  promptId: null,
  promptName: null,
  promptVersion: null,
  totalInputTokens: 0,
  totalOutputTokens: 0,
  totalUsage: 0,
  totalCost: null,
  latency: null,
  timeToFirstToken: null,
  tags: [],
  usageDetails: {},
  costDetails: {},
  providedUsageDetails: {},
  providedCostDetails: {},
  toolDefinitionsCount: null,
  toolCallsCount: null,
  inputPreview: null,
  outputPreview: null,
};

function dependencies() {
  return {
    repository: {
      list: vi.fn(),
      listForTrace: vi.fn(),
      get: vi.fn(),
      count: vi.fn(),
      countForTrace: vi.fn(),
    },
    findTraceControls: vi
      .fn()
      .mockResolvedValue([
        { traceId: "trace-1", bookmarked: true, public: true },
      ]),
    findModels: vi.fn().mockResolvedValue([]),
  };
}

describe("Doris public observation reads", () => {
  it("uses the locator-backed point path without requiring a date range", async () => {
    const deps = dependencies();
    deps.repository.get.mockResolvedValue(observation);

    const result = await getDorisObservationsForPublicApi(
      {
        projectId: "project-1",
        page: 0,
        limit: 1,
        fields: ["core", "basic"],
        advancedFilters: [
          {
            type: "stringOptions",
            column: "id",
            operator: "any of",
            value: ["span-1"],
          },
        ],
      },
      deps,
    );

    expect(deps.repository.get).toHaveBeenCalledWith({
      projectId: "project-1",
      observationId: "span-1",
      traceId: undefined,
    });
    expect(deps.repository.list).not.toHaveBeenCalled();
    expect(result).toEqual([
      expect.objectContaining({
        id: "span-1",
        bookmarked: true,
        public: true,
      }),
    ]);
  });

  it("uses trace locators for a trace-scoped list without dates", async () => {
    const deps = dependencies();
    deps.repository.listForTrace.mockResolvedValue({
      items: [observation],
      nextCursor: null,
    });

    await getDorisObservationsForPublicApi(
      {
        projectId: "project-1",
        page: 0,
        limit: 10,
        traceId: "trace-1",
        fields: ["core"],
      },
      deps,
    );

    expect(deps.repository.listForTrace).toHaveBeenCalledWith(
      expect.objectContaining({ traceId: "trace-1" }),
    );
  });

  it("passes explicit time bounds and filters to list and count", async () => {
    const deps = dependencies();
    deps.repository.list.mockResolvedValue({ items: [], nextCursor: null });
    deps.repository.count.mockResolvedValue(3);
    const input = {
      projectId: "project-1",
      page: 1,
      limit: 10,
      fromStartTime: "2026-07-17T00:00:00.000Z",
      toStartTime: "2026-07-18T00:00:00.000Z",
      environment: ["production"],
      fields: ["core"] as const,
    };

    await getDorisObservationsForPublicApi(input, deps);
    await expect(
      getDorisObservationsCountForPublicApi(input, deps),
    ).resolves.toBe(3);

    expect(deps.repository.list).toHaveBeenCalledWith(
      expect.objectContaining({
        range: {
          from: new Date("2026-07-17T00:00:00.000Z"),
          to: new Date("2026-07-18T00:00:00.000Z"),
        },
        filters: expect.arrayContaining([
          expect.objectContaining({ column: "environment" }),
        ]),
      }),
    );
  });
});
