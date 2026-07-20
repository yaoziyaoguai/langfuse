import { beforeEach, describe, expect, it, vi } from "vitest";

import type { TimeFilter } from "../../types";

const mocks = vi.hoisted(() => ({
  aggregateGroups: vi.fn(),
}));

vi.mock("./telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => ({
    scores: { aggregateGroups: mocks.aggregateGroups },
  }),
}));

import { getDistinctScoreNames } from "./scores";

describe("legacy score repository Doris routing", () => {
  beforeEach(() => vi.clearAllMocks());

  it("reads distinct export score columns from Doris with the export cutoff", async () => {
    mocks.aggregateGroups.mockResolvedValue([
      { name: "quality", count: 2 },
      { name: "helpfulness", count: 1 },
    ]);
    const cutoffCreatedAt = new Date("2026-07-20T10:00:00.000Z");
    const lowerBound = new Date("2026-07-01T00:00:00.000Z");

    await expect(
      getDistinctScoreNames({
        projectId: "project-1",
        cutoffCreatedAt,
        filter: [
          {
            type: "datetime",
            column: "Timestamp",
            operator: ">=",
            value: lowerBound,
          },
        ],
        isTimestampFilter: (item): item is TimeFilter =>
          item.type === "datetime" && item.column === "Timestamp",
      }),
    ).resolves.toEqual(["quality", "helpfulness"]);

    expect(mocks.aggregateGroups).toHaveBeenCalledWith({
      projectId: "project-1",
      range: {
        from: new Date("2026-06-30T23:00:00.000Z"),
        to: expect.any(Date),
      },
      filters: [
        {
          type: "datetime",
          column: "createdAt",
          operator: "<=",
          value: cutoffCreatedAt,
        },
        {
          type: "stringOptions",
          column: "dataType",
          operator: "any of",
          value: ["NUMERIC", "BOOLEAN", "CATEGORICAL", "TEXT"],
        },
      ],
      columns: ["name"],
      limit: 200,
    });
  });
});
