import { describe, expect, it, vi } from "vitest";

import { getViewDeclaration } from "../../../dataModel";
import type { QueryType } from "../../../types";
import { getValidAggregationsForMeasureType } from "../../../types";
import { executeDorisAnalyticsQuery } from "./DorisAnalyticsQueryEngine";

const base = {
  filters: [],
  fromTimestamp: "2026-07-17T00:00:00.000Z",
  toTimestamp: "2026-07-18T00:00:00.000Z",
  orderBy: null,
} satisfies Pick<
  QueryType,
  "filters" | "fromTimestamp" | "toTimestamp" | "orderBy"
>;

describe("Doris analytics query engine", () => {
  it("compiles every R1A dimension, measure, and aggregation declared by the shared model", async () => {
    const deferred = new Set([
      "datasetRunId",
      "experimentName",
      "experimentDatasetId",
      "experimentId",
    ]);
    for (const version of ["v1", "v2"] as const) {
      for (const view of [
        "traces",
        "observations",
        "scores-numeric",
        "scores-categorical",
      ] as const) {
        const declaration = getViewDeclaration(view, version);
        for (const field of Object.keys(declaration.dimensions).filter(
          (candidate) => !deferred.has(candidate),
        )) {
          await executeDorisAnalyticsQuery({
            executor: { query: vi.fn().mockResolvedValue([]) },
            projectId: "project-1",
            version,
            query: {
              ...base,
              view,
              dimensions: [{ field }],
              metrics: [{ measure: "count", aggregation: "count" }],
              timeDimension: null,
              chartConfig: { type: "TABLE", row_limit: 10 },
              orderBy: [{ field: "count_count", direction: "desc" }],
            },
          });
        }
        for (const [measure, definition] of Object.entries(
          declaration.measures,
        )) {
          for (const aggregation of getValidAggregationsForMeasureType(
            definition.type,
          )) {
            await executeDorisAnalyticsQuery({
              executor: { query: vi.fn().mockResolvedValue([]) },
              projectId: "project-1",
              version,
              query: {
                ...base,
                view,
                dimensions: [],
                metrics: [{ measure, aggregation }],
                timeDimension: null,
              },
            });
          }
        }
      }
    }
  });

  it("compiles observation metrics with bounded visibility and canonical aliases", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        time_dimension: "2026-07-17 10:00:00",
        providedModelName: "gpt-4",
        sum_totalCost: "1.5",
        p95_latency: "250",
      },
    ]);
    const result = await executeDorisAnalyticsQuery({
      executor: { query },
      projectId: "project-1",
      version: "v2",
      query: {
        ...base,
        fromTimestamp: "2026-07-17T10:00:00.000Z",
        toTimestamp: "2026-07-17T11:00:00.000Z",
        view: "observations",
        dimensions: [{ field: "providedModelName" }],
        metrics: [
          { measure: "totalCost", aggregation: "sum" },
          { measure: "latency", aggregation: "p95" },
        ],
        filters: [
          {
            type: "stringOptions",
            column: "type",
            operator: "any of",
            value: ["GENERATION"],
          },
        ],
        timeDimension: { granularity: "hour" },
      },
    });

    expect(result).toEqual([
      {
        time_dimension: "2026-07-17T10:00:00.000Z",
        providedModelName: "gpt-4",
        sum_totalCost: 1.5,
        p95_latency: 250,
      },
    ]);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM events_current e");
    expect(sql).toContain("LEFT JOIN trace_tombstones");
    expect(sql).toContain("LEFT JOIN project_tombstones");
    expect(sql).toContain("DATE_TRUNC(b.event_time, 'hour')");
    expect(sql).toContain("PERCENTILE_APPROX");
    expect(sql).not.toContain("project-1");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["project-1", "GENERATION"]),
    );
  });

  it("normalizes Doris histogram JSON to the existing tuple contract", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        histogram_value: JSON.stringify({
          num_buckets: 2,
          buckets: [
            { lower: "0.1", upper: "0.2", count: 2, pre_sum: 0 },
            { lower: "0.8", upper: "1.0", count: 3, pre_sum: 2 },
          ],
        }),
      },
    ]);

    await expect(
      executeDorisAnalyticsQuery({
        executor: { query },
        projectId: "project-1",
        version: "v2",
        query: {
          ...base,
          view: "scores-numeric",
          dimensions: [],
          metrics: [{ measure: "value", aggregation: "histogram" }],
          timeDimension: null,
          chartConfig: { type: "HISTOGRAM", bins: 20 },
        },
      }),
    ).resolves.toEqual([
      {
        histogram_value: [
          [0.1, 0.2, 2],
          [0.8, 1, 3],
        ],
      },
    ]);
    expect(query.mock.calls[0]?.[0]).toContain("HISTOGRAM(b.value, 20)");
  });

  it("fills UTC time buckets deterministically across the half-open range", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        time_dimension: "2026-07-17 01:00:00",
        count_count: "2",
      },
    ]);

    await expect(
      executeDorisAnalyticsQuery({
        executor: { query },
        projectId: "project-1",
        version: "v2",
        query: {
          ...base,
          toTimestamp: "2026-07-17T03:00:00.000Z",
          view: "observations",
          dimensions: [],
          metrics: [{ measure: "count", aggregation: "count" }],
          timeDimension: { granularity: "hour" },
        },
      }),
    ).resolves.toEqual([
      { time_dimension: "2026-07-17T00:00:00.000Z", count_count: 0 },
      { time_dimension: "2026-07-17T01:00:00.000Z", count_count: 2 },
      { time_dimension: "2026-07-17T02:00:00.000Z", count_count: 0 },
    ]);
  });

  it("honors the public v2 config alias for histogram bins and row limits", async () => {
    const query = vi.fn().mockResolvedValue([]);
    await executeDorisAnalyticsQuery({
      executor: { query },
      projectId: "project-1",
      version: "v2",
      query: {
        ...base,
        view: "scores-numeric",
        dimensions: [],
        metrics: [{ measure: "value", aggregation: "histogram" }],
        timeDimension: null,
        config: { bins: 7, row_limit: 25 },
      } as QueryType,
    });

    expect(query.mock.calls[0]?.[0]).toContain("HISTOGRAM(b.value, 7)");
    expect(query.mock.calls[0]?.[0]).toContain("LIMIT ?");
    expect(query.mock.calls[0]?.[1]).toEqual(expect.arrayContaining([25]));
  });

  it("builds trace rows before aggregating to avoid observation fan-out", async () => {
    const query = vi.fn().mockResolvedValue([{ count_count: "2" }]);
    await executeDorisAnalyticsQuery({
      executor: { query },
      projectId: "project-1",
      version: "v1",
      query: {
        ...base,
        view: "traces",
        dimensions: [],
        metrics: [{ measure: "count", aggregation: "count" }],
        timeDimension: null,
      },
    });
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("trace_rows AS");
    expect(sql).toContain("GROUP BY e.project_id, e.trace_id");
    expect(sql).toContain("COUNT(*) AS count_count");
  });

  it("fails explicitly for deferred experiment dimensions", async () => {
    await expect(
      executeDorisAnalyticsQuery({
        executor: { query: vi.fn() },
        projectId: "project-1",
        version: "v2",
        query: {
          ...base,
          view: "observations",
          dimensions: [{ field: "experimentName" }],
          metrics: [{ measure: "count", aggregation: "count" }],
          timeDimension: null,
        },
      }),
    ).rejects.toThrow("not available on the Doris R1A backend");
  });
});
