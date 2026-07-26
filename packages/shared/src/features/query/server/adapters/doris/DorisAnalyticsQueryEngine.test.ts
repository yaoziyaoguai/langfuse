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
    for (const version of ["v1", "v2"] as const) {
      for (const view of [
        "traces",
        "observations",
        "scores-numeric",
        "scores-categorical",
      ] as const) {
        const declaration = getViewDeclaration(view, version);
        for (const field of Object.keys(declaration.dimensions)) {
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

  it("queries experiment dimensions from the canonical event projection", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        experimentName: "prompt-eval",
        experimentDatasetId: "dataset-1",
        experimentId: "run-1",
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
          view: "observations",
          dimensions: [
            { field: "experimentName" },
            { field: "experimentDatasetId" },
            { field: "experimentId" },
          ],
          metrics: [{ measure: "count", aggregation: "count" }],
          filters: [
            {
              type: "string",
              column: "experimentId",
              operator: "=",
              value: "run-1",
            },
          ],
          timeDimension: null,
          chartConfig: { type: "TABLE", row_limit: 100 },
          orderBy: [{ field: "count_count", direction: "desc" }],
        },
      }),
    ).resolves.toEqual([
      {
        experimentName: "prompt-eval",
        experimentDatasetId: "dataset-1",
        experimentId: "run-1",
        count_count: 2,
      },
    ]);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("NULLIF(b.experiment_name, '')");
    expect(sql).toContain("NULLIF(b.experiment_dataset_id, '')");
    expect(sql).toContain("NULLIF(b.experiment_id, '')");
    expect(query.mock.calls[0]?.[1]).toContain("run-1");
  });

  it("queries dataset-run and experiment dimensions for scores", async () => {
    const query = vi.fn().mockResolvedValue([]);

    await executeDorisAnalyticsQuery({
      executor: { query },
      projectId: "project-1",
      version: "v2",
      query: {
        ...base,
        view: "scores-numeric",
        dimensions: [
          { field: "datasetRunId" },
          { field: "experimentName" },
          { field: "experimentId" },
        ],
        metrics: [{ measure: "value", aggregation: "avg" }],
        filters: [
          {
            type: "stringOptions",
            column: "datasetRunId",
            operator: "any of",
            value: ["run-1"],
          },
        ],
        timeDimension: null,
        chartConfig: { type: "TABLE", row_limit: 100 },
        orderBy: [{ field: "avg_value", direction: "desc" }],
      },
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("NULLIF(b.dataset_run_id, '')");
    expect(sql).toContain("o.experiment_name");
    expect(sql).toContain("o.experiment_id");
    expect(query.mock.calls[0]?.[1]).toContain("run-1");
  });

  it.each([
    ["root", "ASC", 1],
    ["first", "ASC", 1],
    ["last", "DESC", 1],
    ["nthFromStart", "ASC", 3],
    ["nthFromEnd", "DESC", 3],
  ] as const)(
    "ranks the %s observation position before aggregation",
    async (key, direction, position) => {
      const query = vi.fn().mockResolvedValue([]);

      await executeDorisAnalyticsQuery({
        executor: { query },
        projectId: "project-1",
        version: "v2",
        query: {
          ...base,
          view: "observations",
          dimensions: [{ field: "name" }],
          metrics: [{ measure: "count", aggregation: "count" }],
          filters: [
            {
              type: "stringOptions",
              column: "environment",
              operator: "any of",
              value: ["production"],
            },
            {
              type: "positionInTrace",
              column: "startTime",
              operator: "=",
              key,
              ...(key === "nthFromStart" || key === "nthFromEnd"
                ? { value: position }
                : {}),
            },
          ],
          timeDimension: null,
        },
      });

      const sql = String(query.mock.calls[0]?.[0]);
      expect(sql).toContain("DENSE_RANK() OVER");
      expect(sql).toContain("PARTITION BY b.project_id, b.trace_id");
      expect(sql).toContain(`ORDER BY b.event_time ${direction}`);
      expect(sql).toContain("WHERE _position_rank = ?");
      expect(query.mock.calls[0]?.[1]).toContain(position);
    },
  );

  it("keeps exploded dimension values on one observation position", async () => {
    const query = vi.fn().mockResolvedValue([]);

    await executeDorisAnalyticsQuery({
      executor: { query },
      projectId: "project-1",
      version: "v2",
      query: {
        ...base,
        view: "observations",
        dimensions: [{ field: "toolNames" }],
        metrics: [{ measure: "count", aggregation: "count" }],
        filters: [
          {
            type: "positionInTrace",
            column: "startTime",
            operator: "=",
            key: "first",
          },
        ],
        timeDimension: null,
      },
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("DENSE_RANK() OVER");
    expect(sql).toMatch(/SELECT\s+b\.\*,\s+tool_name,/);
  });

  it("adds dynamic-key expansion when costType is filtered but not selected", async () => {
    const query = vi.fn().mockResolvedValue([]);

    await executeDorisAnalyticsQuery({
      executor: { query },
      projectId: "project-1",
      version: "v2",
      query: {
        ...base,
        view: "observations",
        dimensions: [],
        metrics: [{ measure: "count", aggregation: "count" }],
        filters: [
          {
            type: "string",
            column: "costType",
            operator: "=",
            value: "input",
          },
        ],
        timeDimension: null,
      },
    });

    expect(String(query.mock.calls[0]?.[0])).toContain(
      "LATERAL VIEW EXPLODE(JSON_KEYS(b.cost_details))",
    );
  });

  it.each([
    ["stringObject", "CAST(ELEMENT_AT(b.metadata, ?) AS STRING)", "eu"],
    ["numberObject", "CAST(ELEMENT_AT(b.metadata, ?) AS DOUBLE)", 0.75],
    ["booleanObject", "CAST(ELEMENT_AT(b.metadata, ?) AS BOOLEAN)", true],
    [
      "categoryOptions",
      "CAST(ELEMENT_AT(b.metadata, ?) AS STRING)",
      ["a", "b"],
    ],
  ] as const)(
    "keeps %s metadata extraction typed and binds the object key",
    async (type, expectedExpression, value) => {
      const query = vi.fn().mockResolvedValue([]);
      const filter =
        type === "categoryOptions"
          ? {
              type,
              column: "metadata",
              key: 'nested."quoted"',
              operator: "any of" as const,
              value: [...value],
            }
          : {
              type,
              column: "metadata",
              key: 'nested."quoted"',
              operator: "=" as const,
              value,
            };

      await executeDorisAnalyticsQuery({
        executor: { query },
        projectId: "project-1",
        version: "v2",
        query: {
          ...base,
          view: "observations",
          dimensions: [],
          metrics: [{ measure: "count", aggregation: "count" }],
          filters: [filter as QueryType["filters"][number]],
          timeDimension: null,
        },
      });

      const sql = String(query.mock.calls[0]?.[0]);
      expect(sql).toContain(expectedExpression);
      expect(sql).not.toContain('nested."quoted"');
      expect(query.mock.calls[0]?.[1]).toContain('nested."quoted"');
    },
  );

  it.each([
    [
      "arrayOptions on a scalar string dimension",
      "observations",
      {
        type: "arrayOptions",
        column: "name",
        operator: "any of",
        value: ["checkout"],
      },
    ],
    [
      "number on a scalar string dimension",
      "observations",
      { type: "number", column: "name", operator: ">", value: 1 },
    ],
    [
      "string on a numeric dimension",
      "scores-numeric",
      { type: "string", column: "value", operator: "=", value: "1" },
    ],
    [
      "string on the time dimension",
      "observations",
      {
        type: "string",
        column: "start_time",
        operator: "=",
        value: "2026-07-17",
      },
    ],
    [
      "stringOptions on an array dimension",
      "observations",
      {
        type: "stringOptions",
        column: "tags",
        operator: "any of",
        value: ["production"],
      },
    ],
  ] as const)(
    "rejects incompatible filter type: %s",
    async (_, view, filter) => {
      const query = vi.fn().mockResolvedValue([]);

      await expect(
        executeDorisAnalyticsQuery({
          executor: { query },
          projectId: "project-1",
          version: "v2",
          query: {
            ...base,
            view,
            dimensions: [],
            metrics: [{ measure: "count", aggregation: "count" }],
            filters: [filter as QueryType["filters"][number]],
            timeDimension: null,
          },
        }),
      ).rejects.toThrow("Invalid Doris analytics filter");
      expect(query).not.toHaveBeenCalled();
    },
  );

  it("rejects analytics queries that exceed the Doris resource budget", async () => {
    const query = vi.fn().mockResolvedValue([]);
    const filters = Array.from({ length: 101 }, (_, index) => ({
      type: "string" as const,
      column: "name",
      operator: "=" as const,
      value: `name-${index}`,
    }));

    await expect(
      executeDorisAnalyticsQuery({
        executor: { query },
        projectId: "project-1",
        version: "v2",
        query: {
          ...base,
          view: "observations",
          dimensions: [],
          metrics: [{ measure: "count", aggregation: "count" }],
          filters,
          timeDimension: null,
        },
      }),
    ).rejects.toThrow("too many filters");

    await expect(
      executeDorisAnalyticsQuery({
        executor: { query },
        projectId: "project-1",
        version: "v2",
        query: {
          ...base,
          fromTimestamp: "2025-07-16T00:00:00.000Z",
          view: "observations",
          dimensions: [],
          metrics: [{ measure: "count", aggregation: "count" }],
          timeDimension: null,
        },
      }),
    ).rejects.toThrow("time range exceeds");
    expect(query).not.toHaveBeenCalled();
  });

  it("uses explicit null placement and selected aliases as stable order ties", async () => {
    const query = vi.fn().mockResolvedValue([]);

    await executeDorisAnalyticsQuery({
      executor: { query },
      projectId: "project-1",
      version: "v2",
      query: {
        ...base,
        view: "observations",
        dimensions: [{ field: "environment" }, { field: "name" }],
        metrics: [{ measure: "count", aggregation: "count" }],
        timeDimension: null,
        orderBy: [{ field: "count_count", direction: "desc" }],
      },
    });

    expect(String(query.mock.calls[0]?.[0])).toContain(
      "ORDER BY count_count IS NULL ASC, count_count DESC, `environment` IS NULL ASC, `environment` ASC, `name` IS NULL ASC, `name` ASC",
    );
  });

  it("forwards cancellation to the Doris executor", async () => {
    const controller = new AbortController();
    const query = vi.fn().mockResolvedValue([]);
    controller.abort(new Error("cancelled by caller"));

    await executeDorisAnalyticsQuery({
      executor: { query },
      projectId: "project-1",
      version: "v2",
      signal: controller.signal,
      query: {
        ...base,
        view: "observations",
        dimensions: [],
        metrics: [{ measure: "count", aggregation: "count" }],
        timeDimension: null,
      },
    });

    expect(query.mock.calls[0]?.[2]).toEqual({ signal: controller.signal });
  });
});
