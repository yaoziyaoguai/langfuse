import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { getViewDeclaration } from "../../../dataModel";
import {
  getValidAggregationsForMeasureType,
  type QueryType,
} from "../../../types";
import { DorisPoCMysqlClient } from "../../../../../server/doris-poc/mysqlClient";
import {
  assertOwnedDorisTestDatabase,
  parseDorisTestNamespace,
} from "../../../../../server/doris/testDatabase";
import type { DorisQueryExecutor } from "../../../../../server/doris/client";
import { executeDorisAnalyticsQuery } from "./DorisAnalyticsQueryEngine";

const enabled = process.env.DORIS_POC_ENABLED === "1";
const describeDoris = enabled ? describe : describe.skip;
const testNamespace = enabled ? parseDorisTestNamespace() : null;

describeDoris("Doris analytics query engine integration", () => {
  let db: DorisPoCMysqlClient;
  let executor: DorisQueryExecutor;

  beforeAll(async () => {
    if (!testNamespace) throw new Error("Doris test namespace is required");
    db = new DorisPoCMysqlClient({
      host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
      port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
      user: process.env.DORIS_POC_USER ?? "root",
      password: process.env.DORIS_POC_PASSWORD ?? "",
      database: testNamespace.database,
    });
    await assertOwnedDorisTestDatabase(db, testNamespace);
    executor = {
      query: async <T extends object>(
        sql: string,
        params?: readonly unknown[],
      ) => (await db.query(sql, params)) as readonly T[],
    };
    const insertEvent = (
      spanId: string,
      start: string,
      end: string,
      completionStart: string,
      parent: string | null,
      metadata: Readonly<Record<string, unknown>>,
    ) =>
      db.execute(
        `INSERT INTO events_current
          (project_id, partition_date, trace_id, span_id, parent_span_id,
           is_app_root, version_token, type, environment, name, user_id,
           session_id, trace_name, start_time, end_time, completion_start_time,
           created_at, updated_at, source, ingestion_sdk_name,
           ingestion_sdk_version, tags, metadata, usage_details, cost_details,
           tool_definitions, tool_calls, tool_call_names, total_input_tokens,
           total_output_tokens, total_cost, provided_model_name)
         VALUES ('analytics-engine-project', '2026-07-17', 'trace-1', ?, ?,
           ?, 1000, 'GENERATION', 'production', ?, 'user-1',
           'session-1', 'checkout', ?, ?, ?, ?, ?, 'api', 'js', '5.0.0',
           ARRAY('prod'), CAST(? AS VARIANT),
           CAST('{"input":10,"output":5,"total":15}' AS VARIANT),
           CAST('{"input":0.1,"output":0.025,"total":0.125}' AS VARIANT),
           CAST('{"search":"{}","lookup":"{}"}' AS VARIANT), ARRAY('search'), ARRAY('search'),
           10, 5, 0.125, 'gpt-4')`,
        [
          spanId,
          parent,
          parent === null,
          spanId,
          start,
          end,
          completionStart,
          start,
          start,
          JSON.stringify(metadata),
        ],
      );
    await insertEvent(
      "root",
      "2026-07-17 10:00:00.000000",
      "2026-07-17 10:00:01.000000",
      "2026-07-17 10:00:00.100000",
      null,
      { region: "eu", quality: 0.8, approved: true, category: "a" },
    );
    await insertEvent(
      "child",
      "2026-07-17 10:00:02.000000",
      "2026-07-17 10:00:03.000000",
      "2026-07-17 10:00:02.100000",
      "root",
      { region: "eu", quality: 0.4, approved: false, category: "b" },
    );
    await db.execute(
      `INSERT INTO scores_current
        (project_id, score_date, score_id, version_token, trace_id,
         observation_id, name, source, data_type, value, environment,
         metadata, timestamp, created_at, updated_at)
       VALUES ('analytics-engine-project', '2026-07-17', 'score-1', 1000,
         'trace-1', 'child', 'quality', 'API', 'NUMERIC', 0.9,
         'production', CAST('{"region":"eu"}' AS VARIANT),
         '2026-07-17 10:00:04.000000', '2026-07-17 10:00:04.000000',
         '2026-07-17 10:00:04.000000')`,
    );
  }, 60_000);

  afterAll(async () => {
    await db?.end();
  });

  it("executes observation, trace, and score metrics without ClickHouse", async () => {
    const scope = {
      executor,
      projectId: "analytics-engine-project",
      fromTimestamp: "2026-07-17T00:00:00.000Z",
      toTimestamp: "2026-07-18T00:00:00.000Z",
    } as const;

    const observations = await executeDorisAnalyticsQuery({
      executor: scope.executor,
      projectId: scope.projectId,
      version: "v2",
      query: {
        view: "observations",
        dimensions: [{ field: "providedModelName" }],
        metrics: [
          { measure: "count", aggregation: "count" },
          { measure: "totalCost", aggregation: "sum" },
          { measure: "latency", aggregation: "p95" },
        ],
        filters: [],
        timeDimension: { granularity: "day" },
        fromTimestamp: scope.fromTimestamp,
        toTimestamp: scope.toTimestamp,
        orderBy: null,
      },
    });
    expect(observations).toEqual([
      expect.objectContaining({
        providedModelName: "gpt-4",
        count_count: 2,
        sum_totalCost: 0.25,
        p95_latency: 1000,
      }),
    ]);

    await expect(
      executeDorisAnalyticsQuery({
        executor: scope.executor,
        projectId: scope.projectId,
        version: "v1",
        query: {
          view: "traces",
          dimensions: [],
          metrics: [{ measure: "count", aggregation: "count" }],
          filters: [],
          timeDimension: null,
          fromTimestamp: scope.fromTimestamp,
          toTimestamp: scope.toTimestamp,
          orderBy: null,
        },
      }),
    ).resolves.toEqual([{ count_count: 1 }]);

    await expect(
      executeDorisAnalyticsQuery({
        executor: scope.executor,
        projectId: scope.projectId,
        version: "v2",
        query: {
          view: "scores-numeric",
          dimensions: [{ field: "name" }],
          metrics: [{ measure: "value", aggregation: "avg" }],
          filters: [],
          timeDimension: null,
          fromTimestamp: scope.fromTimestamp,
          toTimestamp: scope.toTimestamp,
          orderBy: null,
        },
      }),
    ).resolves.toEqual([{ name: "quality", avg_value: 0.9 }]);

    await expect(
      executeDorisAnalyticsQuery({
        executor: scope.executor,
        projectId: scope.projectId,
        version: "v2",
        query: {
          view: "observations",
          dimensions: [{ field: "costType" }],
          metrics: [{ measure: "costByType", aggregation: "sum" }],
          filters: [],
          timeDimension: null,
          fromTimestamp: scope.fromTimestamp,
          toTimestamp: scope.toTimestamp,
          orderBy: null,
        },
      }),
    ).resolves.toEqual([
      { costType: "total", sum_costByType: 0.25 },
      { costType: "input", sum_costByType: 0.2 },
      { costType: "output", sum_costByType: 0.05 },
    ]);

    await expect(
      executeDorisAnalyticsQuery({
        executor: scope.executor,
        projectId: scope.projectId,
        version: "v1",
        query: {
          view: "traces",
          dimensions: [],
          metrics: [
            { measure: "observationsCount", aggregation: "sum" },
            { measure: "scoresCount", aggregation: "sum" },
            { measure: "totalTokens", aggregation: "sum" },
            { measure: "totalCost", aggregation: "sum" },
          ],
          filters: [],
          timeDimension: null,
          fromTimestamp: scope.fromTimestamp,
          toTimestamp: scope.toTimestamp,
          orderBy: null,
        },
      }),
    ).resolves.toEqual([
      {
        sum_observationsCount: 2,
        sum_scoresCount: 1,
        sum_totalTokens: 30,
        sum_totalCost: 0.25,
      },
    ]);

    const histogramRows = await executeDorisAnalyticsQuery({
      executor: scope.executor,
      projectId: scope.projectId,
      version: "v2",
      query: {
        view: "scores-numeric",
        dimensions: [],
        metrics: [{ measure: "value", aggregation: "histogram" }],
        filters: [],
        timeDimension: null,
        fromTimestamp: scope.fromTimestamp,
        toTimestamp: scope.toTimestamp,
        orderBy: null,
        chartConfig: { type: "HISTOGRAM", bins: 10 },
      },
    });
    expect(histogramRows).toEqual([{ histogram_value: [[0.9, 0.9, 1]] }]);

    for (const view of ["traces", "observations", "scores-numeric"] as const) {
      await expect(
        executeDorisAnalyticsQuery({
          executor: scope.executor,
          projectId: scope.projectId,
          version: view === "traces" ? "v1" : "v2",
          query: {
            view,
            dimensions: [],
            metrics: [{ measure: "count", aggregation: "count" }],
            filters: [
              {
                type: "stringObject",
                column: "metadata",
                key: "region",
                operator: "=",
                value: "eu",
              },
            ],
            timeDimension: null,
            fromTimestamp: scope.fromTimestamp,
            toTimestamp: scope.toTimestamp,
            orderBy: null,
          },
        }),
      ).resolves.toEqual([{ count_count: view === "observations" ? 2 : 1 }]);
    }
  }, 60_000);

  it("executes every R1A dimension, measure, and aggregation against Doris", async () => {
    const deferred = new Set([
      "datasetRunId",
      "experimentName",
      "experimentDatasetId",
      "experimentId",
    ]);
    const baseQuery = {
      filters: [],
      timeDimension: null,
      fromTimestamp: "2026-07-17T00:00:00.000Z",
      toTimestamp: "2026-07-18T00:00:00.000Z",
      orderBy: null,
    } satisfies Pick<
      QueryType,
      "filters" | "timeDimension" | "fromTimestamp" | "toTimestamp" | "orderBy"
    >;

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
            executor,
            projectId: "analytics-engine-project",
            version,
            query: {
              ...baseQuery,
              view,
              dimensions: [{ field }],
              metrics: [{ measure: "count", aggregation: "count" }],
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
            try {
              await executeDorisAnalyticsQuery({
                executor,
                projectId: "analytics-engine-project",
                version,
                query: {
                  ...baseQuery,
                  view,
                  dimensions: [],
                  metrics: [{ measure, aggregation }],
                },
              });
            } catch (error) {
              throw new Error(
                `Failed Doris analytics matrix case ${version}/${view}/${measure}/${aggregation}`,
                { cause: error },
              );
            }
          }
        }
      }
    }
  }, 120_000);

  it.each([
    [
      {
        type: "numberObject",
        column: "metadata",
        key: "quality",
        operator: ">",
        value: 0.5,
      },
      "root",
    ],
    [
      {
        type: "booleanObject",
        column: "metadata",
        key: "approved",
        operator: "=",
        value: true,
      },
      "root",
    ],
    [
      {
        type: "categoryOptions",
        column: "metadata",
        key: "category",
        operator: "any of",
        value: ["b"] as string[],
      },
      "child",
    ],
  ] as const)("executes typed metadata filter %#", async (filter, id) => {
    await expect(
      executeDorisAnalyticsQuery({
        executor,
        projectId: "analytics-engine-project",
        version: "v2",
        query: {
          view: "observations",
          dimensions: [{ field: "id" }],
          metrics: [{ measure: "count", aggregation: "count" }],
          filters: [filter],
          timeDimension: null,
          fromTimestamp: "2026-07-17T00:00:00.000Z",
          toTimestamp: "2026-07-18T00:00:00.000Z",
          chartConfig: { type: "TABLE", row_limit: 10 },
          orderBy: [{ field: "count_count", direction: "desc" }],
        },
      }),
    ).resolves.toEqual([{ id, count_count: 1 }]);
  });

  it.each([
    ["first", "root"],
    ["last", "child"],
  ] as const)("executes the %s position before grouping", async (key, id) => {
    await expect(
      executeDorisAnalyticsQuery({
        executor,
        projectId: "analytics-engine-project",
        version: "v2",
        query: {
          view: "observations",
          dimensions: [{ field: "id" }],
          metrics: [{ measure: "count", aggregation: "count" }],
          filters: [
            {
              type: "positionInTrace",
              column: "startTime",
              operator: "=",
              key,
            },
          ],
          timeDimension: null,
          fromTimestamp: "2026-07-17T00:00:00.000Z",
          toTimestamp: "2026-07-18T00:00:00.000Z",
          chartConfig: { type: "TABLE", row_limit: 10 },
          orderBy: [{ field: "count_count", direction: "desc" }],
        },
      }),
    ).resolves.toEqual([{ id, count_count: 1 }]);
  });

  it("keeps all exploded values for the selected observation position", async () => {
    await expect(
      executeDorisAnalyticsQuery({
        executor,
        projectId: "analytics-engine-project",
        version: "v2",
        query: {
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
          fromTimestamp: "2026-07-17T00:00:00.000Z",
          toTimestamp: "2026-07-18T00:00:00.000Z",
          chartConfig: { type: "TABLE", row_limit: 10 },
          orderBy: [{ field: "toolNames", direction: "asc" }],
        },
      }),
    ).resolves.toEqual([
      { toolNames: "lookup", count_count: 1 },
      { toolNames: "search", count_count: 1 },
    ]);
  });

  it("filters a dynamic cost key without selecting it", async () => {
    await expect(
      executeDorisAnalyticsQuery({
        executor,
        projectId: "analytics-engine-project",
        version: "v2",
        query: {
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
          fromTimestamp: "2026-07-17T00:00:00.000Z",
          toTimestamp: "2026-07-18T00:00:00.000Z",
          orderBy: null,
        },
      }),
    ).resolves.toEqual([{ count_count: 2 }]);
  });
});
