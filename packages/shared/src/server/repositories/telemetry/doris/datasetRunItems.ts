import type { DatasetRunItemDomain } from "../../../../domain/dataset-run-items";
import { InvalidRequestError } from "../../../../errors";
import type { OrderByState } from "../../../../interfaces/orderBy";
import type { FilterState } from "../../../../types";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import { compileDorisEventFilters } from "../../../queries/doris-sql/filterCompiler";
import {
  assertDorisFilterBudget,
  type LogicalEventFilter,
} from "../../../queries/logical/filterPlan";
import Decimal from "decimal.js";

type BoundParameters = {
  readonly params: unknown[];
  bind(value: unknown): string;
};

type DatasetRunItemRow = Record<string, unknown> & {
  readonly project_id: string;
  readonly run_item_id: string;
  readonly dataset_run_id: string;
  readonly dataset_item_id: string;
  readonly dataset_id: string;
  readonly trace_id: string;
};

export type DatasetRunItemsQuery = {
  readonly projectId: string;
  readonly datasetId?: string;
  readonly filters: FilterState;
  readonly orderBy?: OrderByState | readonly OrderByState[];
  readonly limit?: number;
  readonly offset?: number;
  readonly includeIO?: boolean;
};

export type DatasetRunItemComparisonQuery = {
  readonly projectId: string;
  readonly datasetId?: string;
  readonly runIds: readonly string[];
  readonly filtersByRun: readonly {
    readonly runId: string;
    readonly filters: FilterState;
  }[];
  readonly limit?: number;
  readonly offset?: number;
};

export type DorisDatasetRunRow = {
  readonly id: string;
  readonly name: string;
  readonly projectId: string;
  readonly datasetId: string;
  readonly createdAt: Date;
  readonly description: string;
  readonly metadata: string;
};

export type DorisDatasetRunMetrics = {
  readonly id: string;
  readonly name: string;
  readonly projectId: string;
  readonly datasetId: string;
  readonly countRunItems: number;
  readonly avgTotalCost: Decimal;
  readonly totalCost: Decimal;
  readonly avgLatency: number;
  readonly aggScoresAvg: Array<[string, number]>;
  readonly aggScoreCategories: string[];
  readonly aggScoreBooleans: string[];
};

const MAX_PAGE_SIZE = 1_000;

const DATASET_RUN_ITEM_PROJECTION = `
  dri.project_id,
  dri.run_item_id,
  dri.dataset_run_id,
  dri.dataset_item_id,
  dri.dataset_id,
  dri.trace_id,
  dri.observation_id,
  dri.\`error\`,
  dri.created_at,
  dri.updated_at,
  dri.dataset_run_name,
  dri.dataset_run_description,
  COALESCE(dri.dataset_run_metadata_json, CAST(dri.dataset_run_metadata AS STRING)) AS dataset_run_metadata,
  dri.dataset_run_created_at,
  dri.dataset_item_version,
  dri.dataset_item_input,
  dri.dataset_item_expected_output,
  COALESCE(dri.dataset_item_metadata_json, CAST(dri.dataset_item_metadata AS STRING)) AS dataset_item_metadata`;

const DATASET_RUN_ITEM_PROJECTION_WITHOUT_IO = `
  dri.project_id,
  dri.run_item_id,
  dri.dataset_run_id,
  dri.dataset_item_id,
  dri.dataset_id,
  dri.trace_id,
  dri.observation_id,
  dri.\`error\`,
  dri.created_at,
  dri.updated_at,
  dri.dataset_run_name,
  dri.dataset_run_description,
  dri.dataset_run_created_at,
  dri.dataset_item_version`;

const FILTER_COLUMNS: Readonly<Record<string, string>> = {
  id: "dri.run_item_id",
  datasetRunId: "dri.dataset_run_id",
  datasetItemId: "dri.dataset_item_id",
  datasetId: "dri.dataset_id",
  traceId: "dri.trace_id",
  observationId: "dri.observation_id",
  createdAt: "dri.created_at",
  updatedAt: "dri.updated_at",
  error: "dri.`error`",
};

const ORDER_COLUMNS: Readonly<Record<string, string>> = {
  id: "dri.run_item_id",
  datasetRunId: "dri.dataset_run_id",
  datasetItemId: "dri.dataset_item_id",
  datasetId: "dri.dataset_id",
  createdAt: "dri.created_at",
  updatedAt: "dri.updated_at",
};

function dateTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value !== "string") {
    throw new TypeError("Doris returned an invalid dataset-run-item timestamp");
  }
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (!Number.isFinite(parsed.getTime())) {
    throw new TypeError("Doris returned an invalid dataset-run-item timestamp");
  }
  return parsed;
}

function nullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

function finiteNumber(value: unknown, label: string): number {
  const parsed = Number(value ?? 0);
  if (!Number.isFinite(parsed)) {
    throw new TypeError(`Doris returned an invalid ${label}`);
  }
  return parsed;
}

function jsonValue(value: unknown): unknown {
  return parseJsonIfString(value);
}

function metadataValue(value: unknown): Record<string, unknown> {
  const parsed = jsonValue(value);
  return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
    ? (parsed as Record<string, unknown>)
    : {};
}

function decodeDatasetRunItem<WithIO extends boolean>(
  row: DatasetRunItemRow,
  includeIO: WithIO,
): DatasetRunItemDomain<WithIO> {
  const base = {
    id: String(row.run_item_id),
    projectId: String(row.project_id),
    datasetRunId: String(row.dataset_run_id),
    datasetItemId: String(row.dataset_item_id),
    datasetId: String(row.dataset_id),
    traceId: String(row.trace_id),
    observationId: nullableString(row.observation_id),
    error: nullableString(row.error),
    createdAt: dateTime(row.created_at),
    updatedAt: dateTime(row.updated_at),
    datasetRunName: String(row.dataset_run_name),
    datasetRunDescription: nullableString(row.dataset_run_description),
    datasetRunCreatedAt: dateTime(row.dataset_run_created_at),
    datasetItemVersion:
      row.dataset_item_version === null ||
      row.dataset_item_version === undefined
        ? null
        : dateTime(row.dataset_item_version),
  };
  if (!includeIO) return base as DatasetRunItemDomain<WithIO>;
  return {
    ...base,
    datasetRunMetadata: metadataValue(row.dataset_run_metadata),
    datasetItemInput: jsonValue(row.dataset_item_input),
    datasetItemExpectedOutput: jsonValue(row.dataset_item_expected_output),
    datasetItemMetadata: metadataValue(row.dataset_item_metadata),
  } as DatasetRunItemDomain<WithIO>;
}

function visibilityJoins(): string {
  return `FROM dataset_run_items_current dri
LEFT JOIN dataset_tombstones dataset_deletion
  ON dataset_deletion.project_id = dri.project_id
 AND dataset_deletion.dataset_id = dri.dataset_id
LEFT JOIN dataset_run_tombstones run_deletion
  ON run_deletion.project_id = dri.project_id
 AND run_deletion.dataset_run_id = dri.dataset_run_id
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = dri.project_id`;
}

function visibilityPredicates(): readonly string[] {
  return [
    "project_deletion.project_id IS NULL",
    "(dataset_deletion.dataset_id IS NULL OR dataset_deletion.deletion_generation <= dri.dataset_deletion_generation)",
    "(run_deletion.dataset_run_id IS NULL OR run_deletion.deletion_generation <= dri.run_deletion_generation)",
  ];
}

function scoreFilterPredicate(
  filter: FilterState[number],
  bound: BoundParameters,
): string | null {
  const scoreColumn =
    /^(agg|obs|trace)_score(s_avg|_categories|_booleans)$/.exec(filter.column);
  if (!scoreColumn) {
    return null;
  }
  if (
    filter.type !== "numberObject" &&
    filter.type !== "categoryOptions" &&
    filter.type !== "booleanObject"
  ) {
    throw new InvalidRequestError(
      `Unsupported Doris dataset-run-item score filter: ${filter.type}`,
    );
  }
  if (!filter.key) {
    throw new InvalidRequestError(
      "Doris dataset-run-item score filter requires a score name",
    );
  }

  const scope = [
    "score_filter.project_id = dri.project_id",
    "score_filter.trace_id = dri.trace_id",
    "score_filter.`name` = " + bound.bind(filter.key),
    ...(scoreColumn[1] === "trace"
      ? ["score_filter.observation_id IS NULL"]
      : scoreColumn[1] === "obs"
        ? [
            `score_filter.observation_id IN (
        SELECT experiment_event.span_id
        FROM events_current experiment_event
        WHERE experiment_event.project_id = dri.project_id
          AND experiment_event.trace_id = dri.trace_id
          AND (
            (dri.observation_id IS NOT NULL AND experiment_event.span_id = dri.observation_id)
            OR (
              dri.observation_id IS NULL
              AND (
                experiment_event.is_app_root = TRUE
                OR experiment_event.parent_span_id IS NULL
                OR experiment_event.parent_span_id = ''
              )
            )
          )
      )`,
          ]
        : []),
    `NOT EXISTS (
      SELECT 1
      FROM trace_tombstones score_trace_deletion
      WHERE score_trace_deletion.project_id = score_filter.project_id
        AND score_trace_deletion.trace_id = score_filter.trace_id
    )`,
  ];

  if (filter.type === "numberObject") {
    if (!["=", "<>", "<", "<=", ">", ">="].includes(filter.operator)) {
      throw new InvalidRequestError(
        "Unsupported Doris dataset-run-item numeric score operator",
      );
    }
    return `EXISTS (
  SELECT 1
  FROM scores_current score_filter
  WHERE ${scope.join("\n    AND ")}
    AND score_filter.data_type IN ('NUMERIC', 'BOOLEAN')
  GROUP BY score_filter.project_id, score_filter.trace_id, score_filter.\`name\`
  HAVING AVG(COALESCE(
    score_filter.\`value\`,
    IF(score_filter.boolean_value, 1, 0)
  )) ${filter.operator} ${bound.bind(filter.value)}
)`;
  }

  if (filter.type === "categoryOptions") {
    if (filter.value.length === 0) {
      return filter.operator === "none of" ? "TRUE" : "FALSE";
    }
    const values = filter.value.map((value) => bound.bind(value)).join(", ");
    const exists = `EXISTS (
  SELECT 1
  FROM scores_current score_filter
  WHERE ${scope.join("\n    AND ")}
    AND score_filter.data_type = 'CATEGORICAL'
    AND score_filter.string_value IN (${values})
)`;
    return filter.operator === "none of" ? `NOT ${exists}` : exists;
  }

  const expected = bound.bind(filter.value);
  const exists = `EXISTS (
  SELECT 1
  FROM scores_current score_filter
  WHERE ${scope.join("\n    AND ")}
    AND score_filter.data_type = 'BOOLEAN'
    AND score_filter.boolean_value = ${expected}
)`;
  return filter.operator === "<>" ? `NOT ${exists}` : exists;
}

function filterPredicates(
  filters: FilterState,
  bound: BoundParameters,
): readonly string[] {
  assertDorisFilterBudget(filters, "Doris dataset-run-item query");
  const basicPlans: LogicalEventFilter[] = [];
  const scorePredicates: string[] = [];
  for (const filter of filters) {
    const scorePredicate = scoreFilterPredicate(filter, bound);
    if (scorePredicate) {
      scorePredicates.push(scorePredicate);
      continue;
    }
    if (filter.type === "positionInTrace") {
      throw new InvalidRequestError(
        "Unsupported Doris dataset-run-item position filter",
      );
    }
    const expression = FILTER_COLUMNS[filter.column];
    if (!expression) {
      throw new InvalidRequestError(
        `Unsupported Doris dataset-run-item filter: ${filter.column}`,
      );
    }
    if (
      filter.type === "stringObject" ||
      filter.type === "numberObject" ||
      filter.type === "booleanObject" ||
      filter.type === "categoryOptions"
    ) {
      throw new InvalidRequestError(
        `Unsupported Doris dataset-run-item object filter: ${filter.column}`,
      );
    }
    basicPlans.push({ filter, expression });
  }
  return [...compileDorisEventFilters(basicPlans, bound), ...scorePredicates];
}

function orderByClause(
  orderBy: OrderByState | readonly OrderByState[] | undefined,
): string {
  const orders = orderBy
    ? Array.isArray(orderBy)
      ? orderBy
      : [orderBy]
    : [{ column: "createdAt", order: "DESC" as const }];
  const compiled = orders.map(({ column, order }) => {
    const expression = ORDER_COLUMNS[column];
    if (!expression) {
      throw new InvalidRequestError(
        `Unsupported Doris dataset-run-item order column: ${column}`,
      );
    }
    return `${expression} ${order}`;
  });
  compiled.push("dri.run_item_id DESC");
  return compiled.join(", ");
}

function createBoundParameters(): BoundParameters {
  const params: unknown[] = [];
  return {
    params,
    bind(value) {
      params.push(value);
      return "?";
    },
  };
}

function compileScope(input: {
  readonly projectId: string;
  readonly datasetId?: string;
  readonly filters: FilterState;
}): {
  readonly fromSql: string;
  readonly whereSql: string;
  readonly params: readonly unknown[];
} {
  if (!input.projectId) {
    throw new InvalidRequestError("Invalid Doris dataset-run-item scope");
  }
  const bound = createBoundParameters();
  const predicates = [
    `dri.project_id = ${bound.bind(input.projectId)}`,
    ...(input.datasetId
      ? [`dri.dataset_id = ${bound.bind(input.datasetId)}`]
      : []),
    ...visibilityPredicates(),
    ...filterPredicates(input.filters, bound),
  ];
  return {
    fromSql: visibilityJoins(),
    whereSql: predicates.join("\n  AND "),
    params: bound.params,
  };
}

function compileComparisonQuery(
  input: DatasetRunItemComparisonQuery,
  select: "rows" | "count",
): { readonly sql: string; readonly params: readonly unknown[] } {
  if (
    input.runIds.length === 0 ||
    new Set(input.runIds).size !== input.runIds.length ||
    input.filtersByRun.some(({ runId }) => !input.runIds.includes(runId)) ||
    (input.limit !== undefined &&
      (!Number.isSafeInteger(input.limit) ||
        input.limit < 1 ||
        input.limit > MAX_PAGE_SIZE)) ||
    !Number.isSafeInteger(input.offset ?? 0) ||
    (input.offset ?? 0) < 0
  ) {
    throw new InvalidRequestError(
      "Invalid Doris dataset-run-item comparison scope",
    );
  }
  const bound = createBoundParameters();
  const basePredicates = [
    `dri.project_id = ${bound.bind(input.projectId)}`,
    ...(input.datasetId
      ? [`dri.dataset_id = ${bound.bind(input.datasetId)}`]
      : []),
    ...visibilityPredicates(),
  ];
  const runPredicates = input.runIds.map((runId) => {
    const runFilters =
      input.filtersByRun.find((candidate) => candidate.runId === runId)
        ?.filters ?? [];
    return `(dri.dataset_run_id = ${bound.bind(runId)}${
      runFilters.length > 0
        ? ` AND ${filterPredicates(runFilters, bound).join(" AND ")}`
        : ""
    })`;
  });
  const having =
    input.filtersByRun.length > 0
      ? `\nHAVING COUNT(DISTINCT dri.dataset_run_id) = ${bound.bind(input.runIds.length)}`
      : "";
  const grouped = `SELECT dri.dataset_item_id
${visibilityJoins()}
WHERE ${basePredicates.join("\n  AND ")}
  AND (${runPredicates.join("\n    OR ")})
GROUP BY dri.dataset_item_id${having}`;
  if (select === "count") {
    return {
      sql: `SELECT COUNT(*) AS count
FROM (
${grouped}
) qualified_items`,
      params: bound.params,
    };
  }
  const limitSql =
    input.limit === undefined
      ? ""
      : `\nLIMIT ${bound.bind(input.limit)}${
          input.offset ? ` OFFSET ${bound.bind(input.offset)}` : ""
        }`;
  return {
    sql: `${grouped}
ORDER BY dri.dataset_item_id ASC${limitSql}`,
    params: bound.params,
  };
}

export class DorisDatasetRunItemsRepository {
  constructor(
    private readonly dependencies: {
      readonly query: DorisQueryExecutor["query"];
      readonly streamQuery?: NonNullable<DorisQueryExecutor["streamQuery"]>;
    },
  ) {}

  async list(
    input: DatasetRunItemsQuery & { readonly includeIO?: true },
  ): Promise<DatasetRunItemDomain[]>;
  async list(
    input: DatasetRunItemsQuery & { readonly includeIO: false },
  ): Promise<DatasetRunItemDomain<false>[]>;
  async list(
    input: DatasetRunItemsQuery,
  ): Promise<DatasetRunItemDomain[] | DatasetRunItemDomain<false>[]> {
    if (
      (input.limit !== undefined &&
        (!Number.isSafeInteger(input.limit) ||
          input.limit < 1 ||
          input.limit > MAX_PAGE_SIZE)) ||
      !Number.isSafeInteger(input.offset ?? 0) ||
      (input.offset ?? 0) < 0 ||
      (input.offset !== undefined &&
        input.offset > 0 &&
        input.limit === undefined)
    ) {
      throw new InvalidRequestError("Invalid Doris dataset-run-item page");
    }
    const scope = compileScope(input);
    const orderBy = orderByClause(input.orderBy);
    const limitSql =
      input.limit === undefined
        ? ""
        : `\nLIMIT ?${input.offset ? " OFFSET ?" : ""}`;
    const rows = await this.dependencies.query<DatasetRunItemRow>(
      `SELECT ${input.includeIO === false ? DATASET_RUN_ITEM_PROJECTION_WITHOUT_IO : DATASET_RUN_ITEM_PROJECTION}
${scope.fromSql}
WHERE ${scope.whereSql}
ORDER BY ${orderBy}${limitSql}`,
      [
        ...scope.params,
        ...(input.limit === undefined ? [] : [input.limit]),
        ...(input.offset ? [input.offset] : []),
      ],
    );
    return rows.map((row) =>
      decodeDatasetRunItem(row, input.includeIO !== false),
    );
  }

  async count(input: {
    readonly projectId: string;
    readonly datasetId?: string;
    readonly filters: FilterState;
  }): Promise<number> {
    const scope = compileScope(input);
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      `SELECT COUNT(*) AS count
${scope.fromSql}
WHERE ${scope.whereSql}`,
      scope.params,
    );
    const count = Number(rows[0]?.count ?? 0);
    if (!Number.isFinite(count)) {
      throw new TypeError("Doris returned an invalid dataset-run-item count");
    }
    return count;
  }

  async existingDatasetItemIds(input: {
    readonly projectId: string;
    readonly datasetId: string;
    readonly datasetRunId: string;
  }): Promise<Set<string>> {
    const scope = compileScope({
      projectId: input.projectId,
      datasetId: input.datasetId,
      filters: [
        {
          type: "string",
          column: "datasetRunId",
          operator: "=",
          value: input.datasetRunId,
        },
      ],
    });
    const rows = await this.dependencies.query<{
      readonly dataset_item_id: string;
    }>(
      `SELECT DISTINCT dri.dataset_item_id
${scope.fromSql}
WHERE ${scope.whereSql}
ORDER BY dri.dataset_item_id ASC`,
      scope.params,
    );
    return new Set(rows.map(({ dataset_item_id }) => String(dataset_item_id)));
  }

  async qualifyingDatasetItemIds(
    input: DatasetRunItemComparisonQuery,
  ): Promise<string[]> {
    if (input.runIds.length === 0) return [];
    const compiled = compileComparisonQuery(input, "rows");
    const rows = await this.dependencies.query<{
      readonly dataset_item_id: string;
    }>(compiled.sql, compiled.params);
    return rows.map(({ dataset_item_id }) => String(dataset_item_id));
  }

  async countQualifyingDatasetItemIds(
    input: Omit<DatasetRunItemComparisonQuery, "limit" | "offset">,
  ): Promise<number> {
    if (input.runIds.length === 0) return 0;
    const compiled = compileComparisonQuery(input, "count");
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      compiled.sql,
      compiled.params,
    );
    const count = Number(rows[0]?.count ?? 0);
    if (!Number.isFinite(count)) {
      throw new TypeError(
        "Doris returned an invalid dataset-run-item comparison count",
      );
    }
    return count;
  }

  async listWithoutIOByItemIds(input: {
    readonly projectId: string;
    readonly datasetId: string;
    readonly runIds: readonly string[];
    readonly datasetItemIds: readonly string[];
  }): Promise<DatasetRunItemDomain<false>[]> {
    if (input.runIds.length === 0 || input.datasetItemIds.length === 0) {
      return [];
    }
    return this.list({
      projectId: input.projectId,
      datasetId: input.datasetId,
      filters: [
        {
          type: "stringOptions",
          column: "datasetRunId",
          operator: "any of",
          value: [...input.runIds],
        },
        {
          type: "stringOptions",
          column: "datasetItemId",
          operator: "any of",
          value: [...input.datasetItemIds],
        },
      ],
      includeIO: false,
    });
  }

  async datasetItemIdsByTraceId(input: {
    readonly projectId: string;
    readonly traceId: string;
    readonly filters: FilterState;
  }): Promise<
    {
      readonly id: string;
      readonly datasetId: string;
      readonly observationId: string | null;
    }[]
  > {
    const scope = compileScope({
      projectId: input.projectId,
      filters: [
        ...input.filters,
        {
          type: "string",
          column: "traceId",
          operator: "=",
          value: input.traceId,
        },
      ],
    });
    const rows = await this.dependencies.query<{
      readonly dataset_item_id: string;
      readonly dataset_id: string;
      readonly observation_id: string | null;
    }>(
      `SELECT DISTINCT
  dri.dataset_item_id,
  dri.dataset_id,
  dri.observation_id
${scope.fromSql}
WHERE ${scope.whereSql}
ORDER BY dri.dataset_id ASC, dri.dataset_item_id ASC`,
      scope.params,
    );
    return rows.map((row) => ({
      id: String(row.dataset_item_id),
      datasetId: String(row.dataset_id),
      observationId: nullableString(row.observation_id),
    }));
  }

  async runRows(input: {
    readonly projectId: string;
    readonly datasetId: string;
    readonly filters: FilterState;
    readonly limit?: number;
    readonly offset?: number;
  }): Promise<DorisDatasetRunRow[]> {
    if (
      (input.limit !== undefined &&
        (!Number.isSafeInteger(input.limit) ||
          input.limit < 1 ||
          input.limit > MAX_PAGE_SIZE)) ||
      !Number.isSafeInteger(input.offset ?? 0) ||
      (input.offset ?? 0) < 0
    ) {
      throw new InvalidRequestError("Invalid Doris dataset-run page");
    }
    const scope = compileScope(input);
    const limitSql =
      input.limit === undefined
        ? ""
        : `\nLIMIT ?${input.offset ? " OFFSET ?" : ""}`;
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `SELECT
  dri.dataset_run_id,
  ANY_VALUE(dri.dataset_run_name) AS dataset_run_name,
  dri.project_id,
  dri.dataset_id,
  MAX(dri.dataset_run_created_at) AS dataset_run_created_at,
  ANY_VALUE(dri.dataset_run_description) AS dataset_run_description,
  ANY_VALUE(COALESCE(dri.dataset_run_metadata_json, CAST(dri.dataset_run_metadata AS STRING))) AS dataset_run_metadata
${scope.fromSql}
WHERE ${scope.whereSql}
GROUP BY dri.project_id, dri.dataset_id, dri.dataset_run_id
ORDER BY dataset_run_created_at DESC, dri.dataset_run_id ASC${limitSql}`,
      [
        ...scope.params,
        ...(input.limit === undefined ? [] : [input.limit]),
        ...(input.offset ? [input.offset] : []),
      ],
    );
    return rows.map((row) => ({
      id: String(row.dataset_run_id),
      name: String(row.dataset_run_name),
      projectId: String(row.project_id),
      datasetId: String(row.dataset_id),
      createdAt: dateTime(row.dataset_run_created_at),
      description: nullableString(row.dataset_run_description) ?? "",
      metadata: nullableString(row.dataset_run_metadata) ?? "{}",
    }));
  }

  async runCount(input: {
    readonly projectId: string;
    readonly datasetId: string;
    readonly filters: FilterState;
  }): Promise<number> {
    const scope = compileScope(input);
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      `SELECT COUNT(DISTINCT dri.dataset_run_id) AS count
${scope.fromSql}
WHERE ${scope.whereSql}`,
      scope.params,
    );
    return finiteNumber(rows[0]?.count, "dataset-run count");
  }

  async runMetrics(input: {
    readonly projectId: string;
    readonly datasetId?: string;
    readonly runIds?: readonly string[];
    readonly filters: FilterState;
  }): Promise<DorisDatasetRunMetrics[]> {
    const scope = compileScope({
      projectId: input.projectId,
      datasetId: input.datasetId,
      filters: [
        ...input.filters,
        ...(input.runIds && input.runIds.length > 0
          ? [
              {
                type: "stringOptions" as const,
                column: "datasetRunId",
                operator: "any of" as const,
                value: [...input.runIds],
              },
            ]
          : []),
      ],
    });
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `WITH visible_run_items AS (
  SELECT dri.*
  ${scope.fromSql}
  WHERE ${scope.whereSql}
),
visible_events AS (
  SELECT event_row.*
  FROM events_current event_row
  LEFT JOIN trace_tombstones event_trace_deletion
    ON event_trace_deletion.project_id = event_row.project_id
   AND event_trace_deletion.trace_id = event_row.trace_id
  LEFT JOIN project_tombstones event_project_deletion
    ON event_project_deletion.project_id = event_row.project_id
  WHERE event_row.project_id IN (
    SELECT DISTINCT project_id FROM visible_run_items
  )
    AND event_row.trace_id IN (
      SELECT DISTINCT trace_id FROM visible_run_items
    )
    AND event_trace_deletion.trace_id IS NULL
    AND event_project_deletion.project_id IS NULL
),
trace_metrics AS (
  SELECT
    project_id,
    trace_id,
    MICROSECONDS_DIFF(MAX(end_time), MIN(start_time)) / 1000000.0 AS latency_seconds,
    SUM(total_cost) AS total_cost
  FROM visible_events
  GROUP BY project_id, trace_id
)
SELECT
  dri.dataset_run_id,
  ANY_VALUE(dri.dataset_run_name) AS dataset_run_name,
  dri.project_id,
  dri.dataset_id,
  COUNT(DISTINCT dri.run_item_id) AS count_run_items,
  AVG(
    CASE
      WHEN dri.observation_id IS NULL THEN trace_metric.latency_seconds
      ELSE MICROSECONDS_DIFF(
        observation_event.end_time,
        observation_event.start_time
      ) / 1000000.0
    END
  ) AS avg_latency_seconds,
  AVG(
    CASE
      WHEN dri.observation_id IS NULL THEN trace_metric.total_cost
      ELSE observation_event.total_cost
    END
  ) AS avg_total_cost,
  SUM(
    CASE
      WHEN dri.observation_id IS NULL THEN trace_metric.total_cost
      ELSE observation_event.total_cost
    END
  ) AS total_cost
FROM visible_run_items dri
LEFT JOIN trace_metrics trace_metric
  ON trace_metric.project_id = dri.project_id
 AND trace_metric.trace_id = dri.trace_id
LEFT JOIN visible_events observation_event
  ON observation_event.project_id = dri.project_id
 AND observation_event.trace_id = dri.trace_id
 AND observation_event.span_id = dri.observation_id
GROUP BY dri.project_id, dri.dataset_id, dri.dataset_run_id
ORDER BY MAX(dri.dataset_run_created_at) DESC, dri.dataset_run_id ASC`,
      scope.params,
    );
    return rows.map((row) => ({
      id: String(row.dataset_run_id),
      name: String(row.dataset_run_name),
      projectId: String(row.project_id),
      datasetId: String(row.dataset_id),
      countRunItems: finiteNumber(
        row.count_run_items,
        "dataset-run item count",
      ),
      avgTotalCost: new Decimal(row.avg_total_cost?.toString() ?? 0),
      totalCost: new Decimal(row.total_cost?.toString() ?? 0),
      avgLatency: finiteNumber(
        row.avg_latency_seconds,
        "dataset-run average latency",
      ),
      aggScoresAvg: [],
      aggScoreCategories: [],
      aggScoreBooleans: [],
    }));
  }
}
