import { InvalidRequestError } from "../../../../errors";
import type { OrderByState } from "../../../../interfaces/orderBy";
import type { EventsTableFilterState, FilterState } from "../../../../types";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import { compileDorisEventFilters } from "../../../queries/doris-sql/filterCompiler";
import {
  assertDorisFilterBudget,
  type LogicalEventFilter,
} from "../../../queries/logical/filterPlan";
import {
  DorisDatasetRunItemsRepository,
  type DorisDatasetRunMetrics,
} from "./datasetRunItems";
import type { ScoreRecordReadType } from "../../definitions";

type BoundParameters = {
  readonly params: unknown[];
  bind(value: unknown): string;
};

export type DorisExperimentSummary = {
  readonly id: string;
  readonly name: string;
  readonly description: string | null;
  readonly datasetId: string;
  readonly itemCount: number;
  readonly errorCount: number;
  readonly prompts: Array<[string, number | null]>;
  readonly metadata: Record<string, string>;
  readonly startTime: Date;
};

export type DorisExperimentItemData = {
  readonly experimentId: string;
  readonly level: string;
  readonly startTime: Date;
  readonly totalCost: number | null;
  readonly latencyMs: number | null;
  readonly observationId: string;
  readonly traceId: string;
};

export type DorisGroupedExperimentItem = {
  readonly itemId: string;
  readonly experiments: DorisExperimentItemData[];
};

export type DorisExperimentItemBatchIO = {
  readonly itemId: string;
  readonly input: string | null;
  readonly expectedOutput: string | null;
  readonly outputs: Array<{
    readonly experimentId: string;
    readonly output: string | null;
  }>;
};

export type DorisPublicExperimentSummary = {
  readonly experiment_id: string;
  readonly experiment_name: string;
  readonly experiment_description: string | null;
  readonly experiment_dataset_id: string | null;
  readonly start_time: string;
  readonly end_time: string;
  readonly cursor_time: string;
  readonly cursor_trace_id: string;
  readonly cursor_span_id: string;
  readonly item_count: number;
  readonly experiment_metadata?: Record<string, unknown> | null;
  readonly scores?: ScoreRecordReadType[];
};

export type DorisPublicExperimentItem = {
  readonly id: string;
  readonly trace_id: string;
  readonly start_time: string;
  readonly end_time: string | null;
  readonly level: "DEBUG" | "DEFAULT" | "WARNING" | "ERROR";
  readonly environment: string;
  readonly experiment_id: string;
  readonly experiment_name: string | null;
  readonly experiment_item_id: string;
  readonly experiment_dataset_id?: string | null;
  readonly experiment_item_version?: string | null;
  readonly input?: unknown;
  readonly output?: unknown;
  readonly experiment_item_expected_output?: unknown;
  readonly metadata?: Record<string, unknown> | null;
  readonly experiment_item_metadata?: Record<string, unknown> | null;
  readonly experiment_metadata?: Record<string, unknown> | null;
  readonly experiment_description?: string | null;
  readonly scores?: ScoreRecordReadType[];
};

export type DorisExperimentScoreFilterOptions = {
  readonly numeric: string[];
  readonly boolean: string[];
  readonly categorical: Array<{ label: string; values: string[] }>;
  readonly scoreColumns: Array<{
    name: string;
    dataType: "NUMERIC" | "BOOLEAN" | "CATEGORICAL";
    source: string;
  }>;
};

type ExperimentItemQuery = {
  readonly projectId: string;
  readonly baseExperimentId?: string;
  readonly compExperimentIds: readonly string[];
  readonly filtersByExperiment: readonly {
    readonly experimentId: string;
    readonly filters: FilterState;
  }[];
  readonly requireBaselinePresence?: boolean;
  readonly limit?: number;
  readonly offset?: number;
};

const IO_TRUNCATE_LENGTH = 1_000;
const MAX_PAGE_SIZE = 1_000;

const RUN_METADATA =
  "COALESCE(dri.dataset_run_metadata_json, CAST(dri.dataset_run_metadata AS STRING))";
const EXPERIMENT_NAME = `COALESCE(
  NULLIF(JSON_EXTRACT_STRING(${RUN_METADATA}, '$.experiment_name'), ''),
  dri.dataset_run_name
)`;

const EXPERIMENT_FILTER_COLUMNS: Readonly<Record<string, string>> = {
  id: "dri.dataset_run_id",
  name: EXPERIMENT_NAME,
  description: "dri.dataset_run_description",
  experimentDatasetId: "dri.dataset_id",
  datasetId: "dri.dataset_id",
  startTime: "dri.dataset_run_created_at",
};

const EXPERIMENT_ORDER_COLUMNS: Readonly<Record<string, string>> = {
  id: "experiment_id",
  name: "experiment_name",
  startTime: "start_time",
  itemCount: "item_count",
  errorCount: "error_count",
};

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

function dateTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value !== "string") {
    throw new TypeError("Doris returned an invalid experiment timestamp");
  }
  const result = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (!Number.isFinite(result.getTime())) {
    throw new TypeError("Doris returned an invalid experiment timestamp");
  }
  return result;
}

function numberValue(value: unknown): number {
  const result = Number(value ?? 0);
  if (!Number.isFinite(result)) {
    throw new TypeError("Doris returned an invalid experiment number");
  }
  return result;
}

function nullableNumber(value: unknown): number | null {
  return value === null || value === undefined ? null : numberValue(value);
}

function nullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

function metadataValue(value: unknown): Record<string, unknown> {
  const parsed = parseJsonIfString(value);
  return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
    ? (parsed as Record<string, unknown>)
    : {};
}

function stringMetadataValue(value: unknown): Record<string, string> {
  return Object.fromEntries(
    Object.entries(metadataValue(value)).map(([key, item]) => [
      key,
      typeof item === "string" ? item : JSON.stringify(item),
    ]),
  );
}

function promptsValue(value: unknown): Array<[string, number | null]> {
  const parsed = parseJsonIfString(value);
  if (!Array.isArray(parsed)) return [];
  return parsed.flatMap((item): Array<[string, number | null]> => {
    if (!Array.isArray(item) || typeof item[0] !== "string") return [];
    const version =
      item[1] === null || item[1] === undefined ? null : Number(item[1]);
    return [
      [item[0], version !== null && Number.isFinite(version) ? version : null],
    ];
  });
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

function experimentEventJoinPredicate(
  eventAlias: string,
  runItemAlias = "dri",
): string {
  return `${eventAlias}.project_id = ${runItemAlias}.project_id
 AND ${eventAlias}.trace_id = ${runItemAlias}.trace_id
 AND (
   (${runItemAlias}.observation_id IS NOT NULL AND ${eventAlias}.span_id = ${runItemAlias}.observation_id)
   OR (
     ${runItemAlias}.observation_id IS NULL
     AND (
       ${eventAlias}.is_app_root = TRUE
       OR ${eventAlias}.parent_span_id IS NULL
       OR ${eventAlias}.parent_span_id = ''
     )
   )
 )`;
}

function experimentEventPreferenceOrder(
  eventAlias: string,
  runItemAlias = "dri",
): string {
  return `IF(
    ${runItemAlias}.observation_id IS NOT NULL,
    0,
    IF(${eventAlias}.is_app_root = TRUE, 1, 2)
  ) ASC,
  ${eventAlias}.start_time DESC,
  ${eventAlias}.span_id DESC`;
}

const PUBLIC_EXPERIMENT_FILTER_COLUMNS: Readonly<Record<string, string>> = {
  experimentId: "dri.dataset_run_id",
  experimentName: EXPERIMENT_NAME,
  experimentDatasetId: "dri.dataset_id",
  experimentItemId: "dri.dataset_item_id",
};

function compilePublicExperimentFilters(
  filters: EventsTableFilterState,
  bound: BoundParameters,
): readonly string[] {
  assertDorisFilterBudget(filters, "Doris public experiment query");
  const plans = filters.map((filter): LogicalEventFilter => {
    if (
      filter.type === "positionInTrace" ||
      filter.type === "stringObject" ||
      filter.type === "numberObject" ||
      filter.type === "booleanObject" ||
      filter.type === "categoryOptions"
    ) {
      throw new InvalidRequestError(
        "Unsupported Doris public experiment filter",
      );
    }
    const expression = PUBLIC_EXPERIMENT_FILTER_COLUMNS[filter.column];
    if (!expression) {
      throw new InvalidRequestError(
        `Unsupported Doris public experiment filter: ${filter.column}`,
      );
    }
    return { filter, expression };
  });
  return compileDorisEventFilters(plans, bound);
}

function utcDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function exclusivePartitionTo(value: Date): string {
  const lastIncluded = new Date(value.getTime() - 1);
  lastIncluded.setUTCDate(lastIncluded.getUTCDate() + 1);
  return utcDate(lastIncluded);
}

function compileExperimentFilters(
  filters: FilterState,
  bound: BoundParameters,
): readonly string[] {
  assertDorisFilterBudget(filters, "Doris experiment query");
  const plans: LogicalEventFilter[] = [];
  const scorePredicates: string[] = [];
  for (const filter of filters) {
    if (filter.type === "positionInTrace") {
      throw new InvalidRequestError("Unsupported Doris experiment filter");
    }
    const scorePredicate = experimentScoreFilterPredicate(filter, bound);
    if (scorePredicate) {
      scorePredicates.push(scorePredicate);
      continue;
    }
    if (filter.column === "metadata") {
      if (
        filter.type !== "stringObject" &&
        filter.type !== "numberObject" &&
        filter.type !== "booleanObject" &&
        filter.type !== "categoryOptions"
      ) {
        throw new InvalidRequestError(
          "Unsupported Doris experiment metadata filter",
        );
      }
      const castType =
        filter.type === "numberObject"
          ? "DOUBLE"
          : filter.type === "booleanObject"
            ? "BOOLEAN"
            : "STRING";
      plans.push({
        filter,
        expression: `CAST(JSON_EXTRACT(${RUN_METADATA}, CONCAT('$.', ?)) AS ${castType})`,
        objectKey: filter.key,
      });
      continue;
    }
    const expression = EXPERIMENT_FILTER_COLUMNS[filter.column];
    if (!expression) {
      throw new InvalidRequestError(
        `Unsupported Doris experiment filter: ${filter.column}`,
      );
    }
    if (
      filter.type === "stringObject" ||
      filter.type === "numberObject" ||
      filter.type === "booleanObject" ||
      filter.type === "categoryOptions"
    ) {
      throw new InvalidRequestError(
        `Unsupported Doris experiment object filter: ${filter.column}`,
      );
    }
    plans.push({ filter, expression });
  }
  return [...compileDorisEventFilters(plans, bound), ...scorePredicates];
}

function experimentScoreFilterPredicate(
  filter: FilterState[number],
  bound: BoundParameters,
): string | null {
  const scoreColumn = /^(obs|trace)_score(s_avg|_categories|_booleans)$/.exec(
    filter.column,
  );
  if (!scoreColumn) return null;
  if (
    filter.type !== "numberObject" &&
    filter.type !== "categoryOptions" &&
    filter.type !== "booleanObject"
  ) {
    throw new InvalidRequestError(
      `Unsupported Doris experiment score filter: ${filter.type}`,
    );
  }
  if (!filter.key) {
    throw new InvalidRequestError(
      "Doris experiment score filter requires a score name",
    );
  }

  const scoreRelation = `(
    SELECT DISTINCT
      score_dri.project_id,
      score_dri.dataset_run_id,
      score_dri.trace_id${
        scoreColumn[1] === "obs"
          ? `,
      experiment_event.span_id AS observation_id`
          : ""
      }
    FROM dataset_run_items_current score_dri
    ${
      scoreColumn[1] === "obs"
        ? `INNER JOIN events_current experiment_event
      ON ${experimentEventJoinPredicate("experiment_event", "score_dri")}`
        : ""
    }
    LEFT JOIN dataset_tombstones score_dataset_deletion
      ON score_dataset_deletion.project_id = score_dri.project_id
     AND score_dataset_deletion.dataset_id = score_dri.dataset_id
    LEFT JOIN dataset_run_tombstones score_run_deletion
      ON score_run_deletion.project_id = score_dri.project_id
     AND score_run_deletion.dataset_run_id = score_dri.dataset_run_id
    WHERE (
      score_dataset_deletion.dataset_id IS NULL
      OR score_dataset_deletion.deletion_generation <= score_dri.dataset_deletion_generation
    )
      AND (
        score_run_deletion.dataset_run_id IS NULL
        OR score_run_deletion.deletion_generation <= score_dri.run_deletion_generation
      )
  ) score_relation`;
  const scoreSource = `scores_current score_filter
  INNER JOIN ${scoreRelation}
    ON score_relation.project_id = score_filter.project_id
   AND score_relation.trace_id = score_filter.trace_id${
     scoreColumn[1] === "obs"
       ? "\n   AND score_relation.observation_id = score_filter.observation_id"
       : ""
   }
  LEFT JOIN trace_tombstones score_trace_deletion
    ON score_trace_deletion.project_id = score_filter.project_id
   AND score_trace_deletion.trace_id = score_filter.trace_id`;
  const scope = [
    "score_relation.project_id = dri.project_id",
    "score_relation.dataset_run_id = dri.dataset_run_id",
    `score_filter.\`name\` = ${bound.bind(filter.key)}`,
    ...(scoreColumn[1] === "trace"
      ? ["score_filter.observation_id IS NULL"]
      : []),
    "score_trace_deletion.trace_id IS NULL",
  ];

  if (filter.type === "numberObject") {
    if (!["=", "<>", "<", "<=", ">", ">="].includes(filter.operator)) {
      throw new InvalidRequestError(
        "Unsupported Doris experiment numeric score operator",
      );
    }
    return `EXISTS (
  SELECT 1
  FROM ${scoreSource}
  WHERE ${scope.join("\n    AND ")}
    AND score_filter.data_type IN ('NUMERIC', 'BOOLEAN')
  GROUP BY score_relation.project_id, score_relation.dataset_run_id, score_filter.\`name\`
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
  FROM ${scoreSource}
  WHERE ${scope.join("\n    AND ")}
    AND score_filter.data_type = 'CATEGORICAL'
    AND score_filter.string_value IN (${values})
)`;
    return filter.operator === "none of" ? `NOT ${exists}` : exists;
  }

  const exists = `EXISTS (
  SELECT 1
  FROM ${scoreSource}
  WHERE ${scope.join("\n    AND ")}
    AND score_filter.data_type = 'BOOLEAN'
    AND score_filter.boolean_value = ${bound.bind(filter.value)}
)`;
  return filter.operator === "<>" ? `NOT ${exists}` : exists;
}

function itemExperimentIds(input: ExperimentItemQuery): string[] {
  return [
    ...(input.baseExperimentId ? [input.baseExperimentId] : []),
    ...input.compExperimentIds,
  ].filter((id, index, ids) => ids.indexOf(id) === index);
}

function placeholders(values: readonly unknown[]): string {
  return values.map(() => "?").join(", ");
}

function publicExperimentFilters(input: {
  readonly advancedFilters?: EventsTableFilterState;
  readonly simpleFilters: readonly {
    readonly column: string;
    readonly values?: readonly string[];
  }[];
}): EventsTableFilterState {
  const columnAliases: Readonly<Record<string, string>> = {
    id: "experimentId",
    name: "experimentName",
    datasetId: "experimentDatasetId",
  };
  const filters = (input.advancedFilters ?? []).map((filter) => ({
    ...filter,
    column: columnAliases[filter.column] ?? filter.column,
  })) as EventsTableFilterState;
  for (const { column, values } of input.simpleFilters) {
    if (!values || values.length === 0) continue;
    filters.push({
      type: "stringOptions",
      column,
      operator: "any of",
      value: [...values],
    });
  }
  return filters;
}

function publicCursorPredicate(
  columns: readonly string[],
  values: readonly unknown[],
): { readonly sql: string; readonly params: readonly unknown[] } {
  const clauses: string[] = [];
  const params: unknown[] = [];
  for (let index = 0; index < columns.length; index += 1) {
    clauses.push(
      `(${columns
        .slice(0, index)
        .map((column) => `${column} = ?`)
        .concat(`${columns[index]} < ?`)
        .join(" AND ")})`,
    );
    params.push(...values.slice(0, index + 1));
  }
  return {
    sql: `(${clauses.join(" OR ")})`,
    params,
  };
}

export class DorisExperimentsRepository {
  private readonly datasetRunItems: DorisDatasetRunItemsRepository;

  constructor(
    private readonly dependencies: {
      readonly query: DorisQueryExecutor["query"];
      readonly streamQuery?: NonNullable<DorisQueryExecutor["streamQuery"]>;
    },
  ) {
    this.datasetRunItems = new DorisDatasetRunItemsRepository(dependencies);
  }

  async list(input: {
    readonly projectId: string;
    readonly filters: FilterState;
    readonly orderBy?: OrderByState;
    readonly limit?: number;
    readonly page?: number;
  }): Promise<DorisExperimentSummary[]> {
    if (
      (input.limit !== undefined &&
        (!Number.isSafeInteger(input.limit) ||
          input.limit < 1 ||
          input.limit > MAX_PAGE_SIZE)) ||
      !Number.isSafeInteger(input.page ?? 0) ||
      (input.page ?? 0) < 0
    ) {
      throw new InvalidRequestError("Invalid Doris experiment page");
    }
    const bound = createBoundParameters();
    const predicates = [
      `dri.project_id = ${bound.bind(input.projectId)}`,
      ...visibilityPredicates(),
      ...compileExperimentFilters(input.filters, bound),
    ];
    const orderColumn = input.orderBy?.column
      ? EXPERIMENT_ORDER_COLUMNS[input.orderBy.column]
      : "start_time";
    if (!orderColumn) {
      throw new InvalidRequestError("Unsupported Doris experiment order");
    }
    const order = input.orderBy?.order ?? "DESC";
    const limitSql =
      input.limit === undefined
        ? ""
        : `\nLIMIT ${bound.bind(input.limit)}${
            input.page ? ` OFFSET ${bound.bind(input.page * input.limit)}` : ""
          }`;
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `SELECT
  dri.dataset_run_id AS experiment_id,
  ANY_VALUE(${EXPERIMENT_NAME}) AS experiment_name,
  ANY_VALUE(dri.dataset_run_description) AS experiment_description,
  dri.dataset_id AS experiment_dataset_id,
  MIN(dri.dataset_run_created_at) AS start_time,
  COUNT(DISTINCT dri.dataset_item_id) AS item_count,
  COUNT(DISTINCT IF(dri.\`error\` IS NOT NULL, dri.dataset_item_id, NULL)) AS error_count,
  CAST(ARRAY_AGG(DISTINCT ARRAY(
    event_row.prompt_name,
    CAST(event_row.prompt_version AS STRING)
  )) AS STRING) AS prompts,
  ANY_VALUE(${RUN_METADATA}) AS experiment_metadata
${visibilityJoins()}
LEFT JOIN events_current event_row
  ON ${experimentEventJoinPredicate("event_row")}
WHERE ${predicates.join("\n  AND ")}
GROUP BY dri.project_id, dri.dataset_id, dri.dataset_run_id
ORDER BY ${orderColumn} ${order}, experiment_id ASC${limitSql}`,
      bound.params,
    );
    return rows.map((row) => ({
      id: String(row.experiment_id),
      name: String(row.experiment_name),
      description: nullableString(row.experiment_description),
      datasetId: String(row.experiment_dataset_id),
      startTime: dateTime(row.start_time),
      itemCount: numberValue(row.item_count),
      errorCount: numberValue(row.error_count),
      prompts: promptsValue(row.prompts).filter(([name]) => name.length > 0),
      metadata: stringMetadataValue(row.experiment_metadata),
    }));
  }

  async count(input: {
    readonly projectId: string;
    readonly filters: FilterState;
  }): Promise<number> {
    const bound = createBoundParameters();
    const predicates = [
      `dri.project_id = ${bound.bind(input.projectId)}`,
      ...visibilityPredicates(),
      ...compileExperimentFilters(input.filters, bound),
    ];
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      `SELECT COUNT(DISTINCT dri.dataset_run_id) AS count
${visibilityJoins()}
WHERE ${predicates.join("\n  AND ")}`,
      bound.params,
    );
    return numberValue(rows[0]?.count);
  }

  async metrics(input: {
    readonly projectId: string;
    readonly experimentIds: readonly string[];
  }): Promise<
    Array<{
      readonly id: string;
      totalCost: number | null;
      latencyAvg: number | null;
    }>
  > {
    const metrics: DorisDatasetRunMetrics[] =
      await this.datasetRunItems.runMetrics({
        projectId: input.projectId,
        runIds: input.experimentIds,
        filters: [],
      });
    return metrics.map((metric) => ({
      id: metric.id,
      totalCost: metric.totalCost.toNumber(),
      latencyAvg: metric.avgLatency,
    }));
  }

  private async qualifiedItemIds(
    input: ExperimentItemQuery,
  ): Promise<string[]> {
    const allExperimentIds = itemExperimentIds(input);
    if (allExperimentIds.length === 0) return [];
    const filteredExperimentIds = input.filtersByExperiment.map(
      ({ experimentId }) => experimentId,
    );
    const qualificationIds = [
      ...(input.requireBaselinePresence && input.baseExperimentId
        ? [input.baseExperimentId]
        : []),
      ...(filteredExperimentIds.length > 0
        ? filteredExperimentIds
        : allExperimentIds),
    ].filter((id, index, ids) => ids.indexOf(id) === index);
    return this.datasetRunItems.qualifyingDatasetItemIds({
      projectId: input.projectId,
      runIds: qualificationIds,
      filtersByRun: input.filtersByExperiment.map(
        ({ experimentId, filters }) => ({
          runId: experimentId,
          filters,
        }),
      ),
      limit: input.limit,
      offset: input.offset,
    });
  }

  async items(
    input: ExperimentItemQuery,
  ): Promise<DorisGroupedExperimentItem[]> {
    const itemIds = await this.qualifiedItemIds(input);
    if (itemIds.length === 0) return [];
    const experimentIds = itemExperimentIds(input);
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `SELECT
  item_id,
  experiment_id,
  \`level\`,
  start_time,
  total_cost,
  latency_ms,
  observation_id,
  trace_id
FROM (
  SELECT
    dri.dataset_item_id AS item_id,
    dri.dataset_run_id AS experiment_id,
    event_row.\`level\` AS \`level\`,
    event_row.start_time AS start_time,
    event_row.total_cost AS total_cost,
    IF(
      event_row.end_time IS NULL,
      NULL,
      MICROSECONDS_DIFF(event_row.end_time, event_row.start_time) / 1000.0
    ) AS latency_ms,
    event_row.span_id AS observation_id,
    event_row.trace_id AS trace_id,
    ROW_NUMBER() OVER (
      PARTITION BY dri.dataset_item_id, dri.dataset_run_id
      ORDER BY
        IF(dri.observation_id IS NOT NULL, 0, IF(event_row.is_app_root = TRUE, 1, 2)) ASC,
        event_row.start_time DESC,
        event_row.span_id DESC
    ) AS item_rank
  ${visibilityJoins()}
  INNER JOIN events_current event_row
    ON event_row.project_id = dri.project_id
   AND event_row.trace_id = dri.trace_id
   AND (
     (dri.observation_id IS NOT NULL AND event_row.span_id = dri.observation_id)
     OR (
       dri.observation_id IS NULL
       AND (
         event_row.is_app_root = TRUE
         OR event_row.parent_span_id IS NULL
         OR event_row.parent_span_id = ''
       )
     )
   )
  LEFT JOIN trace_tombstones trace_deletion
    ON trace_deletion.project_id = event_row.project_id
   AND trace_deletion.trace_id = event_row.trace_id
  WHERE dri.project_id = ?
    AND dri.dataset_run_id IN (${placeholders(experimentIds)})
    AND dri.dataset_item_id IN (${placeholders(itemIds)})
    AND ${visibilityPredicates().join("\n    AND ")}
    AND trace_deletion.trace_id IS NULL
) ranked_items
WHERE item_rank = 1
ORDER BY item_id ASC, experiment_id ASC`,
      [input.projectId, ...experimentIds, ...itemIds],
    );
    const items = new Map<string, DorisExperimentItemData[]>(
      itemIds.map((itemId) => [itemId, []]),
    );
    for (const row of rows) {
      items.get(String(row.item_id))?.push({
        experimentId: String(row.experiment_id),
        level: String(row.level),
        startTime: dateTime(row.start_time),
        totalCost: nullableNumber(row.total_cost),
        latencyMs: nullableNumber(row.latency_ms),
        observationId: String(row.observation_id),
        traceId: String(row.trace_id),
      });
    }
    return itemIds.map((itemId) => ({
      itemId,
      experiments: items.get(itemId) ?? [],
    }));
  }

  async itemsCount(
    input: Omit<ExperimentItemQuery, "limit" | "offset">,
  ): Promise<number> {
    const allExperimentIds = itemExperimentIds(input);
    if (allExperimentIds.length === 0) return 0;
    const filteredExperimentIds = input.filtersByExperiment.map(
      ({ experimentId }) => experimentId,
    );
    const qualificationIds = [
      ...(input.requireBaselinePresence && input.baseExperimentId
        ? [input.baseExperimentId]
        : []),
      ...(filteredExperimentIds.length > 0
        ? filteredExperimentIds
        : allExperimentIds),
    ].filter((id, index, ids) => ids.indexOf(id) === index);
    return this.datasetRunItems.countQualifyingDatasetItemIds({
      projectId: input.projectId,
      runIds: qualificationIds,
      filtersByRun: input.filtersByExperiment.map(
        ({ experimentId, filters }) => ({
          runId: experimentId,
          filters,
        }),
      ),
    });
  }

  async batchIO(input: {
    readonly projectId: string;
    readonly itemIds: readonly string[];
    readonly baseExperimentId?: string;
    readonly compExperimentIds: readonly string[];
  }): Promise<DorisExperimentItemBatchIO[]> {
    if (input.itemIds.length === 0) return [];
    const experimentIds = [
      ...(input.baseExperimentId ? [input.baseExperimentId] : []),
      ...input.compExperimentIds,
    ];
    if (experimentIds.length === 0) {
      return input.itemIds.map((itemId) => ({
        itemId,
        input: null,
        expectedOutput: null,
        outputs: [],
      }));
    }
    const runRows = await this.dependencies.query<Record<string, unknown>>(
      `SELECT
  dri.dataset_item_id AS item_id,
  dri.dataset_run_id AS experiment_id,
  LEFT(dri.dataset_item_input, ?) AS input,
  LEFT(dri.dataset_item_expected_output, ?) AS expected_output
${visibilityJoins()}
WHERE dri.project_id = ?
  AND dri.dataset_run_id IN (${placeholders(experimentIds)})
  AND dri.dataset_item_id IN (${placeholders(input.itemIds)})
  AND ${visibilityPredicates().join("\n  AND ")}
ORDER BY dri.dataset_item_id ASC, dri.dataset_run_id ASC`,
      [
        IO_TRUNCATE_LENGTH,
        IO_TRUNCATE_LENGTH,
        input.projectId,
        ...experimentIds,
        ...input.itemIds,
      ],
    );
    const outputRows = await this.dependencies.query<Record<string, unknown>>(
      `SELECT item_id, experiment_id, output
FROM (
  SELECT
    dri.dataset_item_id AS item_id,
    dri.dataset_run_id AS experiment_id,
    LEFT(event_row.output, ?) AS output,
    ROW_NUMBER() OVER (
      PARTITION BY dri.dataset_item_id, dri.dataset_run_id
      ORDER BY ${experimentEventPreferenceOrder("event_row")}
    ) AS item_rank
  ${visibilityJoins()}
  INNER JOIN events_current event_row
    ON ${experimentEventJoinPredicate("event_row")}
  LEFT JOIN trace_tombstones trace_deletion
    ON trace_deletion.project_id = event_row.project_id
   AND trace_deletion.trace_id = event_row.trace_id
  WHERE dri.project_id = ?
    AND dri.dataset_run_id IN (${placeholders(experimentIds)})
    AND dri.dataset_item_id IN (${placeholders(input.itemIds)})
    AND ${visibilityPredicates().join("\n    AND ")}
    AND trace_deletion.trace_id IS NULL
) ranked_outputs
WHERE item_rank = 1`,
      [IO_TRUNCATE_LENGTH, input.projectId, ...experimentIds, ...input.itemIds],
    );
    const base = new Map<
      string,
      { input: string | null; expectedOutput: string | null }
    >();
    for (const row of runRows) {
      const itemId = String(row.item_id);
      const isBaseline =
        input.baseExperimentId !== undefined &&
        row.experiment_id === input.baseExperimentId;
      const existing = base.get(itemId);
      if (!existing || isBaseline) {
        base.set(itemId, {
          input: nullableString(row.input),
          expectedOutput: nullableString(row.expected_output),
        });
      }
    }
    const outputs = new Map<
      string,
      Array<{ experimentId: string; output: string | null }>
    >();
    for (const row of outputRows) {
      const itemId = String(row.item_id);
      outputs.set(itemId, [
        ...(outputs.get(itemId) ?? []),
        {
          experimentId: String(row.experiment_id),
          output: nullableString(row.output),
        },
      ]);
    }
    return input.itemIds.map((itemId) => ({
      itemId,
      input: base.get(itemId)?.input ?? null,
      expectedOutput: base.get(itemId)?.expectedOutput ?? null,
      outputs: outputs.get(itemId) ?? [],
    }));
  }

  async names(input: {
    readonly projectId: string;
  }): Promise<Array<{ experimentName: string; experimentId: string }>> {
    const rows = await this.list({
      projectId: input.projectId,
      filters: [],
      limit: MAX_PAGE_SIZE,
      page: 0,
    });
    const byName = new Map<string, string>();
    for (const experiment of rows) {
      if (experiment.name) byName.set(experiment.name, experiment.id);
    }
    return [...byName].map(([experimentName, experimentId]) => ({
      experimentName,
      experimentId,
    }));
  }

  async scoreFilterOptions(input: {
    readonly projectId: string;
    readonly experimentIds: readonly string[];
    readonly level: "observation" | "trace" | "experiment";
  }): Promise<DorisExperimentScoreFilterOptions> {
    const experimentIds = [...new Set(input.experimentIds)];
    if (experimentIds.length === 0) {
      return {
        numeric: [],
        boolean: [],
        categorical: [],
        scoreColumns: [],
      };
    }
    const experimentIdPlaceholders = placeholders(experimentIds);
    const scoreScope =
      input.level === "experiment"
        ? `s.dataset_run_id IN (${experimentIdPlaceholders})`
        : `EXISTS (
    SELECT 1
    FROM dataset_run_items_current score_dri
    ${
      input.level === "observation"
        ? `INNER JOIN events_current experiment_event
      ON ${experimentEventJoinPredicate("experiment_event", "score_dri")}`
        : ""
    }
    LEFT JOIN dataset_tombstones score_dataset_deletion
      ON score_dataset_deletion.project_id = score_dri.project_id
     AND score_dataset_deletion.dataset_id = score_dri.dataset_id
    LEFT JOIN dataset_run_tombstones score_run_deletion
      ON score_run_deletion.project_id = score_dri.project_id
     AND score_run_deletion.dataset_run_id = score_dri.dataset_run_id
    LEFT JOIN project_tombstones score_run_project_deletion
      ON score_run_project_deletion.project_id = score_dri.project_id
    WHERE score_dri.project_id = s.project_id
      AND score_dri.dataset_run_id IN (${experimentIdPlaceholders})
      AND score_dri.trace_id = s.trace_id
      ${
        input.level === "observation"
          ? "AND experiment_event.span_id = s.observation_id"
          : "AND s.observation_id IS NULL"
      }
      AND score_run_project_deletion.project_id IS NULL
      AND (
        score_dataset_deletion.dataset_id IS NULL
        OR score_dataset_deletion.deletion_generation <= score_dri.dataset_deletion_generation
      )
      AND (
        score_run_deletion.dataset_run_id IS NULL
        OR score_run_deletion.deletion_generation <= score_dri.run_deletion_generation
      )
  )`;
    const rows = await this.dependencies.query<{
      readonly name: string;
      readonly source: string;
      readonly data_type: "NUMERIC" | "BOOLEAN" | "CATEGORICAL";
      readonly categorical_value: string | null;
    }>(
      `SELECT
  s.\`name\` AS name,
  s.\`source\` AS source,
  s.data_type AS data_type,
  s.string_value AS categorical_value
FROM scores_current s
LEFT JOIN trace_tombstones score_trace_deletion
  ON score_trace_deletion.project_id = s.project_id
 AND score_trace_deletion.trace_id = s.trace_id
LEFT JOIN project_tombstones score_project_deletion
  ON score_project_deletion.project_id = s.project_id
WHERE s.project_id = ?
  AND ${scoreScope}
  AND s.data_type IN ('NUMERIC', 'BOOLEAN', 'CATEGORICAL')
  AND score_trace_deletion.trace_id IS NULL
  AND score_project_deletion.project_id IS NULL
GROUP BY s.\`name\`, s.\`source\`, s.data_type, s.string_value
ORDER BY s.\`name\` ASC, s.\`source\` ASC, s.data_type ASC, s.string_value ASC
LIMIT 1000`,
      [input.projectId, ...experimentIds],
    );
    const numeric = new Set<string>();
    const boolean = new Set<string>();
    const categorical = new Map<string, Set<string>>();
    const scoreColumns = new Map<
      string,
      {
        name: string;
        dataType: "NUMERIC" | "BOOLEAN" | "CATEGORICAL";
        source: string;
      }
    >();
    for (const row of rows) {
      scoreColumns.set(`${row.name}\u0000${row.source}\u0000${row.data_type}`, {
        name: row.name,
        dataType: row.data_type,
        source: row.source,
      });
      if (row.data_type === "NUMERIC" || row.data_type === "BOOLEAN") {
        numeric.add(row.name);
      }
      if (row.data_type === "BOOLEAN") {
        boolean.add(row.name);
      }
      if (
        row.data_type === "CATEGORICAL" &&
        row.categorical_value !== null &&
        row.categorical_value !== ""
      ) {
        const values = categorical.get(row.name) ?? new Set<string>();
        if (values.size < 20) values.add(row.categorical_value);
        categorical.set(row.name, values);
      }
    }
    return {
      numeric: [...numeric],
      boolean: [...boolean],
      categorical: [...categorical].map(([label, values]) => ({
        label,
        values: [...values],
      })),
      scoreColumns: [...scoreColumns.values()],
    };
  }

  async publicSummaries(input: {
    readonly projectId: string;
    readonly fromTime?: Date;
    readonly toTime?: Date;
    readonly limit: number;
    readonly id?: readonly string[];
    readonly name?: readonly string[];
    readonly datasetId?: readonly string[];
    readonly advancedFilters?: EventsTableFilterState;
    readonly cursor?: {
      readonly lastTime: string;
      readonly lastTraceId: string;
      readonly lastId: string;
      readonly lastExperimentId: string;
    };
    readonly includeMetadata: boolean;
  }): Promise<DorisPublicExperimentSummary[]> {
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > MAX_PAGE_SIZE
    ) {
      throw new InvalidRequestError("Invalid Doris public experiment page");
    }
    const fromTime = input.fromTime ?? new Date(0);
    const toTime = input.toTime ?? new Date();
    const bound = createBoundParameters();
    const predicates = [
      `dri.project_id = ${bound.bind(input.projectId)}`,
      `e.partition_date >= ${bound.bind(utcDate(fromTime))}`,
      `e.partition_date < ${bound.bind(exclusivePartitionTo(toTime))}`,
      `e.start_time >= ${bound.bind(fromTime)}`,
      `e.start_time < ${bound.bind(toTime)}`,
      ...visibilityPredicates(),
      "trace_deletion.trace_id IS NULL",
      ...compilePublicExperimentFilters(
        publicExperimentFilters({
          advancedFilters: input.advancedFilters,
          simpleFilters: [
            { column: "experimentId", values: input.id },
            { column: "experimentName", values: input.name },
            { column: "experimentDatasetId", values: input.datasetId },
          ],
        }),
        bound,
      ),
    ];
    const cursor = input.cursor
      ? publicCursorPredicate(
          ["cursor_time", "experiment_id", "cursor_span_id"],
          [
            input.cursor.lastTime,
            input.cursor.lastExperimentId,
            input.cursor.lastId,
          ],
        )
      : null;
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `WITH ranked_items AS (
  SELECT
    dri.dataset_item_id,
    dri.dataset_run_id AS experiment_id,
    ${EXPERIMENT_NAME} AS experiment_name,
    dri.dataset_run_description AS experiment_description,
    dri.dataset_id AS experiment_dataset_id,
    ${RUN_METADATA} AS experiment_metadata,
    e.trace_id,
    e.span_id,
    e.start_time,
    e.end_time,
    ROW_NUMBER() OVER (
      PARTITION BY dri.project_id, dri.dataset_run_id, dri.dataset_item_id
      ORDER BY ${experimentEventPreferenceOrder("e")}
    ) AS item_rank
  ${visibilityJoins()}
  INNER JOIN events_current e
    ON ${experimentEventJoinPredicate("e")}
  LEFT JOIN trace_tombstones trace_deletion
    ON trace_deletion.project_id = e.project_id
   AND trace_deletion.trace_id = e.trace_id
  WHERE ${predicates.join("\n    AND ")}
),
visible_items AS (
  SELECT
    *
  FROM ranked_items
  WHERE item_rank = 1
),
ranked_experiments AS (
  SELECT
    visible_items.*,
    ROW_NUMBER() OVER (
      PARTITION BY experiment_id
      ORDER BY start_time DESC, experiment_id DESC, span_id DESC
    ) AS experiment_rank
  FROM visible_items
),
experiment_summaries AS (
  SELECT
    experiment_id,
    MAX(IF(experiment_rank = 1, experiment_name, NULL)) AS experiment_name,
    MAX(IF(experiment_rank = 1, experiment_description, NULL)) AS experiment_description,
    MAX(IF(experiment_rank = 1, experiment_dataset_id, NULL)) AS experiment_dataset_id,
    MIN(start_time) AS start_time,
    MAX(GREATEST(start_time, COALESCE(end_time, start_time))) AS end_time,
    MAX(IF(experiment_rank = 1, start_time, NULL)) AS cursor_time,
    MAX(IF(experiment_rank = 1, trace_id, NULL)) AS cursor_trace_id,
    MAX(IF(experiment_rank = 1, span_id, NULL)) AS cursor_span_id,
    COUNT(DISTINCT dataset_item_id) AS item_count${
      input.includeMetadata
        ? `,
    MAX(IF(
      experiment_rank = 1,
      experiment_metadata,
      NULL
    )) AS experiment_metadata`
        : ""
    }
  FROM ranked_experiments
  GROUP BY experiment_id
)
SELECT *
FROM experiment_summaries${cursor ? `\nWHERE ${cursor.sql}` : ""}
ORDER BY cursor_time DESC, experiment_id DESC, cursor_span_id DESC
LIMIT ?`,
      [...bound.params, ...(cursor?.params ?? []), input.limit],
    );
    return rows.map((row) => ({
      experiment_id: String(row.experiment_id),
      experiment_name: nullableString(row.experiment_name) ?? "",
      experiment_description: nullableString(row.experiment_description),
      experiment_dataset_id: nullableString(row.experiment_dataset_id),
      start_time: String(row.start_time),
      end_time: String(row.end_time),
      cursor_time: String(row.cursor_time),
      cursor_trace_id: String(row.cursor_trace_id),
      cursor_span_id: String(row.cursor_span_id),
      item_count: numberValue(row.item_count),
      ...(input.includeMetadata
        ? { experiment_metadata: metadataValue(row.experiment_metadata) }
        : {}),
    }));
  }

  async publicItems(input: {
    readonly projectId: string;
    readonly fromTime?: Date;
    readonly toTime?: Date;
    readonly limit: number;
    readonly experimentId?: readonly string[];
    readonly experimentName?: readonly string[];
    readonly experimentItemId?: readonly string[];
    readonly datasetId?: readonly string[];
    readonly advancedFilters?: EventsTableFilterState;
    readonly cursor?: {
      readonly lastTime: string;
      readonly lastTraceId: string;
      readonly lastId: string;
      readonly lastExperimentId: string;
    };
    readonly includeDataset: boolean;
    readonly includeIo: boolean;
    readonly includeMetadata: boolean;
    readonly includeItemMetadata: boolean;
    readonly includeExperimentMetadata: boolean;
  }): Promise<DorisPublicExperimentItem[]> {
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > MAX_PAGE_SIZE
    ) {
      throw new InvalidRequestError(
        "Invalid Doris public experiment-item page",
      );
    }
    const fromTime = input.fromTime ?? new Date(0);
    const toTime = input.toTime ?? new Date();
    const bound = createBoundParameters();
    const predicates = [
      `dri.project_id = ${bound.bind(input.projectId)}`,
      `e.partition_date >= ${bound.bind(utcDate(fromTime))}`,
      `e.partition_date < ${bound.bind(exclusivePartitionTo(toTime))}`,
      `e.start_time >= ${bound.bind(fromTime)}`,
      `e.start_time < ${bound.bind(toTime)}`,
      ...visibilityPredicates(),
      "trace_deletion.trace_id IS NULL",
      ...compilePublicExperimentFilters(
        publicExperimentFilters({
          advancedFilters: input.advancedFilters,
          simpleFilters: [
            { column: "experimentId", values: input.experimentId },
            { column: "experimentName", values: input.experimentName },
            { column: "experimentItemId", values: input.experimentItemId },
            { column: "experimentDatasetId", values: input.datasetId },
          ],
        }),
        bound,
      ),
    ];
    const cursor = input.cursor
      ? publicCursorPredicate(
          ["start_time", "trace_id", "span_id", "experiment_id"],
          [
            input.cursor.lastTime,
            input.cursor.lastTraceId,
            input.cursor.lastId,
            input.cursor.lastExperimentId,
          ],
        )
      : null;
    const optionalProjection = [
      ...(input.includeDataset
        ? [
            "dri.dataset_id AS experiment_dataset_id",
            "dri.dataset_item_version AS experiment_item_version",
          ]
        : []),
      ...(input.includeIo
        ? [
            "e.input",
            "e.output",
            "dri.dataset_item_expected_output AS experiment_item_expected_output",
          ]
        : []),
      ...(input.includeMetadata
        ? ["COALESCE(e.metadata_json, CAST(e.metadata AS STRING)) AS metadata"]
        : []),
      ...(input.includeItemMetadata
        ? [
            "COALESCE(dri.dataset_item_metadata_json, CAST(dri.dataset_item_metadata AS STRING)) AS experiment_item_metadata",
          ]
        : []),
      ...(input.includeExperimentMetadata
        ? [
            `${RUN_METADATA} AS experiment_metadata`,
            "dri.dataset_run_description AS experiment_description",
          ]
        : []),
    ];
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `WITH ranked_items AS (
  SELECT
    e.span_id,
    e.trace_id,
    e.start_time,
    e.end_time,
    e.\`level\`,
    e.environment,
    dri.dataset_run_id AS experiment_id,
    ${EXPERIMENT_NAME} AS experiment_name,
    dri.dataset_item_id AS experiment_item_id${
      optionalProjection.length > 0
        ? `,
    ${optionalProjection.join(",\n    ")}`
        : ""
    },
    ROW_NUMBER() OVER (
      PARTITION BY dri.project_id, dri.dataset_run_id, dri.dataset_item_id
      ORDER BY ${experimentEventPreferenceOrder("e")}
    ) AS item_rank
  ${visibilityJoins()}
  INNER JOIN events_current e
    ON ${experimentEventJoinPredicate("e")}
  LEFT JOIN trace_tombstones trace_deletion
    ON trace_deletion.project_id = e.project_id
   AND trace_deletion.trace_id = e.trace_id
  WHERE ${predicates.join("\n    AND ")}
)
SELECT *
FROM ranked_items
WHERE item_rank = 1${cursor ? `\n  AND ${cursor.sql}` : ""}
ORDER BY start_time DESC, trace_id DESC, span_id DESC, experiment_id DESC
LIMIT ?`,
      [...bound.params, ...(cursor?.params ?? []), input.limit],
    );
    return rows.map((row) => ({
      id: String(row.span_id),
      trace_id: String(row.trace_id),
      start_time: String(row.start_time),
      end_time: nullableString(row.end_time),
      level: String(
        row.level ?? "DEFAULT",
      ) as DorisPublicExperimentItem["level"],
      environment: String(row.environment),
      experiment_id: String(row.experiment_id),
      experiment_name: nullableString(row.experiment_name),
      experiment_item_id: String(row.experiment_item_id),
      ...(input.includeDataset
        ? {
            experiment_dataset_id: nullableString(row.experiment_dataset_id),
            experiment_item_version: nullableString(
              row.experiment_item_version,
            ),
          }
        : {}),
      ...(input.includeIo
        ? {
            input: parseJsonIfString(row.input),
            output: parseJsonIfString(row.output),
            experiment_item_expected_output: parseJsonIfString(
              row.experiment_item_expected_output,
            ),
          }
        : {}),
      ...(input.includeMetadata
        ? { metadata: metadataValue(row.metadata) }
        : {}),
      ...(input.includeItemMetadata
        ? {
            experiment_item_metadata: metadataValue(
              row.experiment_item_metadata,
            ),
          }
        : {}),
      ...(input.includeExperimentMetadata
        ? {
            experiment_metadata: metadataValue(row.experiment_metadata),
            experiment_description: nullableString(row.experiment_description),
          }
        : {}),
    }));
  }
}

export type DorisExperimentMetric = Awaited<
  ReturnType<DorisExperimentsRepository["metrics"]>
>[number];
