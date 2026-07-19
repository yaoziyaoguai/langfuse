import { InvalidRequestError } from "../../../../../errors";
import type { DorisQueryExecutor } from "../../../../../server/doris/client";
import { compileDorisEventFilters } from "../../../../../server/queries/doris-sql/filterCompiler";
import type { LogicalEventFilter } from "../../../../../server/queries/logical/filterPlan";
import { getViewDeclaration } from "../../../dataModel";
import {
  getValidAggregationsForMeasureType,
  type QueryType,
  type ViewVersion,
} from "../../../types";
import { validateQuery } from "../../../validateQuery";

type ExpressionCatalog = Readonly<Record<string, string>>;

type DorisAnalyticsCatalog = {
  readonly ctes: string;
  readonly baseTable: string;
  readonly dimensions: ExpressionCatalog;
  readonly measures: ExpressionCatalog;
  readonly timeExpression: string;
  readonly metadataExpression: string;
};

type BoundParameters = {
  readonly params: unknown[];
  bind(value: unknown): string;
};

const DEFERRED_DIMENSIONS = new Set([
  "datasetRunId",
  "experimentName",
  "experimentDatasetId",
  "experimentId",
]);

const MAX_FILLED_ROWS = 10_000;

type QueryChartConfig = QueryType["chartConfig"];

function queryChartConfig(query: QueryType): QueryChartConfig {
  return (
    (query as QueryType & { readonly config?: QueryChartConfig }).config ??
    query.chartConfig
  );
}

const OBSERVATION_DIMENSIONS: ExpressionCatalog = {
  id: "b.span_id",
  traceId: "b.trace_id",
  traceName:
    "COALESCE(NULLIF(b.trace_name, ''), IF(b.parent_span_id IS NULL OR b.parent_span_id = '' OR b.is_app_root = TRUE, NULLIF(b.name, ''), NULL))",
  environment: "NULLIF(b.environment, '')",
  parentObservationId: "NULLIF(b.parent_span_id, '')",
  type: "b.`type`",
  name: "b.`name`",
  level: "b.`level`",
  version: "NULLIF(b.`version`, '')",
  tags: "b.tags",
  providedModelName: "NULLIF(b.provided_model_name, '')",
  promptName: "NULLIF(b.prompt_name, '')",
  promptVersion: "b.prompt_version",
  userId: "NULLIF(b.user_id, '')",
  sessionId: "NULLIF(b.session_id, '')",
  release: "NULLIF(b.`release`, '')",
  traceRelease: "NULLIF(b.`release`, '')",
  traceVersion: "NULLIF(b.`version`, '')",
  startTime: "b.event_time",
  startTimeMonth: "DATE_FORMAT(b.event_time, '%Y-%m')",
  toolNames: "JSON_KEYS(b.tool_definitions)",
  calledToolNames: "b.tool_call_names",
  costType: "cost_type",
  usageType: "usage_type",
};

const OBSERVATION_MEASURES: ExpressionCatalog = {
  count: "1",
  traceId: "b.trace_id",
  uniqueUserIds: "NULLIF(b.user_id, '')",
  uniqueSessionIds: "NULLIF(b.session_id, '')",
  latency: "MILLISECONDS_DIFF(b.end_time, b.event_time)",
  streamingLatency: "MILLISECONDS_DIFF(b.end_time, b.completion_start_time)",
  inputTokens: "b.total_input_tokens",
  outputTokens: "b.total_output_tokens",
  totalTokens:
    "COALESCE(b.total_input_tokens, 0) + COALESCE(b.total_output_tokens, 0)",
  outputTokensPerSecond:
    "b.total_output_tokens * 1000.0 / NULLIF(MILLISECONDS_DIFF(b.end_time, b.completion_start_time), 0)",
  tokensPerSecond:
    "(COALESCE(b.total_input_tokens, 0) + COALESCE(b.total_output_tokens, 0)) * 1000.0 / NULLIF(MILLISECONDS_DIFF(b.end_time, b.event_time), 0)",
  inputCost:
    "ARRAY_SUM(ARRAY_MAP(k -> IF(LOCATE('input', LOWER(k)) > 0, JSON_EXTRACT_DOUBLE(CAST(b.cost_details AS JSON), CONCAT('$.', k)), 0.0), JSON_KEYS(CAST(b.cost_details AS JSON))))",
  outputCost:
    "ARRAY_SUM(ARRAY_MAP(k -> IF(LOCATE('output', LOWER(k)) > 0, JSON_EXTRACT_DOUBLE(CAST(b.cost_details AS JSON), CONCAT('$.', k)), 0.0), JSON_KEYS(CAST(b.cost_details AS JSON))))",
  totalCost: "b.total_cost",
  timeToFirstToken: "MILLISECONDS_DIFF(b.completion_start_time, b.event_time)",
  countScores: "b.count_scores",
  toolDefinitions: "NULLIF(CARDINALITY(JSON_KEYS(b.tool_definitions)), 0)",
  toolCalls: "NULLIF(CARDINALITY(b.tool_calls), 0)",
  costByType:
    "JSON_EXTRACT_DOUBLE(CAST(b.cost_details AS JSON), CONCAT('$.', cost_type))",
  usageByType:
    "JSON_EXTRACT_DOUBLE(CAST(b.usage_details AS JSON), CONCAT('$.', usage_type))",
};

const TRACE_DIMENSIONS: ExpressionCatalog = {
  id: "b.trace_id",
  name: "b.trace_name",
  tags: "b.tags",
  userId: "b.user_id",
  sessionId: "b.session_id",
  release: "b.`release`",
  version: "b.`version`",
  environment: "b.environment",
  timestamp: "b.event_time",
  timestampMonth: "DATE_FORMAT(b.event_time, '%Y-%m')",
};

const TRACE_MEASURES_V2: ExpressionCatalog = {
  count: "1",
  observationsCount: "b.observations_count",
  scoresCount: "b.scores_count",
  uniqueUserIds: "b.user_id",
  uniqueSessionIds: "b.session_id",
  latency: "b.latency",
  totalTokens: "b.total_tokens",
  totalCost: "b.total_cost",
};

const TRACE_MEASURES_V1: ExpressionCatalog = {
  ...TRACE_MEASURES_V2,
  uniqueUserIds: "IF(NULLIF(b.user_id, '') IS NULL, 0, 1)",
  uniqueSessionIds: "IF(NULLIF(b.session_id, '') IS NULL, 0, 1)",
};

const SCORE_DIMENSIONS: ExpressionCatalog = {
  id: "b.score_id",
  environment: "b.environment",
  name: "b.`name`",
  source: "b.`source`",
  dataType: "b.data_type",
  traceId: "b.trace_id",
  configId: "b.config_id",
  timestamp: "b.event_time",
  timestampMonth: "DATE_FORMAT(b.event_time, '%Y-%m')",
  timestampDay: "DATE_FORMAT(b.event_time, '%Y-%m-%d')",
  observationId: "b.observation_id",
  sessionId: "b.session_id",
  value: "b.value",
  stringValue: "b.string_value",
  traceName: "b.trace_name",
  tags: "b.trace_tags",
  userId: "b.trace_user_id",
  traceRelease: "b.trace_release",
  traceVersion: "b.trace_version",
  observationName: "b.observation_name",
  observationModelName: "b.observation_model_name",
  observationPromptName: "b.observation_prompt_name",
  observationPromptVersion: "b.observation_prompt_version",
};

const SCORE_MEASURES: ExpressionCatalog = {
  count: "1",
  value: "b.value",
};

function utcDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function exclusivePartitionTo(value: Date): string {
  const lastIncluded = new Date(value.getTime() - 1);
  lastIncluded.setUTCDate(lastIncluded.getUTCDate() + 1);
  return utcDate(lastIncluded);
}

function eventVisibilityCte(
  projectId: string,
  from: Date,
  to: Date,
  bound: BoundParameters,
): string {
  return `visible_events AS (
  SELECT e.*
  FROM events_current e
  LEFT JOIN trace_tombstones trace_deletion
    ON trace_deletion.project_id = e.project_id
   AND trace_deletion.trace_id = e.trace_id
  LEFT JOIN project_tombstones project_deletion
    ON project_deletion.project_id = e.project_id
  WHERE e.project_id = ${bound.bind(projectId)}
    AND e.partition_date >= ${bound.bind(utcDate(from))}
    AND e.partition_date < ${bound.bind(exclusivePartitionTo(to))}
    AND e.start_time >= ${bound.bind(from)}
    AND e.start_time < ${bound.bind(to)}
    AND trace_deletion.trace_id IS NULL
    AND project_deletion.project_id IS NULL
)`;
}

function scoreVisibilityQuery(
  projectId: string,
  from: Date,
  to: Date,
  bound: BoundParameters,
): string {
  return `SELECT s.*
  FROM scores_current s
  LEFT JOIN trace_tombstones score_trace_deletion
    ON score_trace_deletion.project_id = s.project_id
   AND score_trace_deletion.trace_id = s.trace_id
  LEFT JOIN project_tombstones score_project_deletion
    ON score_project_deletion.project_id = s.project_id
  WHERE s.project_id = ${bound.bind(projectId)}
    AND s.score_date >= ${bound.bind(utcDate(from))}
    AND s.score_date < ${bound.bind(exclusivePartitionTo(to))}
    AND s.\`timestamp\` >= ${bound.bind(from)}
    AND s.\`timestamp\` < ${bound.bind(to)}
    AND score_trace_deletion.trace_id IS NULL
    AND score_project_deletion.project_id IS NULL`;
}

function scoreVisibilityCte(
  projectId: string,
  from: Date,
  to: Date,
  bound: BoundParameters,
): string {
  return `visible_scores AS (
  ${scoreVisibilityQuery(projectId, from, to, bound)}
)`;
}

function observationsCatalog(
  projectId: string,
  from: Date,
  to: Date,
  bound: BoundParameters,
): DorisAnalyticsCatalog {
  return {
    ctes: `${eventVisibilityCte(projectId, from, to, bound)},
score_counts AS (
  SELECT project_id, observation_id, COUNT(*) AS count_scores
  FROM (${scoreVisibilityQuery(projectId, from, to, bound)}) visible_score_rows
  WHERE observation_id IS NOT NULL
  GROUP BY project_id, observation_id
),
observation_rows AS (
  SELECT e.*, e.start_time AS event_time, COALESCE(sc.count_scores, 0) AS count_scores
  FROM visible_events e
  LEFT JOIN score_counts sc
    ON sc.project_id = e.project_id
   AND sc.observation_id = e.span_id
)`,
    baseTable: "observation_rows b",
    dimensions: OBSERVATION_DIMENSIONS,
    measures: OBSERVATION_MEASURES,
    timeExpression: "b.event_time",
    metadataExpression: "b.metadata",
  };
}

function tracesCatalog(
  projectId: string,
  from: Date,
  to: Date,
  bound: BoundParameters,
  version: ViewVersion,
): DorisAnalyticsCatalog {
  return {
    ctes: `${eventVisibilityCte(projectId, from, to, bound)},
trace_score_counts AS (
  SELECT project_id, trace_id, COUNT(*) AS scores_count
  FROM (${scoreVisibilityQuery(projectId, from, to, bound)}) visible_score_rows
  WHERE trace_id IS NOT NULL
  GROUP BY project_id, trace_id
),
trace_rows AS (
  SELECT
    e.project_id,
    e.trace_id,
    COALESCE(MAX(NULLIF(e.trace_name, '')), MAX(IF(e.is_app_root = TRUE OR e.parent_span_id IS NULL OR e.parent_span_id = '', NULLIF(e.\`name\`, ''), NULL))) AS trace_name,
    ANY_VALUE(e.tags) AS tags,
    CAST(ANY_VALUE(CAST(e.metadata AS STRING)) AS VARIANT) AS metadata,
    MAX(NULLIF(e.user_id, '')) AS user_id,
    MAX(NULLIF(e.session_id, '')) AS session_id,
    MAX(NULLIF(e.\`release\`, '')) AS \`release\`,
    MAX(NULLIF(e.\`version\`, '')) AS \`version\`,
    MAX(NULLIF(e.environment, '')) AS environment,
    MIN(e.start_time) AS event_time,
    COUNT(DISTINCT e.span_id) AS observations_count,
    MAX(COALESCE(sc.scores_count, 0)) AS scores_count,
    MILLISECONDS_DIFF(MAX(e.end_time), MIN(e.start_time)) AS latency,
    SUM(COALESCE(e.total_input_tokens, 0) + COALESCE(e.total_output_tokens, 0)) AS total_tokens,
    SUM(e.total_cost) AS total_cost
  FROM visible_events e
  LEFT JOIN trace_score_counts sc
    ON sc.project_id = e.project_id
   AND sc.trace_id = e.trace_id
  GROUP BY e.project_id, e.trace_id
)`,
    baseTable: "trace_rows b",
    dimensions: TRACE_DIMENSIONS,
    measures: version === "v1" ? TRACE_MEASURES_V1 : TRACE_MEASURES_V2,
    timeExpression: "b.event_time",
    metadataExpression: "b.metadata",
  };
}

function scoresCatalog(
  projectId: string,
  from: Date,
  to: Date,
  bound: BoundParameters,
): DorisAnalyticsCatalog {
  return {
    ctes: `${eventVisibilityCte(projectId, from, to, bound)},
trace_context AS (
  SELECT
    project_id,
    trace_id,
    COALESCE(MAX(NULLIF(trace_name, '')), MAX(IF(is_app_root = TRUE OR parent_span_id IS NULL OR parent_span_id = '', NULLIF(\`name\`, ''), NULL))) AS trace_name,
    ANY_VALUE(tags) AS trace_tags,
    MAX(NULLIF(user_id, '')) AS trace_user_id,
    MAX(NULLIF(\`release\`, '')) AS trace_release,
    MAX(NULLIF(\`version\`, '')) AS trace_version
  FROM visible_events
  GROUP BY project_id, trace_id
),
observation_context AS (
  SELECT
    project_id,
    trace_id,
    span_id,
    MAX(NULLIF(\`name\`, '')) AS observation_name,
    MAX(NULLIF(provided_model_name, '')) AS observation_model_name,
    MAX(NULLIF(prompt_name, '')) AS observation_prompt_name,
    MAX(prompt_version) AS observation_prompt_version
  FROM visible_events
  GROUP BY project_id, trace_id, span_id
),
${scoreVisibilityCte(projectId, from, to, bound)},
score_rows AS (
  SELECT
    s.*,
    s.\`timestamp\` AS event_time,
    t.trace_name,
    t.trace_tags,
    t.trace_user_id,
    t.trace_release,
    t.trace_version,
    o.observation_name,
    o.observation_model_name,
    o.observation_prompt_name,
    o.observation_prompt_version
  FROM visible_scores s
  LEFT JOIN trace_context t
    ON t.project_id = s.project_id
   AND t.trace_id = s.trace_id
  LEFT JOIN observation_context o
    ON o.project_id = s.project_id
   AND o.trace_id = s.trace_id
   AND o.span_id = s.observation_id
)`,
    baseTable: "score_rows b",
    dimensions: SCORE_DIMENSIONS,
    measures: SCORE_MEASURES,
    timeExpression: "b.event_time",
    metadataExpression: "b.metadata",
  };
}

function catalogFor(input: {
  readonly projectId: string;
  readonly query: QueryType;
  readonly version: ViewVersion;
  readonly from: Date;
  readonly to: Date;
  readonly bound: BoundParameters;
}): DorisAnalyticsCatalog {
  switch (input.query.view) {
    case "observations":
      return observationsCatalog(
        input.projectId,
        input.from,
        input.to,
        input.bound,
      );
    case "traces":
      return tracesCatalog(
        input.projectId,
        input.from,
        input.to,
        input.bound,
        input.version,
      );
    case "scores-numeric":
    case "scores-categorical":
      return scoresCatalog(input.projectId, input.from, input.to, input.bound);
  }
}

type AnalyticsGranularity = Exclude<
  QueryType["timeDimension"],
  null
>["granularity"];
type ResolvedAnalyticsGranularity = Exclude<AnalyticsGranularity, "auto">;

function granularity(input: QueryType): ResolvedAnalyticsGranularity {
  const requested = input.timeDimension?.granularity ?? "day";
  if (requested !== "auto") return requested;
  const duration =
    new Date(input.toTimestamp).getTime() -
    new Date(input.fromTimestamp).getTime();
  if (duration <= 2 * 60 * 60 * 1_000) return "minute";
  if (duration <= 3 * 24 * 60 * 60 * 1_000) return "hour";
  if (duration <= 90 * 24 * 60 * 60 * 1_000) return "day";
  if (duration <= 2 * 365 * 24 * 60 * 60 * 1_000) return "week";
  return "month";
}

function fixedBucketMilliseconds(
  value: ResolvedAnalyticsGranularity,
): number | undefined {
  const values: Partial<Record<ResolvedAnalyticsGranularity, number>> = {
    minute: 60_000,
    hour: 3_600_000,
    day: 86_400_000,
    "5m": 300_000,
    "10m": 600_000,
    "15m": 900_000,
    "30m": 1_800_000,
    "1h": 3_600_000,
    "2h": 7_200_000,
    "4h": 14_400_000,
    "1d": 86_400_000,
    "2d": 172_800_000,
  };
  return values[value];
}

function floorBucket(value: Date, bucket: ResolvedAnalyticsGranularity): Date {
  const fixed = fixedBucketMilliseconds(bucket);
  if (fixed) {
    return new Date(Math.floor(value.getTime() / fixed) * fixed);
  }
  const result = new Date(value);
  if (bucket === "week" || bucket === "1w") {
    result.setUTCHours(0, 0, 0, 0);
    result.setUTCDate(result.getUTCDate() - ((result.getUTCDay() + 6) % 7));
    return result;
  }
  if (bucket === "month") {
    result.setUTCDate(1);
    result.setUTCHours(0, 0, 0, 0);
    return result;
  }
  throw new InvalidRequestError(
    `Unsupported Doris analytics granularity: ${bucket}`,
  );
}

function nextBucket(value: Date, bucket: ResolvedAnalyticsGranularity): Date {
  const fixed = fixedBucketMilliseconds(bucket);
  if (fixed) return new Date(value.getTime() + fixed);
  const result = new Date(value);
  if (bucket === "week" || bucket === "1w") {
    result.setUTCDate(result.getUTCDate() + 7);
    return result;
  }
  if (bucket === "month") {
    result.setUTCMonth(result.getUTCMonth() + 1);
    return result;
  }
  throw new InvalidRequestError(
    `Unsupported Doris analytics granularity: ${bucket}`,
  );
}

function emptyMetricValue(
  aggregation: QueryType["metrics"][number]["aggregation"],
): number | readonly [] {
  return aggregation === "histogram" ? [] : 0;
}

function fillTimeBuckets(input: {
  readonly rows: Array<Record<string, unknown>>;
  readonly query: QueryType;
  readonly dimensionFields: readonly string[];
}): Array<Record<string, unknown>> {
  if (!input.query.timeDimension || input.query.orderBy?.length) {
    return input.rows;
  }
  const bucket = granularity(input.query);
  const from = floorBucket(new Date(input.query.fromTimestamp), bucket);
  const to = new Date(input.query.toTimestamp);
  const toFloor = floorBucket(to, bucket);
  const exclusiveEnd =
    toFloor.getTime() === to.getTime() ? toFloor : nextBucket(toFloor, bucket);
  const buckets: string[] = [];
  for (
    let current = from;
    current < exclusiveEnd;
    current = nextBucket(current, bucket)
  ) {
    buckets.push(current.toISOString());
    if (buckets.length > MAX_FILLED_ROWS) {
      throw new InvalidRequestError(
        "Doris analytics time range produces too many buckets",
      );
    }
  }

  const groupKey = (row: Record<string, unknown>) =>
    JSON.stringify(input.dimensionFields.map((field) => row[field] ?? null));
  const groups = new Map<
    string,
    {
      readonly dimensions: Record<string, unknown>;
      readonly rows: Map<string, Record<string, unknown>>;
    }
  >();
  for (const row of input.rows) {
    const key = groupKey(row);
    const group = groups.get(key) ?? {
      dimensions: Object.fromEntries(
        input.dimensionFields.map((field) => [field, row[field] ?? null]),
      ),
      rows: new Map<string, Record<string, unknown>>(),
    };
    group.rows.set(String(row.time_dimension), row);
    groups.set(key, group);
  }
  if (groups.size === 0 && input.dimensionFields.length === 0) {
    groups.set("[]", { dimensions: {}, rows: new Map() });
  }
  if (groups.size * buckets.length > MAX_FILLED_ROWS) {
    throw new InvalidRequestError(
      "Doris analytics time fill produces too many rows",
    );
  }

  const metricDefaults = Object.fromEntries(
    input.query.metrics.map((metric) => [
      `${metric.aggregation}_${metric.measure}`,
      emptyMetricValue(metric.aggregation),
    ]),
  );
  return [...groups.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .flatMap(([, group]) =>
      buckets.map(
        (time) =>
          group.rows.get(time) ?? {
            time_dimension: time,
            ...group.dimensions,
            ...metricDefaults,
          },
      ),
    )
    .sort(
      (left, right) =>
        String(left.time_dimension).localeCompare(
          String(right.time_dimension),
        ) || groupKey(left).localeCompare(groupKey(right)),
    );
}

function bucketExpression(expression: string, input: QueryType): string {
  const value = granularity(input);
  const units: Partial<Record<typeof value, string>> = {
    minute: "minute",
    hour: "hour",
    day: "day",
    week: "week",
    month: "month",
    "1h": "hour",
    "1d": "day",
    "1w": "week",
  };
  const unit = units[value];
  if (unit) return `DATE_TRUNC(${expression}, '${unit}')`;
  const seconds: Partial<Record<typeof value, number>> = {
    "5m": 300,
    "10m": 600,
    "15m": 900,
    "30m": 1800,
    "2h": 7200,
    "4h": 14400,
    "2d": 172800,
  };
  const interval = seconds[value];
  if (!interval) {
    throw new InvalidRequestError(
      `Unsupported Doris analytics granularity: ${value}`,
    );
  }
  return `FROM_UNIXTIME(FLOOR(UNIX_TIMESTAMP(${expression}) / ${interval}) * ${interval})`;
}

function expressionForFilter(
  filter: QueryType["filters"][number],
  catalog: DorisAnalyticsCatalog,
): LogicalEventFilter {
  if (DEFERRED_DIMENSIONS.has(filter.column)) {
    throw new InvalidRequestError(
      `Analytics dimension ${filter.column} is not available on the Doris R1A backend`,
    );
  }
  if (filter.type === "positionInTrace") {
    throw new InvalidRequestError(
      "Position-in-trace metrics filters are not available on the Doris backend",
    );
  }
  if (
    filter.type === "numberObject" ||
    filter.type === "booleanObject" ||
    filter.type === "categoryOptions"
  ) {
    throw new InvalidRequestError(
      `Unsupported Doris analytics object filter: ${filter.column}`,
    );
  }
  if (filter.type === "stringObject") {
    if (filter.column !== "metadata") {
      throw new InvalidRequestError(
        `Unsupported Doris analytics object filter: ${filter.column}`,
      );
    }
    return {
      filter,
      expression: `JSON_UNQUOTE(CAST(ELEMENT_AT(${catalog.metadataExpression}, ?) AS STRING))`,
      objectKey: filter.key,
    };
  }
  const fallback = filter.column.endsWith("Name")
    ? catalog.dimensions.name
    : undefined;
  const expression =
    catalog.dimensions[filter.column] ??
    (filter.column === "startTime" || filter.column === "timestamp"
      ? catalog.timeExpression
      : fallback);
  if (!expression) {
    throw new InvalidRequestError(
      `Invalid Doris analytics filter column: ${filter.column}`,
    );
  }
  return { filter, expression };
}

function metricExpression(
  expression: string,
  aggregation: QueryType["metrics"][number]["aggregation"],
  measure: string,
  bins: number,
): string {
  switch (aggregation) {
    case "sum":
      return `SUM(${expression})`;
    case "avg":
      return `AVG(${expression})`;
    case "count":
      return measure === "count" ? "COUNT(*)" : `COUNT(${expression})`;
    case "max":
      return `MAX(${expression})`;
    case "min":
      return `MIN(${expression})`;
    case "p50":
    case "p75":
    case "p90":
    case "p95":
    case "p99":
      return `PERCENTILE_APPROX(CAST(${expression} AS DOUBLE), ${Number(aggregation.slice(1)) / 100})`;
    case "histogram":
      return `HISTOGRAM(${expression}, ${bins})`;
    case "uniq":
      return `COUNT(DISTINCT ${expression})`;
  }
}

function explodedDimension(input: {
  readonly field: string;
  readonly expression: string;
}): { readonly expression: string; readonly join?: string } {
  switch (input.field) {
    case "toolNames":
      return {
        expression: "tool_name",
        join: "LATERAL VIEW EXPLODE(JSON_KEYS(b.tool_definitions)) exploded_tools AS tool_name",
      };
    case "calledToolNames":
      return {
        expression: "called_tool_name",
        join: "LATERAL VIEW EXPLODE(b.tool_call_names) exploded_calls AS called_tool_name",
      };
    case "costType":
      return {
        expression: "cost_type",
        join: "LATERAL VIEW EXPLODE(JSON_KEYS(b.cost_details)) exploded_costs AS cost_type",
      };
    case "usageType":
      return {
        expression: "usage_type",
        join: "LATERAL VIEW EXPLODE(JSON_KEYS(b.usage_details)) exploded_usage AS usage_type",
      };
    default:
      return { expression: input.expression };
  }
}

function dateTime(value: unknown): string {
  const date =
    value instanceof Date
      ? value
      : new Date(
          typeof value === "string" && !value.includes("T")
            ? `${value.replace(" ", "T")}Z`
            : String(value),
        );
  if (!Number.isFinite(date.getTime())) {
    throw new TypeError("Doris returned an invalid analytics time bucket");
  }
  return date.toISOString();
}

function histogram(value: unknown): readonly [number, number, number][] {
  const parsed =
    typeof value === "string"
      ? (JSON.parse(value) as { buckets?: unknown[] })
      : (value as { buckets?: unknown[] } | null);
  if (!parsed || !Array.isArray(parsed.buckets)) return [];
  return parsed.buckets.map((bucket) => {
    const row = bucket as Record<string, unknown>;
    const lower = Number(row.lower);
    const upper = Number(row.upper);
    const count = Number(row.count);
    if (![lower, upper, count].every(Number.isFinite)) {
      throw new TypeError("Doris returned an invalid analytics histogram");
    }
    return [lower, upper, count];
  });
}

function decodeRows(
  rows: readonly Record<string, unknown>[],
  query: QueryType,
): Array<Record<string, unknown>> {
  const metricAliases = new Map(
    query.metrics.map((metric) => [
      `${metric.aggregation}_${metric.measure}`,
      metric.aggregation,
    ]),
  );
  return rows.map((row) =>
    Object.fromEntries(
      Object.entries(row).map(([key, value]) => {
        if (key === "time_dimension") return [key, dateTime(value)];
        const aggregation = metricAliases.get(key);
        if (aggregation === "histogram") return [key, histogram(value)];
        if (aggregation && value !== null && value !== undefined) {
          const numeric = Number(value);
          if (Number.isFinite(numeric)) return [key, numeric];
        }
        return [key, value];
      }),
    ),
  );
}

export async function executeDorisAnalyticsQuery(input: {
  readonly executor: DorisQueryExecutor;
  readonly projectId: string;
  readonly query: QueryType;
  readonly version: ViewVersion;
}): Promise<Array<Record<string, unknown>>> {
  const deferredDimension = [
    ...input.query.dimensions.map(({ field }) => field),
    ...(input.query.entityDimension ? [input.query.entityDimension.field] : []),
  ].find((field) => DEFERRED_DIMENSIONS.has(field));
  if (deferredDimension) {
    throw new InvalidRequestError(
      `Analytics dimension ${deferredDimension} is not available on the Doris R1A backend`,
    );
  }
  const validation = validateQuery(input.query, input.version);
  if (!validation.valid) throw new InvalidRequestError(validation.reason);
  const from = new Date(input.query.fromTimestamp);
  const to = new Date(input.query.toTimestamp);
  if (
    !input.projectId ||
    !Number.isFinite(from.getTime()) ||
    !Number.isFinite(to.getTime()) ||
    from >= to
  ) {
    throw new InvalidRequestError("Invalid Doris analytics query scope");
  }
  const declaration = getViewDeclaration(input.query.view, input.version);
  for (const dimension of input.query.dimensions) {
    if (DEFERRED_DIMENSIONS.has(dimension.field)) {
      throw new InvalidRequestError(
        `Analytics dimension ${dimension.field} is not available on the Doris R1A backend`,
      );
    }
    if (!declaration.dimensions[dimension.field]) {
      throw new InvalidRequestError(
        `Invalid analytics dimension: ${dimension.field}`,
      );
    }
  }
  for (const metric of input.query.metrics) {
    const definition = declaration.measures[metric.measure];
    if (!definition) {
      throw new InvalidRequestError(
        `Invalid analytics metric: ${metric.measure}`,
      );
    }
    if (
      !getValidAggregationsForMeasureType(definition.type).includes(
        metric.aggregation,
      )
    ) {
      throw new InvalidRequestError(
        `Invalid aggregation ${metric.aggregation} for ${metric.measure}`,
      );
    }
  }

  const params: unknown[] = [];
  const bound: BoundParameters = {
    params,
    bind(value) {
      params.push(value);
      return "?";
    },
  };
  const catalog = catalogFor({
    projectId: input.projectId,
    query: input.query,
    version: input.version,
    from,
    to,
    bound,
  });
  const dimensionFields = input.query.dimensions.map(({ field }) => field);
  for (const metric of input.query.metrics) {
    if (
      (metric.measure === "costByType" || metric.measure === "usageByType") &&
      !dimensionFields.includes(
        metric.measure === "costByType" ? "costType" : "usageType",
      )
    ) {
      dimensionFields.push(
        metric.measure === "costByType" ? "costType" : "usageType",
      );
    }
  }
  const joins: string[] = [];
  const dimensions = dimensionFields.map((field) => {
    if (DEFERRED_DIMENSIONS.has(field)) {
      throw new InvalidRequestError(
        `Analytics dimension ${field} is not available on the Doris R1A backend`,
      );
    }
    const expression = catalog.dimensions[field];
    if (!expression) {
      throw new InvalidRequestError(
        `Invalid Doris analytics dimension: ${field}`,
      );
    }
    const exploded = explodedDimension({ field, expression });
    if (exploded.join && !joins.includes(exploded.join))
      joins.push(exploded.join);
    return { field, expression: exploded.expression };
  });
  let bucket: { alias: string; expression: string } | undefined;
  if (input.query.timeDimension) {
    bucket = {
      alias: "time_dimension",
      expression: bucketExpression(catalog.timeExpression, input.query),
    };
  } else if (input.query.entityDimension) {
    const field = input.query.entityDimension.field;
    if (DEFERRED_DIMENSIONS.has(field)) {
      throw new InvalidRequestError(
        `Analytics dimension ${field} is not available on the Doris R1A backend`,
      );
    }
    const expression = catalog.dimensions[field];
    if (!expression) {
      throw new InvalidRequestError(`Invalid Doris entity dimension: ${field}`);
    }
    bucket = { alias: "entity_dimension", expression };
  }
  const metricSelects = input.query.metrics.map((metric) => {
    const expression = catalog.measures[metric.measure];
    if (!expression) {
      throw new InvalidRequestError(
        `Invalid Doris analytics metric: ${metric.measure}`,
      );
    }
    return `${metricExpression(
      expression,
      metric.aggregation,
      metric.measure,
      queryChartConfig(input.query)?.bins ?? 10,
    )} AS ${metric.aggregation}_${metric.measure}`;
  });
  const filterPlans = input.query.filters.map((filter) =>
    expressionForFilter(filter, catalog),
  );
  if (input.query.view === "scores-numeric") {
    filterPlans.push({
      filter: {
        type: "stringOptions",
        column: "dataType",
        operator: "any of",
        value: ["NUMERIC", "BOOLEAN"],
      },
      expression: catalog.dimensions.dataType!,
    });
  } else if (input.query.view === "scores-categorical") {
    filterPlans.push({
      filter: {
        type: "string",
        column: "dataType",
        operator: "=",
        value: "CATEGORICAL",
      },
      expression: catalog.dimensions.dataType!,
    });
  }
  const predicates = compileDorisEventFilters(filterPlans, bound);
  const selects = [
    ...(bucket ? [`${bucket.expression} AS ${bucket.alias}`] : []),
    ...dimensions.map(
      ({ field, expression }) => `${expression} AS \`${field}\``,
    ),
    ...metricSelects,
  ];
  const groups = [
    ...(bucket ? [bucket.expression] : []),
    ...dimensions.map(({ expression }) => expression),
  ];
  const selectableAliases = new Map<string, string>([
    ...(bucket ? [[bucket.alias, bucket.alias] as const] : []),
    ...dimensions.map(({ field }) => [field, `\`${field}\``] as const),
    ...input.query.metrics.map(
      (metric) =>
        [
          `${metric.aggregation}_${metric.measure}`,
          `${metric.aggregation}_${metric.measure}`,
        ] as const,
    ),
  ]);
  const orderBy = input.query.orderBy?.length
    ? input.query.orderBy.map((order) => {
        const expression = selectableAliases.get(order.field);
        if (!expression) {
          throw new InvalidRequestError(
            `Invalid Doris analytics order field: ${order.field}`,
          );
        }
        return `${expression} ${order.direction.toUpperCase()}`;
      })
    : bucket
      ? [`${bucket.alias} ASC`]
      : input.query.metrics[0]
        ? [
            `${input.query.metrics[0].aggregation}_${input.query.metrics[0].measure} DESC`,
          ]
        : dimensions[0]
          ? [`\`${dimensions[0].field}\` ASC`]
          : [];
  const limit = queryChartConfig(input.query)?.row_limit;
  const sql = `WITH ${catalog.ctes}
SELECT
  ${selects.join(",\n  ")}
FROM ${catalog.baseTable}
${joins.join("\n")}
${predicates.length ? `WHERE ${predicates.join("\n  AND ")}` : ""}
${groups.length ? `GROUP BY ${groups.join(", ")}` : ""}
${orderBy.length ? `ORDER BY ${orderBy.join(", ")}` : ""}
${limit ? "LIMIT ?" : ""}`;
  if (limit) params.push(limit);
  const rows = await input.executor.query<Record<string, unknown>>(sql, params);
  return fillTimeBuckets({
    rows: decodeRows(rows, input.query),
    query: input.query,
    dimensionFields,
  });
}
