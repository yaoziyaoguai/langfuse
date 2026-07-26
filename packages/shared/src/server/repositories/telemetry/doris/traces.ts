import type { TracingSearchType } from "../../../../interfaces/search";
import type { EventsTableFilterState } from "../../../../types";
import { InvalidRequestError } from "../../../../errors";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import { compileDorisVisibleEventScope } from "../../../queries/doris-sql/eventQueryCompiler";
import {
  buildSearchPlan,
  type AnalyticsTimeRange,
} from "../../../queries/logical/searchPlan";
import {
  findTraceEventHeadLocators,
  findTraceEventHeadLocatorsByIds,
  type EventHeadLocator,
} from "./entityHeadLocator";
import {
  ObservationLevelDomain,
  type ObservationLevelType,
} from "../../../../domain";

const MAX_PAGE_SIZE = 999;

type LocateTrace = (input: {
  readonly projectId: string;
  readonly traceId: string;
}) => Promise<readonly EventHeadLocator[]>;

type LocateTraces = (input: {
  readonly projectId: string;
  readonly traceIds: readonly string[];
}) => Promise<readonly EventHeadLocator[]>;

type DorisTraceRow = Record<string, unknown> & {
  readonly project_id: string;
  readonly trace_id: string;
  readonly trace_timestamp: string | Date;
};

export type DorisTrace = {
  readonly id: string;
  readonly projectId: string;
  readonly timestamp: Date;
  readonly endTime: Date;
  readonly name: string | null;
  readonly environment: string;
  readonly userId: string | null;
  readonly sessionId: string | null;
  readonly release: string | null;
  readonly version: string | null;
  readonly tags: readonly string[];
  readonly inputPreview: string | null;
  readonly outputPreview: string | null;
  readonly input?: unknown;
  readonly output?: unknown;
  readonly metadata?: Readonly<Record<string, unknown>>;
  readonly rootObservationId: string | null;
  readonly fallbackObservationId: string;
  readonly incomplete: boolean;
  readonly observationCount: number;
  readonly totalInputTokens: number;
  readonly totalOutputTokens: number;
  readonly totalUsage: number;
  readonly totalCost: number | null;
  readonly latency: number;
};

export type DorisTracesPage = {
  readonly items: readonly DorisTrace[];
  readonly nextCursor: string | null;
};

export type DorisTraceMetrics = {
  readonly id: string;
  readonly projectId: string;
  readonly timestamp: Date;
  readonly latency: number | null;
  readonly level: ObservationLevelType;
  readonly observationCount: number;
  readonly usageDetails: Readonly<Record<string, number>>;
  readonly costDetails: Readonly<Record<string, number>>;
  readonly errorCount: number;
  readonly warningCount: number;
  readonly defaultCount: number;
  readonly debugCount: number;
};

export type DorisTraceOrderBy = {
  readonly column:
    | "timestamp"
    | "name"
    | "userId"
    | "sessionId"
    | "environment"
    | "version"
    | "release";
  readonly order: "ASC" | "DESC";
};

export type DorisTraceFilterOptionColumn =
  | "name"
  | "userId"
  | "sessionId"
  | "tags";

const TRACE_FILTER_OPTION_EXPRESSIONS: Readonly<
  Record<DorisTraceFilterOptionColumn, string>
> = {
  name: "r.name",
  userId: "r.user_id",
  sessionId: "r.session_id",
  tags: "r.tags",
};

function traceFilterOptionColumn(column: string): DorisTraceFilterOptionColumn {
  if (!Object.hasOwn(TRACE_FILTER_OPTION_EXPRESSIONS, column)) {
    throw new InvalidRequestError(
      `Unsupported Doris trace filter option column: ${column}`,
    );
  }
  return column as DorisTraceFilterOptionColumn;
}

const TRACE_ORDER_BY_EXPRESSIONS: Readonly<
  Record<DorisTraceOrderBy["column"], string>
> = {
  timestamp: "trace_timestamp",
  name: "name",
  userId: "user_id",
  sessionId: "session_id",
  environment: "environment",
  version: "`version`",
  release: "`release`",
};

function dateTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value !== "string") {
    throw new TypeError("Doris returned an invalid trace timestamp");
  }
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (Number.isNaN(parsed.getTime())) {
    throw new TypeError("Doris returned an invalid trace timestamp");
  }
  return parsed;
}

function nullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

function numberValue(value: unknown): number {
  const parsed = Number(value ?? 0);
  if (!Number.isFinite(parsed)) {
    throw new TypeError("Doris returned an invalid trace number");
  }
  return parsed;
}

function nullableNumber(value: unknown): number | null {
  return value === null || value === undefined || value === ""
    ? null
    : numberValue(value);
}

function booleanValue(value: unknown): boolean {
  return value === true || value === 1 || value === "1";
}

function stringArray(value: unknown): readonly string[] {
  const parsed = parseJsonIfString(value);
  return Array.isArray(parsed) ? parsed.map(String) : [];
}

function objectValue(value: unknown): Readonly<Record<string, unknown>> {
  const parsed = parseJsonIfString(value);
  return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
    ? (parsed as Readonly<Record<string, unknown>>)
    : {};
}

function numericRecord(value: unknown): Readonly<Record<string, number>> {
  return Object.fromEntries(
    Object.entries(objectValue(value)).flatMap(([key, item]) => {
      const parsed = nullableNumber(item);
      return parsed === null ? [] : [[key, parsed]];
    }),
  );
}

function exactTraceIdsFromFilters(
  filters: EventsTableFilterState,
): readonly string[] {
  let exactIds: string[] | null = null;
  for (const filter of filters) {
    if (filter.column !== "traceId") continue;
    const candidateIds =
      filter.type === "string" && filter.operator === "="
        ? [filter.value]
        : filter.type === "stringOptions" && filter.operator === "any of"
          ? filter.value
          : null;
    if (candidateIds === null) continue;
    const candidates = new Set(candidateIds);
    exactIds =
      exactIds === null
        ? [...candidates]
        : exactIds.filter((traceId) => candidates.has(traceId));
  }
  return exactIds ?? [];
}

function locatorRange(locators: readonly EventHeadLocator[]): {
  readonly range: AnalyticsTimeRange;
  readonly partitionDates: readonly string[];
} {
  const partitionDates = [
    ...new Set(locators.map(({ partitionDate }) => partitionDate)),
  ].sort();
  const from = new Date(`${partitionDates[0]}T00:00:00.000Z`);
  const to = new Date(
    `${partitionDates[partitionDates.length - 1]}T00:00:00.000Z`,
  );
  to.setUTCDate(to.getUTCDate() + 1);
  return { range: { from, to }, partitionDates };
}

function decodeTrace(row: DorisTraceRow): DorisTrace {
  const timestamp = dateTime(row.trace_timestamp);
  const endTime = dateTime(row.trace_end_time);
  const totalInputTokens = numberValue(row.total_input_tokens);
  const totalOutputTokens = numberValue(row.total_output_tokens);
  const representativeIsRoot = booleanValue(row.representative_is_root);
  const fallbackObservationId = String(row.representative_span_id);
  return {
    id: row.trace_id,
    projectId: row.project_id,
    timestamp,
    endTime,
    name: nullableString(row.name),
    environment: String(row.environment),
    userId: nullableString(row.user_id),
    sessionId: nullableString(row.session_id),
    release: nullableString(row.release),
    version: nullableString(row.version),
    tags: stringArray(row.tags),
    inputPreview: nullableString(row.input_preview),
    outputPreview: nullableString(row.output_preview),
    ...(Object.hasOwn(row, "input") && { input: parseJsonIfString(row.input) }),
    ...(Object.hasOwn(row, "output") && {
      output: parseJsonIfString(row.output),
    }),
    ...(Object.hasOwn(row, "metadata") && {
      metadata: objectValue(row.metadata),
    }),
    rootObservationId: representativeIsRoot ? fallbackObservationId : null,
    fallbackObservationId,
    incomplete: !representativeIsRoot,
    observationCount: numberValue(row.observation_count),
    totalInputTokens,
    totalOutputTokens,
    totalUsage: totalInputTokens + totalOutputTokens,
    totalCost: nullableNumber(row.total_cost),
    latency: (endTime.getTime() - timestamp.getTime()) / 1_000,
  };
}

export function encodeDorisTraceCursor(
  trace: Pick<DorisTrace, "timestamp" | "id">,
): string {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      timestamp: trace.timestamp.toISOString(),
      traceId: trace.id,
    }),
    "utf8",
  ).toString("base64url");
}

function decodeCursor(
  cursor: string | undefined,
): { readonly timestamp: Date; readonly traceId: string } | undefined {
  if (!cursor) return undefined;
  try {
    const decoded = Buffer.from(cursor, "base64url");
    if (decoded.toString("base64url") !== cursor) throw new Error();
    const value = JSON.parse(decoded.toString("utf8")) as Record<
      string,
      unknown
    >;
    if (
      value.version !== 1 ||
      typeof value.timestamp !== "string" ||
      typeof value.traceId !== "string" ||
      !value.traceId
    ) {
      throw new Error();
    }
    return {
      timestamp: dateTime(value.timestamp),
      traceId: value.traceId,
    };
  } catch {
    throw new InvalidRequestError("Invalid Doris trace cursor");
  }
}

function compileTraceList(input: {
  readonly projectId: string;
  readonly range: AnalyticsTimeRange | null;
  readonly filters: EventsTableFilterState;
  readonly search?: {
    readonly query: string;
    readonly searchType?: readonly TracingSearchType[];
  };
  readonly cursor?: string;
  readonly limit: number;
  readonly offset?: number;
  readonly orderBy?: DorisTraceOrderBy;
  readonly partitionDates?: readonly string[];
  readonly includeFullContent?: boolean;
}): { readonly sql: string; readonly params: readonly unknown[] } {
  if (
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > MAX_PAGE_SIZE ||
    !Number.isSafeInteger(input.offset ?? 0) ||
    (input.offset ?? 0) < 0 ||
    (input.cursor && input.orderBy)
  ) {
    throw new InvalidRequestError("Invalid Doris trace page size");
  }
  const allEvents = compileDorisVisibleEventScope({
    projectId: input.projectId,
    range: input.range,
    filters: [],
  });
  const matchedEvents = compileDorisVisibleEventScope({
    projectId: input.projectId,
    range: input.range,
    filters: input.filters,
    search: input.search,
  });
  const cursor = decodeCursor(input.cursor);
  const partitionDates = [...new Set(input.partitionDates ?? [])].sort();
  const partitionSql =
    partitionDates.length > 0
      ? `AND e.partition_date IN (${partitionDates.map(() => "?").join(", ")})`
      : "";
  const cursorSql = cursor
    ? `AND (
      trace_timestamp < ?
      OR (trace_timestamp = ? AND trace_id < ?)
    )`
    : "";
  const direction = input.orderBy?.order ?? "DESC";
  const primaryOrder = input.orderBy
    ? TRACE_ORDER_BY_EXPRESSIONS[input.orderBy.column]
    : "trace_timestamp";
  const orderSql = [
    primaryOrder,
    ...(primaryOrder === "trace_timestamp" ? [] : ["trace_timestamp"]),
    "trace_id",
  ]
    .map((expression) => `${expression} ${direction}`)
    .join(", ");
  const offsetSql = input.offset ? " OFFSET ?" : "";
  const rankedFullContent = input.includeFullContent
    ? ",\n    e.input,\n    e.output,\n    e.metadata"
    : "";
  const selectedFullContent = input.includeFullContent
    ? ",\n  input,\n  output,\n  metadata"
    : "";
  const params = [
    ...matchedEvents.params,
    ...partitionDates,
    ...allEvents.params,
    ...partitionDates,
    ...(cursor ? [cursor.timestamp, cursor.timestamp, cursor.traceId] : []),
    input.limit + 1,
    ...(input.offset ? [input.offset] : []),
  ];
  const sql = `WITH matched_trace_ids AS (
  SELECT DISTINCT e.trace_id
  ${matchedEvents.fromSql}
  WHERE ${matchedEvents.whereSql}
  ${partitionSql}
), ranked_events AS (
  SELECT
    e.project_id,
    e.trace_id,
    e.span_id,
    e.is_app_root,
    e.name,
    e.environment,
    e.user_id,
    e.session_id,
    e.\`release\`,
    e.\`version\`,
    e.tags,
    e.input_preview,
    e.output_preview${rankedFullContent},
    ROW_NUMBER() OVER (
      PARTITION BY e.trace_id
      ORDER BY e.is_app_root DESC,
        CASE WHEN e.parent_span_id IS NULL OR e.parent_span_id = '' THEN 0 ELSE 1 END,
        e.start_time ASC,
        e.span_id ASC
    ) AS representative_rank,
    MIN(e.start_time) OVER (PARTITION BY e.trace_id) AS trace_timestamp,
    MAX(COALESCE(e.end_time, e.start_time)) OVER (PARTITION BY e.trace_id) AS trace_end_time,
    COUNT(*) OVER (PARTITION BY e.trace_id) AS observation_count,
    SUM(COALESCE(e.total_input_tokens, 0)) OVER (PARTITION BY e.trace_id) AS total_input_tokens,
    SUM(COALESCE(e.total_output_tokens, 0)) OVER (PARTITION BY e.trace_id) AS total_output_tokens,
    SUM(e.total_cost) OVER (PARTITION BY e.trace_id) AS total_cost
  ${allEvents.fromSql}
  INNER JOIN matched_trace_ids matched ON matched.trace_id = e.trace_id
  WHERE ${allEvents.whereSql}
  ${partitionSql}
)
SELECT
  project_id,
  trace_id,
  trace_timestamp,
  trace_end_time,
  span_id AS representative_span_id,
  is_app_root AS representative_is_root,
  name,
  environment,
  user_id,
  session_id,
  \`release\`,
  \`version\`,
  tags,
  input_preview,
  output_preview${selectedFullContent},
  observation_count,
  total_input_tokens,
  total_output_tokens,
  total_cost
FROM ranked_events
WHERE representative_rank = 1
${cursorSql}
ORDER BY ${orderSql}
LIMIT ?${offsetSql}`;
  return { sql, params };
}

function compileTraceMetrics(input: {
  readonly projectId: string;
  readonly range: AnalyticsTimeRange | null;
  readonly filters: EventsTableFilterState;
  readonly search?: {
    readonly query: string;
    readonly searchType?: readonly TracingSearchType[];
  };
  readonly limit: number;
  readonly offset?: number;
  readonly orderBy?: DorisTraceOrderBy;
  readonly exactEventRange?: AnalyticsTimeRange;
  readonly partitionDates?: readonly string[];
}): { readonly sql: string; readonly params: readonly unknown[] } {
  if (
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > MAX_PAGE_SIZE ||
    !Number.isSafeInteger(input.offset ?? 0) ||
    (input.offset ?? 0) < 0
  ) {
    throw new InvalidRequestError("Invalid Doris trace metrics page size");
  }
  const matchedEvents = compileDorisVisibleEventScope({
    projectId: input.projectId,
    range: input.range,
    filters: input.filters,
    search: input.search,
  });
  const allEvents = compileDorisVisibleEventScope({
    projectId: input.projectId,
    range: input.exactEventRange ?? input.range,
    filters: [],
    partitionDates: input.partitionDates,
  });
  const direction = input.orderBy?.order ?? "DESC";
  const offsetSql = input.offset ? " OFFSET ?" : "";
  return {
    sql: `WITH matched_trace_ids AS (
  SELECT DISTINCT e.trace_id
  ${matchedEvents.fromSql}
  WHERE ${matchedEvents.whereSql}
), scoped_events AS (
  SELECT
    e.project_id,
    e.trace_id,
    e.start_time,
    e.end_time,
    e.level,
    COALESCE(e.usage_details_json, CAST(e.usage_details AS STRING)) AS usage_details,
    COALESCE(e.cost_details_json, CAST(e.cost_details AS STRING)) AS cost_details
  ${allEvents.fromSql}
  INNER JOIN matched_trace_ids matched ON matched.trace_id = e.trace_id
  WHERE ${allEvents.whereSql}
), trace_metrics AS (
  SELECT
    e.project_id,
    e.trace_id,
    MIN(e.start_time) AS trace_timestamp,
    CASE
      WHEN COUNT(e.end_time) = 0 THEN NULL
      ELSE (
        UNIX_TIMESTAMP(GREATEST(MAX(e.start_time), MAX(e.end_time)))
        - UNIX_TIMESTAMP(LEAST(MIN(e.start_time), MIN(e.end_time)))
      ) * 1000
    END AS latency_milliseconds,
    COUNT(*) AS observation_count,
    CASE
      WHEN SUM(CASE WHEN e.level = 'ERROR' THEN 1 ELSE 0 END) > 0 THEN 'ERROR'
      WHEN SUM(CASE WHEN e.level = 'WARNING' THEN 1 ELSE 0 END) > 0 THEN 'WARNING'
      WHEN SUM(CASE WHEN e.level = 'DEFAULT' THEN 1 ELSE 0 END) > 0 THEN 'DEFAULT'
      ELSE 'DEBUG'
    END AS aggregated_level,
    SUM(CASE WHEN e.level = 'ERROR' THEN 1 ELSE 0 END) AS error_count,
    SUM(CASE WHEN e.level = 'WARNING' THEN 1 ELSE 0 END) AS warning_count,
    SUM(CASE WHEN e.level = 'DEFAULT' THEN 1 ELSE 0 END) AS default_count,
    SUM(CASE WHEN e.level = 'DEBUG' THEN 1 ELSE 0 END) AS debug_count
  FROM scoped_events e
  GROUP BY e.project_id, e.trace_id
), usage_detail_values AS (
  SELECT
    e.project_id,
    e.trace_id,
    usage_key,
    SUM(CAST(JSON_EXTRACT_DOUBLE(
      CAST(e.usage_details AS JSON),
      CONCAT('$.', CHAR(34), REPLACE(usage_key, CHAR(34), CONCAT(CHAR(92), CHAR(34))), CHAR(34))
    ) AS DECIMAL(38, 18))) AS usage_value
  FROM scoped_events e
  LATERAL VIEW explode(JSON_KEYS(CAST(e.usage_details AS JSON))) exploded_usage AS usage_key
  GROUP BY e.project_id, e.trace_id, usage_key
), usage_details AS (
  SELECT project_id, trace_id, MAP_AGG(usage_key, usage_value) AS usage_details
  FROM usage_detail_values
  GROUP BY project_id, trace_id
), cost_detail_values AS (
  SELECT
    e.project_id,
    e.trace_id,
    cost_key,
    SUM(CAST(JSON_EXTRACT_DOUBLE(
      CAST(e.cost_details AS JSON),
      CONCAT('$.', CHAR(34), REPLACE(cost_key, CHAR(34), CONCAT(CHAR(92), CHAR(34))), CHAR(34))
    ) AS DECIMAL(38, 18))) AS cost_value
  FROM scoped_events e
  LATERAL VIEW explode(JSON_KEYS(CAST(e.cost_details AS JSON))) exploded_cost AS cost_key
  GROUP BY e.project_id, e.trace_id, cost_key
), cost_details AS (
  SELECT project_id, trace_id, MAP_AGG(cost_key, cost_value) AS cost_details
  FROM cost_detail_values
  GROUP BY project_id, trace_id
)
SELECT
  e.project_id,
  e.trace_id,
  e.trace_timestamp,
  e.latency_milliseconds,
  e.observation_count,
  usage.usage_details,
  cost.cost_details,
  e.aggregated_level,
  e.error_count,
  e.warning_count,
  e.default_count,
  e.debug_count
FROM trace_metrics e
LEFT JOIN usage_details usage
  ON usage.project_id = e.project_id AND usage.trace_id = e.trace_id
LEFT JOIN cost_details cost
  ON cost.project_id = e.project_id AND cost.trace_id = e.trace_id
ORDER BY e.trace_timestamp ${direction}, e.trace_id ${direction}
LIMIT ?${offsetSql}`,
    params: [
      ...matchedEvents.params,
      ...allEvents.params,
      input.limit,
      ...(input.offset ? [input.offset] : []),
    ],
  };
}

type DorisTraceMetricsRow = Record<string, unknown> & {
  readonly project_id: string;
  readonly trace_id: string;
  readonly trace_timestamp: string | Date;
};

function decodeTraceMetrics(row: DorisTraceMetricsRow): DorisTraceMetrics {
  const latencyMilliseconds = nullableNumber(row.latency_milliseconds);
  return {
    id: row.trace_id,
    projectId: row.project_id,
    timestamp: dateTime(row.trace_timestamp),
    latency: latencyMilliseconds === null ? null : latencyMilliseconds / 1_000,
    level: ObservationLevelDomain.parse(row.aggregated_level),
    observationCount: numberValue(row.observation_count),
    usageDetails: numericRecord(row.usage_details),
    costDetails: numericRecord(row.cost_details),
    errorCount: numberValue(row.error_count),
    warningCount: numberValue(row.warning_count),
    defaultCount: numberValue(row.default_count),
    debugCount: numberValue(row.debug_count),
  };
}

export class DorisTracesRepository {
  private readonly locateTrace: LocateTrace;
  private readonly locateTraces: LocateTraces;

  constructor(
    private readonly dependencies: {
      readonly query: DorisQueryExecutor["query"];
      readonly streamQuery?: NonNullable<DorisQueryExecutor["streamQuery"]>;
      readonly locateTrace?: LocateTrace;
      readonly locateTraces?: LocateTraces;
    },
  ) {
    this.locateTrace = dependencies.locateTrace ?? findTraceEventHeadLocators;
    this.locateTraces =
      dependencies.locateTraces ?? findTraceEventHeadLocatorsByIds;
  }

  async list(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly cursor?: string;
    readonly limit: number;
    readonly offset?: number;
    readonly orderBy?: DorisTraceOrderBy;
    readonly includeFullContent?: boolean;
  }): Promise<DorisTracesPage> {
    if (input.includeFullContent) {
      buildSearchPlan({
        range: input.range,
        filtersRequireFullContent: true,
      });
    }
    const compiled = compileTraceList(input);
    const rows = await this.dependencies.query<DorisTraceRow>(
      compiled.sql,
      compiled.params,
    );
    const items = rows.slice(0, input.limit).map(decodeTrace);
    return {
      items,
      nextCursor:
        !input.orderBy && rows.length > input.limit && items.length > 0
          ? encodeDorisTraceCursor(items[items.length - 1]!)
          : null,
    };
  }

  async *scanIdentities(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly limit: number;
    readonly signal?: AbortSignal;
  }): AsyncIterable<{ readonly id: string }> {
    if (!Number.isSafeInteger(input.limit) || input.limit < 1) {
      throw new RangeError("Doris trace identity limit is invalid");
    }
    const scope = compileDorisVisibleEventScope(input);
    const sql = `SELECT DISTINCT e.trace_id\n${scope.fromSql}\nWHERE ${scope.whereSql}\nORDER BY e.trace_id ASC\nLIMIT ?`;
    const params = scope.params.concat(input.limit);
    const rows = this.dependencies.streamQuery
      ? this.dependencies.streamQuery<{ readonly trace_id: string }>(
          sql,
          params,
          { signal: input.signal },
        )
      : await this.dependencies.query<{ readonly trace_id: string }>(
          sql,
          params,
          { signal: input.signal },
        );
    for await (const row of rows) yield { id: String(row.trace_id) };
  }

  async *scanEvaluationTargets(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly limit: number;
    readonly signal?: AbortSignal;
  }): AsyncIterable<{
    readonly id: string;
    readonly timestamp: Date;
    readonly environment: string;
  }> {
    if (!Number.isSafeInteger(input.limit) || input.limit < 1) {
      throw new RangeError("Doris evaluation trace limit is invalid");
    }
    const matched = compileDorisVisibleEventScope({
      ...input,
      allowUnboundedFullContent: true,
    });
    const allEvents = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: input.range,
      filters: [],
    });
    const sql = `WITH matched_trace_ids AS (
  SELECT DISTINCT e.trace_id
  ${matched.fromSql}
  WHERE ${matched.whereSql}
)
SELECT
  e.trace_id,
  MIN(e.start_time) AS trace_timestamp,
  MIN(e.environment) AS environment
${allEvents.fromSql}
INNER JOIN matched_trace_ids matched ON matched.trace_id = e.trace_id
WHERE ${allEvents.whereSql}
GROUP BY e.trace_id
ORDER BY e.trace_id ASC
LIMIT ?`;
    const rows = this.dependencies.streamQuery
      ? this.dependencies.streamQuery<{
          readonly trace_id: string;
          readonly trace_timestamp: string | Date;
          readonly environment: string;
        }>(sql, [...matched.params, ...allEvents.params, input.limit], {
          signal: input.signal,
        })
      : await this.dependencies.query<{
          readonly trace_id: string;
          readonly trace_timestamp: string | Date;
          readonly environment: string;
        }>(sql, [...matched.params, ...allEvents.params, input.limit], {
          signal: input.signal,
        });
    for await (const row of rows) {
      yield {
        id: String(row.trace_id),
        timestamp: dateTime(row.trace_timestamp),
        environment: String(row.environment),
      };
    }
  }

  async count(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
  }): Promise<number> {
    const scope = compileDorisVisibleEventScope(input);
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      `SELECT COUNT(DISTINCT e.trace_id) AS count\n${scope.fromSql}\nWHERE ${scope.whereSql}`,
      scope.params,
    );
    return numberValue(rows[0]?.count);
  }

  async metrics(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly limit: number;
    readonly offset?: number;
    readonly orderBy?: DorisTraceOrderBy;
  }): Promise<readonly DorisTraceMetrics[]> {
    const exactTraceIds = exactTraceIdsFromFilters(input.filters);
    let exactEventRange: AnalyticsTimeRange | undefined;
    let partitionDates: readonly string[] | undefined;
    if (exactTraceIds.length > 0) {
      const locators = await this.locateTraces({
        projectId: input.projectId,
        traceIds: exactTraceIds,
      });
      if (locators.length === 0) return [];
      const located = locatorRange(locators);
      exactEventRange = located.range;
      partitionDates = located.partitionDates;
    }
    const compiled = compileTraceMetrics({
      ...input,
      exactEventRange,
      partitionDates,
    });
    const rows = await this.dependencies.query<DorisTraceMetricsRow>(
      compiled.sql,
      compiled.params,
    );
    return rows.map(decodeTraceMetrics);
  }

  async filterOptionValues(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly column: string;
    readonly limit: number;
    readonly offset?: number;
    readonly valueQuery?: string;
  }): Promise<readonly { readonly value: string; readonly count: number }[]> {
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > 1_000 ||
      !Number.isSafeInteger(input.offset ?? 0) ||
      (input.offset ?? 0) < 0
    ) {
      throw new InvalidRequestError("Invalid Doris trace facet page size");
    }
    const column = traceFilterOptionColumn(input.column);
    const matchedEvents = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: input.range,
      filters: input.filters,
    });
    const allEvents = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: input.range,
      filters: [],
    });
    const isTags = column === "tags";
    const expression = isTags
      ? "value"
      : TRACE_FILTER_OPTION_EXPRESSIONS[column];
    const lateralView = isTags
      ? "\nLATERAL VIEW explode(r.tags) exploded AS value"
      : "";
    const valueQuery = input.valueQuery?.trim();
    const includeWhen = `${expression} IS NOT NULL AND ${expression} != ''${valueQuery ? ` AND LOWER(${expression}) LIKE CONCAT('%', LOWER(?), '%')` : ""}`;
    const offsetSql = input.offset ? " OFFSET ?" : "";
    const rows = await this.dependencies.query<{
      readonly value: unknown;
      readonly count: unknown;
    }>(
      `WITH matched_trace_ids AS (
  SELECT DISTINCT e.trace_id
  ${matchedEvents.fromSql}
  WHERE ${matchedEvents.whereSql}
), ranked_events AS (
  SELECT
    e.trace_id,
    e.name,
    e.user_id,
    e.session_id,
    e.tags,
    ROW_NUMBER() OVER (
      PARTITION BY e.trace_id
      ORDER BY e.is_app_root DESC,
        CASE WHEN e.parent_span_id IS NULL OR e.parent_span_id = '' THEN 0 ELSE 1 END,
        e.start_time ASC,
        e.span_id ASC
    ) AS representative_rank
  ${allEvents.fromSql}
  INNER JOIN matched_trace_ids matched ON matched.trace_id = e.trace_id
  WHERE ${allEvents.whereSql}
), representatives AS (
  SELECT name, user_id, session_id, tags
  FROM ranked_events
  WHERE representative_rank = 1
)
SELECT ${expression} AS value, COUNT(*) AS count
FROM representatives r${lateralView}
WHERE ${includeWhen}
GROUP BY value
ORDER BY ${isTags ? "value ASC" : "count DESC, value ASC"}
LIMIT ?${offsetSql}`,
      [
        ...matchedEvents.params,
        ...allEvents.params,
        ...(valueQuery ? [valueQuery] : []),
        input.limit,
        ...(input.offset ? [input.offset] : []),
      ],
    );
    return rows.map((row) => ({
      value: String(row.value),
      count: numberValue(row.count),
    }));
  }

  async get(input: {
    readonly projectId: string;
    readonly traceId: string;
  }): Promise<DorisTrace | null> {
    const locators = await this.locateTrace(input);
    if (locators.length === 0) return null;
    const { range, partitionDates } = locatorRange(locators);
    const compiled = compileTraceList({
      projectId: input.projectId,
      range,
      filters: [
        {
          type: "string",
          column: "traceId",
          operator: "=",
          value: input.traceId,
        },
      ],
      limit: 1,
      partitionDates,
      includeFullContent: true,
    });
    const rows = await this.dependencies.query<DorisTraceRow>(
      compiled.sql,
      compiled.params,
    );
    return rows[0] ? decodeTrace(rows[0]) : null;
  }

  async getMany(input: {
    readonly projectId: string;
    readonly traceIds: readonly string[];
  }): Promise<readonly DorisTrace[]> {
    const traceIds = [...new Set(input.traceIds)];
    if (traceIds.length === 0) return [];
    const locators = await this.locateTraces({
      projectId: input.projectId,
      traceIds,
    });
    if (locators.length === 0) return [];
    const locatedTraceIds = [
      ...new Set(locators.map(({ traceId }) => traceId)),
    ];
    const traces: DorisTrace[] = [];
    for (
      let offset = 0;
      offset < locatedTraceIds.length;
      offset += MAX_PAGE_SIZE
    ) {
      const chunk = locatedTraceIds.slice(offset, offset + MAX_PAGE_SIZE);
      const chunkIds = new Set(chunk);
      const chunkLocators = locators.filter(({ traceId }) =>
        chunkIds.has(traceId),
      );
      const { range, partitionDates } = locatorRange(chunkLocators);
      const compiled = compileTraceList({
        projectId: input.projectId,
        range,
        filters: [
          {
            type: "stringOptions",
            column: "traceId",
            operator: "any of",
            value: chunk,
          },
        ],
        limit: chunk.length,
        partitionDates,
        includeFullContent: true,
      });
      const rows = await this.dependencies.query<DorisTraceRow>(
        compiled.sql,
        compiled.params,
      );
      for (const row of rows) {
        traces.push(decodeTrace(row));
      }
    }
    return traces;
  }

  async countByProjectCreatedAt(input: {
    readonly start: Date;
    readonly end: Date;
  }): Promise<
    readonly { readonly projectId: string; readonly count: number }[]
  > {
    const rows = await this.dependencies.query<{
      readonly project_id: string;
      readonly count: unknown;
    }>(
      `SELECT project_id, COUNT(*) AS count
FROM (
  SELECT e.project_id, e.trace_id, MIN(e.created_at) AS created_at
  FROM events_current e
  LEFT JOIN trace_tombstones trace_deletion
    ON trace_deletion.project_id = e.project_id
   AND trace_deletion.trace_id = e.trace_id
  LEFT JOIN project_tombstones project_deletion
    ON project_deletion.project_id = e.project_id
  WHERE trace_deletion.trace_id IS NULL
    AND project_deletion.project_id IS NULL
  GROUP BY e.project_id, e.trace_id
) traces
WHERE created_at >= ? AND created_at < ?
GROUP BY project_id`,
      [input.start, input.end],
    );
    return rows.map((row) => ({
      projectId: row.project_id,
      count: numberValue(row.count),
    }));
  }

  async countProjectsSince(input: {
    readonly projectIds: readonly string[];
    readonly start: Date;
  }): Promise<number> {
    if (input.projectIds.length === 0) return 0;
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      `SELECT COUNT(*) AS count
FROM (
  SELECT e.project_id, e.trace_id, MIN(e.created_at) AS created_at
  FROM events_current e
  LEFT JOIN trace_tombstones trace_deletion
    ON trace_deletion.project_id = e.project_id
   AND trace_deletion.trace_id = e.trace_id
  LEFT JOIN project_tombstones project_deletion
    ON project_deletion.project_id = e.project_id
  WHERE e.project_id IN (?)
    AND trace_deletion.trace_id IS NULL
    AND project_deletion.project_id IS NULL
  GROUP BY e.project_id, e.trace_id
) traces
WHERE created_at >= ?`,
      [[...input.projectIds], input.start],
    );
    return rows[0] ? numberValue(rows[0].count) : 0;
  }

  async countByProjectAndDay(input: {
    readonly start: Date;
    readonly end: Date;
  }): Promise<
    readonly {
      readonly projectId: string;
      readonly date: string;
      readonly count: number;
    }[]
  > {
    const rows = await this.dependencies.query<{
      readonly project_id: string;
      readonly date: string;
      readonly count: unknown;
    }>(
      `SELECT project_id, CAST(DATE(trace_timestamp) AS STRING) AS date, COUNT(*) AS count
FROM (
  SELECT e.project_id, e.trace_id, MIN(e.start_time) AS trace_timestamp
  FROM events_current e
  LEFT JOIN trace_tombstones trace_deletion
    ON trace_deletion.project_id = e.project_id
   AND trace_deletion.trace_id = e.trace_id
  LEFT JOIN project_tombstones project_deletion
    ON project_deletion.project_id = e.project_id
  WHERE trace_deletion.trace_id IS NULL
    AND project_deletion.project_id IS NULL
  GROUP BY e.project_id, e.trace_id
) traces
WHERE trace_timestamp >= ? AND trace_timestamp < ?
GROUP BY project_id, DATE(trace_timestamp)`,
      [input.start, input.end],
    );
    return rows.map((row) => ({
      projectId: row.project_id,
      date: row.date,
      count: numberValue(row.count),
    }));
  }
}
