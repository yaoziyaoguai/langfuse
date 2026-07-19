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
  type EventHeadLocator,
} from "./entityHeadLocator";

const MAX_PAGE_SIZE = 999;

type LocateTrace = (input: {
  readonly projectId: string;
  readonly traceId: string;
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

function encodeCursor(trace: DorisTrace): string {
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

export class DorisTracesRepository {
  private readonly locateTrace: LocateTrace;

  constructor(
    private readonly dependencies: {
      readonly query: DorisQueryExecutor["query"];
      readonly locateTrace?: LocateTrace;
    },
  ) {
    this.locateTrace = dependencies.locateTrace ?? findTraceEventHeadLocators;
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
          ? encodeCursor(items[items.length - 1]!)
          : null,
    };
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
    const partitionDates = [
      ...new Set(locators.map(({ partitionDate }) => partitionDate)),
    ].sort();
    const from = new Date(`${partitionDates[0]}T00:00:00.000Z`);
    const to = new Date(
      `${partitionDates[partitionDates.length - 1]}T00:00:00.000Z`,
    );
    to.setUTCDate(to.getUTCDate() + 1);
    const compiled = compileTraceList({
      projectId: input.projectId,
      range: { from, to },
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
