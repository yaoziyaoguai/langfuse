import type { TracingSearchType } from "../../../../interfaces/search";
import type { EventsTableFilterState } from "../../../../types";
import { InvalidRequestError } from "../../../../errors";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import { compileDorisVisibleEventScope } from "../../../queries/doris-sql/eventQueryCompiler";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";
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
  readonly partitionDates?: readonly string[];
  readonly includeFullContent?: boolean;
}): { readonly sql: string; readonly params: readonly unknown[] } {
  if (
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > MAX_PAGE_SIZE
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
ORDER BY trace_timestamp DESC, trace_id DESC
LIMIT ?`;
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
  }): Promise<DorisTracesPage> {
    const compiled = compileTraceList(input);
    const rows = await this.dependencies.query<DorisTraceRow>(
      compiled.sql,
      compiled.params,
    );
    const items = rows.slice(0, input.limit).map(decodeTrace);
    return {
      items,
      nextCursor:
        rows.length > input.limit && items.length > 0
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
}
