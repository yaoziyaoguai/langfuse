import type { TracingSearchType } from "../../../../interfaces/search";
import type { EventsTableFilterState } from "../../../../types";
import { InvalidRequestError } from "../../../../errors";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import { compileDorisVisibleEventScope } from "../../../queries/doris-sql/eventQueryCompiler";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";

const MAX_PAGE_SIZE = 999;

type DorisSessionRow = Record<string, unknown> & {
  readonly project_id: string;
  readonly session_id: string;
  readonly min_timestamp: string | Date;
  readonly max_timestamp: string | Date;
};

export type DorisSession = {
  readonly id: string;
  readonly projectId: string;
  readonly minTimestamp: Date;
  readonly maxTimestamp: Date;
  readonly traceIds: readonly string[];
  readonly userIds: readonly string[];
  readonly environments: readonly string[];
  readonly tags: readonly string[];
  readonly traceCount: number;
  readonly observationCount: number;
  readonly totalInputTokens: number;
  readonly totalOutputTokens: number;
  readonly totalUsage: number;
  readonly totalCost: number | null;
  readonly duration: number;
};

export type DorisSessionsPage = {
  readonly items: readonly DorisSession[];
  readonly nextCursor: string | null;
};

function dateTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value !== "string") {
    throw new TypeError("Doris returned an invalid session timestamp");
  }
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (Number.isNaN(parsed.getTime())) {
    throw new TypeError("Doris returned an invalid session timestamp");
  }
  return parsed;
}

function numberValue(value: unknown): number {
  const parsed = Number(value ?? 0);
  if (!Number.isFinite(parsed)) {
    throw new TypeError("Doris returned an invalid session number");
  }
  return parsed;
}

function nullableNumber(value: unknown): number | null {
  return value === null || value === undefined || value === ""
    ? null
    : numberValue(value);
}

function stringArray(value: unknown): readonly string[] {
  const parsed = parseJsonIfString(value);
  return Array.isArray(parsed)
    ? [...new Set(parsed.filter((item) => item !== null).map(String))].sort()
    : [];
}

function nestedStringArray(value: unknown): readonly string[] {
  const parsed = parseJsonIfString(value);
  if (!Array.isArray(parsed)) return [];
  return [
    ...new Set(
      parsed.flatMap((item) => {
        const array = parseJsonIfString(item);
        return Array.isArray(array)
          ? array.filter((value) => value !== null).map(String)
          : [];
      }),
    ),
  ].sort();
}

function decodeSession(row: DorisSessionRow): DorisSession {
  const minTimestamp = dateTime(row.min_timestamp);
  const maxTimestamp = dateTime(row.max_timestamp);
  const totalInputTokens = numberValue(row.total_input_tokens);
  const totalOutputTokens = numberValue(row.total_output_tokens);
  return {
    id: row.session_id,
    projectId: row.project_id,
    minTimestamp,
    maxTimestamp,
    traceIds: stringArray(row.trace_ids),
    userIds: stringArray(row.user_ids),
    environments: stringArray(row.environments),
    tags: nestedStringArray(row.trace_tag_sets),
    traceCount: numberValue(row.trace_count),
    observationCount: numberValue(row.observation_count),
    totalInputTokens,
    totalOutputTokens,
    totalUsage: totalInputTokens + totalOutputTokens,
    totalCost: nullableNumber(row.total_cost),
    duration: (maxTimestamp.getTime() - minTimestamp.getTime()) / 1_000,
  };
}

function encodeCursor(session: DorisSession): string {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      maxTimestamp: session.maxTimestamp.toISOString(),
      sessionId: session.id,
    }),
    "utf8",
  ).toString("base64url");
}

function decodeCursor(
  cursor: string | undefined,
): { readonly maxTimestamp: Date; readonly sessionId: string } | undefined {
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
      typeof value.maxTimestamp !== "string" ||
      typeof value.sessionId !== "string" ||
      !value.sessionId
    ) {
      throw new Error();
    }
    return {
      maxTimestamp: dateTime(value.maxTimestamp),
      sessionId: value.sessionId,
    };
  } catch {
    throw new InvalidRequestError("Invalid Doris session cursor");
  }
}

function identifierPattern(value: string): string {
  return `%${value
    .toLowerCase()
    .replaceAll("\\", "\\\\")
    .replaceAll("%", "\\%")
    .replaceAll("_", "\\_")}%`;
}

function compileSessionList(input: {
  readonly projectId: string;
  readonly range: AnalyticsTimeRange | null;
  readonly filters: EventsTableFilterState;
  readonly search?: {
    readonly query: string;
    readonly searchType?: readonly TracingSearchType[];
  };
  readonly identifierQuery?: string;
  readonly cursor?: string;
  readonly limit: number;
}): { readonly sql: string; readonly params: readonly unknown[] } {
  if (
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > MAX_PAGE_SIZE
  ) {
    throw new InvalidRequestError("Invalid Doris session page size");
  }
  const matchedEvents = compileDorisVisibleEventScope({
    projectId: input.projectId,
    range: input.range,
    filters: input.filters,
    search: input.search,
  });
  const allEvents = compileDorisVisibleEventScope({
    projectId: input.projectId,
    range: input.range,
    filters: [],
  });
  const cursor = decodeCursor(input.cursor);
  const identifierSql = input.identifierQuery
    ? "AND LOWER(e.session_id) LIKE ? ESCAPE '\\\\'"
    : "";
  const cursorSql = cursor
    ? `WHERE (
      max_timestamp < ?
      OR (max_timestamp = ? AND session_id < ?)
    )`
    : "";
  return {
    sql: `WITH matched_session_ids AS (
  SELECT DISTINCT e.session_id
  ${matchedEvents.fromSql}
  WHERE ${matchedEvents.whereSql}
    AND e.session_id IS NOT NULL
    AND e.session_id != ''
    ${identifierSql}
), aggregated_sessions AS (
  SELECT
    e.project_id,
    e.session_id,
    MIN(e.start_time) AS min_timestamp,
    MAX(COALESCE(e.end_time, e.start_time)) AS max_timestamp,
    COLLECT_SET(e.trace_id) AS trace_ids,
    COLLECT_SET(e.user_id) AS user_ids,
    COLLECT_SET(e.environment) AS environments,
    COLLECT_SET(CAST(e.tags AS STRING)) AS trace_tag_sets,
    COUNT(DISTINCT e.trace_id) AS trace_count,
    COUNT(*) AS observation_count,
    SUM(COALESCE(e.total_input_tokens, 0)) AS total_input_tokens,
    SUM(COALESCE(e.total_output_tokens, 0)) AS total_output_tokens,
    SUM(e.total_cost) AS total_cost
  ${allEvents.fromSql}
  INNER JOIN matched_session_ids matched ON matched.session_id = e.session_id
  WHERE ${allEvents.whereSql}
  GROUP BY e.project_id, e.session_id
)
SELECT *
FROM aggregated_sessions
${cursorSql}
ORDER BY max_timestamp DESC, session_id DESC
LIMIT ?`,
    params: [
      ...matchedEvents.params,
      ...(input.identifierQuery
        ? [identifierPattern(input.identifierQuery)]
        : []),
      ...allEvents.params,
      ...(cursor
        ? [cursor.maxTimestamp, cursor.maxTimestamp, cursor.sessionId]
        : []),
      input.limit + 1,
    ],
  };
}

export class DorisSessionsRepository {
  constructor(
    private readonly dependencies: {
      readonly query: DorisQueryExecutor["query"];
    },
  ) {}

  async list(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly identifierQuery?: string;
    readonly cursor?: string;
    readonly limit: number;
  }): Promise<DorisSessionsPage> {
    const compiled = compileSessionList(input);
    const rows = await this.dependencies.query<DorisSessionRow>(
      compiled.sql,
      compiled.params,
    );
    const items = rows.slice(0, input.limit).map(decodeSession);
    return {
      items,
      nextCursor:
        rows.length > input.limit && items.length > 0
          ? encodeCursor(items[items.length - 1]!)
          : null,
    };
  }

  async get(input: {
    readonly projectId: string;
    readonly sessionId: string;
    readonly range: AnalyticsTimeRange | null;
  }): Promise<DorisSession | null> {
    const page = await this.list({
      projectId: input.projectId,
      range: input.range,
      filters: [
        {
          type: "string",
          column: "sessionId",
          operator: "=",
          value: input.sessionId,
        },
      ],
      limit: 1,
    });
    return page.items[0] ?? null;
  }
}
