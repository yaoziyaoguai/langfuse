import type { TracingSearchType } from "../../../../interfaces/search";
import type { EventsTableFilterState } from "../../../../types";
import { InvalidRequestError } from "../../../../errors";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import { compileDorisVisibleEventScope } from "../../../queries/doris-sql/eventQueryCompiler";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";

const MAX_PAGE_SIZE = 999;

type DorisUserRow = Record<string, unknown> & {
  readonly project_id: string;
  readonly user_id: string;
  readonly min_timestamp: string | Date;
  readonly max_timestamp: string | Date;
};

export type DorisUser = {
  readonly id: string;
  readonly projectId: string;
  readonly minTimestamp: Date;
  readonly maxTimestamp: Date;
  readonly sessionIds: readonly string[];
  readonly environments: readonly string[];
  readonly traceCount: number;
  readonly sessionCount: number;
  readonly observationCount: number;
  readonly totalInputTokens: number;
  readonly totalOutputTokens: number;
  readonly totalUsage: number;
  readonly totalCost: number | null;
};

export type DorisUsersPage = {
  readonly items: readonly DorisUser[];
  readonly nextCursor: string | null;
};

function dateTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value !== "string") {
    throw new TypeError("Doris returned an invalid user timestamp");
  }
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (Number.isNaN(parsed.getTime())) {
    throw new TypeError("Doris returned an invalid user timestamp");
  }
  return parsed;
}

function numberValue(value: unknown): number {
  const parsed = Number(value ?? 0);
  if (!Number.isFinite(parsed)) {
    throw new TypeError("Doris returned an invalid user number");
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

function decodeUser(row: DorisUserRow): DorisUser {
  const totalInputTokens = numberValue(row.total_input_tokens);
  const totalOutputTokens = numberValue(row.total_output_tokens);
  return {
    id: row.user_id,
    projectId: row.project_id,
    minTimestamp: dateTime(row.min_timestamp),
    maxTimestamp: dateTime(row.max_timestamp),
    sessionIds: stringArray(row.session_ids),
    environments: stringArray(row.environments),
    traceCount: numberValue(row.trace_count),
    sessionCount: numberValue(row.session_count),
    observationCount: numberValue(row.observation_count),
    totalInputTokens,
    totalOutputTokens,
    totalUsage: totalInputTokens + totalOutputTokens,
    totalCost: nullableNumber(row.total_cost),
  };
}

function encodeCursor(user: DorisUser): string {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      maxTimestamp: user.maxTimestamp.toISOString(),
      userId: user.id,
    }),
    "utf8",
  ).toString("base64url");
}

function decodeCursor(
  cursor: string | undefined,
): { readonly maxTimestamp: Date; readonly userId: string } | undefined {
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
      typeof value.userId !== "string" ||
      !value.userId
    ) {
      throw new Error();
    }
    return {
      maxTimestamp: dateTime(value.maxTimestamp),
      userId: value.userId,
    };
  } catch {
    throw new InvalidRequestError("Invalid Doris user cursor");
  }
}

function identifierPattern(value: string): string {
  return `%${value
    .toLowerCase()
    .replaceAll("\\", "\\\\")
    .replaceAll("%", "\\%")
    .replaceAll("_", "\\_")}%`;
}

function compileUserList(input: {
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
    throw new InvalidRequestError("Invalid Doris user page size");
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
    ? "AND LOWER(e.user_id) LIKE ? ESCAPE '\\\\'"
    : "";
  const cursorSql = cursor
    ? `WHERE (
      max_timestamp < ?
      OR (max_timestamp = ? AND user_id < ?)
    )`
    : "";
  return {
    sql: `WITH matched_user_ids AS (
  SELECT DISTINCT e.user_id
  ${matchedEvents.fromSql}
  WHERE ${matchedEvents.whereSql}
    AND e.user_id IS NOT NULL
    AND e.user_id != ''
    ${identifierSql}
), aggregated_users AS (
  SELECT
    e.project_id,
    e.user_id,
    MIN(e.start_time) AS min_timestamp,
    MAX(COALESCE(e.end_time, e.start_time)) AS max_timestamp,
    COLLECT_SET(e.session_id) AS session_ids,
    COLLECT_SET(e.environment) AS environments,
    COUNT(DISTINCT e.trace_id) AS trace_count,
    COUNT(DISTINCT e.session_id) AS session_count,
    COUNT(*) AS observation_count,
    SUM(COALESCE(e.total_input_tokens, 0)) AS total_input_tokens,
    SUM(COALESCE(e.total_output_tokens, 0)) AS total_output_tokens,
    SUM(e.total_cost) AS total_cost
  ${allEvents.fromSql}
  INNER JOIN matched_user_ids matched ON matched.user_id = e.user_id
  WHERE ${allEvents.whereSql}
  GROUP BY e.project_id, e.user_id
)
SELECT *
FROM aggregated_users
${cursorSql}
ORDER BY max_timestamp DESC, user_id DESC
LIMIT ?`,
    params: [
      ...matchedEvents.params,
      ...(input.identifierQuery
        ? [identifierPattern(input.identifierQuery)]
        : []),
      ...allEvents.params,
      ...(cursor
        ? [cursor.maxTimestamp, cursor.maxTimestamp, cursor.userId]
        : []),
      input.limit + 1,
    ],
  };
}

export class DorisUsersRepository {
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
  }): Promise<DorisUsersPage> {
    const compiled = compileUserList(input);
    const rows = await this.dependencies.query<DorisUserRow>(
      compiled.sql,
      compiled.params,
    );
    const items = rows.slice(0, input.limit).map(decodeUser);
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
    readonly userId: string;
    readonly range: AnalyticsTimeRange | null;
  }): Promise<DorisUser | null> {
    const page = await this.list({
      projectId: input.projectId,
      range: input.range,
      filters: [
        {
          type: "string",
          column: "userId",
          operator: "=",
          value: input.userId,
        },
      ],
      limit: 1,
    });
    return page.items[0] ?? null;
  }
}
