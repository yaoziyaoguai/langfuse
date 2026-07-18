import type { TracingSearchType } from "../../../../interfaces/search";
import type { EventsTableFilterState, FilterState } from "../../../../types";
import { InvalidRequestError } from "../../../../errors";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import { compileDorisVisibleEventScope } from "../../../queries/doris-sql/eventQueryCompiler";
import { compileDorisEventFilters } from "../../../queries/doris-sql/filterCompiler";
import type { LogicalEventFilter } from "../../../queries/logical/filterPlan";
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
    environments: Object.hasOwn(row, "environment")
      ? [String(row.environment)]
      : stringArray(row.environments),
    tags: Object.hasOwn(row, "trace_tags")
      ? stringArray(row.trace_tags)
      : nestedStringArray(row.trace_tag_sets),
    traceCount: numberValue(row.trace_count),
    observationCount: numberValue(row.observation_count),
    totalInputTokens,
    totalOutputTokens,
    totalUsage: totalInputTokens + totalOutputTokens,
    totalCost: nullableNumber(row.total_cost),
    duration: (maxTimestamp.getTime() - minTimestamp.getTime()) / 1_000,
  };
}

function encodeCursor(session: DorisSession, order: "ASC" | "DESC"): string {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      order,
      minTimestamp: session.minTimestamp.toISOString(),
      sessionId: session.id,
    }),
    "utf8",
  ).toString("base64url");
}

function decodeCursor(
  cursor: string | undefined,
  expectedOrder: "ASC" | "DESC",
): { readonly minTimestamp: Date; readonly sessionId: string } | undefined {
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
      value.order !== expectedOrder ||
      typeof value.minTimestamp !== "string" ||
      typeof value.sessionId !== "string" ||
      !value.sessionId
    ) {
      throw new Error();
    }
    return {
      minTimestamp: dateTime(value.minTimestamp),
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

type SessionQueryInput = {
  readonly projectId: string;
  readonly range: AnalyticsTimeRange | null;
  readonly filters: EventsTableFilterState;
  readonly sessionFilters?: FilterState;
  readonly search?: {
    readonly query: string;
    readonly searchType?: readonly TracingSearchType[];
  };
  readonly identifierQuery?: string;
  readonly order?: "ASC" | "DESC";
};

function compileSessionFilterPredicates(
  filters: FilterState,
  params: unknown[],
): readonly string[] {
  const plans: LogicalEventFilter[] = [];
  const predicates: string[] = [];
  for (const filter of filters) {
    if (filter.type === "stringOptions" && filter.value.length === 0) {
      predicates.push(filter.operator === "any of" ? "FALSE" : "TRUE");
      continue;
    }
    switch (filter.column) {
      case "createdAt":
        if (filter.type !== "datetime") break;
        plans.push({ filter, expression: "min_timestamp" });
        continue;
      case "id":
        if (filter.type !== "string" && filter.type !== "stringOptions") break;
        plans.push({ filter, expression: "session_id" });
        continue;
      case "userIds":
        if (filter.type !== "arrayOptions") break;
        plans.push({ filter, expression: "user_ids" });
        continue;
      case "environment":
        if (
          filter.type !== "string" &&
          filter.type !== "stringOptions" &&
          filter.type !== "null"
        ) {
          break;
        }
        plans.push({ filter, expression: "environment" });
        continue;
      case "traceTags":
        if (filter.type !== "arrayOptions") break;
        plans.push({ filter, expression: "trace_tags" });
        continue;
      case "metadata":
        if (filter.type !== "stringObject") break;
        plans.push({
          filter,
          expression:
            "JSON_UNQUOTE(CAST(ELEMENT_AT(CAST(metadata_json AS VARIANT), ?) AS STRING))",
          objectKey: filter.key,
        });
        continue;
    }
    throw new InvalidRequestError(
      `Unsupported Doris session aggregate filter: ${filter.column}`,
    );
  }
  return [
    ...predicates,
    ...compileDorisEventFilters(plans, {
      params,
      bind(value: unknown) {
        params.push(value);
        return "?";
      },
    }),
  ];
}

function compileSessionAggregation(input: SessionQueryInput): {
  readonly sql: string;
  readonly params: unknown[];
} {
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
  const identifierSql = input.identifierQuery
    ? "AND LOWER(e.session_id) LIKE ? ESCAPE '\\\\'"
    : "";
  const latestOrder = `CONCAT(
      DATE_FORMAT(e.start_time, '%Y%m%d%H%i%s.%f'), ':',
      LPAD(CAST(e.version_token AS STRING), 20, '0'), ':', e.span_id
    )`;
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
    MAX_BY(e.environment, ${latestOrder}) AS environment,
    ARRAY_DISTINCT(ARRAY_FLATTEN(COLLECT_LIST(e.tags))) AS trace_tags,
    MAX_BY(CAST(e.metadata AS STRING), ${latestOrder}) AS metadata_json,
    COUNT(DISTINCT e.trace_id) AS trace_count,
    COUNT(*) AS observation_count,
    SUM(COALESCE(e.total_input_tokens, 0)) AS total_input_tokens,
    SUM(COALESCE(e.total_output_tokens, 0)) AS total_output_tokens,
    SUM(e.total_cost) AS total_cost
  ${allEvents.fromSql}
  INNER JOIN matched_session_ids matched ON matched.session_id = e.session_id
  WHERE ${allEvents.whereSql}
  GROUP BY e.project_id, e.session_id
)`,
    params: [
      ...matchedEvents.params,
      ...(input.identifierQuery
        ? [identifierPattern(input.identifierQuery)]
        : []),
      ...allEvents.params,
    ],
  };
}

function compileSessionList(
  input: SessionQueryInput & {
    readonly cursor?: string;
    readonly limit: number;
  },
): { readonly sql: string; readonly params: readonly unknown[] } {
  if (
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > MAX_PAGE_SIZE
  ) {
    throw new InvalidRequestError("Invalid Doris session page size");
  }
  const aggregation = compileSessionAggregation(input);
  const params = [...aggregation.params];
  const sessionPredicates = [
    ...compileSessionFilterPredicates(input.sessionFilters ?? [], params),
  ];
  const order = input.order ?? "DESC";
  const cursor = decodeCursor(input.cursor, order);
  if (cursor) {
    const comparison = order === "DESC" ? "<" : ">";
    sessionPredicates.push(`(
      min_timestamp ${comparison} ?
      OR (min_timestamp = ? AND session_id ${comparison} ?)
    )`);
    params.push(cursor.minTimestamp, cursor.minTimestamp, cursor.sessionId);
  }
  const whereSql =
    sessionPredicates.length > 0
      ? `WHERE ${sessionPredicates.join("\n  AND ")}`
      : "";
  return {
    sql: `${aggregation.sql}
SELECT *
FROM aggregated_sessions
${whereSql}
ORDER BY min_timestamp ${order}, session_id ${order}
LIMIT ?`,
    params: [...params, input.limit + 1],
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
    readonly sessionFilters?: FilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly identifierQuery?: string;
    readonly order?: "ASC" | "DESC";
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
          ? encodeCursor(items[items.length - 1]!, input.order ?? "DESC")
          : null,
    };
  }

  async count(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly sessionFilters?: FilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly identifierQuery?: string;
  }): Promise<number> {
    const aggregation = compileSessionAggregation(input);
    const params = [...aggregation.params];
    const predicates = compileSessionFilterPredicates(
      input.sessionFilters ?? [],
      params,
    );
    const whereSql =
      predicates.length > 0 ? `\nWHERE ${predicates.join("\n  AND ")}` : "";
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      `${aggregation.sql}
SELECT COUNT(*) AS count
FROM aggregated_sessions${whereSql}`,
      params,
    );
    return numberValue(rows[0]?.count);
  }

  async get(input: {
    readonly projectId: string;
    readonly sessionId: string;
    readonly range: AnalyticsTimeRange | null;
  }): Promise<DorisSession | null> {
    const page = await this.list({
      projectId: input.projectId,
      range: input.range,
      filters: [],
      sessionFilters: [
        {
          type: "string",
          column: "id",
          operator: "=",
          value: input.sessionId,
        },
      ],
      limit: 1,
    });
    return page.items[0] ?? null;
  }
}
