import type {
  ScoreDataTypeType,
  ScoreDomain,
  ScoreSourceType,
  ListableScore,
} from "../../../../domain/scores";
import { InvalidRequestError, LangfuseConflictError } from "../../../../errors";
import type { EventsTableFilterState } from "../../../../types";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import { compileDorisEventFilters } from "../../../queries/doris-sql/filterCompiler";
import type { LogicalEventFilter } from "../../../queries/logical/filterPlan";
import {
  assertAnalyticsTimeRange,
  type AnalyticsTimeRange,
} from "../../../queries/logical/searchPlan";
import {
  findScoreHeadLocators,
  type ScoreHeadLocator,
} from "./entityHeadLocator";

const MAX_PAGE_SIZE = 999;

const SCORE_PROJECTION = `
  s.project_id,
  s.score_date,
  s.score_id,
  s.trace_id,
  s.observation_id,
  s.session_id,
  s.\`name\`,
  s.\`source\`,
  s.data_type,
  s.\`value\`,
  s.string_value,
  s.long_string_value,
  s.boolean_value,
  s.\`comment\`,
  s.author_user_id,
  s.config_id,
  s.queue_id,
  s.environment,
  s.metadata,
  s.\`timestamp\`,
  s.created_at,
  s.updated_at`;

const SCORE_COLUMNS: Readonly<Record<string, string>> = {
  id: "s.score_id",
  scoreId: "s.score_id",
  traceId: "s.trace_id",
  observationId: "s.observation_id",
  sessionId: "s.session_id",
  name: "s.`name`",
  source: "s.`source`",
  dataType: "s.data_type",
  value: "s.`value`",
  stringValue: "s.string_value",
  booleanValue: "s.boolean_value",
  comment: "s.`comment`",
  authorUserId: "s.author_user_id",
  configId: "s.config_id",
  queueId: "s.queue_id",
  environment: "s.environment",
  timestamp: "s.`timestamp`",
  createdAt: "s.created_at",
  updatedAt: "s.updated_at",
};

const SCORE_ORDER_COLUMNS: Readonly<Record<string, string>> = {
  id: "s.score_id",
  timestamp: "s.`timestamp`",
  environment: "s.environment",
  traceId: "s.trace_id",
  observationId: "s.observation_id",
  sessionId: "s.session_id",
  name: "s.`name`",
  value: "s.`value`",
  booleanValue: "s.boolean_value",
  source: "s.`source`",
  comment: "s.`comment`",
  authorUserId: "s.author_user_id",
  dataType: "s.data_type",
  stringValue: "s.string_value",
  createdAt: "s.created_at",
  updatedAt: "s.updated_at",
};

type LocateScore = (input: {
  readonly projectId: string;
  readonly scoreId: string;
}) => Promise<readonly ScoreHeadLocator[]>;

type DorisScoreRow = Record<string, unknown> & {
  readonly project_id: string;
  readonly score_id: string;
};

export type DorisScoreAnalyticsIdentifier = {
  readonly name: string;
  readonly source: string;
  readonly dataType: string;
};

export type DorisScoreAnalyticsObjectType =
  | "all"
  | "trace"
  | "session"
  | "observation";

export type DorisScoresPage = {
  readonly items: readonly ScoreDomain[];
  readonly nextCursor: string | null;
};

export type DorisPromptScore = ListableScore & {
  readonly promptId: string;
  readonly hasMetadata: boolean;
};

function dateTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value !== "string") {
    throw new TypeError("Doris returned an invalid score timestamp");
  }
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (Number.isNaN(parsed.getTime())) {
    throw new TypeError("Doris returned an invalid score timestamp");
  }
  return parsed;
}

function nullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

function numberValue(value: unknown): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new TypeError("Doris returned an invalid score value");
  }
  return parsed;
}

function metadataValue(value: unknown): Readonly<Record<string, unknown>> {
  const parsed = parseJsonIfString(value);
  return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
    ? (parsed as Readonly<Record<string, unknown>>)
    : {};
}

function decodeScore(row: DorisScoreRow): ScoreDomain {
  const dataType = String(row.data_type) as ScoreDataTypeType;
  const numericValue =
    row.value === null || row.value === undefined
      ? row.boolean_value === true ||
        row.boolean_value === 1 ||
        row.boolean_value === "1"
        ? 1
        : 0
      : numberValue(row.value);
  const base = {
    id: row.score_id,
    projectId: row.project_id,
    environment: String(row.environment),
    name: String(row.name),
    value: numericValue,
    source: String(row.source) as ScoreSourceType,
    authorUserId: nullableString(row.author_user_id),
    comment: nullableString(row.comment),
    metadata: metadataValue(row.metadata),
    configId: nullableString(row.config_id),
    queueId: nullableString(row.queue_id),
    executionTraceId: null,
    createdAt: dateTime(row.created_at),
    updatedAt: dateTime(row.updated_at),
    timestamp: dateTime(row.timestamp),
    traceId: nullableString(row.trace_id),
    sessionId: nullableString(row.session_id),
    datasetRunId: null,
    observationId: nullableString(row.observation_id),
    longStringValue: nullableString(row.long_string_value) ?? "",
  };

  if (dataType === "NUMERIC" || dataType === "CORRECTION") {
    return { ...base, dataType, stringValue: null } as ScoreDomain;
  }
  const stringValue =
    nullableString(row.string_value) ??
    (dataType === "BOOLEAN" ? (numericValue === 1 ? "True" : "False") : "");
  return { ...base, dataType, stringValue } as ScoreDomain;
}

function utcDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function exclusivePartitionTo(value: Date): string {
  const lastIncluded = new Date(value.getTime() - 1);
  lastIncluded.setUTCDate(lastIncluded.getUTCDate() + 1);
  return utcDate(lastIncluded);
}

function nextUtcDay(partitionDate: string): Date {
  const value = new Date(`${partitionDate}T00:00:00.000Z`);
  value.setUTCDate(value.getUTCDate() + 1);
  return value;
}

function encodeCursor(score: ScoreDomain): string {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      timestamp: score.timestamp.toISOString(),
      scoreId: score.id,
    }),
    "utf8",
  ).toString("base64url");
}

function decodeCursor(
  cursor: string | undefined,
): { readonly timestamp: Date; readonly scoreId: string } | undefined {
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
      typeof value.scoreId !== "string" ||
      !value.scoreId
    ) {
      throw new Error();
    }
    return { timestamp: dateTime(value.timestamp), scoreId: value.scoreId };
  } catch {
    throw new InvalidRequestError("Invalid Doris score cursor");
  }
}

function scoreFilterPlans(
  filters: EventsTableFilterState,
): LogicalEventFilter[] {
  return filters.map((filter) => {
    if (filter.type === "positionInTrace") {
      throw new InvalidRequestError("Unsupported Doris score position filter");
    }
    if (
      filter.type === "stringObject" ||
      filter.type === "numberObject" ||
      filter.type === "booleanObject" ||
      filter.type === "categoryOptions"
    ) {
      if (filter.type !== "stringObject" || filter.column !== "metadata") {
        throw new InvalidRequestError(
          `Unsupported Doris score object filter: ${filter.column}`,
        );
      }
      return {
        filter,
        expression: "JSON_UNQUOTE(CAST(ELEMENT_AT(s.metadata, ?) AS STRING))",
        objectKey: filter.key,
      };
    }
    const expression = SCORE_COLUMNS[filter.column];
    if (!expression) {
      throw new InvalidRequestError(
        `Unsupported Doris score filter column: ${filter.column}`,
      );
    }
    return { filter, expression };
  });
}

function compileScope(input: {
  readonly projectId: string;
  readonly range: AnalyticsTimeRange;
  readonly filters: EventsTableFilterState;
  readonly cursor?: { readonly timestamp: Date; readonly scoreId: string };
}): {
  readonly fromSql: string;
  readonly whereSql: string;
  readonly params: readonly unknown[];
} {
  if (!input.projectId) {
    throw new InvalidRequestError("Invalid Doris score query input");
  }
  assertAnalyticsTimeRange(input.range);
  const params: unknown[] = [];
  const bound = {
    params,
    bind(value: unknown) {
      params.push(value);
      return "?";
    },
  };
  const predicates = [
    `s.project_id = ${bound.bind(input.projectId)}`,
    `s.score_date >= ${bound.bind(utcDate(input.range.from))}`,
    `s.score_date < ${bound.bind(exclusivePartitionTo(input.range.to))}`,
    `s.\`timestamp\` >= ${bound.bind(input.range.from)}`,
    `s.\`timestamp\` < ${bound.bind(input.range.to)}`,
    "trace_deletion.trace_id IS NULL",
    "project_deletion.project_id IS NULL",
    ...compileDorisEventFilters(scoreFilterPlans(input.filters), bound),
  ];
  if (input.cursor) {
    predicates.push(
      `(s.\`timestamp\` < ${bound.bind(input.cursor.timestamp)} OR (s.\`timestamp\` = ${bound.bind(input.cursor.timestamp)} AND s.score_id < ${bound.bind(input.cursor.scoreId)}))`,
    );
  }
  return {
    fromSql: `FROM scores_current s
LEFT JOIN trace_tombstones trace_deletion
  ON trace_deletion.project_id = s.project_id
 AND trace_deletion.trace_id = s.trace_id
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = s.project_id`,
    whereSql: predicates.join("\n  AND "),
    params,
  };
}

function analyticsFilters(
  identifier: DorisScoreAnalyticsIdentifier,
  objectType: DorisScoreAnalyticsObjectType,
): EventsTableFilterState {
  const filters: EventsTableFilterState = [
    { type: "string", column: "name", operator: "=", value: identifier.name },
    {
      type: "string",
      column: "source",
      operator: "=",
      value: identifier.source,
    },
    {
      type: "string",
      column: "dataType",
      operator: "=",
      value: identifier.dataType,
    },
  ];
  if (objectType === "trace") {
    filters.push(
      { type: "null", column: "traceId", operator: "is not null", value: "" },
      { type: "null", column: "observationId", operator: "is null", value: "" },
      { type: "null", column: "sessionId", operator: "is null", value: "" },
    );
  } else if (objectType === "observation") {
    filters.push({
      type: "null",
      column: "observationId",
      operator: "is not null",
      value: "",
    });
  } else if (objectType === "session") {
    filters.push(
      { type: "null", column: "sessionId", operator: "is not null", value: "" },
      { type: "null", column: "observationId", operator: "is null", value: "" },
      { type: "null", column: "traceId", operator: "is null", value: "" },
    );
  }
  return filters;
}

export class DorisScoresRepository {
  private readonly locateScore: LocateScore;

  constructor(
    private readonly dependencies: {
      readonly query: DorisQueryExecutor["query"];
      readonly locateScore?: LocateScore;
    },
  ) {
    this.locateScore = dependencies.locateScore ?? findScoreHeadLocators;
  }

  async list(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange;
    readonly filters: EventsTableFilterState;
    readonly cursor?: string;
    readonly limit: number;
    readonly offset?: number;
    readonly orderBy?: {
      readonly column: string;
      readonly order: "ASC" | "DESC";
    };
  }): Promise<DorisScoresPage> {
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > MAX_PAGE_SIZE ||
      !Number.isSafeInteger(input.offset ?? 0) ||
      (input.offset ?? 0) < 0
    ) {
      throw new InvalidRequestError("Invalid Doris score page size");
    }
    if (input.cursor && input.offset) {
      throw new InvalidRequestError(
        "Doris score cursor and offset are exclusive",
      );
    }
    if (input.cursor && input.orderBy) {
      throw new InvalidRequestError(
        "Doris score cursor cannot be combined with custom ordering",
      );
    }
    const orderExpression = input.orderBy
      ? SCORE_ORDER_COLUMNS[input.orderBy.column]
      : undefined;
    if (input.orderBy && !orderExpression) {
      throw new InvalidRequestError(
        `Unsupported Doris score order column: ${input.orderBy.column}`,
      );
    }
    const scope = compileScope({
      ...input,
      cursor: decodeCursor(input.cursor),
    });
    const offsetSql = input.offset ? " OFFSET ?" : "";
    const rows = await this.dependencies.query<DorisScoreRow>(
      `SELECT ${SCORE_PROJECTION}\n${scope.fromSql}\nWHERE ${scope.whereSql}\nORDER BY ${orderExpression ? `${orderExpression} ${input.orderBy!.order}, ` : ""}s.\`timestamp\` DESC, s.score_id DESC\nLIMIT ?${offsetSql}`,
      [
        ...scope.params,
        input.limit + 1,
        ...(input.offset ? [input.offset] : []),
      ],
    );
    const items = rows.slice(0, input.limit).map(decodeScore);
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
    readonly range: AnalyticsTimeRange;
    readonly filters: EventsTableFilterState;
  }): Promise<number> {
    const scope = compileScope(input);
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      `SELECT COUNT(*) AS count\n${scope.fromSql}\nWHERE ${scope.whereSql}`,
      scope.params,
    );
    return rows[0] ? numberValue(rows[0].count) : 0;
  }

  async comparisonCounts(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange;
    readonly score1: DorisScoreAnalyticsIdentifier;
    readonly score2: DorisScoreAnalyticsIdentifier;
    readonly objectType: DorisScoreAnalyticsObjectType;
  }): Promise<{
    readonly score1Count: number;
    readonly score2Count: number;
    readonly matchedCount: number;
  }> {
    const first = compileScope({
      projectId: input.projectId,
      range: input.range,
      filters: analyticsFilters(input.score1, input.objectType),
    });
    const identical =
      input.score1.name === input.score2.name &&
      input.score1.source === input.score2.source &&
      input.score1.dataType === input.score2.dataType;
    if (identical) {
      const rows = await this.dependencies.query<{ readonly count: unknown }>(
        `SELECT COUNT(*) AS count\n${first.fromSql}\nWHERE ${first.whereSql}`,
        first.params,
      );
      const count = rows[0] ? numberValue(rows[0].count) : 0;
      return { score1Count: count, score2Count: count, matchedCount: count };
    }
    const second = compileScope({
      projectId: input.projectId,
      range: input.range,
      filters: analyticsFilters(input.score2, input.objectType),
    });
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `WITH score1 AS (
  SELECT s.trace_id, s.observation_id, s.session_id
  ${first.fromSql}
  WHERE ${first.whereSql}
), score2 AS (
  SELECT s.trace_id, s.observation_id, s.session_id
  ${second.fromSql}
  WHERE ${second.whereSql}
), matched AS (
  SELECT 1 AS matched
  FROM score1 a
  INNER JOIN score2 b
    ON COALESCE(a.trace_id, '') = COALESCE(b.trace_id, '')
   AND COALESCE(a.observation_id, '') = COALESCE(b.observation_id, '')
   AND COALESCE(a.session_id, '') = COALESCE(b.session_id, '')
  LIMIT 1000000
)
SELECT
  (SELECT COUNT(*) FROM score1) AS score1_count,
  (SELECT COUNT(*) FROM score2) AS score2_count,
  (SELECT COUNT(*) FROM matched) AS matched_count`,
      [...first.params, ...second.params],
    );
    const row = rows[0];
    return {
      score1Count: numberValue(row?.score1_count ?? 0),
      score2Count: numberValue(row?.score2_count ?? 0),
      matchedCount: numberValue(row?.matched_count ?? 0),
    };
  }

  async analyticsRows(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange;
    readonly score: DorisScoreAnalyticsIdentifier;
    readonly objectType: DorisScoreAnalyticsObjectType;
    readonly limit: number;
  }): Promise<readonly ScoreDomain[]> {
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > 100_000
    ) {
      throw new InvalidRequestError(
        "Invalid Doris score analytics sample size",
      );
    }
    const scope = compileScope({
      projectId: input.projectId,
      range: input.range,
      filters: analyticsFilters(input.score, input.objectType),
    });
    const rows = await this.dependencies.query<DorisScoreRow>(
      `SELECT ${SCORE_PROJECTION}\n${scope.fromSql}\nWHERE ${scope.whereSql}\nORDER BY COALESCE(s.trace_id, ''), COALESCE(s.observation_id, ''), COALESCE(s.session_id, ''), s.score_id ASC\nLIMIT ?`,
      [...scope.params, input.limit],
    );
    return rows.map(decodeScore);
  }

  async aggregateGroups(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange;
    readonly filters: EventsTableFilterState;
    readonly columns: readonly string[];
    readonly limit: number;
  }): Promise<
    readonly (Readonly<Record<string, unknown>> & { count: number })[]
  > {
    if (
      input.columns.length === 0 ||
      input.columns.some((column) => !SCORE_COLUMNS[column]) ||
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > 10_000
    ) {
      throw new InvalidRequestError("Invalid Doris score grouping request");
    }
    const scope = compileScope(input);
    const expressions = input.columns.map((column) => SCORE_COLUMNS[column]!);
    const projection = input.columns
      .map((column, index) => `${expressions[index]} AS \`${column}\``)
      .join(", ");
    const rows = await this.dependencies.query<
      Readonly<Record<string, unknown>>
    >(
      `SELECT ${projection}, COUNT(*) AS count\n${scope.fromSql}\nWHERE ${scope.whereSql}\nGROUP BY ${expressions.join(", ")}\nORDER BY count DESC, ${expressions.join(", ")}\nLIMIT ?`,
      [...scope.params, input.limit],
    );
    return rows.map((row) => ({ ...row, count: numberValue(row.count) }));
  }

  async get(input: {
    readonly projectId: string;
    readonly scoreId: string;
  }): Promise<ScoreDomain | null> {
    const locators = await this.locateScore(input);
    if (locators.length === 0) return null;
    if (locators.length > 1) {
      throw new LangfuseConflictError(
        "Score locator returned multiple partitions",
      );
    }
    const locator = locators[0]!;
    const from = new Date(`${locator.partitionDate}T00:00:00.000Z`);
    const scope = compileScope({
      projectId: input.projectId,
      range: { from, to: nextUtcDay(locator.partitionDate) },
      filters: [
        {
          type: "string",
          column: "scoreId",
          operator: "=",
          value: locator.scoreId,
        },
      ],
    });
    const rows = await this.dependencies.query<DorisScoreRow>(
      `SELECT ${SCORE_PROJECTION}\n${scope.fromSql}\nWHERE ${scope.whereSql}\nLIMIT 2`,
      scope.params,
    );
    if (rows.length > 1) {
      throw new LangfuseConflictError("Score locator returned duplicates");
    }
    return rows[0] ? decodeScore(rows[0]) : null;
  }

  async listForPrompts(input: {
    readonly projectId: string;
    readonly promptIds: readonly string[];
    readonly relation: "observation" | "trace";
    readonly from?: Date;
    readonly to?: Date;
  }): Promise<readonly DorisPromptScore[]> {
    if (!input.projectId || input.promptIds.length === 0) return [];
    const params: unknown[] = [input.projectId, [...input.promptIds]];
    const timePredicates: string[] = [];
    if (input.from) {
      timePredicates.push("o.start_time >= ?");
      params.push(input.from);
    }
    if (input.to) {
      timePredicates.push("o.start_time <= ?");
      params.push(input.to);
    }
    const rows = await this.dependencies.query<
      DorisScoreRow & { readonly prompt_id: string }
    >(
      `SELECT ${SCORE_PROJECTION}, o.prompt_id
FROM scores_current s
JOIN events_current o
  ON o.project_id = s.project_id
 AND o.trace_id = s.trace_id
 ${input.relation === "observation" ? "AND o.span_id = s.observation_id" : ""}
LEFT JOIN trace_tombstones trace_deletion
  ON trace_deletion.project_id = s.project_id
 AND trace_deletion.trace_id = s.trace_id
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = s.project_id
WHERE s.project_id = ?
  AND o.prompt_id IN (?)
  AND UPPER(o.\`type\`) = 'GENERATION'
  AND trace_deletion.trace_id IS NULL
  AND project_deletion.project_id IS NULL
  AND s.data_type IN ('NUMERIC', 'BOOLEAN', 'CATEGORICAL', 'TEXT')
  ${input.relation === "trace" ? "AND s.observation_id IS NULL" : ""}
  ${timePredicates.length > 0 ? `AND ${timePredicates.join(" AND ")}` : ""}`,
      params,
    );
    return rows.map((row) => ({
      ...(decodeScore(row) as ListableScore),
      promptId: row.prompt_id,
      hasMetadata: Object.keys(metadataValue(row.metadata)).length > 0,
    }));
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
      `SELECT s.project_id, COUNT(*) AS count
FROM scores_current s
LEFT JOIN trace_tombstones trace_deletion
  ON trace_deletion.project_id = s.project_id
 AND trace_deletion.trace_id = s.trace_id
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = s.project_id
WHERE s.created_at >= ? AND s.created_at < ?
  AND trace_deletion.trace_id IS NULL
  AND project_deletion.project_id IS NULL
GROUP BY s.project_id`,
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
FROM scores_current s
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = s.project_id
WHERE s.project_id IN (?)
  AND s.created_at >= ?
  AND project_deletion.project_id IS NULL`,
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
      `SELECT s.project_id, CAST(DATE(s.\`timestamp\`) AS STRING) AS date, COUNT(*) AS count
FROM scores_current s
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = s.project_id
WHERE s.\`timestamp\` >= ? AND s.\`timestamp\` < ?
  AND project_deletion.project_id IS NULL
GROUP BY s.project_id, DATE(s.\`timestamp\`)`,
      [input.start, input.end],
    );
    return rows.map((row) => ({
      projectId: row.project_id,
      date: row.date,
      count: numberValue(row.count),
    }));
  }
}
