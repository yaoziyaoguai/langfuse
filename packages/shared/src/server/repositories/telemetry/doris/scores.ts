import type {
  ScoreDataTypeType,
  ScoreDomain,
  ScoreSourceType,
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

export type DorisScoresPage = {
  readonly items: readonly ScoreDomain[];
  readonly nextCursor: string | null;
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
}
