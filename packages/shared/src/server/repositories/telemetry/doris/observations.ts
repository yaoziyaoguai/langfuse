import type { TracingSearchType } from "../../../../interfaces/search";
import type { EventsTableFilterState } from "../../../../types";
import { InvalidRequestError, LangfuseConflictError } from "../../../../errors";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import {
  compileDorisVisibleEventScope,
  compileDorisVisibleEventsQuery,
  type DorisEventCursor,
  type DorisEventOrderBy,
} from "../../../queries/doris-sql/eventQueryCompiler";
import {
  buildSearchPlan,
  type AnalyticsTimeRange,
} from "../../../queries/logical/searchPlan";
import {
  findObservationHeadLocators,
  findTraceEventHeadLocators,
  type EventHeadLocator,
} from "./entityHeadLocator";

const MAX_PAGE_SIZE = 999;

type LocateObservation = (input: {
  readonly projectId: string;
  readonly observationId: string;
  readonly traceId?: string;
}) => Promise<readonly EventHeadLocator[]>;

type LocateTrace = (input: {
  readonly projectId: string;
  readonly traceId: string;
}) => Promise<readonly EventHeadLocator[]>;

type DorisEventRow = Record<string, unknown> & {
  readonly project_id: string;
  readonly partition_date: string | Date;
  readonly trace_id: string;
  readonly span_id: string;
  readonly start_time: string | Date;
};

export type DorisObservation = {
  readonly id: string;
  readonly traceId: string;
  readonly projectId: string;
  readonly partitionDate: string;
  readonly parentObservationId: string | null;
  readonly type: string;
  readonly name: string | null;
  readonly environment: string;
  readonly userId: string | null;
  readonly sessionId: string | null;
  readonly traceName: string | null;
  readonly release: string | null;
  readonly version: string | null;
  readonly level: string | null;
  readonly statusMessage: string | null;
  readonly isAppRoot: boolean;
  readonly bookmarked: boolean;
  readonly public: boolean;
  readonly startTime: Date;
  readonly endTime: Date | null;
  readonly completionStartTime: Date | null;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly providedModelName: string | null;
  readonly internalModelId: string | null;
  readonly promptId: string | null;
  readonly promptName: string | null;
  readonly promptVersion: number | null;
  readonly totalInputTokens: number | null;
  readonly totalOutputTokens: number | null;
  readonly totalUsage: number;
  readonly totalCost: number | null;
  readonly latency: number | null;
  readonly timeToFirstToken: number | null;
  readonly tags: readonly string[];
  readonly usageDetails: Readonly<Record<string, number>>;
  readonly costDetails: Readonly<Record<string, number>>;
  readonly providedUsageDetails: Readonly<Record<string, number>>;
  readonly providedCostDetails: Readonly<Record<string, number>>;
  readonly toolDefinitionsCount: number | null;
  readonly toolCallsCount: number | null;
  readonly inputPreview: string | null;
  readonly outputPreview: string | null;
  readonly input?: unknown;
  readonly output?: unknown;
  readonly metadata?: Readonly<Record<string, unknown>>;
  readonly modelParameters?: Readonly<Record<string, unknown>>;
  readonly toolDefinitions?: Readonly<Record<string, unknown>>;
  readonly toolCalls?: readonly string[];
  readonly toolCallNames?: readonly string[];
  readonly experimentId?: string | null;
  readonly experimentName?: string | null;
  readonly experimentDescription?: string | null;
  readonly experimentDatasetId?: string | null;
  readonly experimentItemId?: string | null;
  readonly experimentItemExpectedOutput?: string | null;
  readonly experimentItemMetadata?: Readonly<Record<string, unknown>> | null;
  readonly experimentItemRootSpanId?: string | null;
};

export type DorisObservationsPage = {
  readonly items: readonly DorisObservation[];
  readonly nextCursor: string | null;
};

export type DorisEventFilterOptionColumn =
  | "providedModelName"
  | "modelId"
  | "name"
  | "promptName"
  | "traceTags"
  | "traceName"
  | "type"
  | "userId"
  | "version"
  | "sessionId"
  | "level"
  | "environment"
  | "experimentDatasetId"
  | "experimentId"
  | "experimentName"
  | "isRootObservation"
  | "hasParentObservation"
  | "toolNames"
  | "calledToolNames";

export type DorisEventNumericColumn =
  | "promptVersion"
  | "totalCost"
  | "totalTokens"
  | "latency"
  | "timeToFirstToken"
  | "tokensPerSecond"
  | "toolDefinitions"
  | "toolCalls";

type DorisEventFacetDefinition = {
  readonly kind: "scalar" | "array" | "boolean";
  readonly expression: string;
  readonly includeWhen?: string;
  readonly order: "count" | "alpha" | "boolean";
  readonly requiresFullContent?: boolean;
};

const EVENT_FACETS: Readonly<
  Record<DorisEventFilterOptionColumn, DorisEventFacetDefinition>
> = {
  providedModelName: {
    kind: "scalar",
    expression: "e.provided_model_name",
    includeWhen:
      "e.provided_model_name IS NOT NULL AND e.provided_model_name != ''",
    order: "count",
  },
  modelId: {
    kind: "scalar",
    expression: "e.internal_model_id",
    includeWhen:
      "e.internal_model_id IS NOT NULL AND e.internal_model_id != ''",
    order: "count",
  },
  name: {
    kind: "scalar",
    expression: "e.name",
    includeWhen: "e.name IS NOT NULL AND e.name != ''",
    order: "count",
  },
  promptName: {
    kind: "scalar",
    expression: "e.prompt_name",
    includeWhen:
      "e.`type` = 'GENERATION' AND e.prompt_name IS NOT NULL AND e.prompt_name != ''",
    order: "count",
  },
  traceTags: {
    kind: "array",
    expression: "e.tags",
    order: "alpha",
  },
  traceName: {
    kind: "scalar",
    expression: "e.trace_name",
    includeWhen: "e.trace_name IS NOT NULL AND e.trace_name != ''",
    order: "count",
  },
  type: {
    kind: "scalar",
    expression: "e.`type`",
    includeWhen: "e.`type` IS NOT NULL AND e.`type` != ''",
    order: "count",
  },
  userId: {
    kind: "scalar",
    expression: "e.user_id",
    includeWhen: "e.user_id IS NOT NULL AND e.user_id != ''",
    order: "count",
  },
  version: {
    kind: "scalar",
    expression: "e.`version`",
    includeWhen: "e.`version` IS NOT NULL AND e.`version` != ''",
    order: "count",
  },
  sessionId: {
    kind: "scalar",
    expression: "e.session_id",
    includeWhen: "e.session_id IS NOT NULL AND e.session_id != ''",
    order: "count",
  },
  level: {
    kind: "scalar",
    expression: "e.`level`",
    includeWhen: "e.`level` IS NOT NULL AND e.`level` != ''",
    order: "count",
  },
  environment: {
    kind: "scalar",
    expression: "e.environment",
    includeWhen: "e.environment IS NOT NULL AND e.environment != ''",
    order: "count",
  },
  experimentDatasetId: {
    kind: "scalar",
    expression: "e.experiment_dataset_id",
    includeWhen:
      "e.experiment_dataset_id IS NOT NULL AND e.experiment_dataset_id != ''",
    order: "count",
  },
  experimentId: {
    kind: "scalar",
    expression: "e.experiment_id",
    includeWhen: "e.experiment_id IS NOT NULL AND e.experiment_id != ''",
    order: "count",
  },
  experimentName: {
    kind: "scalar",
    expression: "e.experiment_name",
    includeWhen: "e.experiment_name IS NOT NULL AND e.experiment_name != ''",
    order: "count",
  },
  isRootObservation: {
    kind: "boolean",
    expression:
      "(e.parent_span_id IS NULL OR e.parent_span_id = '' OR e.is_app_root = TRUE)",
    order: "boolean",
  },
  hasParentObservation: {
    kind: "boolean",
    expression: "(e.parent_span_id IS NOT NULL AND e.parent_span_id != '')",
    order: "boolean",
  },
  toolNames: {
    kind: "array",
    expression: "JSON_KEYS(e.tool_definitions)",
    order: "count",
    requiresFullContent: true,
  },
  calledToolNames: {
    kind: "array",
    expression: "e.tool_call_names",
    order: "count",
    requiresFullContent: true,
  },
};

const EVENT_NUMERIC_EXPRESSIONS: Readonly<
  Record<DorisEventNumericColumn, string>
> = {
  promptVersion: "e.prompt_version",
  totalCost: "e.total_cost",
  totalTokens:
    "COALESCE(e.total_input_tokens, 0) + COALESCE(e.total_output_tokens, 0)",
  latency: "MICROSECONDS_DIFF(e.end_time, e.start_time) / 1000000.0",
  timeToFirstToken:
    "MICROSECONDS_DIFF(e.completion_start_time, e.start_time) / 1000000.0",
  tokensPerSecond:
    "e.total_output_tokens / NULLIF(MICROSECONDS_DIFF(e.end_time, e.start_time) / 1000000.0, 0)",
  toolDefinitions: "CARDINALITY(JSON_KEYS(e.tool_definitions))",
  toolCalls: "CARDINALITY(e.tool_calls)",
};

const FULL_CONTENT_ORDER_COLUMNS = new Set<DorisEventOrderBy["column"]>([
  "hasInput",
  "hasOutput",
  "toolDefinitions",
  "toolCalls",
]);

const FULL_CONTENT_NUMERIC_COLUMNS = new Set<DorisEventNumericColumn>([
  "toolDefinitions",
  "toolCalls",
]);

function eventFacetDefinition(column: string): {
  readonly column: DorisEventFilterOptionColumn;
  readonly definition: DorisEventFacetDefinition;
} {
  if (!Object.hasOwn(EVENT_FACETS, column)) {
    throw new InvalidRequestError(
      `Unsupported Doris event filter option column: ${column}`,
    );
  }
  const typedColumn = column as DorisEventFilterOptionColumn;
  return { column: typedColumn, definition: EVENT_FACETS[typedColumn] };
}

function eventNumericExpression(column: string): {
  readonly column: DorisEventNumericColumn;
  readonly expression: string;
} {
  if (!Object.hasOwn(EVENT_NUMERIC_EXPRESSIONS, column)) {
    throw new InvalidRequestError(
      `Unsupported Doris event numeric column: ${column}`,
    );
  }
  const typedColumn = column as DorisEventNumericColumn;
  return {
    column: typedColumn,
    expression: EVENT_NUMERIC_EXPRESSIONS[typedColumn],
  };
}

function dateOnly(value: unknown): string {
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value)) {
    return value.slice(0, 10);
  }
  throw new TypeError("Doris returned an invalid partition date");
}

function dateTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value !== "string") {
    throw new TypeError("Doris returned an invalid timestamp");
  }
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (Number.isNaN(parsed.getTime())) {
    throw new TypeError("Doris returned an invalid timestamp");
  }
  return parsed;
}

function nullableDateTime(value: unknown): Date | null {
  return value === null || value === undefined || value === ""
    ? null
    : dateTime(value);
}

function nullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

function nullableNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new TypeError("Doris returned an invalid number");
  }
  return parsed;
}

function numberValue(value: unknown): number {
  return nullableNumber(value) ?? 0;
}

function booleanValue(value: unknown): boolean {
  return value === true || value === 1 || value === "1";
}

function objectValue(value: unknown): Readonly<Record<string, unknown>> {
  const parsed = parseJsonIfString(value);
  return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
    ? (parsed as Readonly<Record<string, unknown>>)
    : {};
}

function nullableObjectValue(
  value: unknown,
): Readonly<Record<string, unknown>> | null {
  return value === null || value === undefined ? null : objectValue(value);
}

function numericRecord(value: unknown): Readonly<Record<string, number>> {
  return Object.fromEntries(
    Object.entries(objectValue(value)).flatMap(([key, item]) => {
      const parsed = nullableNumber(item);
      return parsed === null ? [] : [[key, parsed]];
    }),
  );
}

function stringArray(value: unknown): readonly string[] {
  const parsed = parseJsonIfString(value);
  return Array.isArray(parsed) ? parsed.map(String) : [];
}

function secondsBetween(from: Date, to: Date | null): number | null {
  return to ? (to.getTime() - from.getTime()) / 1_000 : null;
}

function decodeObservation(row: DorisEventRow): DorisObservation {
  const startTime = dateTime(row.start_time);
  const endTime = nullableDateTime(row.end_time);
  const completionStartTime = nullableDateTime(row.completion_start_time);
  const usageDetails = numericRecord(row.usage_details);
  const inputTokens = nullableNumber(row.total_input_tokens);
  const outputTokens = nullableNumber(row.total_output_tokens);
  const observation: DorisObservation = {
    id: row.span_id,
    traceId: row.trace_id,
    projectId: row.project_id,
    partitionDate: dateOnly(row.partition_date),
    parentObservationId: nullableString(row.parent_span_id),
    type: String(row.type),
    name: nullableString(row.name),
    environment: String(row.environment),
    userId: nullableString(row.user_id),
    sessionId: nullableString(row.session_id),
    traceName: nullableString(row.trace_name),
    release: nullableString(row.release),
    version: nullableString(row.version),
    level: nullableString(row.level),
    statusMessage: nullableString(row.status_message),
    isAppRoot: booleanValue(row.is_app_root),
    bookmarked: booleanValue(row.bookmarked),
    public: booleanValue(row.public),
    startTime,
    endTime,
    completionStartTime,
    createdAt: dateTime(row.created_at),
    updatedAt: dateTime(row.updated_at),
    providedModelName: nullableString(row.provided_model_name),
    internalModelId: nullableString(row.internal_model_id),
    promptId: nullableString(row.prompt_id),
    promptName: nullableString(row.prompt_name),
    promptVersion: nullableNumber(row.prompt_version),
    totalInputTokens: inputTokens,
    totalOutputTokens: outputTokens,
    totalUsage:
      nullableNumber(usageDetails.total) ??
      (inputTokens ?? 0) + (outputTokens ?? 0),
    totalCost: nullableNumber(row.total_cost),
    latency: secondsBetween(startTime, endTime),
    timeToFirstToken: secondsBetween(startTime, completionStartTime),
    tags: stringArray(row.tags),
    usageDetails,
    costDetails: numericRecord(row.cost_details),
    providedUsageDetails: numericRecord(row.provided_usage_details),
    providedCostDetails: numericRecord(row.provided_cost_details),
    toolDefinitionsCount: nullableNumber(row.tool_definitions_count),
    toolCallsCount: nullableNumber(row.tool_calls_count),
    inputPreview: nullableString(row.input_preview),
    outputPreview: nullableString(row.output_preview),
    ...(Object.hasOwn(row, "input") && { input: parseJsonIfString(row.input) }),
    ...(Object.hasOwn(row, "output") && {
      output: parseJsonIfString(row.output),
    }),
    ...(Object.hasOwn(row, "metadata") && {
      metadata: objectValue(row.metadata),
    }),
    ...(Object.hasOwn(row, "model_parameters") && {
      modelParameters: objectValue(row.model_parameters),
    }),
    ...(Object.hasOwn(row, "tool_definitions") && {
      toolDefinitions: objectValue(row.tool_definitions),
    }),
    ...(Object.hasOwn(row, "tool_calls") && {
      toolCalls: stringArray(row.tool_calls),
    }),
    ...(Object.hasOwn(row, "tool_call_names") && {
      toolCallNames: stringArray(row.tool_call_names),
    }),
    ...(Object.hasOwn(row, "experiment_id") && {
      experimentId: nullableString(row.experiment_id),
      experimentName: nullableString(row.experiment_name),
      experimentDescription: nullableString(row.experiment_description),
      experimentDatasetId: nullableString(row.experiment_dataset_id),
      experimentItemId: nullableString(row.experiment_item_id),
      experimentItemExpectedOutput: nullableString(
        row.experiment_item_expected_output,
      ),
      experimentItemMetadata: nullableObjectValue(row.experiment_item_metadata),
      experimentItemRootSpanId: nullableString(
        row.experiment_item_root_span_id,
      ),
    }),
  };
  return observation;
}

export function encodeDorisObservationCursor(
  observation: Pick<DorisObservation, "startTime" | "traceId" | "id">,
): string {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      startTime: observation.startTime.toISOString(),
      traceId: observation.traceId,
      spanId: observation.id,
    }),
    "utf8",
  ).toString("base64url");
}

function decodeCursor(
  cursor: string | undefined,
): DorisEventCursor | undefined {
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
      typeof value.startTime !== "string" ||
      typeof value.traceId !== "string" ||
      !value.traceId ||
      typeof value.spanId !== "string" ||
      !value.spanId
    ) {
      throw new Error();
    }
    return {
      startTime: dateTime(value.startTime),
      traceId: value.traceId,
      spanId: value.spanId,
    };
  } catch {
    throw new InvalidRequestError("Invalid Doris observation cursor");
  }
}

function nextUtcDay(partitionDate: string): Date {
  const start = new Date(`${partitionDate}T00:00:00.000Z`);
  start.setUTCDate(start.getUTCDate() + 1);
  return start;
}

export class DorisObservationsRepository {
  private readonly locateObservation: LocateObservation;
  private readonly locateTrace: LocateTrace;

  constructor(
    private readonly dependencies: {
      readonly query: DorisQueryExecutor["query"];
      readonly streamQuery?: NonNullable<DorisQueryExecutor["streamQuery"]>;
      readonly locateObservation?: LocateObservation;
      readonly locateTrace?: LocateTrace;
    },
  ) {
    this.locateObservation =
      dependencies.locateObservation ?? findObservationHeadLocators;
    this.locateTrace = dependencies.locateTrace ?? findTraceEventHeadLocators;
  }

  private async listInternal(
    input: {
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
      readonly orderBy?: DorisEventOrderBy;
      readonly includeFullContent?: boolean;
      readonly partitionDates?: readonly string[];
    },
    enforceFullContentRange: boolean,
  ): Promise<DorisObservationsPage> {
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > MAX_PAGE_SIZE
    ) {
      throw new RangeError("Doris observation page size is invalid");
    }
    if (
      enforceFullContentRange &&
      (input.includeFullContent ||
        (input.orderBy && FULL_CONTENT_ORDER_COLUMNS.has(input.orderBy.column)))
    ) {
      buildSearchPlan({
        range: input.range,
        filtersRequireFullContent: true,
      });
    }
    const compiled = compileDorisVisibleEventsQuery({
      projectId: input.projectId,
      range: input.range,
      projection: input.includeFullContent ? "detail" : "list",
      filters: input.filters,
      search: input.search,
      cursor: decodeCursor(input.cursor),
      partitionDates: input.partitionDates,
      orderBy: input.orderBy,
      offset: input.offset,
      limit: input.limit + 1,
      allowUnboundedFullContent: !enforceFullContentRange,
    });
    const rows = await this.dependencies.query<DorisEventRow>(
      compiled.sql,
      compiled.params,
    );
    const items = rows.slice(0, input.limit).map(decodeObservation);
    return {
      items,
      nextCursor:
        !input.orderBy && rows.length > input.limit && items.length > 0
          ? encodeDorisObservationCursor(items[items.length - 1]!)
          : null,
    };
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
    readonly orderBy?: DorisEventOrderBy;
    readonly includeFullContent?: boolean;
    readonly partitionDates?: readonly string[];
  }): Promise<DorisObservationsPage> {
    return this.listInternal(input, true);
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
  }): AsyncIterable<{ readonly id: string; readonly traceId: string }> {
    if (!Number.isSafeInteger(input.limit) || input.limit < 1) {
      throw new RangeError("Doris observation identity limit is invalid");
    }
    const scope = compileDorisVisibleEventScope(input);
    const streamQuery = this.dependencies.streamQuery;
    const rows = streamQuery
      ? streamQuery<{ readonly span_id: string; readonly trace_id: string }>(
          `SELECT e.span_id, e.trace_id\n${scope.fromSql}\nWHERE ${scope.whereSql}\nORDER BY e.trace_id ASC, e.span_id ASC\nLIMIT ?`,
          scope.params.concat(input.limit),
          { signal: input.signal },
        )
      : await this.dependencies.query<{
          readonly span_id: string;
          readonly trace_id: string;
        }>(
          `SELECT e.span_id, e.trace_id\n${scope.fromSql}\nWHERE ${scope.whereSql}\nORDER BY e.trace_id ASC, e.span_id ASC\nLIMIT ?`,
          scope.params.concat(input.limit),
          { signal: input.signal },
        );
    for await (const row of rows) {
      yield { id: String(row.span_id), traceId: String(row.trace_id) };
    }
  }

  /**
   * Batch exports are already bounded by their row limit and cutoff timestamp,
   * so they may scan full content outside the interactive 30-day window.
   */
  async scan(input: {
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
    readonly orderBy?: DorisEventOrderBy;
    readonly includeFullContent?: boolean;
    readonly partitionDates?: readonly string[];
  }): Promise<DorisObservationsPage> {
    return this.listInternal(input, false);
  }

  async count(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly partitionDates?: readonly string[];
  }): Promise<number> {
    const scope = compileDorisVisibleEventScope(input);
    const rows = await this.dependencies.query<{ readonly count: unknown }>(
      `SELECT COUNT(*) AS count\n${scope.fromSql}\nWHERE ${scope.whereSql}`,
      scope.params,
    );
    return numberValue(rows[0]?.count);
  }

  async counts(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
  }): Promise<{
    readonly totalCount: number;
    readonly uniqueTraceCount: number;
  }> {
    const scope = compileDorisVisibleEventScope(input);
    const rows = await this.dependencies.query<{
      readonly count: unknown;
      readonly trace_count: unknown;
    }>(
      `SELECT COUNT(*) AS count, COUNT(DISTINCT e.trace_id) AS trace_count\n${scope.fromSql}\nWHERE ${scope.whereSql}`,
      scope.params,
    );
    return {
      totalCount: numberValue(rows[0]?.count),
      uniqueTraceCount: numberValue(rows[0]?.trace_count),
    };
  }

  async filterOptionValues(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly column: string;
    readonly limit: number;
    readonly offset?: number;
    readonly requireScore?: {
      readonly scoreName?: string;
      readonly scoreSource?: string;
    };
  }): Promise<
    readonly {
      readonly column: DorisEventFilterOptionColumn;
      readonly value: string;
      readonly count: number;
    }[]
  > {
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > 1_000 ||
      !Number.isSafeInteger(input.offset ?? 0) ||
      (input.offset ?? 0) < 0
    ) {
      throw new InvalidRequestError("Invalid Doris event facet page size");
    }
    const { column, definition } = eventFacetDefinition(input.column);
    if (definition.requiresFullContent) {
      buildSearchPlan({
        range: input.range,
        filtersRequireFullContent: true,
      });
    }
    const scope = compileDorisVisibleEventScope(input);
    const scorePredicates = input.requireScore
      ? [
          "s.project_id = e.project_id",
          "s.trace_id = e.trace_id",
          ...(input.requireScore.scoreName ? ["s.`name` = ?"] : []),
          ...(input.requireScore.scoreSource ? ["s.`source` = ?"] : []),
        ]
      : [];
    const scoreParams = input.requireScore
      ? [
          ...(input.requireScore.scoreName
            ? [input.requireScore.scoreName]
            : []),
          ...(input.requireScore.scoreSource
            ? [input.requireScore.scoreSource]
            : []),
        ]
      : [];
    const scoreExists = input.requireScore
      ? `\n  AND EXISTS (\n    SELECT 1\n    FROM scores_current s\n    WHERE ${scorePredicates.join("\n      AND ")}\n  )`
      : "";
    const valueExpression =
      definition.kind === "boolean"
        ? `IF(${definition.expression}, 'true', 'false')`
        : definition.kind === "array"
          ? "value"
          : definition.expression;
    const fromSql =
      definition.kind === "array"
        ? `FROM (\n  SELECT ${definition.expression} AS facet_values\n  ${scope.fromSql}\n  WHERE ${scope.whereSql}${scoreExists}\n) scoped\nLATERAL VIEW explode(scoped.facet_values) exploded AS value`
        : `${scope.fromSql}\nWHERE ${scope.whereSql}${scoreExists}`;
    const includeWhen =
      definition.kind === "array"
        ? "value IS NOT NULL AND value != ''"
        : definition.includeWhen;
    const orderBy =
      definition.order === "alpha"
        ? "value ASC"
        : definition.order === "boolean"
          ? "value ASC"
          : "count DESC, value ASC";
    const offsetSql = input.offset ? " OFFSET ?" : "";
    const rows = await this.dependencies.query<{
      readonly value: unknown;
      readonly count: unknown;
    }>(
      `SELECT ${valueExpression} AS value, COUNT(*) AS count\n${fromSql}${definition.kind === "array" ? "\nWHERE" : " AND"} ${includeWhen ?? "TRUE"}\nGROUP BY value\nORDER BY ${orderBy}\nLIMIT ?${offsetSql}`,
      [
        ...scope.params,
        ...scoreParams,
        input.limit,
        ...(input.offset ? [input.offset] : []),
      ],
    );
    return rows.map((row) => ({
      column,
      value: String(row.value),
      count: numberValue(row.count),
    }));
  }

  async numericStats(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly column: string;
  }): Promise<{
    readonly min: number;
    readonly max: number;
    readonly avg: number;
    readonly count: number;
  } | null> {
    const { column, expression } = eventNumericExpression(input.column);
    if (FULL_CONTENT_NUMERIC_COLUMNS.has(column)) {
      buildSearchPlan({
        range: input.range,
        filtersRequireFullContent: true,
      });
    }
    const scope = compileDorisVisibleEventScope(input);
    const rows = await this.dependencies.query<{
      readonly min: unknown;
      readonly max: unknown;
      readonly avg: unknown;
      readonly count: unknown;
    }>(
      `SELECT MIN(${expression}) AS min, MAX(${expression}) AS max, AVG(${expression}) AS avg, COUNT(${expression}) AS count\n${scope.fromSql}\nWHERE ${scope.whereSql} AND ${expression} IS NOT NULL`,
      scope.params,
    );
    const row = rows[0];
    if (
      !row ||
      row.min === null ||
      row.min === undefined ||
      row.max === null ||
      row.max === undefined ||
      row.avg === null ||
      row.avg === undefined
    ) {
      return null;
    }
    return {
      min: numberValue(row.min),
      max: numberValue(row.max),
      avg: numberValue(row.avg),
      count: numberValue(row.count),
    };
  }

  async promptNameCounts(input: {
    readonly projectId: string;
    readonly promptNames: readonly string[];
    readonly range: AnalyticsTimeRange;
  }): Promise<
    readonly { readonly promptName: string; readonly count: number }[]
  > {
    if (input.promptNames.length === 0) return [];
    const scope = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: input.range,
      filters: [
        {
          type: "stringOptions",
          column: "promptName",
          operator: "any of",
          value: [...input.promptNames],
        },
      ],
    });
    const rows = await this.dependencies.query<{
      readonly prompt_name: unknown;
      readonly count: unknown;
    }>(
      `SELECT e.prompt_name, COUNT(*) AS count\n${scope.fromSql}\nWHERE ${scope.whereSql}\nGROUP BY e.prompt_name`,
      scope.params,
    );
    return rows.map((row) => ({
      promptName: String(row.prompt_name),
      count: numberValue(row.count),
    }));
  }

  async promptMetrics(input: {
    readonly projectId: string;
    readonly promptIds: readonly string[];
    readonly range: AnalyticsTimeRange;
  }): Promise<
    readonly {
      readonly count: number;
      readonly promptId: string;
      readonly promptVersion: number;
      readonly firstObservation: Date;
      readonly lastObservation: Date;
      readonly medianInputUsage: number;
      readonly medianOutputUsage: number;
      readonly medianTotalCost: number;
      readonly medianLatencyMs: number;
    }[]
  > {
    if (input.promptIds.length === 0) return [];
    const scope = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: input.range,
      filters: [
        {
          type: "stringOptions",
          column: "promptId",
          operator: "any of",
          value: [...input.promptIds],
        },
        {
          type: "string",
          column: "type",
          operator: "=",
          value: "GENERATION",
        },
      ],
    });
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `SELECT
  e.prompt_id,
  e.prompt_version,
  COUNT(*) AS count,
  MIN(e.start_time) AS first_observation,
  MAX(e.start_time) AS last_observation,
  PERCENTILE_APPROX(COALESCE(e.total_input_tokens, 0), 0.5) AS median_input_usage,
  PERCENTILE_APPROX(COALESCE(e.total_output_tokens, 0), 0.5) AS median_output_usage,
  PERCENTILE_APPROX(COALESCE(e.total_cost, 0), 0.5) AS median_total_cost,
  PERCENTILE_APPROX(MICROSECONDS_DIFF(e.end_time, e.start_time) / 1000.0, 0.5) AS median_latency_ms
${scope.fromSql}
WHERE ${scope.whereSql}
GROUP BY e.prompt_id, e.prompt_version
ORDER BY e.prompt_version DESC`,
      scope.params,
    );
    return rows.map((row) => ({
      count: numberValue(row.count),
      promptId: String(row.prompt_id),
      promptVersion: numberValue(row.prompt_version),
      firstObservation: dateTime(row.first_observation),
      lastObservation: dateTime(row.last_observation),
      medianInputUsage: numberValue(row.median_input_usage),
      medianOutputUsage: numberValue(row.median_output_usage),
      medianTotalCost: numberValue(row.median_total_cost),
      medianLatencyMs: numberValue(row.median_latency_ms),
    }));
  }

  async lastUsedByModelIds(input: {
    readonly projectId: string;
    readonly modelIds: readonly string[];
  }): Promise<
    readonly { readonly modelId: string; readonly lastUsed: Date }[]
  > {
    if (input.modelIds.length === 0) return [];
    const scope = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: { from: new Date(0), to: new Date(Date.now() + 1) },
      filters: [
        {
          type: "stringOptions",
          column: "modelId",
          operator: "any of",
          value: [...input.modelIds],
        },
        {
          type: "string",
          column: "type",
          operator: "=",
          value: "GENERATION",
        },
      ],
    });
    const rows = await this.dependencies.query<{
      readonly model_id: unknown;
      readonly last_used: unknown;
    }>(
      `SELECT e.internal_model_id AS model_id, MAX(e.start_time) AS last_used\n${scope.fromSql}\nWHERE ${scope.whereSql}\nGROUP BY e.internal_model_id`,
      scope.params,
    );
    return rows.map((row) => ({
      modelId: String(row.model_id),
      lastUsed: dateTime(row.last_used),
    }));
  }

  async evaluatorCostMetrics(input: {
    readonly projectId: string;
    readonly evaluatorIds: readonly string[];
    readonly now?: Date;
  }): Promise<
    readonly {
      readonly evaluatorId: string;
      readonly totalCost: number;
      readonly avgCost: number;
      readonly executionCount: number;
    }[]
  > {
    const evaluatorIds = [...new Set(input.evaluatorIds)];
    if (evaluatorIds.length === 0) return [];
    if (evaluatorIds.some((evaluatorId) => !evaluatorId)) {
      throw new InvalidRequestError("Invalid evaluator cost query");
    }

    const today = input.now ? new Date(input.now) : new Date();
    today.setUTCHours(0, 0, 0, 0);
    const from = new Date(today);
    from.setUTCDate(from.getUTCDate() - 7);
    const to = new Date(today);
    to.setUTCDate(to.getUTCDate() + 1);
    const scope = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: { from, to },
      filters: [
        {
          type: "string",
          column: "type",
          operator: "=",
          value: "GENERATION",
        },
      ],
    });
    const evaluatorPlaceholders = evaluatorIds.map(() => "?").join(", ");
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `WITH evaluator_events AS (
  SELECT
    JSON_UNQUOTE(CAST(ELEMENT_AT(e.metadata, ?) AS STRING)) AS evaluator_id,
    COALESCE(e.total_cost, 0) AS total_cost
${scope.fromSql}
WHERE ${scope.whereSql}
)
SELECT
  evaluator_id,
  SUM(total_cost) AS total_cost,
  AVG(total_cost) AS avg_cost,
  COUNT(*) AS execution_count
FROM evaluator_events
WHERE evaluator_id IN (${evaluatorPlaceholders})
GROUP BY evaluator_id`,
      ["job_configuration_id", ...scope.params, ...evaluatorIds],
    );
    return rows.map((row) => ({
      evaluatorId: String(row.evaluator_id),
      totalCost: numberValue(row.total_cost),
      avgCost: numberValue(row.avg_cost),
      executionCount: numberValue(row.execution_count),
    }));
  }

  async costAndLatencyByIds(input: {
    readonly projectId: string;
    readonly observationIds: readonly string[];
    readonly from?: Date;
  }): Promise<
    readonly {
      readonly id: string;
      readonly totalCost: number;
      readonly latency: number | null;
    }[]
  > {
    if (input.observationIds.length === 0) return [];
    const scope = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: { from: input.from ?? new Date(0), to: new Date(Date.now() + 1) },
      filters: [
        {
          type: "stringOptions",
          column: "id",
          operator: "any of",
          value: [...input.observationIds],
        },
      ],
    });
    const rows = await this.dependencies.query<Record<string, unknown>>(
      `SELECT e.span_id, e.total_cost, MICROSECONDS_DIFF(e.end_time, e.start_time) / 1000000.0 AS latency\n${scope.fromSql}\nWHERE ${scope.whereSql}`,
      scope.params,
    );
    return rows.map((row) => ({
      id: String(row.span_id),
      totalCost: numberValue(row.total_cost),
      latency:
        row.latency === null || row.latency === undefined
          ? null
          : numberValue(row.latency),
    }));
  }

  async latestSdkMetadata(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange;
  }): Promise<{
    readonly isOtel: boolean;
    readonly name?: string;
    readonly version?: string;
    readonly language?: string;
  }> {
    const scope = compileDorisVisibleEventScope({
      projectId: input.projectId,
      range: input.range,
      filters: [],
    });
    const rows = await this.dependencies.query<{
      readonly ingestion_sdk_name: unknown;
      readonly ingestion_sdk_version: unknown;
      readonly telemetry_sdk_language: unknown;
    }>(
      `SELECT e.ingestion_sdk_name, e.ingestion_sdk_version, e.telemetry_sdk_language\n${scope.fromSql}\nWHERE ${scope.whereSql} AND e.\`source\` LIKE 'otel%'\nORDER BY e.start_time DESC, e.trace_id DESC, e.span_id DESC\nLIMIT 1`,
      scope.params,
    );
    const row = rows[0];
    if (!row) return { isOtel: false };
    const name = nullableString(row.ingestion_sdk_name);
    const version = nullableString(row.ingestion_sdk_version);
    const language = nullableString(row.telemetry_sdk_language);
    const attributedName = name && name !== "unknown" ? name : undefined;
    return {
      isOtel: true,
      ...(attributedName && { name: attributedName }),
      ...(attributedName && version && version !== "unknown" && { version }),
      ...(language && { language }),
    };
  }

  private async resolveTraceRange(input: {
    readonly projectId: string;
    readonly traceId: string;
  }): Promise<
    | {
        readonly range: AnalyticsTimeRange;
        readonly partitionDates: readonly string[];
      }
    | undefined
  > {
    const locators = await this.locateTrace(input);
    if (locators.length === 0) return undefined;
    const partitionDates = [
      ...new Set(locators.map(({ partitionDate }) => partitionDate)),
    ].sort();
    return {
      range: {
        from: new Date(`${partitionDates[0]}T00:00:00.000Z`),
        to: nextUtcDay(partitionDates[partitionDates.length - 1]!),
      },
      partitionDates,
    };
  }

  async listForTrace(input: {
    readonly projectId: string;
    readonly traceId: string;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly cursor?: string;
    readonly limit: number;
    readonly offset?: number;
    readonly orderBy?: DorisEventOrderBy;
    readonly includeFullContent?: boolean;
  }): Promise<DorisObservationsPage> {
    const resolved = await this.resolveTraceRange({
      projectId: input.projectId,
      traceId: input.traceId,
    });
    if (!resolved) return { items: [], nextCursor: null };
    return this.listInternal(
      {
        ...input,
        range: resolved.range,
        filters: [
          ...input.filters,
          {
            type: "string",
            column: "traceId",
            operator: "=",
            value: input.traceId,
          },
        ],
        partitionDates: resolved.partitionDates,
      },
      false,
    );
  }

  async countForTrace(input: {
    readonly projectId: string;
    readonly traceId: string;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
  }): Promise<number> {
    const resolved = await this.resolveTraceRange(input);
    if (!resolved) return 0;
    return this.count({
      ...input,
      range: resolved.range,
      filters: [
        ...input.filters,
        {
          type: "string",
          column: "traceId",
          operator: "=",
          value: input.traceId,
        },
      ],
      partitionDates: resolved.partitionDates,
    });
  }

  async get(input: {
    readonly projectId: string;
    readonly observationId: string;
    readonly traceId?: string;
  }): Promise<DorisObservation | null> {
    const locators = await this.locateObservation({
      projectId: input.projectId,
      observationId: input.observationId,
      traceId: input.traceId,
    });
    if (locators.length === 0) return null;
    if (locators.length > 1) {
      throw new LangfuseConflictError(
        "Observation ID is ambiguous without its trace ID",
      );
    }
    const locator = locators[0]!;
    const from = new Date(`${locator.partitionDate}T00:00:00.000Z`);
    const compiled = compileDorisVisibleEventsQuery({
      projectId: input.projectId,
      range: { from, to: nextUtcDay(locator.partitionDate) },
      projection: "detail",
      filters: [
        {
          type: "stringOptions",
          column: "id",
          operator: "any of",
          value: [input.observationId],
        },
        {
          type: "string",
          column: "traceId",
          operator: "=",
          value: locator.traceId,
        },
      ],
      limit: 2,
    });
    const rows = await this.dependencies.query<DorisEventRow>(
      compiled.sql,
      compiled.params,
    );
    if (rows.length > 1) {
      throw new LangfuseConflictError(
        "Observation locator returned duplicates",
      );
    }
    return rows[0] ? decodeObservation(rows[0]) : null;
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
      `SELECT e.project_id, COUNT(*) AS count
FROM events_current e
LEFT JOIN trace_tombstones trace_deletion
  ON trace_deletion.project_id = e.project_id
 AND trace_deletion.trace_id = e.trace_id
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = e.project_id
WHERE e.created_at >= ? AND e.created_at < ?
  AND trace_deletion.trace_id IS NULL
  AND project_deletion.project_id IS NULL
GROUP BY e.project_id`,
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
FROM events_current e
LEFT JOIN trace_tombstones trace_deletion
  ON trace_deletion.project_id = e.project_id
 AND trace_deletion.trace_id = e.trace_id
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = e.project_id
WHERE e.project_id IN (?) AND e.created_at >= ?
  AND trace_deletion.trace_id IS NULL
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
      `SELECT e.project_id, CAST(DATE(e.start_time) AS STRING) AS date, COUNT(*) AS count
FROM events_current e
LEFT JOIN trace_tombstones trace_deletion
  ON trace_deletion.project_id = e.project_id
 AND trace_deletion.trace_id = e.trace_id
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = e.project_id
WHERE e.start_time >= ? AND e.start_time < ?
  AND trace_deletion.trace_id IS NULL
  AND project_deletion.project_id IS NULL
GROUP BY e.project_id, DATE(e.start_time)`,
      [input.start, input.end],
    );
    return rows.map((row) => ({
      projectId: row.project_id,
      date: row.date,
      count: numberValue(row.count),
    }));
  }
}
