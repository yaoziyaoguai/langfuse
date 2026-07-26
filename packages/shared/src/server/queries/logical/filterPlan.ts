import { eventsTableFilterState } from "../../../interfaces/filters";
import type { EventsTableFilterState } from "../../../types";
import { InvalidRequestError } from "../../../errors";

type DorisEventColumn = {
  readonly expression: string;
  readonly requiresFullContent?: boolean;
};

const EVENT_COLUMNS: Readonly<Record<string, DorisEventColumn>> = {
  id: { expression: "e.span_id" },
  traceId: { expression: "e.trace_id" },
  parentObservationId: { expression: "e.parent_span_id" },
  startTime: { expression: "e.start_time" },
  endTime: { expression: "e.end_time" },
  name: { expression: "e.name" },
  type: { expression: "e.`type`" },
  environment: { expression: "e.environment" },
  release: { expression: "e.`release`" },
  version: { expression: "e.`version`" },
  userId: { expression: "e.user_id" },
  sessionId: { expression: "e.session_id" },
  traceName: { expression: "e.trace_name" },
  experimentId: { expression: "e.experiment_id" },
  experimentName: { expression: "e.experiment_name" },
  experimentDatasetId: { expression: "e.experiment_dataset_id" },
  experimentItemId: { expression: "e.experiment_item_id" },
  level: { expression: "e.`level`" },
  statusMessage: { expression: "e.status_message" },
  promptName: { expression: "e.prompt_name" },
  promptId: { expression: "e.prompt_id" },
  promptVersion: { expression: "e.prompt_version" },
  modelId: { expression: "e.internal_model_id" },
  providedModelName: { expression: "e.provided_model_name" },
  totalCost: { expression: "e.total_cost" },
  inputTokens: { expression: "e.total_input_tokens" },
  outputTokens: { expression: "e.total_output_tokens" },
  totalTokens: {
    expression:
      "COALESCE(e.total_input_tokens, 0) + COALESCE(e.total_output_tokens, 0)",
  },
  inputCost: {
    expression: "JSON_EXTRACT_DOUBLE(e.cost_details, '$.input')",
  },
  outputCost: {
    expression: "JSON_EXTRACT_DOUBLE(e.cost_details, '$.output')",
  },
  latency: {
    expression: "MICROSECONDS_DIFF(e.end_time, e.start_time) / 1000000.0",
  },
  timeToFirstToken: {
    expression:
      "MICROSECONDS_DIFF(e.completion_start_time, e.start_time) / 1000000.0",
  },
  tokensPerSecond: {
    expression:
      "e.total_output_tokens / NULLIF(MICROSECONDS_DIFF(e.end_time, e.start_time) / 1000000.0, 0)",
  },
  input: { expression: "e.input", requiresFullContent: true },
  output: { expression: "e.output", requiresFullContent: true },
  metadata: { expression: "e.metadata", requiresFullContent: true },
  traceTags: { expression: "e.tags" },
  hasParentObservation: {
    expression: "e.parent_span_id IS NOT NULL AND e.parent_span_id != ''",
  },
  isRootObservation: {
    expression:
      "(e.parent_span_id IS NULL OR e.parent_span_id = '' OR e.is_app_root = TRUE)",
  },
  hasInput: {
    expression: "e.input IS NOT NULL AND e.input != ''",
    requiresFullContent: true,
  },
  hasOutput: {
    expression: "e.output IS NOT NULL AND e.output != ''",
    requiresFullContent: true,
  },
  calledToolNames: {
    expression: "e.tool_call_names",
    requiresFullContent: true,
  },
  toolNames: {
    expression: "JSON_KEYS(e.tool_definitions)",
    requiresFullContent: true,
  },
  toolDefinitions: {
    expression: "CARDINALITY(JSON_KEYS(e.tool_definitions))",
    requiresFullContent: true,
  },
  toolCalls: {
    expression: "CARDINALITY(e.tool_calls)",
    requiresFullContent: true,
  },
};

const SCORE_FILTER_LEVELS: Readonly<Record<string, "observation" | "trace">> = {
  scores: "observation",
  scores_avg: "observation",
  score_categories: "observation",
  score_booleans: "observation",
  trace_scores_avg: "trace",
  trace_score_categories: "trace",
  trace_score_booleans: "trace",
};

const EVENT_COLUMN_ALIASES: Readonly<Record<string, string>> = {
  "Trace Tags": "traceTags",
  "User ID": "userId",
  "Session ID": "sessionId",
  "Trace Name": "traceName",
  "Trace Environment": "environment",
  Environment: "environment",
  ID: "id",
  Type: "type",
  Name: "name",
  "Trace ID": "traceId",
  "Parent Observation ID": "parentObservationId",
  "Start Time": "startTime",
  "End Time": "endTime",
  Timestamp: "startTime",
  "Time To First Token (s)": "timeToFirstToken",
  "Latency (s)": "latency",
  "Tokens per second": "tokensPerSecond",
  "Input Cost ($)": "inputCost",
  "Output Cost ($)": "outputCost",
  "Total Cost ($)": "totalCost",
  Level: "level",
  "Status Message": "statusMessage",
  Model: "providedModelName",
  "Model ID": "modelId",
  "Input Tokens": "inputTokens",
  "Output Tokens": "outputTokens",
  "Total Tokens": "totalTokens",
  "Prompt Name": "promptName",
  "Prompt ID": "promptId",
  "Prompt Version": "promptVersion",
  Version: "version",
  Release: "release",
  Tags: "traceTags",
  model: "providedModelName",
  tokens: "totalTokens",
  tags: "traceTags",
  traceEnvironment: "environment",
  Scores: "scores_avg",
  SCORES: "scores_avg",
  "Scores (numeric)": "scores_avg",
  "Scores (categorical)": "score_categories",
  "Scores (boolean)": "score_booleans",
  "Trace Scores (numeric)": "trace_scores_avg",
  "Trace Scores (categorical)": "trace_score_categories",
  "Trace Scores (boolean)": "trace_score_booleans",
};

const MAX_DORIS_FILTERS = 100;
const MAX_DORIS_FILTER_VALUES = 1_000;
const MAX_DORIS_FILTER_VALUE_LENGTH = 10_000;
const MAX_DORIS_OBJECT_KEY_LENGTH = 256;
const MAX_DORIS_OBJECT_KEY_DEPTH = 16;

type DorisFilterBudgetInput = {
  readonly column: string;
  readonly value?: unknown;
  readonly key?: string;
};

export function assertDorisFilterBudget(
  filters: readonly DorisFilterBudgetInput[],
  queryLabel = "Doris analytics query",
): void {
  if (filters.length > MAX_DORIS_FILTERS) {
    throw new InvalidRequestError(`${queryLabel} has too many filters`);
  }

  let valueCount = 0;
  for (const filter of filters) {
    const values = Array.isArray(filter.value)
      ? filter.value
      : filter.value === undefined
        ? []
        : [filter.value];
    valueCount += values.length;
    if (valueCount > MAX_DORIS_FILTER_VALUES) {
      throw new InvalidRequestError(`${queryLabel} has too many filter values`);
    }
    if (
      values.some(
        (value) =>
          typeof value === "string" &&
          value.length > MAX_DORIS_FILTER_VALUE_LENGTH,
      )
    ) {
      throw new InvalidRequestError(`${queryLabel} filter value is too long`);
    }
    if (
      filter.column.length === 0 ||
      filter.column.length > MAX_DORIS_OBJECT_KEY_LENGTH
    ) {
      throw new InvalidRequestError(`Invalid ${queryLabel} filter column`);
    }
    if (
      filter.key !== undefined &&
      (filter.key.length === 0 ||
        filter.key.length > MAX_DORIS_OBJECT_KEY_LENGTH ||
        filter.key.split(".").length > MAX_DORIS_OBJECT_KEY_DEPTH)
    ) {
      throw new InvalidRequestError(`Invalid ${queryLabel} object key`);
    }
  }
}

export function normalizeDorisEventFilters(
  filters: EventsTableFilterState,
): EventsTableFilterState {
  return filters.map((filter) => ({
    ...filter,
    column: EVENT_COLUMN_ALIASES[filter.column] ?? filter.column,
  })) as EventsTableFilterState;
}

export type LogicalEventFilter = {
  readonly filter: EventsTableFilterState[number];
  readonly expression: string;
  readonly objectKey?: string;
};

export type LogicalPositionFilter = Extract<
  EventsTableFilterState[number],
  { readonly type: "positionInTrace" }
>;

export type LogicalEventScoreFilter = {
  readonly filter: Extract<
    EventsTableFilterState[number],
    {
      readonly type: "numberObject" | "booleanObject" | "categoryOptions";
    }
  >;
  readonly level: "observation" | "trace";
};

export function buildEventFilterPlan(filters: EventsTableFilterState): {
  readonly filters: readonly LogicalEventFilter[];
  readonly scoreFilters: readonly LogicalEventScoreFilter[];
  readonly positionFilter?: LogicalPositionFilter;
  readonly requiresFullContent: boolean;
} {
  assertDorisFilterBudget(filters, "Doris event query");
  const parsed = eventsTableFilterState.safeParse(
    normalizeDorisEventFilters(filters),
  );
  if (!parsed.success) {
    throw new InvalidRequestError("Invalid analytics filter state");
  }
  let requiresFullContent = false;
  const positionFilter = parsed.data.find(
    (filter): filter is LogicalPositionFilter =>
      filter.type === "positionInTrace",
  );
  const scoreFilters = parsed.data.flatMap(
    (filter): LogicalEventScoreFilter[] => {
      const level = SCORE_FILTER_LEVELS[filter.column];
      if (!level) return [];
      if (
        filter.type !== "numberObject" &&
        filter.type !== "booleanObject" &&
        filter.type !== "categoryOptions"
      ) {
        throw new InvalidRequestError(
          `Unsupported Doris score filter type: ${filter.type}`,
        );
      }
      return [{ filter, level }];
    },
  );
  const planned = parsed.data
    .filter(
      (filter) =>
        filter.type !== "positionInTrace" &&
        SCORE_FILTER_LEVELS[filter.column] === undefined,
    )
    .map((filter): LogicalEventFilter => {
      const column = EVENT_COLUMNS[filter.column];
      if (!column) {
        throw new InvalidRequestError(
          `Unsupported Doris analytics filter column: ${filter.column}`,
        );
      }
      requiresFullContent ||= column.requiresFullContent === true;
      if (
        filter.type === "stringObject" ||
        filter.type === "numberObject" ||
        filter.type === "booleanObject" ||
        filter.type === "categoryOptions"
      ) {
        if (filter.column !== "metadata") {
          throw new InvalidRequestError(
            `Unsupported Doris analytics object filter: ${filter.column}`,
          );
        }
        const castType =
          filter.type === "numberObject"
            ? "DOUBLE"
            : filter.type === "booleanObject"
              ? "BOOLEAN"
              : "STRING";
        return {
          filter,
          expression: `${castType === "STRING" ? "JSON_UNQUOTE(" : ""}CAST(ELEMENT_AT(e.metadata, ?) AS ${castType})${castType === "STRING" ? ")" : ""}`,
          objectKey: filter.key,
        };
      }
      return { filter, expression: column.expression };
    });
  return {
    filters: planned,
    scoreFilters,
    positionFilter,
    requiresFullContent,
  };
}
