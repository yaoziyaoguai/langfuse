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
  startTime: { expression: "e.start_time" },
  endTime: { expression: "e.end_time" },
  name: { expression: "e.name" },
  type: { expression: "e.`type`" },
  environment: { expression: "e.environment" },
  version: { expression: "e.`version`" },
  userId: { expression: "e.user_id" },
  sessionId: { expression: "e.session_id" },
  traceName: { expression: "e.trace_name" },
  level: { expression: "e.`level`" },
  statusMessage: { expression: "e.status_message" },
  promptName: { expression: "e.prompt_name" },
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
    expression:
      "TIMESTAMPDIFF(MICROSECOND, e.start_time, e.end_time) / 1000000.0",
  },
  timeToFirstToken: {
    expression:
      "TIMESTAMPDIFF(MICROSECOND, e.start_time, e.completion_start_time) / 1000000.0",
  },
  tokensPerSecond: {
    expression:
      "e.total_output_tokens / NULLIF(TIMESTAMPDIFF(MICROSECOND, e.start_time, e.end_time) / 1000000.0, 0)",
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
  hasInput: { expression: "e.input IS NOT NULL AND e.input != ''" },
  hasOutput: { expression: "e.output IS NOT NULL AND e.output != ''" },
  calledToolNames: { expression: "e.tool_call_names" },
  toolNames: { expression: "JSON_KEYS(e.tool_definitions)" },
  toolDefinitions: {
    expression: "CARDINALITY(JSON_KEYS(e.tool_definitions))",
  },
  toolCalls: { expression: "CARDINALITY(e.tool_calls)" },
};

const U6_COLUMNS = new Set([
  "scores",
  "scores_avg",
  "score_categories",
  "score_booleans",
  "trace_scores_avg",
  "trace_score_categories",
  "trace_score_booleans",
]);

export type LogicalEventFilter = {
  readonly filter: EventsTableFilterState[number];
  readonly expression: string;
  readonly objectKey?: string;
};

export function buildEventFilterPlan(filters: EventsTableFilterState): {
  readonly filters: readonly LogicalEventFilter[];
  readonly requiresFullContent: boolean;
} {
  const parsed = eventsTableFilterState.safeParse(filters);
  if (!parsed.success) {
    throw new InvalidRequestError("Invalid analytics filter state");
  }
  let requiresFullContent = false;
  const planned = parsed.data.map((filter): LogicalEventFilter => {
    if (filter.type === "positionInTrace") {
      throw new InvalidRequestError(
        "Position-in-trace filters require the trace query plan",
      );
    }
    if (U6_COLUMNS.has(filter.column)) {
      throw new InvalidRequestError(
        "Score filters are unavailable until the Doris score query plan is active",
      );
    }
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
      if (filter.column !== "metadata" || filter.type !== "stringObject") {
        throw new InvalidRequestError(
          `Unsupported Doris analytics object filter: ${filter.column}`,
        );
      }
      return {
        filter,
        expression: "CAST(ELEMENT_AT(e.metadata, ?) AS STRING)",
        objectKey: filter.key,
      };
    }
    return { filter, expression: column.expression };
  });
  return { filters: planned, requiresFullContent };
}
