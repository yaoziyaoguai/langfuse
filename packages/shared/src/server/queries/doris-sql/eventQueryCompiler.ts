import type { TracingSearchType } from "../../../interfaces/search";
import type { EventsTableFilterState } from "../../../types";
import { InvalidRequestError } from "../../../errors";
import { buildEventFilterPlan } from "../logical/filterPlan";
import {
  assertAnalyticsTimeRange,
  buildSearchPlan,
  type AnalyticsTimeRange,
} from "../logical/searchPlan";
import { compileDorisEventFilters } from "./filterCompiler";
import { compileDorisSearch } from "./searchCompiler";

const LIST_PROJECTION = `
  e.project_id,
  e.partition_date,
  e.trace_id,
  e.span_id,
  e.parent_span_id,
  e.version_token,
  e.\`type\`,
  e.\`name\`,
  e.environment,
  e.user_id,
  e.session_id,
  e.trace_name,
  e.\`release\`,
  e.\`version\`,
  e.\`level\`,
  e.status_message,
  e.is_app_root,
  e.bookmarked,
  e.\`public\`,
  e.start_time,
  e.end_time,
  e.completion_start_time,
  e.created_at,
  e.updated_at,
  e.provided_model_name,
  e.internal_model_id,
  e.prompt_id,
  e.prompt_name,
  e.prompt_version,
  e.total_input_tokens,
  e.total_output_tokens,
  e.total_cost,
  e.tags AS tags,
  e.usage_details AS usage_details,
  e.cost_details AS cost_details,
  e.provided_usage_details AS provided_usage_details,
  e.provided_cost_details AS provided_cost_details,
  CARDINALITY(JSON_KEYS(e.tool_definitions)) AS tool_definitions_count,
  CARDINALITY(e.tool_calls) AS tool_calls_count,
  e.input_preview AS input_preview,
  e.output_preview AS output_preview`;

const DETAIL_PROJECTION = `${LIST_PROJECTION},
  e.input AS input,
  e.output AS output,
  e.metadata AS metadata,
  e.model_parameters AS model_parameters,
  e.tool_definitions AS tool_definitions,
  e.tool_calls AS tool_calls,
  e.tool_call_names AS tool_call_names`;

function utcDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

function exclusivePartitionTo(value: Date): string {
  const lastIncluded = new Date(value.getTime() - 1);
  lastIncluded.setUTCDate(lastIncluded.getUTCDate() + 1);
  return utcDate(lastIncluded);
}

export type DorisEventCursor = {
  readonly startTime: Date;
  readonly traceId: string;
  readonly spanId: string;
};

export function compileDorisVisibleEventsQuery(input: {
  readonly projectId: string;
  readonly range: AnalyticsTimeRange | null;
  readonly projection: "list" | "detail";
  readonly filters: EventsTableFilterState;
  readonly search?: {
    readonly query: string;
    readonly searchType?: readonly TracingSearchType[];
  };
  readonly cursor?: DorisEventCursor;
  readonly limit: number;
}): {
  readonly sql: string;
  readonly params: readonly unknown[];
  readonly selectsFullContent: boolean;
} {
  if (
    !input.projectId ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 1_000
  ) {
    throw new InvalidRequestError("Invalid Doris event query input");
  }
  assertAnalyticsTimeRange(input.range);
  const filterPlan = buildEventFilterPlan(input.filters);
  const searchPlan = buildSearchPlan({
    range: input.range,
    search: input.search,
    filtersRequireFullContent: filterPlan.requiresFullContent,
  });
  const params: unknown[] = [];
  const bound = {
    params,
    bind(value: unknown) {
      params.push(value);
      return "?";
    },
  };
  const predicates = [
    `e.project_id = ${bound.bind(input.projectId)}`,
    `e.partition_date >= ${bound.bind(utcDate(input.range.from))}`,
    `e.partition_date < ${bound.bind(exclusivePartitionTo(input.range.to))}`,
    `e.start_time >= ${bound.bind(input.range.from)}`,
    `e.start_time < ${bound.bind(input.range.to)}`,
    "trace_deletion.trace_id IS NULL",
    "project_deletion.project_id IS NULL",
    ...compileDorisEventFilters(filterPlan.filters, bound),
  ];
  const search = compileDorisSearch(searchPlan, bound);
  if (search) predicates.push(search);
  if (input.cursor) {
    predicates.push(`(
      e.start_time < ${bound.bind(input.cursor.startTime)}
      OR (e.start_time = ${bound.bind(input.cursor.startTime)} AND e.trace_id < ${bound.bind(input.cursor.traceId)})
      OR (e.start_time = ${bound.bind(input.cursor.startTime)} AND e.trace_id = ${bound.bind(input.cursor.traceId)} AND e.span_id < ${bound.bind(input.cursor.spanId)})
    )`);
  }
  const sql = `SELECT
${input.projection === "detail" ? DETAIL_PROJECTION : LIST_PROJECTION}
FROM events_current e
LEFT JOIN trace_tombstones trace_deletion
  ON trace_deletion.project_id = e.project_id
 AND trace_deletion.trace_id = e.trace_id
LEFT JOIN project_tombstones project_deletion
  ON project_deletion.project_id = e.project_id
WHERE ${predicates.join("\n  AND ")}
ORDER BY e.start_time DESC, e.trace_id DESC, e.span_id DESC
LIMIT ${bound.bind(input.limit)}`;
  return {
    sql,
    params,
    selectsFullContent:
      input.projection === "detail" ||
      filterPlan.requiresFullContent ||
      searchPlan?.requiresFullContent === true,
  };
}
