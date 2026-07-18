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

type DorisVisibleEventScopeInput = {
  readonly projectId: string;
  readonly range: AnalyticsTimeRange | null;
  readonly filters: EventsTableFilterState;
  readonly search?: {
    readonly query: string;
    readonly searchType?: readonly TracingSearchType[];
  };
  readonly cursor?: DorisEventCursor;
  readonly partitionDates?: readonly string[];
};

function eventFromSql(
  eventAlias: string,
  traceDeletionAlias: string,
  projectDeletionAlias: string,
): string {
  return `FROM events_current ${eventAlias}
LEFT JOIN trace_tombstones ${traceDeletionAlias}
  ON ${traceDeletionAlias}.project_id = ${eventAlias}.project_id
 AND ${traceDeletionAlias}.trace_id = ${eventAlias}.trace_id
LEFT JOIN project_tombstones ${projectDeletionAlias}
  ON ${projectDeletionAlias}.project_id = ${eventAlias}.project_id`;
}

const EVENT_FROM_SQL = eventFromSql("e", "trace_deletion", "project_deletion");

export function compileDorisVisibleEventScope(
  input: DorisVisibleEventScopeInput,
): {
  readonly fromSql: string;
  readonly whereSql: string;
  readonly params: readonly unknown[];
  readonly selectsFullContent: boolean;
} {
  if (!input.projectId) {
    throw new InvalidRequestError("Invalid Doris event query input");
  }
  assertAnalyticsTimeRange(input.range);
  const range = input.range;
  const filterPlan = buildEventFilterPlan(input.filters);
  const searchPlan = buildSearchPlan({
    range: input.range,
    search: input.search,
    filtersRequireFullContent: filterPlan.requiresFullContent,
  });
  const params: unknown[] = [];
  const partitionDates = [...new Set(input.partitionDates ?? [])].sort();
  const compilePredicates = (aliases: {
    readonly event: string;
    readonly traceDeletion: string;
    readonly projectDeletion: string;
    readonly includeCursor?: boolean;
  }): string[] => {
    const bound = {
      params,
      bind(value: unknown) {
        params.push(value);
        return "?";
      },
    };
    const aliasedFilters = filterPlan.filters.map((planned) => ({
      ...planned,
      expression: planned.expression.replaceAll("e.", `${aliases.event}.`),
    }));
    const predicates = [
      `${aliases.event}.project_id = ${bound.bind(input.projectId)}`,
      `${aliases.event}.partition_date >= ${bound.bind(utcDate(range.from))}`,
      `${aliases.event}.partition_date < ${bound.bind(exclusivePartitionTo(range.to))}`,
      `${aliases.event}.start_time >= ${bound.bind(range.from)}`,
      `${aliases.event}.start_time < ${bound.bind(range.to)}`,
      ...(partitionDates.length > 0
        ? [
            `${aliases.event}.partition_date IN (${partitionDates
              .map((partitionDate) => bound.bind(partitionDate))
              .join(", ")})`,
          ]
        : []),
      `${aliases.traceDeletion}.trace_id IS NULL`,
      `${aliases.projectDeletion}.project_id IS NULL`,
      ...compileDorisEventFilters(aliasedFilters, bound),
    ];
    const search = compileDorisSearch(searchPlan, bound, aliases.event);
    if (search) predicates.push(search);
    if (aliases.includeCursor && input.cursor) {
      predicates.push(`(
      ${aliases.event}.start_time < ${bound.bind(input.cursor.startTime)}
      OR (${aliases.event}.start_time = ${bound.bind(input.cursor.startTime)} AND ${aliases.event}.trace_id < ${bound.bind(input.cursor.traceId)})
      OR (${aliases.event}.start_time = ${bound.bind(input.cursor.startTime)} AND ${aliases.event}.trace_id = ${bound.bind(input.cursor.traceId)} AND ${aliases.event}.span_id < ${bound.bind(input.cursor.spanId)})
    )`);
    }
    return predicates;
  };

  let fromSql = EVENT_FROM_SQL;
  if (filterPlan.positionFilter) {
    const positionAliases = {
      event: "position_event",
      traceDeletion: "position_trace_deletion",
      projectDeletion: "position_project_deletion",
    } as const;
    const positionPredicates = compilePredicates(positionAliases);
    const isFromEnd =
      filterPlan.positionFilter.key === "last" ||
      filterPlan.positionFilter.key === "nthFromEnd";
    const direction = isFromEnd ? "DESC" : "ASC";
    const position =
      filterPlan.positionFilter.key === "nthFromStart" ||
      filterPlan.positionFilter.key === "nthFromEnd"
        ? (filterPlan.positionFilter.value ?? 1)
        : 1;
    params.push(Math.max(1, position));
    fromSql = `${EVENT_FROM_SQL}
INNER JOIN (
  SELECT project_id, partition_date, trace_id, span_id
  FROM (
    SELECT
      position_event.project_id,
      position_event.partition_date,
      position_event.trace_id,
      position_event.span_id,
      ROW_NUMBER() OVER (
        PARTITION BY position_event.project_id, position_event.trace_id
        ORDER BY position_event.start_time ${direction}, position_event.version_token ${direction}, position_event.span_id ${direction}
      ) AS _position_rank
    ${eventFromSql(
      positionAliases.event,
      positionAliases.traceDeletion,
      positionAliases.projectDeletion,
    )}
    WHERE ${positionPredicates.join("\n      AND ")}
  ) ranked_position_events
  WHERE _position_rank = ?
) position_match
  ON position_match.project_id = e.project_id
 AND position_match.partition_date = e.partition_date
 AND position_match.trace_id = e.trace_id
 AND position_match.span_id = e.span_id`;
  }

  const predicates = compilePredicates({
    event: "e",
    traceDeletion: "trace_deletion",
    projectDeletion: "project_deletion",
    includeCursor: true,
  });
  return {
    fromSql,
    whereSql: predicates.join("\n  AND "),
    params,
    selectsFullContent:
      filterPlan.requiresFullContent ||
      searchPlan?.requiresFullContent === true,
  };
}

export function compileDorisVisibleEventsQuery(
  input: DorisVisibleEventScopeInput & {
    readonly projection: "list" | "detail";
    readonly limit: number;
  },
): {
  readonly sql: string;
  readonly params: readonly unknown[];
  readonly selectsFullContent: boolean;
} {
  if (
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 1_000
  ) {
    throw new InvalidRequestError("Invalid Doris event query input");
  }
  const scope = compileDorisVisibleEventScope(input);
  const sql = `SELECT
${input.projection === "detail" ? DETAIL_PROJECTION : LIST_PROJECTION}
${scope.fromSql}
WHERE ${scope.whereSql}
ORDER BY e.start_time DESC, e.trace_id DESC, e.span_id DESC
LIMIT ?`;
  return {
    sql,
    params: [...scope.params, input.limit],
    selectsFullContent:
      input.projection === "detail" || scope.selectsFullContent,
  };
}
