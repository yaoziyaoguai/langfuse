import { InvalidRequestError } from "../../../../errors";
import type { EventsTableFilterState, FilterState } from "../../../../types";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";
import type { DorisSession } from "./sessions";
import type { DorisUser } from "./users";

const SESSION_COLUMNS: Readonly<Record<string, string>> = {
  createdAt: "startTime",
  id: "sessionId",
  userIds: "userId",
  environment: "environment",
  traceTags: "traceTags",
  metadata: "metadata",
};

const USER_COLUMNS: Readonly<Record<string, string>> = {
  timestamp: "startTime",
  Timestamp: "startTime",
  userId: "userId",
  environment: "environment",
};

export function buildDorisDerivedQuery(
  filters: FilterState,
  kind: "session" | "user",
  now = new Date(),
): {
  readonly range: AnalyticsTimeRange;
  readonly filters: EventsTableFilterState;
  readonly sessionFilters: FilterState;
} {
  const columns = kind === "session" ? SESSION_COLUMNS : USER_COLUMNS;
  const mapped: EventsTableFilterState = [];
  const lowerBounds: Date[] = [];
  const upperBounds: Date[] = [];

  for (const filter of filters) {
    const column = columns[filter.column];
    if (!column) {
      throw new InvalidRequestError(
        `Unsupported Doris ${kind} aggregate filter: ${filter.column}`,
      );
    }
    const mappedFilter = { ...filter, column };
    mapped.push(mappedFilter);
    if (mappedFilter.type === "datetime" && column === "startTime") {
      if (mappedFilter.operator === ">" || mappedFilter.operator === ">=") {
        lowerBounds.push(mappedFilter.value);
      } else {
        upperBounds.push(mappedFilter.value);
      }
    }
  }

  const from = lowerBounds.length
    ? new Date(Math.max(...lowerBounds.map((value) => value.getTime())))
    : new Date(0);
  const to = upperBounds.length
    ? new Date(Math.min(...upperBounds.map((value) => value.getTime())))
    : new Date(now);
  return {
    range: { from, to },
    filters: mapped,
    sessionFilters: kind === "session" ? [...filters] : [],
  };
}

export function toDorisSessionEventsRow(session: DorisSession) {
  return {
    session_id: session.id,
    max_timestamp: session.maxTimestamp.toISOString(),
    min_timestamp: session.minTimestamp.toISOString(),
    trace_ids: [...session.traceIds],
    user_ids: [...session.userIds],
    trace_count: session.traceCount,
    trace_tags: [...session.tags],
    environment: session.environments[0],
  };
}

export function toDorisSessionMetricsRow(session: DorisSession) {
  return {
    ...toDorisSessionEventsRow(session),
    total_observations: session.observationCount,
    duration: session.duration,
    session_usage_details: {
      input: session.totalInputTokens,
      output: session.totalOutputTokens,
      total: session.totalUsage,
    },
    session_cost_details: {
      total: session.totalCost ?? 0,
    },
    session_input_cost: "0",
    session_output_cost: "0",
    session_total_cost: String(session.totalCost ?? 0),
    session_input_usage: String(session.totalInputTokens),
    session_output_usage: String(session.totalOutputTokens),
    session_total_usage: String(session.totalUsage),
  };
}

export function toDorisUserMetricsRow(user: DorisUser) {
  return {
    userId: user.id,
    environment: user.environments[0] ?? "",
    maxTimestamp: user.maxTimestamp,
    minTimestamp: user.minTimestamp,
    inputUsage: user.totalInputTokens,
    outputUsage: user.totalOutputTokens,
    totalUsage: user.totalUsage,
    observationCount: user.observationCount,
    traceCount: user.traceCount,
    totalCost: user.totalCost ?? 0,
  };
}
