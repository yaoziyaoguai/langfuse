import { InvalidRequestError } from "../../errors";
import type { OrderByState } from "../../interfaces/orderBy";
import type { FilterState } from "../../types";
import {
  buildDorisDerivedQuery,
  toDorisSessionEventsRow,
  toDorisSessionMetricsRow,
} from "../repositories/telemetry/doris/derivedUi";
import { getDorisTelemetryRepositories } from "../repositories/telemetry/doris/runtime";
import type { DorisSession } from "../repositories/telemetry/doris/sessions";
import type { DorisTrace } from "../repositories/telemetry/doris/traces";

type SessionEventsBaseReturnType = {
  session_id: string;
  max_timestamp: string;
  min_timestamp: string;
  trace_ids: string[];
  user_ids: string[];
  trace_count: number;
  trace_tags: string[];
  environment?: string;
};

type SessionScoreFields = {
  scores_avg?: Array<Array<[string, number]>>;
  score_categories?: Array<Array<string>>;
  score_booleans?: Array<Array<string>>;
};

export type SessionEventsDataReturnType = SessionEventsBaseReturnType &
  SessionScoreFields;

export type SessionTraceFromEvents = {
  id: string;
  name: string | null;
  timestamp: Date;
  environment: string | null;
  userId: string | null;
};

type SessionTableProps = {
  projectId: string;
  filter: FilterState;
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
};

async function getDorisSessionsPage(
  props: SessionTableProps,
): Promise<readonly DorisSession[]> {
  if (props.orderBy && props.orderBy.column !== "createdAt") {
    throw new InvalidRequestError(
      `Unsupported Doris session order: ${props.orderBy.column} ${props.orderBy.order}`,
    );
  }

  const query = buildDorisDerivedQuery(props.filter, "session");
  const limit = props.limit ?? 999;
  const targetPage = props.page ?? 0;
  let cursor: string | undefined;

  for (let page = 0; page <= targetPage; page += 1) {
    const result = await getDorisTelemetryRepositories().sessions.list({
      projectId: props.projectId,
      range: query.range,
      filters: [],
      sessionFilters: query.sessionFilters,
      order: props.orderBy?.order ?? "DESC",
      cursor,
      limit,
    });
    if (page === targetPage) return result.items;
    if (!result.nextCursor) return [];
    cursor = result.nextCursor;
  }

  return [];
}

export const getSessionTracesFromEvents = async (props: {
  projectId: string;
  sessionId: string;
}) => {
  const query = buildDorisDerivedQuery(
    [
      {
        type: "stringOptions",
        column: "id",
        operator: "any of",
        value: [props.sessionId],
      },
    ],
    "session",
  );
  const traces: DorisTrace[] = [];
  let cursor: string | undefined;
  do {
    const page = await getDorisTelemetryRepositories().traces.list({
      projectId: props.projectId,
      range: query.range,
      filters: query.filters,
      cursor,
      limit: 999,
    });
    traces.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor);

  return traces
    .map((trace) => ({
      id: trace.id,
      name: trace.name,
      timestamp: trace.timestamp,
      environment: trace.environment,
      userId: trace.userId,
    }))
    .sort(
      (left, right) => left.timestamp.getTime() - right.timestamp.getTime(),
    );
};

export const getSessionsTableCountFromEvents = (props: SessionTableProps) => {
  const query = buildDorisDerivedQuery(props.filter, "session");
  return getDorisTelemetryRepositories().sessions.count({
    projectId: props.projectId,
    range: query.range,
    filters: [],
    sessionFilters: query.sessionFilters,
  });
};

export const getSessionsTableFromEvents = async (props: SessionTableProps) =>
  (await getDorisSessionsPage(props)).map(toDorisSessionEventsRow);

export const getSessionsWithMetricsFromEvents = async (
  props: SessionTableProps,
) => (await getDorisSessionsPage(props)).map(toDorisSessionMetricsRow);
