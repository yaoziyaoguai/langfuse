import type { AnalyticsEntityType } from "@prisma/client";

import type { FilterState, EventsTableFilterState } from "../../types";
import type {
  ColumnDefinition,
  UiColumnMappings,
} from "../../tableDefinitions";
import type { RenderingProps } from "../utils/rendering";
import { InvalidRequestError } from "../../errors";
import { prisma } from "../../db";
import { traceException } from "../instrumentation";
import { logger } from "../logger";
import { normalizeDorisEventFilters } from "../queries/logical/filterPlan";
import {
  getTraceByIdFromEventsTable,
  getTracesCountFromEventsTableForPublicApi,
  getTracesFromEventsTableForPublicApi,
  getTracesIdentifierForSessionFromEvents,
  getUserMetricsFromEventsTable,
  getUsersCountFromEventsTable,
  hasAnyTraceFromEventsTable,
  hasAnyUserFromEventsTable,
} from "./events";
import { buildDorisDerivedQuery } from "./telemetry/doris/derivedUi";
import { toDorisTraceDomain } from "./telemetry/doris/adapters";
import type { DorisTrace } from "./telemetry/doris/traces";
import { getDorisTelemetryRepositories } from "./telemetry/doris/runtime";
import type { TraceRecordReadType } from "./definitions";

function r1bUnavailable(name: string): never {
  throw new InvalidRequestError(`${name} is unavailable in Doris R1A`);
}

export const checkTraceExistsAndGetTimestamp = async (..._args: unknown[]) =>
  r1bUnavailable("Evaluation trace matching");

export const upsertTrace = async (_trace: Partial<TraceRecordReadType>) =>
  r1bUnavailable("Legacy trace writes");

export const getTracesByIds = async (
  traceIds: string[],
  projectId: string,
  timestamp?: Date,
  _analyticsConfigs?: Record<string, unknown>,
) => {
  const traces = await Promise.all(
    traceIds.map((traceId) =>
      getTraceByIdFromEventsTable({ traceId, projectId, timestamp }),
    ),
  );
  return traces.filter((trace) => trace !== undefined);
};

export const getTracesBySessionId = async (
  projectId: string,
  sessionIds: string[],
  timestamp?: Date,
) => {
  if (sessionIds.length === 0) return [];
  const query = buildDorisDerivedQuery(
    [
      {
        type: "stringOptions",
        column: "id",
        operator: "any of",
        value: sessionIds,
      },
      ...(timestamp
        ? ([
            {
              type: "datetime",
              column: "createdAt",
              operator: ">=",
              value: timestamp,
            },
          ] as const)
        : []),
    ],
    "session",
  );
  const traces: DorisTrace[] = [];
  let cursor: string | undefined;
  do {
    const page = await getDorisTelemetryRepositories().traces.list({
      projectId,
      range: query.range,
      filters: query.filters,
      cursor,
      limit: 999,
    });
    traces.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  const controls = await prisma.traceControlState.findMany({
    where: { projectId, traceId: { in: traces.map((trace) => trace.id) } },
    select: { traceId: true, bookmarked: true, public: true },
  });
  const controlsById = new Map(
    controls.map((control) => [control.traceId, control]),
  );
  return traces.map((trace) =>
    toDorisTraceDomain(
      trace,
      controlsById.get(trace.id) ?? { bookmarked: false, public: false },
    ),
  );
};

export const readProjectHasTracesFlag = async (
  projectId: string,
): Promise<boolean> => {
  try {
    return Boolean(
      (
        await prisma.project.findUnique({
          where: { id: projectId },
          select: { hasTraces: true },
        })
      )?.hasTraces,
    );
  } catch (error) {
    traceException(error);
    logger.error("Failed to read hasTraces flag", { projectId, error });
    return false;
  }
};

export const persistProjectHasTracesFlag = async (
  projectId: string,
): Promise<void> => {
  try {
    await prisma.project.updateMany({
      where: { id: projectId, hasTraces: false },
      data: { hasTraces: true },
    });
  } catch (error) {
    traceException(error);
    logger.error("Failed to persist hasTraces flag", { projectId, error });
  }
};

export const hasAnyTrace = async (projectId: string) => {
  if (await readProjectHasTracesFlag(projectId)) return true;
  const exists = await hasAnyTraceFromEventsTable(projectId);
  if (exists) await persistProjectHasTracesFlag(projectId);
  return exists;
};

export const getTraceCountsByProjectInCreationInterval = async (input: {
  start: Date;
  end: Date;
}) => [
  ...(await getDorisTelemetryRepositories().traces.countByProjectCreatedAt(
    input,
  )),
];

export const getTraceCountOfProjectsSinceCreationDate = (input: {
  projectIds: string[];
  start: Date;
}) => getDorisTelemetryRepositories().traces.countProjectsSince(input);

export const getTraceCountsByProjectAndDay = async (input: {
  startDate: Date;
  endDate: Date;
}) => [
  ...(await getDorisTelemetryRepositories().traces.countByProjectAndDay({
    start: input.startDate,
    end: input.endDate,
  })),
];

export const getTraceByIdFromTracesTable = async (params: {
  traceId: string;
  projectId: string;
  timestamp?: Date;
  fromTimestamp?: Date;
  renderingProps?: RenderingProps;
  excludeInputOutput?: boolean;
  excludeMetadata?: boolean;
}) => getTraceByIdFromEventsTable(params);

function traceFacetQuery(filter: FilterState): {
  range: { from: Date; to: Date };
  filters: EventsTableFilterState;
} {
  const mapped = normalizeDorisEventFilters(
    filter.map((item) => ({
      ...item,
      column:
        item.column === "id"
          ? "traceId"
          : item.column === "timestamp" || item.column === "createdAt"
            ? "startTime"
            : item.column === "traceName"
              ? "name"
              : item.column,
    })) as EventsTableFilterState,
  );
  const lower = mapped.flatMap((item) =>
    item.type === "datetime" &&
    item.column === "startTime" &&
    (item.operator === ">" || item.operator === ">=")
      ? [item.value]
      : [],
  );
  const upper = mapped.flatMap((item) =>
    item.type === "datetime" &&
    item.column === "startTime" &&
    (item.operator === "<" || item.operator === "<=")
      ? [item.value]
      : [],
  );
  return {
    range: {
      from:
        lower.length > 0
          ? new Date(Math.max(...lower.map((value) => value.getTime())))
          : new Date(0),
      to:
        upper.length > 0
          ? new Date(Math.min(...upper.map((value) => value.getTime() + 1)))
          : new Date(),
    },
    filters: mapped,
  };
}

export const getTracesGroupedByName = async (
  projectId: string,
  _tableDefinitions?: UiColumnMappings,
  timestampFilter: FilterState = [],
) => {
  const query = traceFacetQuery(timestampFilter);
  const rows = await getDorisTelemetryRepositories().traces.filterOptionValues({
    projectId,
    range: query.range,
    filters: query.filters,
    column: "name",
    limit: 1_000,
  });
  return rows.map((row) => ({ name: row.value, count: String(row.count) }));
};

export const getTracesGroupedBySessionId = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit = 1_000,
  offset = 0,
  _columns?: UiColumnMappings,
  _columnDefinitions?: ColumnDefinition[],
) => {
  const query = traceFacetQuery(filter);
  const rows = await getDorisTelemetryRepositories().traces.filterOptionValues({
    projectId,
    range: query.range,
    filters: query.filters,
    column: "sessionId",
    valueQuery: searchQuery,
    limit,
    offset,
  });
  return rows.map((row) => ({
    session_id: row.value,
    count: String(row.count),
  }));
};

export const getTracesGroupedByUsers = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit = 1_000,
  offset = 0,
  _columns?: UiColumnMappings,
  _columnDefinitions?: ColumnDefinition[],
) => {
  const query = traceFacetQuery(filter);
  const rows = await getDorisTelemetryRepositories().traces.filterOptionValues({
    projectId,
    range: query.range,
    filters: query.filters,
    column: "userId",
    valueQuery: searchQuery,
    limit,
    offset,
  });
  return rows.map((row) => ({ user: row.value, count: String(row.count) }));
};

export type GroupedTracesQueryProp = {
  projectId: string;
  filter: FilterState;
  columns?: UiColumnMappings;
  columnDefinitions?: ColumnDefinition[];
};

export const getTracesGroupedByTags = async ({
  projectId,
  filter,
}: GroupedTracesQueryProp) => {
  const query = traceFacetQuery(filter);
  const rows = await getDorisTelemetryRepositories().traces.filterOptionValues({
    projectId,
    range: query.range,
    filters: query.filters,
    column: "tags",
    limit: 1_000,
  });
  return rows.map((row) => ({ value: row.value }));
};

export const getTracesIdentifierForSessionFromTracesTable =
  getTracesIdentifierForSessionFromEvents;

export const hasAnyUser = hasAnyUserFromEventsTable;

export const getTotalUserCount = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
): Promise<{ totalCount: bigint }[]> =>
  (await getUsersCountFromEventsTable(projectId, filter, searchQuery)).map(
    ({ totalCount }) => ({ totalCount: BigInt(totalCount) }),
  );

export const getUserMetrics = getUserMetricsFromEventsTable;

export const getTracesByIdsForAnyProject = async (traceIds: string[]) => {
  if (traceIds.length === 0) return [];
  const heads = await prisma.analyticsEntityHead.findMany({
    where: {
      entityType: "EVENT" satisfies AnalyticsEntityType,
      owningTraceId: { in: traceIds },
    },
    select: { projectId: true, owningTraceId: true },
    distinct: ["projectId", "owningTraceId"],
  });
  return heads.flatMap((head) =>
    head.owningTraceId
      ? [{ id: head.owningTraceId, projectId: head.projectId }]
      : [],
  );
};

function dateValue(value: string): Date {
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (!Number.isFinite(parsed.getTime()))
    throw new InvalidRequestError("Invalid graph timestamp");
  return parsed;
}

export async function getAgentGraphData(params: {
  projectId: string;
  traceId: string;
  chMinStartTime: string;
  chMaxStartTime: string;
}) {
  const records: Array<{
    id: string;
    parent_observation_id: string | null;
    type: string;
    name: string;
    start_time: string;
    end_time: string | null;
    node: string | null;
    step: unknown;
  }> = [];
  for (let offset = 0; ; offset += 999) {
    const page =
      await getDorisTelemetryRepositories().observations.listForTrace({
        projectId: params.projectId,
        traceId: params.traceId,
        filters: [
          {
            type: "datetime",
            column: "startTime",
            operator: ">=",
            value: dateValue(params.chMinStartTime),
          },
          {
            type: "datetime",
            column: "startTime",
            operator: "<=",
            value: dateValue(params.chMaxStartTime),
          },
        ],
        includeFullContent: true,
        orderBy: { column: "startTime", order: "ASC" },
        offset,
        limit: 999,
      });
    records.push(
      ...page.items.map((observation) => ({
        id: observation.id,
        parent_observation_id: observation.parentObservationId,
        type: observation.type,
        name: observation.name ?? "",
        start_time: observation.startTime.toISOString(),
        end_time: observation.endTime?.toISOString() ?? null,
        node:
          typeof observation.metadata?.langgraph_node === "string"
            ? observation.metadata.langgraph_node
            : null,
        step: observation.metadata?.langgraph_step ?? null,
      })),
    );
    if (page.items.length < 999) break;
  }
  return records;
}

export const TRACE_FIELD_GROUPS = [
  "core",
  "io",
  "scores",
  "observations",
  "metrics",
] as const;
export type TraceFieldGroup = (typeof TRACE_FIELD_GROUPS)[number];

export type TraceQueryType = {
  page: number;
  limit: number;
  projectId: string;
  traceId?: string;
  userId?: string;
  name?: string;
  type?: string;
  sessionId?: string;
  version?: string;
  release?: string;
  tags?: string | string[];
  environment?: string | string[];
  fromTimestamp?: string;
  toTimestamp?: string;
  fields?: TraceFieldGroup[];
  useEventsTable?: boolean | null;
};

export const generateTracesForPublicApi = async (input: {
  projectId: string;
  filter: FilterState;
  orderBy: { column: string; order: "ASC" | "DESC" } | null;
  pagination?: { limit: number; page: number };
  fields?: TraceFieldGroup[];
}) =>
  getTracesFromEventsTableForPublicApi({
    projectId: input.projectId,
    page: input.pagination?.page ?? 1,
    limit: input.pagination?.limit ?? 50,
    fields: input.fields,
    advancedFilters: input.filter as EventsTableFilterState,
    orderBy: input.orderBy,
  });

export const getTracesCountForPublicApi = async (input: {
  projectId: string;
  filter: FilterState;
  pagination?: { limit: number; page: number };
}) =>
  getTracesCountFromEventsTableForPublicApi({
    projectId: input.projectId,
    page: input.pagination?.page ?? 1,
    limit: input.pagination?.limit ?? 50,
    advancedFilters: input.filter as EventsTableFilterState,
  });

export const deleteTraces = async (..._args: unknown[]) =>
  r1bUnavailable("Direct trace deletion");
export const hasAnyTraceOlderThan = async (..._args: unknown[]) =>
  r1bUnavailable("Trace retention scans");
export const deleteTracesOlderThanDays = async (..._args: unknown[]) =>
  r1bUnavailable("Trace retention deletion");
export const deleteTracesByProjectId = async (..._args: unknown[]) =>
  r1bUnavailable("Direct project trace deletion");
export async function* getTracesForAnalyticsIntegrations(
  ..._args: unknown[]
): AsyncGenerator<never> {
  yield* [] as never[];
  r1bUnavailable("Analytics integrations");
}
