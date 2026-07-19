import Decimal from "decimal.js";

import {
  ObservationLevel,
  type ObservationLevelType,
  type TraceDomain,
} from "../../domain";
import { InvalidRequestError } from "../../errors";
import type { TraceDeleteBatchActionCursor } from "../../features/batchAction/types";
import type { ScoreAggregate } from "../../features/scores";
import type { OrderByState } from "../../interfaces/orderBy";
import type { TracingSearchType } from "../../interfaces/search";
import type { EventsTableFilterState, FilterState } from "../../types";
import { prisma } from "../../db";
import { getTraceDeleteCursorPageFromEvents } from "../repositories/events";
import { getDorisTelemetryRepositories } from "../repositories/telemetry/doris/runtime";
import type { DorisObservation } from "../repositories/telemetry/doris/observations";
import type {
  DorisTrace,
  DorisTraceOrderBy,
} from "../repositories/telemetry/doris/traces";

export type TracesTableUiReturnType = Pick<
  TraceDomain,
  | "id"
  | "projectId"
  | "timestamp"
  | "tags"
  | "bookmarked"
  | "name"
  | "release"
  | "version"
  | "userId"
  | "environment"
  | "sessionId"
  | "public"
>;

export type TracesMetricsUiReturnType = {
  id: string;
  projectId: string;
  promptTokens: bigint;
  completionTokens: bigint;
  totalTokens: bigint;
  latency: number | null;
  level: ObservationLevelType;
  observationCount: bigint;
  calculatedTotalCost: Decimal | null;
  calculatedInputCost: Decimal | null;
  calculatedOutputCost: Decimal | null;
  scores: ScoreAggregate;
  usageDetails: Record<string, number>;
  costDetails: Record<string, number>;
  errorCount: bigint;
  warningCount: bigint;
  defaultCount: bigint;
  debugCount: bigint;
};

type TraceTableProps = {
  projectId: string;
  filter: FilterState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  orderBy?: OrderByState;
  limit?: number;
  page?: number;
};

async function buildDorisTraceReadQuery(
  projectId: string,
  filter: FilterState,
): Promise<{
  readonly range: { readonly from: Date; readonly to: Date };
  readonly filters: EventsTableFilterState;
  readonly impossible: boolean;
}> {
  const mapped: FilterState = [];
  const lowerBounds: Date[] = [];
  const upperBounds: Date[] = [];
  let impossible = false;

  for (const item of filter) {
    if (item.column === "bookmarked") {
      if (item.type !== "boolean" || item.operator !== "=") {
        throw new InvalidRequestError(
          "Unsupported Doris bookmarked trace filter",
        );
      }
      const bookmarked = await prisma.traceControlState.findMany({
        where: { projectId, bookmarked: true },
        select: { traceId: true },
      });
      if (item.value && bookmarked.length === 0) {
        impossible = true;
      } else if (bookmarked.length > 0) {
        mapped.push({
          type: "stringOptions",
          column: "traceId",
          operator: item.value ? "any of" : "none of",
          value: bookmarked.map(({ traceId }) => traceId),
        });
      }
      continue;
    }

    const column =
      item.column === "timestamp"
        ? "startTime"
        : item.column === "id" || item.column === "ID"
          ? "traceId"
          : item.column === "traceName"
            ? "name"
            : item.column;
    const mappedItem = { ...item, column };
    mapped.push(mappedItem);
    if (item.type === "datetime" && item.column === "timestamp") {
      if (item.operator === ">" || item.operator === ">=") {
        lowerBounds.push(item.value);
      } else {
        upperBounds.push(
          item.operator === "<="
            ? new Date(item.value.getTime() + 1)
            : item.value,
        );
      }
    }
  }

  return {
    range: {
      from:
        lowerBounds.length > 0
          ? new Date(Math.max(...lowerBounds.map((value) => value.getTime())))
          : new Date(0),
      to:
        upperBounds.length > 0
          ? new Date(Math.min(...upperBounds.map((value) => value.getTime())))
          : new Date(),
    },
    filters: mapped as EventsTableFilterState,
    impossible,
  };
}

function toDorisTraceOrderBy(
  orderBy: OrderByState | undefined,
): DorisTraceOrderBy | undefined {
  if (!orderBy) return undefined;
  const column = orderBy.column === "traceName" ? "name" : orderBy.column;
  const supported = new Set<DorisTraceOrderBy["column"]>([
    "timestamp",
    "name",
    "userId",
    "sessionId",
    "environment",
    "version",
    "release",
  ]);
  if (!supported.has(column as DorisTraceOrderBy["column"])) {
    throw new InvalidRequestError(
      `Unsupported Doris trace order column: ${orderBy.column}`,
    );
  }
  return {
    column: column as DorisTraceOrderBy["column"],
    order: orderBy.order,
  };
}

async function listDorisTraces(props: TraceTableProps) {
  const query = await buildDorisTraceReadQuery(props.projectId, props.filter);
  if (query.impossible) return [];
  const page = await getDorisTelemetryRepositories().traces.list({
    projectId: props.projectId,
    range: query.range,
    filters: query.filters,
    search: props.searchQuery
      ? { query: props.searchQuery, searchType: props.searchType }
      : undefined,
    orderBy: toDorisTraceOrderBy(props.orderBy),
    offset: (props.page ?? 0) * (props.limit ?? 999),
    limit: props.limit ?? 999,
  });
  return page.items;
}

function sumDetails(
  target: Record<string, number>,
  source: Readonly<Record<string, number>>,
) {
  for (const [key, value] of Object.entries(source)) {
    target[key] = (target[key] ?? 0) + value;
  }
}

async function listTraceObservations(trace: DorisTrace) {
  const observations: DorisObservation[] = [];
  let cursor: string | undefined;
  do {
    const page =
      await getDorisTelemetryRepositories().observations.listForTrace({
        projectId: trace.projectId,
        traceId: trace.id,
        filters: [],
        cursor,
        limit: 999,
      });
    observations.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  return observations;
}

async function toTraceMetrics(
  trace: DorisTrace,
): Promise<Omit<TracesMetricsUiReturnType, "scores">> {
  const observations = await listTraceObservations(trace);
  const usageDetails: Record<string, number> = {};
  const costDetails: Record<string, number> = {};
  const levelCounts: Record<ObservationLevelType, number> = {
    DEBUG: 0,
    DEFAULT: 0,
    WARNING: 0,
    ERROR: 0,
  };
  for (const observation of observations) {
    sumDetails(usageDetails, observation.usageDetails);
    sumDetails(costDetails, observation.costDetails);
    const level = Object.hasOwn(ObservationLevel, observation.level ?? "")
      ? (observation.level as ObservationLevelType)
      : ObservationLevel.DEFAULT;
    levelCounts[level] += 1;
  }
  const level = levelCounts.ERROR
    ? ObservationLevel.ERROR
    : levelCounts.WARNING
      ? ObservationLevel.WARNING
      : levelCounts.DEFAULT
        ? ObservationLevel.DEFAULT
        : ObservationLevel.DEBUG;
  const decimal = (value: number | undefined) =>
    value === undefined ? null : new Decimal(value);
  return {
    id: trace.id,
    projectId: trace.projectId,
    promptTokens: BigInt(trace.totalInputTokens),
    completionTokens: BigInt(trace.totalOutputTokens),
    totalTokens: BigInt(trace.totalUsage),
    latency: trace.latency,
    level,
    observationCount: BigInt(observations.length),
    calculatedTotalCost: decimal(trace.totalCost ?? undefined),
    calculatedInputCost: decimal(costDetails.input),
    calculatedOutputCost: decimal(costDetails.output),
    usageDetails,
    costDetails,
    errorCount: BigInt(levelCounts.ERROR),
    warningCount: BigInt(levelCounts.WARNING),
    defaultCount: BigInt(levelCounts.DEFAULT),
    debugCount: BigInt(levelCounts.DEBUG),
  };
}

export const getTracesTableCount = async (props: TraceTableProps) => {
  const query = await buildDorisTraceReadQuery(props.projectId, props.filter);
  if (query.impossible) return 0;
  return getDorisTelemetryRepositories().traces.count({
    projectId: props.projectId,
    range: query.range,
    filters: query.filters,
    search: props.searchQuery
      ? { query: props.searchQuery, searchType: props.searchType }
      : undefined,
  });
};

export const getTracesTable = async (
  props: TraceTableProps,
): Promise<TracesTableUiReturnType[]> => {
  const traces = await listDorisTraces(props);
  const controls = await prisma.traceControlState.findMany({
    where: {
      projectId: props.projectId,
      traceId: { in: traces.map(({ id }) => id) },
    },
    select: { traceId: true, bookmarked: true, public: true },
  });
  const controlsByTraceId = new Map(
    controls.map((control) => [control.traceId, control]),
  );
  return traces.map((trace) => {
    const control = controlsByTraceId.get(trace.id);
    return {
      id: trace.id,
      projectId: trace.projectId,
      timestamp: trace.timestamp,
      tags: [...trace.tags],
      bookmarked: control?.bookmarked ?? false,
      name: trace.name,
      release: trace.release,
      version: trace.version,
      userId: trace.userId,
      environment: trace.environment,
      sessionId: trace.sessionId,
      public: control?.public ?? false,
    };
  });
};

export const getTracesTableMetrics = async (props: TraceTableProps) =>
  Promise.all((await listDorisTraces(props)).map(toTraceMetrics));

export const getTraceIdentifiers = async (props: TraceTableProps) =>
  (await listDorisTraces(props)).map((trace) => ({
    id: trace.id,
    projectId: trace.projectId,
    timestamp: trace.timestamp,
  }));

export const getTraceDeleteCursorPageFromTraces = (props: {
  projectId: string;
  filter: FilterState;
  cutoffCreatedAt: Date;
  cursor?: TraceDeleteBatchActionCursor | null;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  limit: number;
}) => getTraceDeleteCursorPageFromEvents(props);
