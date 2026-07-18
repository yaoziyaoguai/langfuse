import type { AnalyticsEntityType } from "@prisma/client";

import { prisma } from "../../../../db";
import type { EventsTableFilterState } from "../../../../types";
import type { TraceDomain } from "../../../../domain";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";
import type { TraceRecordExtraFieldsType } from "../../definitions";
import { toDorisTraceDomain } from "./adapters";
import { getDorisTelemetryRepositories } from "./runtime";

export type DorisPublicApiTracesQuery = {
  readonly projectId: string;
  readonly page: number;
  readonly limit: number;
  readonly userId?: string;
  readonly name?: string;
  readonly tags?: string | readonly string[];
  readonly sessionId?: string;
  readonly version?: string;
  readonly release?: string;
  readonly environment?: string | readonly string[];
  readonly fromTimestamp?: string;
  readonly toTimestamp?: string;
  readonly fields?: readonly string[];
  readonly advancedFilters?: EventsTableFilterState;
};

type TraceRepository = Pick<
  ReturnType<typeof getDorisTelemetryRepositories>["traces"],
  "list" | "count"
>;

type DorisTraceReadDependencies = {
  readonly repository: TraceRepository;
  readonly findTraceControls: (input: {
    readonly projectId: string;
    readonly traceIds: readonly string[];
  }) => Promise<
    readonly {
      readonly traceId: string;
      readonly bookmarked: boolean;
      readonly public: boolean;
    }[]
  >;
  readonly findObservationIds: (input: {
    readonly projectId: string;
    readonly traceIds: readonly string[];
  }) => Promise<ReadonlyMap<string, readonly string[]>>;
};

function defaultDependencies(): DorisTraceReadDependencies {
  return {
    repository: getDorisTelemetryRepositories().traces,
    findTraceControls: ({ projectId, traceIds }) =>
      prisma.traceControlState.findMany({
        where: { projectId, traceId: { in: [...traceIds] } },
        select: { traceId: true, bookmarked: true, public: true },
      }),
    findObservationIds: async ({ projectId, traceIds }) => {
      const heads = await prisma.analyticsEntityHead.findMany({
        where: {
          projectId,
          entityType: "EVENT" satisfies AnalyticsEntityType,
          owningTraceId: { in: [...traceIds] },
        },
        select: { owningTraceId: true, lookupId: true },
        orderBy: { entityKey: "asc" },
      });
      const byTraceId = new Map<string, string[]>();
      for (const head of heads) {
        if (!head.owningTraceId || !head.lookupId) continue;
        const ids = byTraceId.get(head.owningTraceId) ?? [];
        ids.push(head.lookupId);
        byTraceId.set(head.owningTraceId, ids);
      }
      return byTraceId;
    },
  };
}

function optionalStringFilter(
  filters: EventsTableFilterState,
  column: string,
  value: string | undefined,
): void {
  if (value) filters.push({ type: "string", column, operator: "=", value });
}

function buildFilters(
  input: DorisPublicApiTracesQuery,
): EventsTableFilterState {
  const filters = [...(input.advancedFilters ?? [])];
  optionalStringFilter(filters, "userId", input.userId);
  optionalStringFilter(filters, "name", input.name);
  optionalStringFilter(filters, "sessionId", input.sessionId);
  optionalStringFilter(filters, "version", input.version);
  optionalStringFilter(filters, "release", input.release);
  const environments = Array.isArray(input.environment)
    ? input.environment
    : input.environment
      ? [input.environment]
      : [];
  if (environments.length > 0) {
    filters.push({
      type: "stringOptions",
      column: "environment",
      operator: "any of",
      value: [...environments],
    });
  }
  const tags = Array.isArray(input.tags)
    ? input.tags
    : input.tags
      ? [input.tags]
      : [];
  if (tags.length > 0) {
    filters.push({
      type: "arrayOptions",
      column: "traceTags",
      operator: "all of",
      value: [...tags],
    });
  }
  return filters;
}

function buildRange(
  input: DorisPublicApiTracesQuery,
): AnalyticsTimeRange | null {
  if (!input.fromTimestamp) return null;
  return {
    from: new Date(input.fromTimestamp),
    to: input.toTimestamp ? new Date(input.toTimestamp) : new Date(),
  };
}

export async function getDorisTracesForPublicApi(
  input: DorisPublicApiTracesQuery,
  dependencies: DorisTraceReadDependencies = defaultDependencies(),
): Promise<Array<TraceDomain & TraceRecordExtraFieldsType>> {
  const offset = Math.max(0, input.page - 1) * input.limit;
  const page = await dependencies.repository.list({
    projectId: input.projectId,
    range: buildRange(input),
    filters: buildFilters(input),
    limit: offset + input.limit,
  });
  const traces = page.items.slice(offset);
  const traceIds = traces.map(({ id }) => id);
  const [controls, observationIds] = await Promise.all([
    dependencies.findTraceControls({ projectId: input.projectId, traceIds }),
    input.fields?.includes("observations")
      ? dependencies.findObservationIds({
          projectId: input.projectId,
          traceIds,
        })
      : Promise.resolve(new Map<string, readonly string[]>()),
  ]);
  const controlsByTraceId = new Map(
    controls.map((control) => [control.traceId, control]),
  );
  const requestedFields = new Set(
    input.fields ?? ["core", "io", "scores", "observations", "metrics"],
  );
  return traces.map((trace) => {
    const domain = toDorisTraceDomain(
      trace,
      controlsByTraceId.get(trace.id) ?? { bookmarked: false, public: false },
    );
    return {
      ...domain,
      input: requestedFields.has("io") ? domain.input : null,
      output: requestedFields.has("io") ? domain.output : null,
      observations: requestedFields.has("observations")
        ? [...(observationIds.get(trace.id) ?? [])]
        : [],
      scores: [],
      totalCost: requestedFields.has("metrics") ? (trace.totalCost ?? 0) : -1,
      latency: requestedFields.has("metrics") ? trace.latency : -1,
      htmlPath: `/project/${input.projectId}/traces/${trace.id}`,
    };
  });
}

export async function getDorisTracesCountForPublicApi(
  input: DorisPublicApiTracesQuery,
  dependencies: DorisTraceReadDependencies = defaultDependencies(),
): Promise<number> {
  return dependencies.repository.count({
    projectId: input.projectId,
    range: buildRange(input),
    filters: buildFilters(input),
  });
}
