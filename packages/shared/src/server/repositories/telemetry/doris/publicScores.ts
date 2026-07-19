import type { ScoreDomain } from "../../../../domain/scores";
import { LISTABLE_SCORE_TYPES } from "../../../../domain/scores";
import { InvalidRequestError } from "../../../../errors";
import type { EventsTableFilterState, FilterState } from "../../../../types";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";
import type { DorisScoresRepository } from "./scores";
import type { DorisTracesRepository } from "./traces";

type PublicScoreQuery = {
  readonly projectId: string;
  readonly page: number;
  readonly limit: number;
  readonly traceId?: string;
  readonly userId?: string;
  readonly name?: string;
  readonly source?: string;
  readonly fromTimestamp?: string;
  readonly toTimestamp?: string;
  readonly value?: number;
  readonly operator?: string;
  readonly scoreIds?: readonly string[];
  readonly configId?: string;
  readonly sessionId?: string;
  readonly datasetRunId?: string;
  readonly queueId?: string;
  readonly traceTags?: string | readonly string[];
  readonly observationId?: readonly string[];
  readonly dataType?: string;
  readonly environment?: string | readonly string[];
  readonly fields?: readonly string[] | null;
  readonly advancedFilters?: FilterState;
};

type TraceProjection = {
  readonly name?: string | null;
  readonly userId?: string | null;
  readonly tags?: readonly string[] | null;
  readonly environment?: string | null;
  readonly sessionId?: string | null;
};

const TRACE_FILTER_COLUMNS = new Set(["traceName", "userId", "trace_tags"]);
const TRACE_FILTER_SCAN_LIMIT = 10_000;
const TRACE_LOOKUP_CONCURRENCY = 25;

export type DorisPublicScore = ScoreDomain & {
  readonly trace: TraceProjection | null;
};

type PublicScoreDependencies = {
  readonly list: DorisScoresRepository["list"];
  readonly count: DorisScoresRepository["count"];
  readonly getTrace: (input: {
    readonly projectId: string;
    readonly traceId: string;
  }) => ReturnType<DorisTracesRepository["get"]>;
};

async function defaultDependencies(): Promise<PublicScoreDependencies> {
  const { getDorisTelemetryRepositories } = await import("./runtime.js");
  const repositories = getDorisTelemetryRepositories();
  return {
    list: repositories.scores.list.bind(repositories.scores),
    count: repositories.scores.count.bind(repositories.scores),
    getTrace: repositories.traces.get.bind(repositories.traces),
  };
}

function dateOr(value: string | undefined, fallback: Date): Date {
  if (!value) return fallback;
  const parsed = new Date(value);
  if (!Number.isFinite(parsed.getTime())) {
    throw new InvalidRequestError("Invalid Doris score timestamp range");
  }
  return parsed;
}

function scoreRange(input: PublicScoreQuery): AnalyticsTimeRange {
  const range = {
    from: dateOr(input.fromTimestamp, new Date(0)),
    to: dateOr(input.toTimestamp, new Date(Date.now() + 1)),
  };
  if (range.from >= range.to) {
    throw new InvalidRequestError("Invalid Doris score timestamp range");
  }
  return range;
}

function stringFilter(
  filters: EventsTableFilterState,
  column: string,
  value: string | undefined,
): void {
  if (value !== undefined) {
    filters.push({ type: "string", column, operator: "=", value });
  }
}

function optionsFilter(
  filters: EventsTableFilterState,
  column: string,
  value: readonly string[] | undefined,
): void {
  if (value?.length) {
    filters.push({
      type: "stringOptions",
      column,
      operator: "any of",
      value: [...value],
    });
  }
}

function scoreFilters(
  input: PublicScoreQuery,
  apiVersion: "v1" | "v2",
): EventsTableFilterState {
  if (input.datasetRunId) {
    throw new InvalidRequestError(
      "Dataset-run score reads are not available on the Doris R1A backend",
    );
  }
  const filters: EventsTableFilterState = [
    ...((input.advancedFilters ?? []).filter(
      (filter) => !TRACE_FILTER_COLUMNS.has(filter.column),
    ) as EventsTableFilterState),
  ];
  stringFilter(filters, "traceId", input.traceId);
  stringFilter(filters, "name", input.name);
  stringFilter(filters, "source", input.source);
  stringFilter(filters, "configId", input.configId);
  stringFilter(filters, "sessionId", input.sessionId);
  stringFilter(filters, "queueId", input.queueId);
  stringFilter(filters, "dataType", input.dataType);
  optionsFilter(filters, "scoreId", input.scoreIds);
  optionsFilter(filters, "observationId", input.observationId);
  optionsFilter(
    filters,
    "environment",
    Array.isArray(input.environment)
      ? input.environment
      : input.environment
        ? [input.environment]
        : undefined,
  );
  if (input.value !== undefined) {
    const operator = input.operator ?? "=";
    if (!["<", ">", "<=", ">=", "!=", "="].includes(operator)) {
      throw new InvalidRequestError("Invalid Doris score value operator");
    }
    filters.push({
      type: "number",
      column: "value",
      operator,
      value: input.value,
    } as EventsTableFilterState[number]);
  }
  if (apiVersion === "v1") {
    optionsFilter(filters, "dataType", LISTABLE_SCORE_TYPES);
    filters.push({
      type: "null",
      column: "traceId",
      operator: "is not null",
      value: "",
    });
    filters.push({
      type: "null",
      column: "sessionId",
      operator: "is null",
      value: "",
    });
  }
  return filters;
}

function traceTags(input: PublicScoreQuery): readonly string[] {
  if (typeof input.traceTags === "string") return [input.traceTags];
  return input.traceTags ?? [];
}

function traceMatches(
  trace: TraceProjection | null,
  input: PublicScoreQuery,
): boolean {
  if (input.userId && trace?.userId !== input.userId) return false;
  if ((input.userId || traceTags(input).length > 0) && input.environment) {
    const environments = Array.isArray(input.environment)
      ? input.environment
      : [input.environment];
    if (!environments.includes(trace?.environment ?? "")) return false;
  }
  const requiredTags = traceTags(input);
  if (!requiredTags.every((tag) => trace?.tags?.includes(tag))) return false;
  return (input.advancedFilters ?? [])
    .filter((filter) => TRACE_FILTER_COLUMNS.has(filter.column))
    .every((filter) => matchesAdvancedTraceFilter(trace, filter));
}

function matchesAdvancedTraceFilter(
  trace: TraceProjection | null,
  filter: FilterState[number],
): boolean {
  const scalar =
    filter.column === "traceName"
      ? (trace?.name ?? null)
      : filter.column === "userId"
        ? (trace?.userId ?? null)
        : null;
  switch (filter.type) {
    case "string": {
      const value = scalar ?? "";
      switch (filter.operator) {
        case "=":
          return value === filter.value;
        case "contains":
          return value.includes(filter.value);
        case "does not contain":
          return !value.includes(filter.value);
        case "starts with":
          return value.startsWith(filter.value);
        case "ends with":
          return value.endsWith(filter.value);
      }
      break;
    }
    case "stringOptions":
      return filter.operator === "any of"
        ? filter.value.includes(scalar ?? "")
        : !filter.value.includes(scalar ?? "");
    case "arrayOptions": {
      const tags = trace?.tags ?? [];
      if (filter.operator === "all of") {
        return filter.value.every((value) => tags.includes(value));
      }
      const any = filter.value.some((value) => tags.includes(value));
      return filter.operator === "any of" ? any : !any;
    }
    case "null":
      if (filter.column === "trace_tags") {
        const empty = !trace?.tags?.length;
        return filter.operator === "is null" ? empty : !empty;
      }
      return filter.operator === "is null" ? scalar === null : scalar !== null;
  }
  throw new InvalidRequestError(
    `Unsupported Doris public score trace filter: ${filter.column}`,
  );
}

async function enrich(
  scores: readonly ScoreDomain[],
  input: PublicScoreQuery,
  dependencies: PublicScoreDependencies,
): Promise<DorisPublicScore[]> {
  const includeTrace = (input.fields ?? ["score", "trace"]).includes("trace");
  const needsTrace =
    includeTrace ||
    Boolean(input.userId) ||
    traceTags(input).length > 0 ||
    (input.advancedFilters ?? []).some((filter) =>
      TRACE_FILTER_COLUMNS.has(filter.column),
    );
  const ids = [
    ...new Set(
      scores.flatMap(({ traceId }) => (traceId && needsTrace ? [traceId] : [])),
    ),
  ];
  const traces = new Map<
    string,
    Awaited<ReturnType<PublicScoreDependencies["getTrace"]>>
  >();
  for (
    let offset = 0;
    offset < ids.length;
    offset += TRACE_LOOKUP_CONCURRENCY
  ) {
    const batch = ids.slice(offset, offset + TRACE_LOOKUP_CONCURRENCY);
    const resolved = await Promise.all(
      batch.map(
        async (traceId) =>
          [
            traceId,
            await dependencies.getTrace({
              projectId: input.projectId,
              traceId,
            }),
          ] as const,
      ),
    );
    resolved.forEach(([traceId, trace]) => traces.set(traceId, trace));
  }
  return scores.flatMap((score) => {
    const trace = score.traceId ? (traces.get(score.traceId) ?? null) : null;
    if (!traceMatches(trace, input)) return [];
    const traceProjection = trace
      ? {
          userId: trace.userId ?? null,
          tags: trace.tags ?? null,
          environment: trace.environment ?? null,
          sessionId: trace.sessionId ?? null,
        }
      : null;
    return [{ ...score, trace: includeTrace ? traceProjection : null }];
  });
}

export async function readDorisScoresForPublicApi(
  input: PublicScoreQuery,
  apiVersion: "v1" | "v2",
  dependencies?: PublicScoreDependencies,
): Promise<{
  readonly items: readonly DorisPublicScore[];
  readonly count: number;
}> {
  const resolvedDependencies = dependencies ?? (await defaultDependencies());
  const range = scoreRange(input);
  const filters = scoreFilters(input, apiVersion);
  const requiresTraceFiltering =
    Boolean(input.userId) ||
    traceTags(input).length > 0 ||
    (input.advancedFilters ?? []).some((filter) =>
      TRACE_FILTER_COLUMNS.has(filter.column),
    );
  const page = Math.max(1, input.page);
  const limit = Math.max(1, input.limit);

  if (!requiresTraceFiltering) {
    const [{ items }, count] = await Promise.all([
      resolvedDependencies.list({
        projectId: input.projectId,
        range,
        filters,
        limit,
        offset: (page - 1) * limit,
      }),
      resolvedDependencies.count({
        projectId: input.projectId,
        range,
        filters,
      }),
    ]);
    return {
      items: await enrich(items, input, resolvedDependencies),
      count,
    };
  }

  const all = await scanAll(resolvedDependencies, input, range, filters);
  const offset = (page - 1) * limit;
  return { items: all.slice(offset, offset + limit), count: all.length };
}

async function scanAll(
  dependencies: PublicScoreDependencies,
  input: PublicScoreQuery,
  range: AnalyticsTimeRange,
  filters: EventsTableFilterState,
): Promise<readonly DorisPublicScore[]> {
  const results: DorisPublicScore[] = [];
  let scanned = 0;
  let cursor: string | undefined;
  do {
    const remaining = TRACE_FILTER_SCAN_LIMIT - scanned;
    if (remaining <= 0) {
      throw new InvalidRequestError(
        `Doris trace-backed score filters are limited to ${TRACE_FILTER_SCAN_LIMIT} candidate scores`,
      );
    }
    const page = await dependencies.list({
      projectId: input.projectId,
      range,
      filters,
      limit: Math.min(999, remaining),
      cursor,
    });
    scanned += page.items.length;
    results.push(...(await enrich(page.items, input, dependencies)));
    if (page.nextCursor && scanned >= TRACE_FILTER_SCAN_LIMIT) {
      throw new InvalidRequestError(
        `Doris trace-backed score filters are limited to ${TRACE_FILTER_SCAN_LIMIT} candidate scores`,
      );
    }
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  return results;
}
