import type { ScoreDomain } from "../../../../domain/scores";
import { LISTABLE_SCORE_TYPES } from "../../../../domain/scores";
import { InvalidRequestError } from "../../../../errors";
import type { EventsTableFilterState, FilterState } from "../../../../types";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";
import type { DorisScoresRepository } from "./scores";

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

export type DorisPublicScore = ScoreDomain & {
  readonly trace: TraceProjection | null;
};

type PublicScoreDependencies = {
  readonly list: DorisScoresRepository["list"];
  readonly count: DorisScoresRepository["count"];
};

async function defaultDependencies(): Promise<PublicScoreDependencies> {
  const { getDorisTelemetryRepositories } = await import("./runtime.js");
  const repositories = getDorisTelemetryRepositories();
  return {
    list: repositories.scores.list.bind(repositories.scores),
    count: repositories.scores.count.bind(repositories.scores),
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
  const filters: EventsTableFilterState = [
    ...((input.advancedFilters ?? []) as EventsTableFilterState),
  ];
  stringFilter(filters, "traceId", input.traceId);
  stringFilter(filters, "name", input.name);
  stringFilter(filters, "source", input.source);
  stringFilter(filters, "configId", input.configId);
  stringFilter(filters, "sessionId", input.sessionId);
  stringFilter(filters, "datasetRunId", input.datasetRunId);
  stringFilter(filters, "queueId", input.queueId);
  stringFilter(filters, "dataType", input.dataType);
  stringFilter(filters, "userId", input.userId);
  optionsFilter(filters, "scoreId", input.scoreIds);
  optionsFilter(filters, "observationId", input.observationId);
  const requiredTraceTags =
    typeof input.traceTags === "string"
      ? [input.traceTags]
      : (input.traceTags ?? []);
  const environments = Array.isArray(input.environment)
    ? input.environment
    : input.environment
      ? [input.environment]
      : undefined;
  optionsFilter(filters, "environment", environments);
  if (
    (input.userId !== undefined || requiredTraceTags.length > 0) &&
    environments
  ) {
    optionsFilter(filters, "traceEnvironment", environments);
  }
  if (requiredTraceTags.length > 0) {
    filters.push({
      type: "arrayOptions",
      column: "trace_tags",
      operator: "all of",
      value: [...requiredTraceTags],
    });
  }
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

function projectScores(
  scores: Awaited<ReturnType<PublicScoreDependencies["list"]>>["items"],
  input: PublicScoreQuery,
): DorisPublicScore[] {
  const includeTrace = (input.fields ?? ["score", "trace"]).includes("trace");
  return scores.map(({ trace, ...score }) => ({
    ...score,
    trace: includeTrace
      ? trace
        ? {
            userId: trace.userId,
            tags: trace.tags,
            environment: trace.environment,
            sessionId: trace.sessionId,
          }
        : null
      : null,
  }));
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
  const page = Math.max(1, input.page);
  const limit = Math.max(1, input.limit);
  const includeTrace = (input.fields ?? ["score", "trace"]).includes("trace");
  const [{ items }, count] = await Promise.all([
    resolvedDependencies.list({
      projectId: input.projectId,
      range,
      filters,
      limit,
      offset: (page - 1) * limit,
      includeTraceContext: includeTrace,
    }),
    resolvedDependencies.count({
      projectId: input.projectId,
      range,
      filters,
    }),
  ]);
  return { items: projectScores(items, input), count };
}
