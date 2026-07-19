import { InvalidRequestError, type FilterState } from "@langfuse/shared";
import { executeQuery } from "@langfuse/shared/query/server";
import type { QueryType } from "@langfuse/shared/query";

type DailyMetricsQueryProps = {
  page: number;
  limit: number;
  projectId: string;
  userId?: string;
  tags?: string | string[];
  traceName?: string;
  fromTimestamp?: string;
  toTimestamp?: string;
  traceEnvironment?: string | string[];
  observationEnvironment?: string | string[];
};

type DailyMetric = {
  date: string;
  countTraces: number;
  countObservations: number;
  totalCost: number;
  usage: Array<{
    model: string | null;
    inputUsage: number;
    outputUsage: number;
    totalUsage: number;
    countObservations: number;
    countTraces: number;
    totalCost: number;
  }>;
};

const dorisDailyReads = new WeakMap<
  object,
  Promise<{ data: DailyMetric[]; count: number }>
>();

function addDorisFilters(
  filters: FilterState,
  props: DailyMetricsQueryProps,
): void {
  if (props.traceName) {
    filters.push({
      type: "string",
      column: "traceName",
      operator: "=",
      value: props.traceName,
    });
  }
  if (props.userId) {
    filters.push({
      type: "string",
      column: "userId",
      operator: "=",
      value: props.userId,
    });
  }
  const tags = Array.isArray(props.tags)
    ? props.tags
    : props.tags
      ? props.tags.split(",")
      : [];
  if (tags.length > 0) {
    filters.push({
      type: "arrayOptions",
      column: "tags",
      operator: "all of",
      value: tags,
    });
  }
  const environment = props.traceEnvironment ?? props.observationEnvironment;
  const environments = Array.isArray(environment)
    ? environment
    : environment
      ? environment.split(",")
      : [];
  if (environments.length > 0) {
    filters.push({
      type: "stringOptions",
      column: "environment",
      operator: "any of",
      value: environments,
    });
  }
}

function readDorisDailyMetrics(
  props: DailyMetricsQueryProps,
): Promise<{ data: DailyMetric[]; count: number }> {
  const cached = dorisDailyReads.get(props);
  if (cached) return cached;
  const read = (async () => {
    if (!props.fromTimestamp || !props.toTimestamp) {
      throw new InvalidRequestError("Daily metrics require a timestamp range");
    }
    const filters: FilterState = [];
    addDorisFilters(filters, props);
    const common = {
      filters,
      timeDimension: { granularity: "day" as const },
      fromTimestamp: new Date(props.fromTimestamp).toISOString(),
      toTimestamp: new Date(props.toTimestamp).toISOString(),
      orderBy: [{ field: "time_dimension", direction: "desc" as const }],
    };
    const tracesQuery: QueryType = {
      ...common,
      view: "traces",
      dimensions: [],
      metrics: [{ measure: "count", aggregation: "count" }],
    };
    const observationsQuery: QueryType = {
      ...common,
      view: "observations",
      dimensions: [{ field: "providedModelName" }],
      metrics: [
        { measure: "count", aggregation: "count" },
        { measure: "traceId", aggregation: "uniq" },
        { measure: "inputTokens", aggregation: "sum" },
        { measure: "outputTokens", aggregation: "sum" },
        { measure: "totalTokens", aggregation: "sum" },
        { measure: "totalCost", aggregation: "sum" },
      ],
    };
    const [traceRows, observationRows] = await Promise.all([
      executeQuery(props.projectId, tracesQuery, "v1"),
      executeQuery(props.projectId, observationsQuery, "v2", true),
    ]);
    const byDate = new Map<string, DailyMetric>();
    const metricFor = (time: unknown) => {
      const date = new Date(String(time)).toISOString().slice(0, 10);
      const existing = byDate.get(date);
      if (existing) return existing;
      const metric: DailyMetric = {
        date,
        countTraces: 0,
        countObservations: 0,
        totalCost: 0,
        usage: [],
      };
      byDate.set(date, metric);
      return metric;
    };
    for (const row of traceRows) {
      metricFor(row.time_dimension).countTraces = Number(row.count_count ?? 0);
    }
    for (const row of observationRows) {
      const metric = metricFor(row.time_dimension);
      const countObservations = Number(row.count_count ?? 0);
      const totalCost = Number(row.sum_totalCost ?? 0);
      metric.countObservations += countObservations;
      metric.totalCost += totalCost;
      metric.usage.push({
        model:
          row.providedModelName == null ? null : String(row.providedModelName),
        inputUsage: Number(row.sum_inputTokens ?? 0),
        outputUsage: Number(row.sum_outputTokens ?? 0),
        totalUsage: Number(row.sum_totalTokens ?? 0),
        countObservations,
        countTraces: Number(row.uniq_traceId ?? 0),
        totalCost,
      });
    }
    const all = [...byDate.values()].sort((left, right) =>
      right.date.localeCompare(left.date),
    );
    const offset = (props.page - 1) * props.limit;
    return { data: all.slice(offset, offset + props.limit), count: all.length };
  })();
  dorisDailyReads.set(props, read);
  return read;
}

export const generateDailyMetrics = (props: DailyMetricsQueryProps) => {
  return readDorisDailyMetrics(props).then(({ data }) => data);
};

export const getDailyMetricsCount = (props: DailyMetricsQueryProps) => {
  return readDorisDailyMetrics(props).then(({ count }) => count);
};
