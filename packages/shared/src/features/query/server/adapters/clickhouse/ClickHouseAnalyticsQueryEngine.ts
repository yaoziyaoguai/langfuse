import { RESOURCE_LIMIT_ERROR_MESSAGE } from "../../../../../errors";
import { env } from "../../../../../env";
import { measureAndReturn } from "../../../../../server/clickhouse/measureAndReturn";
import type { PreferredClickhouseService } from "../../../../../server/clickhouse/client";
import {
  ClickHouseResourceError,
  isException,
  isProgressRow,
  isRow,
  queryClickhouse,
  queryClickhouseWithProgress,
  type ClickhouseQueryOpts,
} from "../../../../../server/repositories/clickhouse";
import type {
  AnalyticsQueryEngine,
  AnalyticsQueryRequest,
} from "../../AnalyticsQueryEngine";
import { AnalyticsQueryError } from "../../AnalyticsQueryEngine";
import { QueryBuilder } from "../../queryBuilder";

type PreparedClickHouseQuery = {
  readonly compiledQuery: string;
  readonly parameters: Record<string, unknown>;
  readonly preferredClickhouseService: PreferredClickhouseService | undefined;
  readonly tags: { readonly projectId: string };
  readonly clickhouseSettings: Record<string, string>;
  readonly usesTraceTable: boolean;
  readonly fromTimestamp: string;
};

async function prepareClickHouseQuery(
  request: AnalyticsQueryRequest,
): Promise<PreparedClickHouseQuery> {
  const chartConfig =
    (
      request.query as unknown as {
        readonly config?: AnalyticsQueryRequest["query"]["chartConfig"];
      }
    ).config ?? request.query.chartConfig;
  const queryBuilder = new QueryBuilder(chartConfig, request.version);
  const { query: compiledQuery, parameters } = await queryBuilder.build(
    request.query,
    request.projectId,
    request.enableSingleLevelOptimization === true ||
      env.LANGFUSE_ENABLE_SINGLE_LEVEL_QUERY_OPTIMIZATION === "true",
  );
  const usesEventsTable =
    compiledQuery.includes("events_core") ||
    compiledQuery.includes("events_full");

  return {
    compiledQuery,
    parameters,
    preferredClickhouseService: usesEventsTable ? "EventsReadOnly" : undefined,
    tags: { projectId: request.projectId },
    clickhouseSettings: {
      date_time_output_format: "iso",
      ...(env.CLICKHOUSE_USE_QUERY_CONDITION_CACHE === "true"
        ? { use_query_condition_cache: "true" }
        : {}),
      max_bytes_before_external_group_by: String(
        env.CLICKHOUSE_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY,
      ),
    },
    usesTraceTable: compiledQuery.includes("traces"),
    fromTimestamp: request.query.fromTimestamp,
  };
}

function toClickHouseQueryOptions(
  prepared: PreparedClickHouseQuery,
  signal?: AbortSignal,
): Omit<ClickhouseQueryOpts, "allowLegacyEventsRead"> {
  return {
    query: prepared.compiledQuery,
    params: prepared.parameters,
    clickhouseSettings: prepared.clickhouseSettings,
    tags: prepared.tags,
    preferredClickhouseService: prepared.preferredClickhouseService,
    abortSignal: signal,
  };
}

function normalizeStreamingError(error: unknown): Error {
  const wrapped = ClickHouseResourceError.wrapIfResourceError(
    error instanceof Error ? error : new Error(String(error)),
  );
  return wrapped instanceof ClickHouseResourceError
    ? new AnalyticsQueryError(
        "RESOURCE_EXHAUSTED",
        RESOURCE_LIMIT_ERROR_MESSAGE,
        { cause: wrapped },
      )
    : wrapped;
}

export function createClickHouseAnalyticsQueryEngine(): AnalyticsQueryEngine {
  return {
    async execute(request) {
      const prepared = await prepareClickHouseQuery(request);
      const queryOptions = toClickHouseQueryOptions(prepared, request.signal);

      if (!prepared.usesTraceTable) {
        return queryClickhouse<Record<string, unknown>>(queryOptions);
      }

      return measureAndReturn({
        operationName: "executeQuery",
        projectId: request.projectId,
        input: {
          query: prepared.compiledQuery,
          params: prepared.parameters,
          fromTimestamp: prepared.fromTimestamp,
          tags: prepared.tags,
        },
        fn: async (input) =>
          queryClickhouse<Record<string, unknown>>({
            ...queryOptions,
            query: input.query,
            params: input.params,
            tags: input.tags,
          }),
      });
    },

    async *stream(request) {
      const prepared = await prepareClickHouseQuery(request);
      try {
        for await (const event of queryClickhouseWithProgress<
          Record<string, unknown>
        >(toClickHouseQueryOptions(prepared, request.signal))) {
          if (request.signal?.aborted) return;
          if (isProgressRow(event)) {
            yield { type: "progress", progress: event.progress };
          } else if (isRow<Record<string, unknown>>(event)) {
            yield { type: "row", row: event.row };
          } else if (isException(event)) {
            throw normalizeStreamingError(new Error(event.exception));
          }
        }
      } catch (error) {
        throw normalizeStreamingError(error);
      }
    },
  };
}
