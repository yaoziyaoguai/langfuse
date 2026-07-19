import type { ObservationType } from "../../domain";
import { env } from "../../env";
import { InvalidRequestError, PayloadTooLargeError } from "../../errors";
import type { OrderByState } from "../../interfaces/orderBy";
import type { TracingSearchType } from "../../interfaces/search";
import type { EventsTableFilterState, FilterState } from "../../types";
import type { FullObservations } from "../queries/createGenerationsQuery";
import type { RenderingProps } from "../utils/rendering";
import {
  getObservationByIdFromEventsTable,
  getObservationsCountFromEventsTable,
  getObservationsFromEventsTableForPublicApi,
  getObservationsCountFromEventsTableForPublicApi,
  getObservationsWithModelDataFromEventsTable,
  type PublicApiObservationsQuery,
} from "./events";
import { toDorisEventsObservation } from "./telemetry/doris/adapters";
import type { DorisObservation } from "./telemetry/doris/observations";
import { getDorisTelemetryRepositories } from "./telemetry/doris/runtime";
import type { ObservationRecordReadType } from "./definitions";

export type GetObservationsForTraceOpts<IncludeIO extends boolean> = {
  traceId: string;
  projectId: string;
  timestamp?: Date;
  includeIO?: IncludeIO;
};

export type ObservationTableQuery = {
  projectId: string;
  filter: FilterState;
  orderBy?: OrderByState;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  limit?: number;
  offset?: number;
  selectIOAndMetadata?: boolean;
  renderingProps?: RenderingProps;
  ioSizeCap?: { inlineChars: number; previewChars: number };
  dedupeBySpanId?: boolean;
};

export type ObservationsTableQueryResult = ObservationRecordReadType & {
  latency?: string;
  time_to_first_token?: string;
  trace_tags?: string[];
  trace_name?: string;
  trace_user_id?: string;
  tool_definitions_count?: string;
  tool_calls_count?: string;
};

function mapLegacyColumns(filter: FilterState): EventsTableFilterState {
  return filter.map((item) => ({
    ...item,
    column:
      item.column === "Start Time"
        ? "startTime"
        : item.column === "End Time"
          ? "endTime"
          : item.column === "model"
            ? "providedModelName"
            : item.column === "tokens"
              ? "totalTokens"
              : item.column === "tags"
                ? "traceTags"
                : item.column === "traceEnvironment"
                  ? "environment"
                  : item.column,
  })) as EventsTableFilterState;
}

export const checkObservationExists = async (
  projectId: string,
  id: string,
  startTime?: Date,
): Promise<boolean> => {
  const observation = await getDorisTelemetryRepositories().observations.get({
    projectId,
    observationId: id,
  });
  return Boolean(
    observation && (!startTime || observation.startTime >= startTime),
  );
};

export const getObservationsForTrace = async <IncludeIO extends boolean>(
  opts: GetObservationsForTraceOpts<IncludeIO>,
) => {
  const observations: DorisObservation[] = [];
  let cursor: string | undefined;
  do {
    const timestampFilters: EventsTableFilterState = opts.timestamp
      ? [
          {
            type: "datetime",
            column: "startTime",
            operator: ">=",
            value: new Date(opts.timestamp.getTime() - 60 * 60 * 1_000),
          },
        ]
      : [];
    const page =
      await getDorisTelemetryRepositories().observations.listForTrace({
        projectId: opts.projectId,
        traceId: opts.traceId,
        filters: timestampFilters,
        cursor,
        limit: 999,
        includeFullContent: Boolean(opts.includeIO),
      });
    observations.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  const result = observations.map(toDorisEventsObservation);
  if (opts.includeIO) {
    const payloadSize = result.reduce((total, observation) => {
      const values = [
        observation.input,
        observation.output,
        ...Object.values(observation.metadata ?? {}),
      ];
      return (
        total +
        values.reduce<number>(
          (sum, value) =>
            sum +
            (typeof value === "string"
              ? value.length
              : (JSON.stringify(value)?.length ?? 0)),
          0,
        )
      );
    }, 0);
    if (payloadSize >= env.LANGFUSE_API_TRACE_OBSERVATIONS_SIZE_LIMIT_BYTES) {
      throw new PayloadTooLargeError(
        `Observations in trace are too large: ${(payloadSize / 1e6).toFixed(2)}MB exceeds limit of ${(env.LANGFUSE_API_TRACE_OBSERVATIONS_SIZE_LIMIT_BYTES / 1e6).toFixed(2)}MB`,
      );
    }
  }
  return result;
};

export const getObservationForTraceIdByName = async (params: {
  traceId: string;
  projectId: string;
  name: string;
  timestamp?: Date;
  fetchWithInputOutput?: boolean;
}) => {
  const page = await getDorisTelemetryRepositories().observations.listForTrace({
    projectId: params.projectId,
    traceId: params.traceId,
    filters: [
      { type: "string", column: "name", operator: "=", value: params.name },
      ...(params.timestamp
        ? [
            {
              type: "datetime" as const,
              column: "startTime",
              operator: ">=" as const,
              value: new Date(params.timestamp.getTime() - 60 * 60 * 1_000),
            },
          ]
        : []),
    ],
    limit: 1,
    includeFullContent: Boolean(params.fetchWithInputOutput),
  });
  return page.items.map(toDorisEventsObservation);
};

export const getObservationByIdFromObservationsTable =
  getObservationByIdFromEventsTable;

export const getObservationsById = async (
  ids: string[],
  projectId: string,
  fetchWithInputOutput = false,
) => {
  const observations = await Promise.all(
    ids.map((observationId) =>
      getDorisTelemetryRepositories().observations.get({
        projectId,
        observationId,
      }),
    ),
  );
  return observations.flatMap((observation) =>
    observation
      ? [
          toDorisEventsObservation({
            ...observation,
            input: fetchWithInputOutput ? observation.input : null,
            output: fetchWithInputOutput ? observation.output : null,
          }),
        ]
      : [],
  );
};

export const getObservationsTableCount = async (opts: ObservationTableQuery) =>
  getObservationsCountFromEventsTable(opts);

export const getObservationsTableWithModelData = async (
  opts: ObservationTableQuery,
): Promise<FullObservations> =>
  (await getObservationsWithModelDataFromEventsTable(opts)) as FullObservations;

async function facet(
  projectId: string,
  filter: FilterState,
  column: string,
  additionalFilters: EventsTableFilterState = [],
) {
  const mapped = mapLegacyColumns(filter);
  const lower = mapped
    .filter(
      (item): item is Extract<typeof item, { type: "datetime" }> =>
        item.type === "datetime" &&
        item.column === "startTime" &&
        (item.operator === ">" || item.operator === ">="),
    )
    .map(({ value }) => value);
  const upper = mapped
    .filter(
      (item): item is Extract<typeof item, { type: "datetime" }> =>
        item.type === "datetime" &&
        item.column === "startTime" &&
        (item.operator === "<" || item.operator === "<="),
    )
    .map(({ value }) => value);
  return getDorisTelemetryRepositories().observations.filterOptionValues({
    projectId,
    range: {
      from:
        lower.length > 0
          ? new Date(Math.max(...lower.map((date) => date.getTime())))
          : new Date(0),
      to:
        upper.length > 0
          ? new Date(Math.min(...upper.map((date) => date.getTime())))
          : new Date(),
    },
    filters: [...mapped, ...additionalFilters],
    column,
    limit: 1_000,
  });
}

export const getObservationsGroupedByModel = async (
  projectId: string,
  filter: FilterState,
) =>
  (
    await facet(projectId, filter, "providedModelName", [
      { type: "string", column: "type", operator: "=", value: "GENERATION" },
    ])
  ).map(({ value }) => ({ model: value }));

export const getObservationsGroupedByModelId = async (
  projectId: string,
  filter: FilterState,
) =>
  (
    await facet(projectId, filter, "modelId", [
      { type: "string", column: "type", operator: "=", value: "GENERATION" },
    ])
  ).map(({ value }) => ({ modelId: value }));

export const getObservationsGroupedByName = async (
  projectId: string,
  filter: FilterState,
  type: ObservationType | null = "GENERATION",
) =>
  (
    await facet(
      projectId,
      filter,
      "name",
      type
        ? [{ type: "string", column: "type", operator: "=", value: type }]
        : [],
    )
  ).map(({ value }) => ({ name: value }));

export const getObservationsGroupedByToolName = async (
  projectId: string,
  filter: FilterState,
) =>
  (await facet(projectId, filter, "toolNames")).map(({ value }) => ({
    toolName: value,
  }));

export const getObservationsGroupedByCalledToolName = async (
  projectId: string,
  filter: FilterState,
) =>
  (await facet(projectId, filter, "calledToolNames")).map(({ value }) => ({
    calledToolName: value,
  }));

export const getObservationsGroupedByPromptName = async (
  projectId: string,
  filter: FilterState,
) =>
  (
    await facet(projectId, filter, "promptName", [
      { type: "string", column: "type", operator: "=", value: "GENERATION" },
    ])
  ).map(({ value }) => ({ promptName: value }));

export const getCostForTraces = async (
  projectId: string,
  _timestamp: Date,
  traceIds: string[],
) => {
  const traces = await Promise.all(
    traceIds.map((traceId) =>
      getDorisTelemetryRepositories().traces.get({ projectId, traceId }),
    ),
  );
  return traces.reduce((total, trace) => total + (trace?.totalCost ?? 0), 0);
};

function promptRange(window: { fromTimestamp?: Date; toTimestamp?: Date }) {
  return {
    from: window.fromTimestamp ?? new Date(0),
    to: window.toTimestamp ?? new Date(Date.now() + 1),
  };
}

export const getObservationsWithPromptName = async (
  projectId: string,
  promptNames: string[],
  window: { fromTimestamp?: Date; toTimestamp?: Date } = {},
) =>
  getDorisTelemetryRepositories().observations.promptNameCounts({
    projectId,
    promptNames,
    range: promptRange(window),
  });

export const getObservationMetricsForPrompts = async (
  projectId: string,
  promptIds: string[],
  window: { fromTimestamp?: Date; toTimestamp?: Date } = {},
) =>
  getDorisTelemetryRepositories().observations.promptMetrics({
    projectId,
    promptIds,
    range: promptRange(window),
  });

export const getLatencyAndTotalCostForObservations = async (
  projectId: string,
  observationIds: string[],
  timestamp?: Date,
) =>
  getDorisTelemetryRepositories().observations.costAndLatencyByIds({
    projectId,
    observationIds,
    from: timestamp,
  });

export const getLatencyAndTotalCostForObservationsByTraces = async (
  projectId: string,
  traceIds: string[],
) => {
  const traces = await Promise.all(
    traceIds.map((traceId) =>
      getDorisTelemetryRepositories().traces.get({ projectId, traceId }),
    ),
  );
  return traces.flatMap((trace, index) =>
    trace
      ? [
          {
            traceId: traceIds[index]!,
            totalCost: trace.totalCost ?? 0,
            latency: trace.latency,
          },
        ]
      : [],
  );
};

export type ObservationTuple = [
  id: string,
  parentObservationId: string | null,
  totalCost: string,
  inputCost: string,
  outputCost: string,
  latencyMs: number,
];

export const getObservationsGroupedByTraceId = async (
  projectId: string,
  traceIds: string[],
  timestamp?: Date,
): Promise<Map<string, ObservationTuple[]>> => {
  const grouped = await Promise.all(
    traceIds.map(async (traceId) => {
      const observations: DorisObservation[] = [];
      let cursor: string | undefined;
      do {
        const page =
          await getDorisTelemetryRepositories().observations.listForTrace({
            projectId,
            traceId,
            filters: timestamp
              ? [
                  {
                    type: "datetime",
                    column: "startTime",
                    operator: ">=",
                    value: timestamp,
                  },
                ]
              : [],
            cursor,
            limit: 999,
          });
        observations.push(...page.items);
        cursor = page.nextCursor ?? undefined;
      } while (cursor);
      return [
        traceId,
        observations.map(
          (observation): ObservationTuple => [
            observation.id,
            observation.parentObservationId,
            String(observation.costDetails.total ?? 0),
            String(observation.costDetails.input ?? 0),
            String(observation.costDetails.output ?? 0),
            (observation.latency ?? 0) * 1_000,
          ],
        ),
      ] as const;
    }),
  );
  return new Map(grouped);
};

export const getTraceIdsForObservations = async (
  projectId: string,
  observationIds: string[],
) => {
  const observations = await Promise.all(
    observationIds.map((observationId) =>
      getDorisTelemetryRepositories().observations.get({
        projectId,
        observationId,
      }),
    ),
  );
  return observations.flatMap((observation) =>
    observation ? [{ id: observation.id, traceId: observation.traceId }] : [],
  );
};

export const hasAnyObservation = async (projectId: string): Promise<boolean> =>
  (await getDorisTelemetryRepositories().observations.count({
    projectId,
    range: { from: new Date(0), to: new Date() },
    filters: [],
  })) > 0;

export const getObservationCountsByProjectInCreationInterval = async (input: {
  start: Date;
  end: Date;
}) => [
  ...(await getDorisTelemetryRepositories().observations.countByProjectCreatedAt(
    input,
  )),
];
export const getObservationCountOfProjectsSinceCreationDate = (input: {
  projectIds: string[];
  start: Date;
}) => getDorisTelemetryRepositories().observations.countProjectsSince(input);
export const getObservationCountsByProjectAndDay = async (input: {
  startDate: Date;
  endDate: Date;
}) => [
  ...(await getDorisTelemetryRepositories().observations.countByProjectAndDay({
    start: input.startDate,
    end: input.endDate,
  })),
];

export const getCostByEvaluatorIds = async (
  _projectId: string,
  _evaluatorIds: string[],
): Promise<Array<{ evaluatorId: string; totalCost: number }>> => {
  throw new InvalidRequestError(
    "Evaluator analytics are unavailable in Doris R1A",
  );
};

export const generateObservationsForPublicApi = async (params: {
  projectId: string;
  filter: EventsTableFilterState;
  pagination: { limit: number; page: number };
}) =>
  getObservationsFromEventsTableForPublicApi({
    projectId: params.projectId,
    page: params.pagination.page,
    limit: params.pagination.limit,
    advancedFilters: params.filter,
  });

export const getObservationsCountForPublicApi = async (params: {
  projectId: string;
  filter: EventsTableFilterState;
}) =>
  getObservationsCountFromEventsTableForPublicApi({
    projectId: params.projectId,
    page: 1,
    limit: 1,
    advancedFilters: params.filter,
  });

export async function* getGenerationsForAnalyticsIntegrations(
  ..._args: unknown[]
): AsyncGenerator<never> {
  yield* [] as never[];
  throw new InvalidRequestError(
    "Analytics integrations are unavailable in Doris R1A",
  );
}

export function getObservationsForBlobStorageExport(
  ..._args: unknown[]
): never {
  throw new InvalidRequestError("Batch exports are unavailable in Doris R1A");
}

export const getObservationsForBlobStorageExportRaw =
  getObservationsForBlobStorageExport;
export const getObservationsForBlobStorageExportParquet =
  getObservationsForBlobStorageExport;

export const deleteObservationsByTraceIds = async (..._args: unknown[]) => {
  throw new InvalidRequestError(
    "Direct observation deletion is replaced by Doris materialized deletion",
  );
};
export const deleteObservationsByProjectId = deleteObservationsByTraceIds;
export const hasAnyObservationOlderThan = async () => false;
export const deleteObservationsOlderThanDays = deleteObservationsByTraceIds;

export const upsertObservation = async () => {
  throw new InvalidRequestError(
    "Legacy observation writes are unavailable in Doris R1A",
  );
};

export type LegacyPublicApiObservationsQuery = PublicApiObservationsQuery;
