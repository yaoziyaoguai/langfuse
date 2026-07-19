import { prisma } from "../../db";
import type {
  EventsObservation,
  MetadataDomain,
  ObservationType,
  TraceDomain,
} from "../../domain";
import {
  OBSERVATION_FIELD_GROUPS_PUBLIC_API,
  type ObservationFieldGroupPublicApi,
} from "../../domain/observation-field-groups";
import { env } from "../../env";
import { InvalidRequestError, LangfuseNotFoundError } from "../../errors";
import type { TraceDeleteBatchActionCursor } from "../../features/batchAction/types";
import type { OrderByState } from "../../interfaces/orderBy";
import type { TracingSearchType } from "../../interfaces/search";
import type { EventsTableFilterState, FilterState } from "../../types";
import type { NumericEventsTableColumnId } from "../../eventsTable";
import { recordDistribution, traceException } from "../instrumentation";
import { logger } from "../logger";
import type {
  EventsObservationPublic,
  FullEventsObservation,
  FullEventsObservations,
  ObservationPriceFields,
} from "../queries/createGenerationsQuery";
import type { DorisEventOrderBy } from "../queries/doris-sql/eventQueryCompiler";
import {
  applyInputOutputRendering,
  DEFAULT_RENDERING_PROPS,
  type RenderingProps,
} from "../utils/rendering";
import type { ObservationTableQuery } from "./observations";
import {
  toDorisEventsObservation,
  toDorisTraceDomain,
} from "./telemetry/doris/adapters";
import {
  buildDorisDerivedQuery,
  toDorisSessionMetricsRow,
  toDorisUserMetricsRow,
} from "./telemetry/doris/derivedUi";
import {
  encodeDorisObservationCursor,
  type DorisEventFilterOptionColumn,
  type DorisObservation,
} from "./telemetry/doris/observations";
import type { DorisPublicApiTracesQuery } from "./telemetry/doris/publicTraces";
import { getDorisTelemetryRepositories } from "./telemetry/doris/runtime";
import type { DorisSession } from "./telemetry/doris/sessions";
import type { DorisTrace } from "./telemetry/doris/traces";
import type { DorisUser } from "./telemetry/doris/users";
import { normalizeDorisEventFilters } from "../queries/logical/filterPlan";

export type EventBatchIOStringOutput = {
  id: string;
  input: string | null;
  output: string | null;
  metadata: MetadataDomain;
};

export type EventBatchIOToolCallFields = {
  toolCalls: string[];
  toolCallNames: string[];
};

export type EventBatchIOWithExperimentOutput = EventBatchIOStringOutput & {
  experimentItemExpectedOutput: string | null;
  experimentItemMetadata: MetadataDomain;
};

export type EventBatchIOResult<
  TIncludeExperiment extends boolean,
  TIncludeToolCalls extends boolean,
> = (TIncludeExperiment extends true
  ? EventBatchIOWithExperimentOutput
  : EventBatchIOStringOutput) &
  (TIncludeToolCalls extends true ? EventBatchIOToolCallFields : object);

export type ObservationIOSizeFields = {
  inputLength: number;
  outputLength: number;
  inputTruncated: boolean;
  outputTruncated: boolean;
  metadataTruncated: boolean;
  metadataLength: number;
};

export type EventFilterOptionColumn =
  | DorisEventFilterOptionColumn
  | "experimentDatasetId"
  | "experimentId"
  | "experimentName";

export type EventFilterOptionRow = {
  column: EventFilterOptionColumn;
  value: string;
  count: number;
};

export type EventFilterOptionScope = {
  scoreName?: string;
  scoreSource?: string;
};

const EVENT_FILTER_OPTION_TOP_N = 1_000;
export const MAX_OBSERVATIONS_PER_TRACE = 10_000;

function exactStringFilterValue(
  filters: readonly {
    readonly column: string;
    readonly type: string;
    readonly operator: string;
    readonly value?: unknown;
  }[],
  column: string,
): string | undefined {
  for (const filter of filters) {
    if (filter.column !== column) continue;
    if (filter.type === "string" && filter.operator === "=") {
      return typeof filter.value === "string" ? filter.value : undefined;
    }
    if (
      filter.type === "stringOptions" &&
      filter.operator === "any of" &&
      Array.isArray(filter.value) &&
      filter.value.length === 1 &&
      typeof filter.value[0] === "string"
    ) {
      return filter.value[0];
    }
  }
  return undefined;
}

function buildObservationReadQuery(filter: FilterState): {
  range: { from: Date; to: Date } | null;
  filters: EventsTableFilterState;
} {
  const normalized = normalizeDorisEventFilters(
    filter as EventsTableFilterState,
  );
  const lowerBounds: Date[] = [];
  const upperBounds: Date[] = [];
  for (const item of normalized) {
    if (item.type !== "datetime" || item.column !== "startTime") continue;
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
  if (
    lowerBounds.length === 0 &&
    exactStringFilterValue(normalized, "traceId")
  ) {
    return { range: null, filters: normalized };
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
    filters: normalized,
  };
}

function toEventOrderBy(
  orderBy: OrderByState | undefined,
): DorisEventOrderBy | undefined {
  if (!orderBy) return undefined;
  const aliases: Readonly<Record<string, DorisEventOrderBy["column"]>> = {
    model: "providedModelName",
    tokens: "totalTokens",
  };
  const column = aliases[orderBy.column] ?? orderBy.column;
  const supported = new Set<DorisEventOrderBy["column"]>([
    "startTime",
    "endTime",
    "completionStartTime",
    "id",
    "traceId",
    "parentObservationId",
    "name",
    "type",
    "environment",
    "userId",
    "sessionId",
    "traceName",
    "version",
    "level",
    "statusMessage",
    "providedModelName",
    "modelId",
    "promptName",
    "promptVersion",
    "totalCost",
    "inputTokens",
    "outputTokens",
    "totalTokens",
    "inputCost",
    "outputCost",
    "latency",
    "timeToFirstToken",
    "tokensPerSecond",
    "toolDefinitions",
    "toolCalls",
    "hasParentObservation",
    "isRootObservation",
    "hasInput",
    "hasOutput",
  ]);
  if (!supported.has(column as DorisEventOrderBy["column"])) {
    throw new InvalidRequestError(
      `Unsupported Doris observation order column: ${orderBy.column}`,
    );
  }
  return {
    column: column as DorisEventOrderBy["column"],
    order: orderBy.order,
  };
}

function rawIo(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  return typeof value === "string" ? value : JSON.stringify(value);
}

function unicodeLength(value: string | null): number {
  return value === null ? 0 : Array.from(value).length;
}

function unicodeHead(value: string, length: number): string {
  return Array.from(value).slice(0, length).join("");
}

async function toFullObservations(
  observations: readonly DorisObservation[],
  projectId: string,
  ioSizeCap?: ObservationTableQuery["ioSizeCap"],
): Promise<Array<FullEventsObservation & Partial<ObservationIOSizeFields>>> {
  if (observations.length === 0) return [];
  const traceIds = [...new Set(observations.map(({ traceId }) => traceId))];
  const modelIds = [
    ...new Set(
      observations.flatMap(({ internalModelId }) =>
        internalModelId ? [internalModelId] : [],
      ),
    ),
  ];
  const [controls, models] = await Promise.all([
    prisma.traceControlState.findMany({
      where: { projectId, traceId: { in: traceIds } },
      select: { traceId: true, bookmarked: true, public: true },
    }),
    modelIds.length > 0
      ? prisma.model.findMany({
          where: {
            id: { in: modelIds },
            OR: [{ projectId }, { projectId: null }],
          },
          include: { Price: true },
        })
      : Promise.resolve([]),
  ]);
  const controlsByTrace = new Map(
    controls.map((control) => [control.traceId, control]),
  );
  const modelsById = new Map(models.map((model) => [model.id, model]));

  return observations.map((observation) => {
    const control = controlsByTrace.get(observation.traceId);
    const domain = toDorisEventsObservation({
      ...observation,
      bookmarked: control?.bookmarked ?? false,
      public: control?.public ?? false,
    });
    const { tags, ...withoutTags } = domain;
    const model = observation.internalModelId
      ? modelsById.get(observation.internalModelId)
      : undefined;
    const fullInput = rawIo(observation.input);
    const fullOutput = rawIo(observation.output);
    let input = fullInput;
    let output = fullOutput;
    let metadata = {
      ...(observation.metadata ?? {}),
    } as EventsObservation["metadata"];
    let sizeFields: ObservationIOSizeFields | undefined;

    if (ioSizeCap) {
      const inputLength = unicodeLength(fullInput);
      const outputLength = unicodeLength(fullOutput);
      const inputTruncated = inputLength > ioSizeCap.inlineChars;
      const outputTruncated = outputLength > ioSizeCap.inlineChars;
      input =
        inputTruncated && fullInput
          ? unicodeHead(fullInput, ioSizeCap.previewChars)
          : fullInput;
      output =
        outputTruncated && fullOutput
          ? unicodeHead(fullOutput, ioSizeCap.previewChars)
          : fullOutput;
      let metadataTruncated = false;
      let metadataLength = 0;
      metadata = Object.fromEntries(
        Object.entries(metadata).map(([key, value]) => {
          const text =
            typeof value === "string" ? value : (JSON.stringify(value) ?? "");
          const length = unicodeLength(text);
          if (length > ioSizeCap.inlineChars) {
            metadataTruncated = true;
            metadataLength += ioSizeCap.previewChars;
            return [key, unicodeHead(text, ioSizeCap.previewChars)];
          }
          metadataLength += length;
          return [key, value];
        }),
      ) as EventsObservation["metadata"];
      sizeFields = {
        inputLength,
        outputLength,
        inputTruncated,
        outputTruncated,
        metadataTruncated,
        metadataLength,
      };
    }

    return {
      ...withoutTags,
      input,
      output,
      metadata,
      modelId: model?.id ?? null,
      inputPrice:
        model?.Price.find(({ usageType }) => usageType === "input")?.price ??
        null,
      outputPrice:
        model?.Price.find(({ usageType }) => usageType === "output")?.price ??
        null,
      totalPrice:
        model?.Price.find(({ usageType }) => usageType === "total")?.price ??
        null,
      traceTags: [...(tags ?? [])],
      traceTimestamp: null,
      toolDefinitions: domain.toolDefinitions ?? null,
      toolCalls: domain.toolCalls ?? null,
      toolDefinitionsCount: observation.toolDefinitionsCount,
      toolCallsCount: observation.toolCallsCount,
      ...sizeFields,
    };
  });
}

export const getObservationsForTraceFromEventsTable = async (params: {
  projectId: string;
  traceId: string;
  timestamp?: Date;
  selectIOAndMetadata?: boolean;
  selectToolData?: boolean;
}): Promise<{ observations: FullEventsObservations; totalCount: number }> => {
  const filters: EventsTableFilterState = params.timestamp
    ? [
        {
          type: "datetime",
          column: "startTime",
          operator: ">=",
          value: new Date(params.timestamp.getTime() - 60 * 60 * 1_000),
        },
      ]
    : [];
  const observations: DorisObservation[] = [];
  let cursor: string | undefined;
  do {
    const page =
      await getDorisTelemetryRepositories().observations.listForTrace({
        projectId: params.projectId,
        traceId: params.traceId,
        filters,
        cursor,
        limit: Math.min(
          999,
          MAX_OBSERVATIONS_PER_TRACE + 1 - observations.length,
        ),
        includeFullContent:
          Boolean(params.selectIOAndMetadata) || Boolean(params.selectToolData),
      });
    observations.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor && observations.length < MAX_OBSERVATIONS_PER_TRACE + 1);
  observations.sort(
    (left, right) =>
      left.startTime.getTime() - right.startTime.getTime() ||
      left.id.localeCompare(right.id),
  );
  return {
    observations: (await toFullObservations(
      observations.slice(0, MAX_OBSERVATIONS_PER_TRACE),
      params.projectId,
    )) as FullEventsObservations,
    totalCount: observations.length,
  };
};

export const getObservationsCountFromEventsTable = async (
  opts: ObservationTableQuery,
): Promise<number> => {
  const query = buildObservationReadQuery(opts.filter);
  const traceId = exactStringFilterValue(opts.filter, "traceId");
  const search = opts.searchQuery
    ? { query: opts.searchQuery, searchType: opts.searchType }
    : undefined;
  return !query.range && traceId
    ? getDorisTelemetryRepositories().observations.countForTrace({
        projectId: opts.projectId,
        traceId,
        filters: query.filters,
        search,
      })
    : getDorisTelemetryRepositories().observations.count({
        projectId: opts.projectId,
        range: query.range,
        filters: query.filters,
        search,
      });
};

export const getObservationsCountsFromEventsTable = async (
  opts: ObservationTableQuery,
): Promise<{ totalCount: number; uniqueTraceCount: number }> => {
  const query = buildObservationReadQuery(opts.filter);
  const traceId = exactStringFilterValue(opts.filter, "traceId");
  const search = opts.searchQuery
    ? { query: opts.searchQuery, searchType: opts.searchType }
    : undefined;
  if (!query.range && traceId) {
    const totalCount =
      await getDorisTelemetryRepositories().observations.countForTrace({
        projectId: opts.projectId,
        traceId,
        filters: query.filters,
        search,
      });
    return { totalCount, uniqueTraceCount: totalCount > 0 ? 1 : 0 };
  }
  return getDorisTelemetryRepositories().observations.counts({
    projectId: opts.projectId,
    range: query.range,
    filters: query.filters,
    search,
  });
};

export async function getObservationsWithModelDataFromEventsTable(
  opts: ObservationTableQuery & {
    ioSizeCap: NonNullable<ObservationTableQuery["ioSizeCap"]>;
  },
): Promise<Array<FullEventsObservation & ObservationIOSizeFields>>;
export async function getObservationsWithModelDataFromEventsTable(
  opts: ObservationTableQuery,
): Promise<FullEventsObservations>;
export async function getObservationsWithModelDataFromEventsTable(
  opts: ObservationTableQuery,
): Promise<FullEventsObservations> {
  const query = buildObservationReadQuery(opts.filter);
  const traceId = exactStringFilterValue(opts.filter, "traceId");
  const common = {
    projectId: opts.projectId,
    filters: query.filters,
    search: opts.searchQuery
      ? { query: opts.searchQuery, searchType: opts.searchType }
      : undefined,
    orderBy: toEventOrderBy(opts.orderBy),
    offset: opts.offset,
    limit: opts.limit ?? 999,
    includeFullContent: Boolean(opts.selectIOAndMetadata),
  };
  const page =
    !query.range && traceId
      ? await getDorisTelemetryRepositories().observations.listForTrace({
          ...common,
          traceId,
        })
      : await getDorisTelemetryRepositories().observations.list({
          ...common,
          range: query.range,
        });
  return (await toFullObservations(
    page.items,
    opts.projectId,
    opts.ioSizeCap,
  )) as FullEventsObservations;
}

export const getTraceDeleteCursorPageFromEvents = async (props: {
  projectId: string;
  filter: FilterState;
  cutoffCreatedAt: Date;
  cursor?: TraceDeleteBatchActionCursor | null;
  searchQuery?: string;
  searchType?: TracingSearchType[];
  limit: number;
}): Promise<TraceDeleteBatchActionCursor[]> => {
  const query = buildObservationReadQuery([
    ...props.filter,
    {
      column: "startTime",
      operator: "<",
      value: props.cutoffCreatedAt,
      type: "datetime",
    },
  ]);
  const page = await getDorisTelemetryRepositories().observations.list({
    projectId: props.projectId,
    range: query.range,
    filters: query.filters,
    search: props.searchQuery
      ? { query: props.searchQuery, searchType: props.searchType }
      : undefined,
    cursor: props.cursor?.id,
    limit: props.limit,
  });
  const seen = new Set<string>();
  const rows = page.items
    .filter((observation) => {
      if (seen.has(observation.traceId)) return false;
      seen.add(observation.traceId);
      return true;
    })
    .map((observation) => ({
      id: observation.id,
      traceId: observation.traceId,
      timestamp: observation.startTime.toISOString(),
    }));
  if (rows.length > 0) {
    const last = page.items.at(-1)!;
    rows[rows.length - 1]!.id =
      page.nextCursor ?? encodeDorisObservationCursor(last);
  }
  return rows;
};

type ObservationByIdReadParams = {
  id: string;
  projectId: string;
  fetchWithInputOutput?: boolean;
  startTime?: Date;
  type?: ObservationType;
  traceId?: string;
  renderingProps?: RenderingProps;
};

export const getObservationByIdFromEventsTable = async (
  params: ObservationByIdReadParams,
) => {
  const observation = await getDorisTelemetryRepositories().observations.get({
    projectId: params.projectId,
    observationId: params.id,
    traceId: params.traceId,
  });
  const matchesType = !params.type || observation?.type === params.type;
  const matchesDate =
    !params.startTime ||
    observation?.startTime.toISOString().slice(0, 10) ===
      params.startTime.toISOString().slice(0, 10);
  if (!observation || !matchesType || !matchesDate) {
    throw new LangfuseNotFoundError(
      `Observation with id ${params.id} not found`,
    );
  }
  const [mapped] = await toFullObservations(
    [
      {
        ...observation,
        input: params.fetchWithInputOutput ? observation.input : null,
        output: params.fetchWithInputOutput ? observation.output : null,
      },
    ],
    params.projectId,
  );
  if (!mapped)
    throw new LangfuseNotFoundError(
      `Observation with id ${params.id} not found`,
    );
  const rendering = params.renderingProps ?? DEFAULT_RENDERING_PROPS;
  recordDistribution(
    "langfuse.query_by_id_age",
    Date.now() - mapped.startTime.getTime(),
    { table: "events" },
  );
  return {
    ...mapped,
    input: params.fetchWithInputOutput
      ? applyInputOutputRendering(rawIo(mapped.input), rendering)
      : null,
    output: params.fetchWithInputOutput
      ? applyInputOutputRendering(rawIo(mapped.output), rendering)
      : null,
  };
};

type TraceByIdReadParams = {
  traceId: string;
  projectId: string;
  timestamp?: Date;
  fromTimestamp?: Date;
  renderingProps?: RenderingProps;
  excludeInputOutput?: boolean;
  excludeMetadata?: boolean;
};

export const getTraceByIdFromEventsTable = async (
  params: TraceByIdReadParams,
) => {
  const trace = await getDorisTelemetryRepositories().traces.get({
    projectId: params.projectId,
    traceId: params.traceId,
  });
  if (!trace) return undefined;
  if (
    params.timestamp &&
    trace.timestamp.toISOString().slice(0, 10) !==
      params.timestamp.toISOString().slice(0, 10)
  ) {
    return undefined;
  }
  if (params.fromTimestamp && trace.timestamp < params.fromTimestamp) {
    return undefined;
  }
  const control = await prisma.traceControlState.findUnique({
    where: {
      projectId_traceId: {
        projectId: params.projectId,
        traceId: params.traceId,
      },
    },
    select: { bookmarked: true, public: true },
  });
  const domain = toDorisTraceDomain(trace, {
    bookmarked: control?.bookmarked ?? false,
    public: control?.public ?? false,
  });
  const rendering = params.renderingProps ?? DEFAULT_RENDERING_PROPS;
  const render = (value: TraceDomain["input"]) =>
    applyInputOutputRendering(rawIo(value), rendering);
  return {
    ...domain,
    input: params.excludeInputOutput ? null : render(domain.input),
    output: params.excludeInputOutput ? null : render(domain.output),
    metadata: params.excludeMetadata ? {} : domain.metadata,
  };
};

export const getTraceById = getTraceByIdFromEventsTable;
export const getObservationById = getObservationByIdFromEventsTable;

export const hasAnyTraceFromEventsTable = async (
  projectId: string,
): Promise<boolean> =>
  (await getDorisTelemetryRepositories().observations.count({
    projectId,
    range: { from: new Date(0), to: new Date() },
    filters: [],
  })) > 0;

export const hasAnyTracingData = async (
  projectId: string,
): Promise<boolean> => {
  try {
    const project = await prisma.project.findUnique({
      where: { id: projectId },
      select: { hasTraces: true },
    });
    if (project?.hasTraces) return true;
  } catch (error) {
    traceException(error);
    logger.error("Failed to read project tracing flag", { projectId, error });
  }
  const hasData = await hasAnyTraceFromEventsTable(projectId);
  if (hasData) {
    await prisma.project
      .updateMany({
        where: { id: projectId, hasTraces: false },
        data: { hasTraces: true },
      })
      .catch((error) => {
        traceException(error);
        logger.error("Failed to persist project tracing flag", {
          projectId,
          error,
        });
      });
  }
  return hasData;
};

export type PublicApiObservationsQuery = {
  projectId: string;
  page: number;
  limit: number;
  traceId?: string;
  userId?: string;
  name?: string;
  type?: string;
  level?: string;
  parentObservationId?: string;
  fromStartTime?: string;
  toStartTime?: string;
  version?: string;
  environment?: string | string[];
  advancedFilters?: EventsTableFilterState;
  cursor?: {
    lastStartTimeTo: Date;
    lastTraceId: string;
    lastId: string;
  };
  fields?: ObservationFieldGroupPublicApi[] | null;
  expandMetadataKeys?: string[];
};

export const getObservationsFromEventsTableForPublicApi = async (
  opts: Omit<PublicApiObservationsQuery, "fields">,
): Promise<Array<EventsObservation & ObservationPriceFields>> => {
  const { getDorisObservationsForPublicApi } =
    await import("./telemetry/doris/publicApi.js");
  return (await getDorisObservationsForPublicApi({
    ...opts,
    fields: OBSERVATION_FIELD_GROUPS_PUBLIC_API,
  })) as Array<EventsObservation & ObservationPriceFields>;
};

export const getObservationsV2FromEventsTableForPublicApi = async (
  opts: PublicApiObservationsQuery & {
    fields: ObservationFieldGroupPublicApi[];
  },
  options?: { allowUnindexedIoFilters?: boolean },
): Promise<Array<EventsObservationPublic>> => {
  const { getDorisObservationsForPublicApi } =
    await import("./telemetry/doris/publicApi.js");
  return getDorisObservationsForPublicApi(
    { ...opts, includeLookahead: true },
    undefined,
    options,
  );
};

export const getObservationsCountFromEventsTableForPublicApi = async (
  opts: PublicApiObservationsQuery,
): Promise<number> => {
  const { getDorisObservationsCountForPublicApi } =
    await import("./telemetry/doris/publicApi.js");
  return getDorisObservationsCountForPublicApi(opts);
};

export type PublicApiTracesQuery = DorisPublicApiTracesQuery;

export const getTracesFromEventsTableForPublicApi = async (
  opts: PublicApiTracesQuery,
) => {
  const { getDorisTracesForPublicApi } =
    await import("./telemetry/doris/publicTraces.js");
  return getDorisTracesForPublicApi(opts);
};

export const getTracesCountFromEventsTableForPublicApi = async (
  opts: PublicApiTracesQuery,
): Promise<number> => {
  const { getDorisTracesCountForPublicApi } =
    await import("./telemetry/doris/publicTraces.js");
  return getDorisTracesCountForPublicApi(opts);
};

type GroupedEventsFilterOptions = {
  limit?: number;
  scope?: EventFilterOptionScope;
};

export const getEventsFilterOptionsForColumns = async (params: {
  projectId: string;
  filter: FilterState;
  columns: readonly EventFilterOptionColumn[];
  topN?: number;
  scope?: EventFilterOptionScope;
}): Promise<EventFilterOptionRow[]> => {
  const query = buildObservationReadQuery(params.filter);
  const inactive = new Set<EventFilterOptionColumn>([
    "experimentDatasetId",
    "experimentId",
    "experimentName",
  ]);
  const rows = await Promise.all(
    params.columns
      .filter((column) => !inactive.has(column))
      .map((column) =>
        getDorisTelemetryRepositories().observations.filterOptionValues({
          projectId: params.projectId,
          range: query.range,
          filters: query.filters,
          column,
          limit: params.topN ?? EVENT_FILTER_OPTION_TOP_N,
          requireScore: params.scope,
        }),
      ),
  );
  return rows.flat() as EventFilterOptionRow[];
};

export const getEventsFilterOptionValuesPage = async (params: {
  projectId: string;
  filter: FilterState;
  column: EventFilterOptionColumn;
  limit: number;
  offset: number;
}): Promise<EventFilterOptionRow[]> => {
  if (
    params.column === "experimentDatasetId" ||
    params.column === "experimentId" ||
    params.column === "experimentName"
  ) {
    throw new InvalidRequestError(
      "Experiment facets are unavailable in Doris R1A",
    );
  }
  const query = buildObservationReadQuery(params.filter);
  return getDorisTelemetryRepositories().observations.filterOptionValues({
    projectId: params.projectId,
    range: query.range,
    filters: query.filters,
    column: params.column,
    limit: params.limit,
    offset: params.offset,
  }) as Promise<EventFilterOptionRow[]>;
};

async function getSingleFacet(
  projectId: string,
  filter: FilterState,
  column: DorisEventFilterOptionColumn,
  opts?: GroupedEventsFilterOptions,
) {
  const query = buildObservationReadQuery(filter);
  return getDorisTelemetryRepositories().observations.filterOptionValues({
    projectId,
    range: query.range,
    filters: query.filters,
    column,
    limit: opts?.limit ?? EVENT_FILTER_OPTION_TOP_N,
    requireScore: opts?.scope,
  });
}

export const getEventsGroupedByTraceName = async (
  projectId: string,
  filter: FilterState,
  opts?: GroupedEventsFilterOptions,
) =>
  (await getSingleFacet(projectId, filter, "traceName", opts)).map((row) => ({
    traceName: row.value,
    count: row.count,
  }));

export const getEventsGroupedByTraceTags = async (
  projectId: string,
  filter: FilterState,
  opts?: GroupedEventsFilterOptions,
) =>
  (await getSingleFacet(projectId, filter, "traceTags", opts)).map((row) => ({
    tag: row.value,
  }));

export const getEventsGroupedByUserId = async (
  projectId: string,
  filter: FilterState,
  opts?: GroupedEventsFilterOptions,
) =>
  (await getSingleFacet(projectId, filter, "userId", opts)).map((row) => ({
    userId: row.value,
    count: row.count,
  }));

export const getEventsNumericStatsByFilterColumn = async (
  projectId: string,
  filter: FilterState,
  columnId: Exclude<
    NumericEventsTableColumnId,
    "inputTokens" | "outputTokens" | "inputCost" | "outputCost"
  >,
) => {
  const query = buildObservationReadQuery(filter);
  return getDorisTelemetryRepositories().observations.numericStats({
    projectId,
    range: query.range,
    filters: query.filters,
    column: columnId,
  });
};

export const getEventsGroupedByExperimentDatasetId = async (
  _projectId: string,
  _filter: FilterState,
): Promise<Array<{ experimentDatasetId: string | null }>> => {
  throw new InvalidRequestError(
    "Experiment facets are unavailable in Doris R1A",
  );
};

export async function getAgentGraphDataFromEventsTable(params: {
  projectId: string;
  traceId: string;
  minStartTime: Date;
  maxStartTime: Date;
}) {
  const observations: DorisObservation[] = [];
  let offset = 0;
  for (;;) {
    const page =
      await getDorisTelemetryRepositories().observations.listForTrace({
        projectId: params.projectId,
        traceId: params.traceId,
        filters: [
          {
            type: "datetime",
            column: "startTime",
            operator: ">=",
            value: params.minStartTime,
          },
          {
            type: "datetime",
            column: "startTime",
            operator: "<=",
            value: params.maxStartTime,
          },
        ],
        includeFullContent: true,
        orderBy: { column: "startTime", order: "ASC" },
        offset,
        limit: 999,
      });
    observations.push(...page.items);
    if (page.items.length < 999) break;
    offset += page.items.length;
  }
  return observations.map((observation) => ({
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
  }));
}

export const getObservationsBatchIOFromEventsTable = async <
  TIncludeExperiment extends boolean = false,
  TIncludeToolCalls extends boolean = false,
>(opts: {
  projectId: string;
  observations: Array<{ id: string; traceId: string }>;
  minStartTime: Date;
  maxStartTime: Date;
  truncated?: boolean;
  includeExperimentFields?: TIncludeExperiment;
  includeToolCallFields?: TIncludeToolCalls;
}): Promise<
  Array<EventBatchIOResult<TIncludeExperiment, TIncludeToolCalls>>
> => {
  if (opts.observations.length === 0) return [];
  if (opts.includeExperimentFields) {
    throw new InvalidRequestError(
      "Experiment observation fields are unavailable in Doris R1A",
    );
  }
  const allowed = new Set(
    opts.observations.map(({ id, traceId }) => `${traceId}\u0000${id}`),
  );
  const observations: DorisObservation[] = [];
  let cursor: string | undefined;
  do {
    const page = await getDorisTelemetryRepositories().observations.list({
      projectId: opts.projectId,
      range: {
        from: new Date(opts.minStartTime.getTime() - 1_000),
        to: new Date(opts.maxStartTime.getTime() + 1_001),
      },
      filters: [
        {
          type: "stringOptions",
          column: "id",
          operator: "any of",
          value: opts.observations.map(({ id }) => id),
        },
        {
          type: "stringOptions",
          column: "traceId",
          operator: "any of",
          value: [...new Set(opts.observations.map(({ traceId }) => traceId))],
        },
      ],
      includeFullContent: true,
      cursor,
      limit: Math.min(999, opts.observations.length),
    });
    observations.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  return observations
    .filter((observation) =>
      allowed.has(`${observation.traceId}\u0000${observation.id}`),
    )
    .map((observation) => {
      const render = (value: unknown) => {
        const raw = rawIo(value);
        return (opts.truncated ?? true) && raw
          ? unicodeHead(raw, env.LANGFUSE_SERVER_SIDE_IO_CHAR_LIMIT)
          : raw;
      };
      return {
        id: observation.id,
        input: render(observation.input),
        output: render(observation.output),
        metadata: { ...(observation.metadata ?? {}) } as MetadataDomain,
        ...(opts.includeToolCallFields
          ? {
              toolCalls: [...(observation.toolCalls ?? [])],
              toolCallNames: [...(observation.toolCallNames ?? [])],
            }
          : {}),
      };
    }) as Array<EventBatchIOResult<TIncludeExperiment, TIncludeToolCalls>>;
};

export const getObservationFullIOForSessionFromEventsTable = async (opts: {
  projectId: string;
  sessionId: string;
  traceId: string;
  observationId: string;
  startTime: Date;
}): Promise<EventBatchIOStringOutput | null> => {
  const observation = await getDorisTelemetryRepositories().observations.get({
    projectId: opts.projectId,
    observationId: opts.observationId,
    traceId: opts.traceId,
  });
  if (
    !observation ||
    observation.sessionId !== opts.sessionId ||
    Math.abs(observation.startTime.getTime() - opts.startTime.getTime()) > 1_000
  ) {
    return null;
  }
  return {
    id: observation.id,
    input: rawIo(observation.input),
    output: rawIo(observation.output),
    metadata: { ...(observation.metadata ?? {}) } as MetadataDomain,
  };
};

export const getObservationsTraceIdsFromEventsTable = async (opts: {
  projectId: string;
  observationIds: string[];
}) => {
  const observations = await Promise.all(
    opts.observationIds.map((observationId) =>
      getDorisTelemetryRepositories().observations.get({
        projectId: opts.projectId,
        observationId,
      }),
    ),
  );
  return observations.flatMap((observation) =>
    observation ? [{ id: observation.id, traceId: observation.traceId }] : [],
  );
};

export const getUsersFromEventsTable = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
  limit = 999,
  offset = 0,
) => {
  const query = buildDorisDerivedQuery(filter, "user");
  let remainingOffset = offset;
  let cursor: string | undefined;
  const users: Array<{ user: string; count: string }> = [];
  while (users.length < limit) {
    const page = await getDorisTelemetryRepositories().users.list({
      projectId,
      range: query.range,
      filters: query.filters,
      identifierQuery: searchQuery,
      cursor,
      limit: Math.min(999, Math.max(limit, 1)),
    });
    const visible = page.items.slice(remainingOffset);
    remainingOffset = Math.max(0, remainingOffset - page.items.length);
    users.push(
      ...visible.slice(0, limit - users.length).map((user) => ({
        user: user.id,
        count: String(user.traceCount),
      })),
    );
    if (!page.nextCursor) break;
    cursor = page.nextCursor;
  }
  return users;
};

export const getUsersCountFromEventsTable = async (
  projectId: string,
  filter: FilterState,
  searchQuery?: string,
): Promise<{ totalCount: string }[]> => {
  const query = buildDorisDerivedQuery(filter, "user");
  const count = await getDorisTelemetryRepositories().users.count({
    projectId,
    range: query.range,
    filters: query.filters,
    identifierQuery: searchQuery,
  });
  return [{ totalCount: String(count) }];
};

export const getUserMetricsFromEventsTable = async (
  projectId: string,
  userIds: string[],
  filter: FilterState,
) => {
  if (userIds.length === 0) return [];
  const query = buildDorisDerivedQuery(
    [
      ...filter,
      {
        type: "stringOptions",
        column: "userId",
        operator: "any of",
        value: userIds,
      },
    ],
    "user",
  );
  const users: DorisUser[] = [];
  let cursor: string | undefined;
  do {
    const page = await getDorisTelemetryRepositories().users.list({
      projectId,
      range: query.range,
      filters: query.filters,
      cursor,
      limit: Math.min(999, userIds.length),
    });
    users.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  return users.map(toDorisUserMetricsRow);
};

export const hasAnyUserFromEventsTable = async (
  projectId: string,
): Promise<boolean> => {
  const query = buildDorisDerivedQuery([], "user");
  return (
    (await getDorisTelemetryRepositories().users.count({
      projectId,
      range: query.range,
      filters: query.filters,
    })) > 0
  );
};

export const hasAnySessionFromEventsTable = async (
  projectId: string,
): Promise<boolean> => {
  const query = buildDorisDerivedQuery([], "session");
  return (
    (await getDorisTelemetryRepositories().sessions.count({
      projectId,
      range: query.range,
      filters: query.filters,
    })) > 0
  );
};

export const getTraceMetadataByIdsFromEvents = async (props: {
  projectId: string;
  traceIds: string[];
}) => {
  const traces = await Promise.all(
    props.traceIds.map((traceId) =>
      getDorisTelemetryRepositories().traces.get({
        projectId: props.projectId,
        traceId,
      }),
    ),
  );
  return traces.flatMap((trace) =>
    trace
      ? [
          {
            id: trace.id,
            name: trace.name,
            user_id: trace.userId,
            tags: [...trace.tags],
          },
        ]
      : [],
  );
};

export const getAvgCostByEvaluatorIds = async (
  _projectId: string,
  _evaluatorIds: string[],
): Promise<
  Array<{ evaluatorId: string; avgCost: number; executionCount: number }>
> => {
  throw new InvalidRequestError(
    "Evaluator analytics are unavailable in Doris R1A",
  );
};

export const getSessionMetricsFromEvents = async (props: {
  projectId: string;
  sessionIds: string[];
  queryFromTimestamp?: Date;
}) => {
  if (props.sessionIds.length === 0) return [];
  const query = buildDorisDerivedQuery(
    [
      ...(props.queryFromTimestamp
        ? [
            {
              type: "datetime" as const,
              column: "createdAt",
              operator: ">=" as const,
              value: props.queryFromTimestamp,
            },
          ]
        : []),
      {
        type: "stringOptions",
        column: "id",
        operator: "any of",
        value: props.sessionIds,
      },
    ],
    "session",
  );
  const sessions: DorisSession[] = [];
  let cursor: string | undefined;
  do {
    const page = await getDorisTelemetryRepositories().sessions.list({
      projectId: props.projectId,
      range: query.range,
      filters: [],
      sessionFilters: query.sessionFilters,
      cursor,
      limit: Math.min(999, props.sessionIds.length),
    });
    sessions.push(...page.items);
    cursor = page.nextCursor ?? undefined;
  } while (cursor);
  return sessions.map(toDorisSessionMetricsRow);
};

export type SdkMetadata = {
  isOtel: boolean;
  name?: string;
  version?: string;
  language?: string;
};

export async function getLatestSdkVersionInfoFromEvents(params: {
  projectId: string;
}): Promise<SdkMetadata> {
  return getDorisTelemetryRepositories().observations.latestSdkMetadata({
    projectId: params.projectId,
    range: {
      from: new Date(Date.now() - 7 * 24 * 60 * 60 * 1_000),
      to: new Date(),
    },
  });
}

export const getTracesIdentifierForSessionFromEvents = async (
  projectId: string,
  sessionId: string,
) => {
  const query = buildDorisDerivedQuery(
    [
      {
        type: "stringOptions",
        column: "id",
        operator: "any of",
        value: [sessionId],
      },
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
  return traces
    .map((trace) => ({
      id: trace.id,
      userId: trace.userId,
      name: trace.name,
      timestamp: trace.timestamp,
      environment: trace.environment,
    }))
    .sort(
      (left, right) => left.timestamp.getTime() - right.timestamp.getTime(),
    );
};

export const getTracesIdentifierForSession =
  getTracesIdentifierForSessionFromEvents;

export async function* getEventsForAnalyticsIntegrations(
  ..._args: unknown[]
): AsyncGenerator<never> {
  yield* [] as never[];
  throw new InvalidRequestError(
    "Analytics integrations are unavailable in Doris R1A",
  );
}

export const deleteEventsOlderThanDays = async (
  _projectId: string,
  _cutoffDate: Date,
): Promise<never> => {
  throw new InvalidRequestError(
    "Per-project analytics retention is unavailable in Doris R1A",
  );
};
