import { randomUUID } from "node:crypto";
import { z } from "zod";

import {
  LISTABLE_SCORE_TYPES,
  ScoreDataTypeEnum,
  type ListableScoreDataType,
  type AggregatableScore,
  type ScoreDataTypeType,
  type ScoreDomain,
  type ScoreSourceType,
} from "../../domain/scores";
import { InvalidRequestError, InternalServerError } from "../../errors";
import type { APIScoreV3 } from "../../features/scores/interfaces/api/v3/schemas";
import type { ScoreFieldGroupV3 } from "../../features/scores/interfaces/api/v3/endpoints";
import { filterAndValidateV3GetScoreList } from "../../features/scores/interfaces/api/v3/validation";
import type { OrderByState } from "../../interfaces/orderBy";
import type { FilterCondition, FilterState } from "../../types";
import { env } from "../../env";
import { prisma } from "../../db";
import {
  acceptAnalyticsIngestion,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
} from "../analytics-persistence";
import { eventTypes } from "../ingestion/types";
import { QueueJobs } from "../queues";
import { ScoreDeleteQueue } from "../redis/scoreDelete";
import { getS3EventStorageClient } from "../s3";
import { getDorisTelemetryRepositories } from "./telemetry/doris";

const SCORE_RANGE_START = new Date(0);
const SCORE_RANGE_END = () => new Date(Date.now() + 1);
const SCORE_FACET_NAME_LIMIT = 200;
const SCORE_FACET_VALUE_LIMIT = 20;
const TRACE_FILTER_SCAN_LIMIT = 10_000;
const TRACE_LOOKUP_CONCURRENCY = 25;

type ScoreWithMetadataFlag = ScoreDomain & { hasMetadata?: boolean };

const SCORE_COLUMN_ALIASES: Readonly<Record<string, string>> = {
  ID: "scoreId",
  Timestamp: "timestamp",
  Environment: "environment",
  "Trace ID": "traceId",
  "Observation ID": "observationId",
  "Session ID": "sessionId",
  Name: "name",
  Value: "value",
  "Boolean Value": "booleanValue",
  Source: "source",
  Comment: "comment",
  "Author User ID": "authorUserId",
  "Data Type": "dataType",
  "String Value": "stringValue",
  Metadata: "metadata",
  "Trace Name": "traceName",
  "User ID": "userId",
  "Trace Tags": "traceTags",
  id: "scoreId",
  trace_tags: "traceTags",
  tags: "traceTags",
};

const TRACE_FILTER_COLUMNS = new Set(["traceName", "userId", "traceTags"]);
const R1B_FILTER_COLUMNS = new Set([
  "datasetRunIds",
  "datasetRunItemRunIds",
  "datasetId",
  "datasetItemIds",
  "experimentIds",
]);

function normalizeScoreFilters(filters: FilterState): FilterState {
  const normalized = filters.map((filter) => ({
    ...filter,
    column: SCORE_COLUMN_ALIASES[filter.column] ?? filter.column,
  })) as FilterState;
  const deferred = normalized.find((filter) =>
    R1B_FILTER_COLUMNS.has(filter.column),
  );
  if (deferred) {
    throw new InvalidRequestError(
      `Score filter ${deferred.column} is unavailable in Doris R1A`,
    );
  }
  return normalized;
}

function normalizedOrder(orderBy: OrderByState): OrderByState {
  if (!orderBy) return null;
  return {
    ...orderBy,
    column: SCORE_COLUMN_ALIASES[orderBy.column] ?? orderBy.column,
  };
}

function rangeFromFilters(filters: FilterState, from?: Date, to?: Date) {
  const lower = filters.flatMap((filter) =>
    filter.type === "datetime" &&
    filter.column === "timestamp" &&
    (filter.operator === ">" || filter.operator === ">=")
      ? [filter.value]
      : [],
  );
  const upper = filters.flatMap((filter) =>
    filter.type === "datetime" &&
    filter.column === "timestamp" &&
    (filter.operator === "<" || filter.operator === "<=")
      ? [filter.value]
      : [],
  );
  return {
    from:
      from ??
      (lower.length > 0
        ? new Date(Math.max(...lower.map((value) => value.getTime())))
        : SCORE_RANGE_START),
    to:
      to ??
      (upper.length > 0
        ? new Date(Math.min(...upper.map((value) => value.getTime() + 1)))
        : SCORE_RANGE_END()),
  };
}

async function readScores(input: {
  projectId: string;
  filters: FilterState;
  from?: Date;
  to?: Date;
  limit?: number;
  offset?: number;
  excludeMetadata?: boolean;
  includeHasMetadata?: boolean;
  orderBy?: OrderByState;
}): Promise<ScoreWithMetadataFlag[]> {
  const filters = normalizeScoreFilters(input.filters);
  const range = rangeFromFilters(filters, input.from, input.to);
  if (range.from >= range.to) {
    throw new InvalidRequestError("Invalid Doris score timestamp range");
  }
  const applyProjection = (scores: readonly ScoreDomain[]) =>
    scores.map((score) => ({
      ...score,
      ...(input.excludeMetadata ? { metadata: {} } : {}),
      ...(input.includeHasMetadata
        ? { hasMetadata: Object.keys(score.metadata).length > 0 }
        : {}),
    }));
  const requestedLimit = input.limit;
  if (requestedLimit !== undefined && requestedLimit <= 999) {
    if (requestedLimit < 1) return [];
    const page = await getDorisTelemetryRepositories().scores.list({
      projectId: input.projectId,
      range,
      filters,
      limit: requestedLimit,
      offset: input.offset,
      orderBy: normalizedOrder(input.orderBy ?? null) ?? undefined,
    });
    return applyProjection(page.items);
  }
  const all: ScoreDomain[] = [];
  const target =
    requestedLimit === undefined
      ? Number.POSITIVE_INFINITY
      : (input.offset ?? 0) + requestedLimit;
  const orderBy = normalizedOrder(input.orderBy ?? null);
  if (orderBy) {
    for (let offset = 0; all.length < target; offset += 999) {
      const page = await getDorisTelemetryRepositories().scores.list({
        projectId: input.projectId,
        range,
        filters,
        limit: 999,
        offset,
        orderBy,
      });
      all.push(...page.items);
      if (page.items.length < 999) break;
    }
  } else {
    let cursor: string | undefined;
    do {
      const page = await getDorisTelemetryRepositories().scores.list({
        projectId: input.projectId,
        range,
        filters,
        limit: 999,
        cursor,
      });
      all.push(...page.items);
      cursor = page.nextCursor ?? undefined;
    } while (cursor && all.length < target);
  }
  const offset = input.offset ?? 0;
  return applyProjection(
    requestedLimit === undefined
      ? all.slice(offset)
      : all.slice(offset, offset + requestedLimit),
  );
}

export const getScoreById = async ({
  projectId,
  scoreId,
  source,
}: {
  projectId: string;
  scoreId: string;
  source?: ScoreSourceType;
}): Promise<ScoreDomain | undefined> => {
  const score = await getDorisTelemetryRepositories().scores.get({
    projectId,
    scoreId,
  });
  return score && (!source || score.source === source) ? score : undefined;
};

export const getScoresByIds = async (
  projectId: string,
  scoreIds: string[],
  source?: ScoreSourceType,
): Promise<ScoreDomain[]> => {
  const scores = await Promise.all(
    scoreIds.map((scoreId) =>
      getDorisTelemetryRepositories().scores.get({ projectId, scoreId }),
    ),
  );
  return scores.filter(
    (score): score is ScoreDomain =>
      score !== null && (!source || score.source === source),
  );
};

function dateValue(value: string | Date | undefined): Date {
  if (value instanceof Date) return value;
  if (!value) return new Date();
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (!Number.isFinite(parsed.getTime())) {
    throw new InvalidRequestError("Invalid score timestamp");
  }
  return parsed;
}

export type AnalyticsScoreUpsert = {
  id?: string;
  project_id?: string;
  name?: string;
  timestamp?: string | Date;
  environment?: string | null;
  trace_id?: string | null;
  observation_id?: string | null;
  session_id?: string | null;
  value?: number | null;
  source?: ScoreSourceType | null;
  comment?: string | null;
  author_user_id?: string | null;
  config_id?: string | null;
  data_type?: ScoreDataTypeType | null;
  string_value?: string | null;
  long_string_value?: string | null;
  queue_id?: string | null;
  metadata?: Record<string, unknown> | null;
  created_at?: string | Date;
  updated_at?: string | Date;
};

export const upsertScore = async (score: AnalyticsScoreUpsert) => {
  if (!score.id || !score.project_id || !score.name || !score.timestamp) {
    throw new InvalidRequestError(
      "Score id, project_id, name, and timestamp are required",
    );
  }
  const dataType = score.data_type as ScoreDataTypeType | undefined;
  if (!dataType) throw new InvalidRequestError("Score data_type is required");
  const value =
    dataType === "CORRECTION"
      ? (score.long_string_value ?? "")
      : dataType === "CATEGORICAL" || dataType === "TEXT"
        ? (score.string_value ?? "")
        : Number(score.value ?? 0);
  const event = {
    id: randomUUID(),
    type: eventTypes.SCORE_CREATE,
    timestamp: dateValue(score.timestamp).toISOString(),
    body: {
      id: score.id,
      name: score.name,
      traceId: score.trace_id ?? null,
      observationId: score.observation_id ?? null,
      sessionId: score.session_id ?? null,
      environment: score.environment ?? "default",
      value,
      dataType,
      source: score.source ?? "ANNOTATION",
      comment: score.comment ?? null,
      metadata: score.metadata ?? {},
      configId: score.config_id ?? null,
      queueId: score.queue_id ?? null,
    },
  };
  const authorUserId = score.author_user_id ?? null;
  await acceptAnalyticsIngestion({
    projectId: score.project_id,
    envelope: {
      formatVersion: 1,
      source: authorUserId ? "annotation-score" : "score",
      payload: authorUserId ? [{ event, authorUserId }] : [event],
      attribution: {
        ingestionApiKey: "internal-score-upsert",
        ingestionSdkName: "langfuse-server",
        ingestionSdkVersion: "internal",
      },
    },
    canonicalizerVersion: CURRENT_ANALYTICS_CANONICALIZER_VERSION,
    schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
    storageService: getS3EventStorageClient(
      env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
    ),
    rawPrefix: env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX,
  });
};

export const searchExistingAnnotationScore = async (
  projectId: string,
  observationId: string | null,
  traceId: string | null,
  sessionId: string | null,
  name: string | undefined,
  configId: string | undefined,
  dataType: ScoreDataTypeType,
) => {
  if (!name && !configId) {
    throw new InvalidRequestError("Either name or configId is required");
  }
  const filters: FilterState = [
    { type: "string", column: "source", operator: "=", value: "ANNOTATION" },
    { type: "string", column: "dataType", operator: "=", value: dataType },
    {
      type: "null",
      column: "traceId",
      operator: traceId ? "is not null" : "is null",
      value: "",
    },
    {
      type: "null",
      column: "observationId",
      operator: observationId ? "is not null" : "is null",
      value: "",
    },
    {
      type: "null",
      column: "sessionId",
      operator: sessionId ? "is not null" : "is null",
      value: "",
    },
  ];
  if (traceId)
    filters.push({
      type: "string",
      column: "traceId",
      operator: "=",
      value: traceId,
    });
  if (observationId)
    filters.push({
      type: "string",
      column: "observationId",
      operator: "=",
      value: observationId,
    });
  if (sessionId)
    filters.push({
      type: "string",
      column: "sessionId",
      operator: "=",
      value: sessionId,
    });
  const candidates = await readScores({ projectId, filters, limit: 100 });
  return candidates.find(
    (score) => score.name === name || score.configId === configId,
  );
};

export type GetScoresForTracesProps<
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
> = {
  projectId: string;
  traceIds: string[];
  level?: "trace" | "observation" | "all";
  timestamp?: Date;
  limit?: number;
  offset?: number;
  excludeMetadata?: ExcludeMetadata;
  includeHasMetadata?: IncludeHasMetadata;
};

async function scoresForTraces(
  props: GetScoresForTracesProps<boolean, boolean>,
  includeCorrections: boolean,
) {
  if (props.traceIds.length === 0) return [];
  const filters: FilterState = [
    {
      type: "stringOptions",
      column: "traceId",
      operator: "any of",
      value: props.traceIds,
    },
  ];
  if (!includeCorrections) {
    filters.push({
      type: "stringOptions",
      column: "dataType",
      operator: "any of",
      value: [...LISTABLE_SCORE_TYPES],
    });
  }
  if (props.level && props.level !== "all") {
    filters.push({
      type: "null",
      column: "observationId",
      operator: props.level === "trace" ? "is null" : "is not null",
      value: "",
    });
  }
  return readScores({
    projectId: props.projectId,
    filters,
    from: props.timestamp
      ? new Date(props.timestamp.getTime() - 60 * 60 * 1_000)
      : undefined,
    limit: props.limit,
    offset: props.offset,
    excludeMetadata: props.excludeMetadata,
    includeHasMetadata: props.includeHasMetadata,
  });
}

export const getScoresForTraces = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForTracesProps<ExcludeMetadata, IncludeHasMetadata>,
) => scoresForTraces(props, false);

export const getScoresAndCorrectionsForTraces = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForTracesProps<ExcludeMetadata, IncludeHasMetadata>,
) => scoresForTraces(props, true);

type GetScoresForSessionsProps<
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
> = {
  projectId: string;
  sessionIds: string[];
  limit?: number;
  offset?: number;
  excludeMetadata?: ExcludeMetadata;
  includeHasMetadata?: IncludeHasMetadata;
};

export const getScoresForSessions = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForSessionsProps<ExcludeMetadata, IncludeHasMetadata>,
) =>
  props.sessionIds.length === 0
    ? []
    : readScores({
        projectId: props.projectId,
        filters: [
          {
            type: "stringOptions",
            column: "sessionId",
            operator: "any of",
            value: props.sessionIds,
          },
          {
            type: "stringOptions",
            column: "dataType",
            operator: "any of",
            value: [...LISTABLE_SCORE_TYPES],
          },
        ],
        limit: props.limit,
        offset: props.offset,
        excludeMetadata: props.excludeMetadata,
        includeHasMetadata: props.includeHasMetadata,
      });

export type GetScoresForObservationsProps<
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
> = {
  projectId: string;
  observationIds: string[];
  minTimestamp?: Date;
  limit?: number;
  offset?: number;
  excludeMetadata?: ExcludeMetadata;
  includeHasMetadata?: IncludeHasMetadata;
};

export const getScoresForObservations = async <
  ExcludeMetadata extends boolean,
  IncludeHasMetadata extends boolean,
>(
  props: GetScoresForObservationsProps<ExcludeMetadata, IncludeHasMetadata>,
) =>
  props.observationIds.length === 0
    ? []
    : readScores({
        projectId: props.projectId,
        filters: [
          {
            type: "stringOptions",
            column: "observationId",
            operator: "any of",
            value: props.observationIds,
          },
          {
            type: "stringOptions",
            column: "dataType",
            operator: "any of",
            value: [...LISTABLE_SCORE_TYPES],
          },
        ],
        from: props.minTimestamp
          ? new Date(props.minTimestamp.getTime() - 60 * 60 * 1_000)
          : undefined,
        limit: props.limit,
        offset: props.offset,
        excludeMetadata: props.excludeMetadata,
        includeHasMetadata: props.includeHasMetadata,
      });

export const getScoresGroupedByNameSourceType = async ({
  projectId,
  filter,
  fromTimestamp,
  toTimestamp,
}: {
  projectId: string;
  filter: FilterCondition[];
  fromTimestamp?: Date;
  toTimestamp?: Date;
}) => {
  const filters = normalizeScoreFilters(filter as FilterState);
  const rows = await getDorisTelemetryRepositories().scores.aggregateGroups({
    projectId,
    range: rangeFromFilters(filters, fromTimestamp, toTimestamp),
    filters: [
      ...filters.filter((item) => !TRACE_FILTER_COLUMNS.has(item.column)),
      {
        type: "stringOptions",
        column: "dataType",
        operator: "any of",
        value: [...LISTABLE_SCORE_TYPES],
      },
    ],
    columns: ["name", "source", "dataType"],
    limit: SCORE_FACET_NAME_LIMIT,
  });
  return rows.map((row) => ({
    name: String(row.name),
    source: String(row.source) as ScoreSourceType,
    dataType: String(row.dataType) as ListableScoreDataType,
  }));
};

async function groupedNames(
  projectId: string,
  filter: FilterState,
  dataTypes: readonly string[],
) {
  const normalized = normalizeScoreFilters(filter);
  return getDorisTelemetryRepositories().scores.aggregateGroups({
    projectId,
    range: rangeFromFilters(normalized),
    filters: [
      ...normalized.filter((item) => !TRACE_FILTER_COLUMNS.has(item.column)),
      {
        type: "stringOptions",
        column: "dataType",
        operator: "any of",
        value: [...dataTypes],
      },
    ],
    columns: ["name"],
    limit: SCORE_FACET_NAME_LIMIT,
  });
}

export const getNumericScoresGroupedByName = async (
  projectId: string,
  filter: FilterState = [],
) =>
  (await groupedNames(projectId, filter, ["NUMERIC", "BOOLEAN"])).map(
    (row) => ({ name: String(row.name) }),
  );

export const getBooleanScoresGroupedByName = async (
  projectId: string,
  filter: FilterState = [],
) =>
  (await groupedNames(projectId, filter, ["BOOLEAN"])).map((row) => ({
    name: String(row.name),
  }));

export const getCategoricalScoresGroupedByName = async (
  projectId: string,
  filter: FilterState = [],
) => {
  const normalized = normalizeScoreFilters(filter);
  const rows = await getDorisTelemetryRepositories().scores.aggregateGroups({
    projectId,
    range: rangeFromFilters(normalized),
    filters: [
      ...normalized.filter((item) => !TRACE_FILTER_COLUMNS.has(item.column)),
      {
        type: "string",
        column: "dataType",
        operator: "=",
        value: "CATEGORICAL",
      },
    ],
    columns: ["name", "stringValue"],
    limit: SCORE_FACET_NAME_LIMIT * SCORE_FACET_VALUE_LIMIT,
  });
  const byName = new Map<string, string[]>();
  for (const row of rows) {
    const name = String(row.name);
    const value = row.stringValue == null ? "" : String(row.stringValue);
    if (!value) continue;
    const values = byName.get(name) ?? [];
    if (!values.includes(value) && values.length < SCORE_FACET_VALUE_LIMIT) {
      values.push(value);
    }
    byName.set(name, values);
  }
  const configs = await prisma.scoreConfig.findMany({
    where: {
      projectId,
      name: { in: [...byName.keys()] },
      dataType: "CATEGORICAL",
      isArchived: false,
    },
    select: { name: true, categories: true },
  });
  for (const config of configs) {
    if (!Array.isArray(config.categories)) continue;
    const values = byName.get(config.name) ?? [];
    for (const category of config.categories as Array<{ label: string }>) {
      if (
        !values.includes(category.label) &&
        values.length < SCORE_FACET_VALUE_LIMIT
      ) {
        values.push(category.label);
      }
    }
    byName.set(config.name, values);
  }
  return [...byName].map(([label, values]) => ({ label, values }));
};

function matchesTraceFilter(
  filter: FilterState[number],
  trace: {
    name?: string | null;
    userId?: string | null;
    tags?: readonly string[];
  } | null,
): boolean {
  const scalar =
    filter.column === "traceName"
      ? (trace?.name ?? null)
      : filter.column === "userId"
        ? (trace?.userId ?? null)
        : null;
  if (filter.type === "string") {
    const value = scalar ?? "";
    if (filter.operator === "=") return value === filter.value;
    if (filter.operator === "contains") return value.includes(filter.value);
    if (filter.operator === "does not contain")
      return !value.includes(filter.value);
    if (filter.operator === "starts with")
      return value.startsWith(filter.value);
    if (filter.operator === "ends with") return value.endsWith(filter.value);
  }
  if (filter.type === "stringOptions") {
    return filter.operator === "any of"
      ? filter.value.includes(scalar ?? "")
      : !filter.value.includes(scalar ?? "");
  }
  if (filter.type === "arrayOptions") {
    const tags = trace?.tags ?? [];
    if (filter.operator === "all of")
      return filter.value.every((value) => tags.includes(value));
    const any = filter.value.some((value) => tags.includes(value));
    return filter.operator === "any of" ? any : !any;
  }
  if (filter.type === "null") {
    const empty =
      filter.column === "traceTags" ? !trace?.tags?.length : scalar === null;
    return filter.operator === "is null" ? empty : !empty;
  }
  throw new InvalidRequestError(
    `Unsupported Doris trace-backed score filter: ${filter.column}`,
  );
}

async function readUiScores(props: {
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
  excludeMetadata?: boolean;
}) {
  const normalized = normalizeScoreFilters(props.filter);
  const traceFilters = normalized.filter((filter) =>
    TRACE_FILTER_COLUMNS.has(filter.column),
  );
  const scoreFilters: FilterState = [
    ...normalized.filter((filter) => !TRACE_FILTER_COLUMNS.has(filter.column)),
    {
      type: "stringOptions",
      column: "dataType",
      operator: "any of",
      value: [...LISTABLE_SCORE_TYPES],
    },
  ];
  const orderBy = normalizedOrder(props.orderBy);
  const traceOrder =
    orderBy && TRACE_FILTER_COLUMNS.has(orderBy.column) ? orderBy : null;
  if (traceFilters.length === 0 && !traceOrder) {
    const range = rangeFromFilters(scoreFilters);
    const [items, count] = await Promise.all([
      readScores({
        projectId: props.projectId,
        filters: scoreFilters,
        limit: props.limit,
        offset: props.offset,
        excludeMetadata: props.excludeMetadata,
        includeHasMetadata: true,
        orderBy,
      }),
      getDorisTelemetryRepositories().scores.count({
        projectId: props.projectId,
        range,
        filters: scoreFilters,
      }),
    ]);
    return { items, count };
  }
  const candidates = await readScores({
    projectId: props.projectId,
    filters: scoreFilters,
    limit: TRACE_FILTER_SCAN_LIMIT + 1,
    excludeMetadata: props.excludeMetadata,
    includeHasMetadata: true,
  });
  if (candidates.length > TRACE_FILTER_SCAN_LIMIT) {
    throw new InvalidRequestError(
      `Doris trace-backed score filters are limited to ${TRACE_FILTER_SCAN_LIMIT} candidates`,
    );
  }
  const traceIds = [
    ...new Set(
      candidates.flatMap((score) => (score.traceId ? [score.traceId] : [])),
    ),
  ];
  const traces = new Map<
    string,
    Awaited<
      ReturnType<
        ReturnType<typeof getDorisTelemetryRepositories>["traces"]["get"]
      >
    >
  >();
  for (
    let offset = 0;
    offset < traceIds.length;
    offset += TRACE_LOOKUP_CONCURRENCY
  ) {
    const resolved = await Promise.all(
      traceIds.slice(offset, offset + TRACE_LOOKUP_CONCURRENCY).map(
        async (traceId) =>
          [
            traceId,
            await getDorisTelemetryRepositories().traces.get({
              projectId: props.projectId,
              traceId,
            }),
          ] as const,
      ),
    );
    resolved.forEach(([traceId, trace]) => traces.set(traceId, trace));
  }
  const matching = candidates.filter((score) => {
    const trace = score.traceId ? (traces.get(score.traceId) ?? null) : null;
    return traceFilters.every((filter) => matchesTraceFilter(filter, trace));
  });
  if (traceOrder) {
    matching.sort((left, right) => {
      const leftTrace = left.traceId ? traces.get(left.traceId) : null;
      const rightTrace = right.traceId ? traces.get(right.traceId) : null;
      const value = (trace: typeof leftTrace) =>
        traceOrder.column === "traceName"
          ? trace?.name
          : traceOrder.column === "userId"
            ? trace?.userId
            : trace?.tags.join(",");
      const comparison = String(value(leftTrace) ?? "").localeCompare(
        String(value(rightTrace) ?? ""),
      );
      return traceOrder.order === "ASC" ? comparison : -comparison;
    });
  }
  const offset = props.offset ?? 0;
  return {
    items:
      props.limit === undefined
        ? matching.slice(offset)
        : matching.slice(offset, offset + props.limit),
    count: matching.length,
  };
}

export const getScoresUiCountFromEvents = async (props: {
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
}) => (await readUiScores(props)).count;

export type ScoreUiTableRowFromEvents = Omit<ScoreDomain, "metadata"> & {
  hasMetadata: boolean;
};

export async function getScoresUiTableFromEvents(props: {
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
  excludeMetadata?: boolean;
}) {
  const { items } = await readUiScores({
    ...props,
    excludeMetadata: props.excludeMetadata ?? true,
  });
  return items.map((score) => ({
    ...score,
    hasMetadata: score.hasMetadata ?? Object.keys(score.metadata).length > 0,
  }));
}

export const getScoresUiCount = getScoresUiCountFromEvents;

export type ScoreUiTableRow = ScoreDomain & {
  traceName: string | null;
  traceUserId: string | null;
  traceTags: string[] | null;
};

export async function getScoresUiTable(props: {
  projectId: string;
  filter: FilterState;
  orderBy: OrderByState;
  limit?: number;
  offset?: number;
  excludeMetadata?: boolean;
  includeHasMetadataFlag?: boolean;
}) {
  const rows = await getScoresUiTableFromEvents(props);
  return Promise.all(
    rows.map(async (score) => {
      const trace = score.traceId
        ? await getDorisTelemetryRepositories().traces.get({
            projectId: props.projectId,
            traceId: score.traceId,
          })
        : null;
      return {
        ...score,
        traceName: trace?.name ?? null,
        traceUserId: trace?.userId ?? null,
        traceTags: trace?.tags ? [...trace.tags] : null,
      };
    }),
  );
}

export const getScoreNames = async (
  projectId: string,
  timestampFilter: FilterState,
) => {
  const filters = normalizeScoreFilters(timestampFilter);
  const rows = await getDorisTelemetryRepositories().scores.aggregateGroups({
    projectId,
    range: rangeFromFilters(filters),
    filters: [
      ...filters,
      {
        type: "stringOptions",
        column: "dataType",
        operator: "any of",
        value: [...LISTABLE_SCORE_TYPES],
      },
    ],
    columns: ["name"],
    limit: 1_000,
  });
  return rows.map((row) => ({ name: String(row.name), count: row.count }));
};

export const getScoreStringValues = async (
  projectId: string,
  timestampFilter: FilterState,
) => {
  const filters = normalizeScoreFilters(timestampFilter);
  const rows = await getDorisTelemetryRepositories().scores.aggregateGroups({
    projectId,
    range: rangeFromFilters(filters),
    filters,
    columns: ["name", "stringValue"],
    limit: 10_000,
  });
  return rows.map((row) => ({
    name: String(row.name),
    value: row.stringValue == null ? "" : String(row.stringValue),
    count: row.count,
  }));
};

export const hasAnyScore = async (projectId: string) =>
  (await getDorisTelemetryRepositories().scores.count({
    projectId,
    range: { from: SCORE_RANGE_START, to: SCORE_RANGE_END() },
    filters: [],
  })) > 0;

export const getScoreMetadataById = async (
  projectId: string,
  id: string,
  source?: ScoreSourceType,
) => (await getScoreById({ projectId, scoreId: id, source }))?.metadata;

export const deleteScores = async (projectId: string, scoreIds: string[]) => {
  if (scoreIds.length === 0) return;
  const queue = ScoreDeleteQueue.getInstance();
  if (!queue) throw new InternalServerError("ScoreDeleteQueue not initialized");
  await queue.add(QueueJobs.ScoreDelete, {
    timestamp: new Date(),
    id: randomUUID(),
    payload: { projectId, scoreIds: [...new Set(scoreIds)] },
    name: QueueJobs.ScoreDelete,
  });
};

export const getAggregatedScoresForPrompts = async (
  projectId: string,
  promptIds: string[],
  fetchScoreRelation: "observation" | "trace",
  window: { fromTimestamp?: Date; toTimestamp?: Date } = {},
) =>
  getDorisTelemetryRepositories().scores.listForPrompts({
    projectId,
    promptIds,
    relation: fetchScoreRelation,
    from: window.fromTimestamp,
    to: window.toTimestamp,
  });

export const getScoreCountsByProjectInCreationInterval = async (input: {
  start: Date;
  end: Date;
}) => [
  ...(await getDorisTelemetryRepositories().scores.countByProjectCreatedAt(
    input,
  )),
];

export const getScoreCountOfProjectsSinceCreationDate = (input: {
  projectIds: string[];
  start: Date;
}) => getDorisTelemetryRepositories().scores.countProjectsSince(input);

export const getScoreCountsByProjectAndDay = async (input: {
  startDate: Date;
  endDate: Date;
}) => [
  ...(await getDorisTelemetryRepositories().scores.countByProjectAndDay({
    start: input.startDate,
    end: input.endDate,
  })),
];

export const ScoresCursorV3 = z.discriminatedUnion("v", [
  z.object({
    v: z.literal(1),
    lastTimestamp: z.coerce.date(),
    lastId: z.string(),
  }),
]);
export type ScoresCursorV3Type = z.infer<typeof ScoresCursorV3>;

export const EncodedScoresCursorV3 = z
  .string()
  .transform((value) => {
    try {
      return JSON.parse(Buffer.from(value, "base64url").toString("utf8"));
    } catch {
      throw new InvalidRequestError("Invalid cursor format");
    }
  })
  .pipe(ScoresCursorV3);

export const encodeCursorV3 = (cursor: ScoresCursorV3Type): string =>
  Buffer.from(
    JSON.stringify({
      v: cursor.v,
      lastTimestamp: cursor.lastTimestamp.toISOString(),
      lastId: cursor.lastId,
    }),
  ).toString("base64url");

export type ScoreQueryType = {
  page: number;
  limit: number;
  projectId: string;
  traceId?: string;
  userId?: string;
  name?: string;
  source?: string;
  fromTimestamp?: string;
  toTimestamp?: string;
  value?: number;
  scoreId?: string;
  configId?: string;
  sessionId?: string;
  datasetRunId?: string;
  queueId?: string;
  traceTags?: string | string[];
  operator?: string;
  scoreIds?: string[];
  observationId?: string[];
  dataType?: string;
  environment?: string | string[];
  fields?: string[] | null;
  advancedFilters?: FilterState;
};

type ListFilterParams = {
  id?: string[];
  name?: string[];
  source?: string[];
  dataType?: string[];
  environment?: string[];
  configId?: string[];
  queueId?: string[];
  authorUserId?: string[];
  value?: string[];
  valueMin?: number;
  valueMax?: number;
  traceId?: string[];
  sessionId?: string[];
  observationId?: string[];
  experimentId?: string[];
  fromTimestamp?: Date;
  toTimestamp?: Date;
};

export function polymorphicValueForV3(score: {
  dataType: ScoreDataTypeType;
  value: number;
  stringValue?: string | null;
  longStringValue?: string | null;
}): number | boolean | string {
  if (score.dataType === ScoreDataTypeEnum.NUMERIC) return score.value;
  if (score.dataType === ScoreDataTypeEnum.BOOLEAN) return score.value === 1;
  if (score.dataType === ScoreDataTypeEnum.CORRECTION) {
    if (score.longStringValue == null)
      throw new InternalServerError("Correction score has no value");
    return score.longStringValue;
  }
  if (score.stringValue == null)
    throw new InternalServerError(`${score.dataType} score has no value`);
  return score.stringValue;
}

function scoreSubject(score: ScoreDomain) {
  if (score.datasetRunId)
    return { kind: "experiment" as const, id: score.datasetRunId };
  if (score.observationId)
    return {
      kind: "observation" as const,
      id: score.observationId,
      ...(score.traceId ? { traceId: score.traceId } : {}),
    };
  if (score.sessionId) return { kind: "session" as const, id: score.sessionId };
  if (!score.traceId)
    throw new InternalServerError(`Score ${score.id} has no subject`);
  return { kind: "trace" as const, id: score.traceId };
}

export function scoreDomainToV3(
  score: ScoreDomain,
  fields: ScoreFieldGroupV3[],
): APIScoreV3 {
  return {
    id: score.id,
    projectId: score.projectId,
    name: score.name,
    dataType: score.dataType,
    value: polymorphicValueForV3(score),
    source: score.source,
    timestamp: score.timestamp,
    environment: score.environment,
    createdAt: score.createdAt,
    updatedAt: score.updatedAt,
    ...(fields.includes("details")
      ? {
          comment: score.comment,
          configId: score.configId,
          metadata: score.metadata,
        }
      : {}),
    ...(fields.includes("annotation")
      ? { authorUserId: score.authorUserId, queueId: score.queueId }
      : {}),
    ...(fields.includes("subject") ? { subject: scoreSubject(score) } : {}),
  } as APIScoreV3;
}

export async function listScoresV3ForPublicApi(
  params: {
    projectId: string;
    limit: number;
    cursor?: ScoresCursorV3Type;
    fields: ScoreFieldGroupV3[];
  } & ListFilterParams,
): Promise<{ data: APIScoreV3[]; cursor?: string }> {
  if (params.experimentId?.length) {
    throw new InvalidRequestError(
      "Experiment score reads are unavailable in Doris R1A",
    );
  }
  const filters: FilterState = [];
  const addOptions = (column: string, values?: readonly string[]) => {
    if (values?.length) {
      filters.push({
        type: "stringOptions",
        column,
        operator: "any of",
        value: [...values],
      });
    }
  };
  addOptions("scoreId", params.id);
  addOptions("name", params.name);
  addOptions("source", params.source);
  addOptions("dataType", params.dataType);
  addOptions("environment", params.environment);
  addOptions("configId", params.configId);
  addOptions("queueId", params.queueId);
  addOptions("authorUserId", params.authorUserId);
  addOptions("traceId", params.traceId);
  addOptions("sessionId", params.sessionId);
  addOptions("observationId", params.observationId);
  if (params.valueMin !== undefined)
    filters.push({
      type: "number",
      column: "value",
      operator: ">=",
      value: params.valueMin,
    });
  if (params.valueMax !== undefined)
    filters.push({
      type: "number",
      column: "value",
      operator: "<=",
      value: params.valueMax,
    });
  if (params.value?.length && params.dataType?.length === 1) {
    const dataType = params.dataType[0];
    if (dataType === "NUMERIC" || dataType === "BOOLEAN") {
      const values = params.value.map((value) =>
        value === "true" ? "1" : value === "false" ? "0" : value,
      );
      if (values.some((value) => !Number.isFinite(Number(value)))) {
        throw new InvalidRequestError("Invalid numeric score value filter");
      }
      addOptions("value", values);
    } else {
      addOptions("stringValue", params.value);
    }
  }
  const from = params.fromTimestamp ?? SCORE_RANGE_START;
  const to = params.toTimestamp ?? SCORE_RANGE_END();
  if (from >= to)
    throw new InvalidRequestError("Invalid Doris score timestamp range");
  const cursor = params.cursor
    ? Buffer.from(
        JSON.stringify({
          version: 1,
          timestamp: params.cursor.lastTimestamp.toISOString(),
          scoreId: params.cursor.lastId,
        }),
      ).toString("base64url")
    : undefined;
  const page = await getDorisTelemetryRepositories().scores.list({
    projectId: params.projectId,
    range: { from, to },
    filters,
    limit: params.limit,
    cursor,
  });
  const data = filterAndValidateV3GetScoreList(
    page.items.map((score) => scoreDomainToV3(score, params.fields)),
  );
  const last = page.items.at(-1);
  return {
    data,
    ...(page.nextCursor && last
      ? {
          cursor: encodeCursorV3({
            v: 1,
            lastTimestamp: last.timestamp,
            lastId: last.id,
          }),
        }
      : {}),
  };
}

function r1bUnavailable(name: string): never {
  throw new InvalidRequestError(`${name} is unavailable in Doris R1A`);
}

type R1BExperimentScore = AggregatableScore & { hasMetadata?: boolean };

export const getScoresForExperiments = async (_props: {
  projectId: string;
  runIds: string[];
  limit?: number;
  offset?: number;
  excludeMetadata?: boolean;
  includeHasMetadata?: boolean;
}): Promise<R1BExperimentScore[]> => r1bUnavailable("Experiment scores");
export const getTraceScoresForDatasetRuns = async (
  _projectId: string,
  _datasetRunIds: string[],
): Promise<
  Array<R1BExperimentScore & { datasetRunId: string; hasMetadata: boolean }>
> => r1bUnavailable("Dataset-run scores");
export const getScoresForExperimentItems = async (
  _projectId: string,
  _experimentIds: string[],
): Promise<
  Array<R1BExperimentScore & { experimentId: string; hasMetadata: boolean }>
> => r1bUnavailable("Experiment item scores");
export const queryScoreRecordsForExperimentItems = async (
  ..._args: unknown[]
) => r1bUnavailable("Experiment item scores");
export const queryScoreRecordsForExperiments = async (..._args: unknown[]) =>
  r1bUnavailable("Experiment scores");
export const deleteScoresOlderThanDays = async (..._args: unknown[]) =>
  r1bUnavailable("Score retention deletion");
export const deleteScoresByTraceIds = async (..._args: unknown[]) =>
  r1bUnavailable("Direct trace score deletion");
export const deleteScoresByProjectId = async (..._args: unknown[]) =>
  r1bUnavailable("Direct project score deletion");
export async function* getScoresForAnalyticsIntegrations(
  ..._args: unknown[]
): AsyncGenerator<never> {
  yield* [] as never[];
  r1bUnavailable("Analytics integrations");
}
