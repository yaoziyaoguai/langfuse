import {
  ActionId,
  AnnotationQueueObjectType,
  BatchEvalSourceTable,
  BatchActionStatus,
  EvalTargetObject,
  ObservationAddToDatasetConfigSchema,
  type BatchActionQuery,
  type FilterState,
} from "@langfuse/shared";
import type { Job } from "bullmq";
import {
  buildDorisObservationReadQuery,
  buildDorisTraceReadQuery,
  createHistoricalAnalyticsEvaluationDispatches,
  findDatasetIdsForBatchDeletion,
  getDorisTelemetryRepositories,
  getObservationsWithModelDataFromEventsTable,
  getScoresUiTableFromEvents,
  getSessionsTable,
  getTraceIdentifiers,
  logger,
  QueueName,
  type TQueueJobTypes,
  type HistoricalAnalyticsEvaluationTargetInput,
} from "@langfuse/shared/src/server";
import { EvalTemplateType } from "@prisma/client";

import { processAnalyticsScoreDelete } from "../scores/processAnalyticsScoreDelete";
import { processAddObservationsToDataset } from "./processAddObservationsToDataset";
import { processAddToAnnotationQueue } from "./processAddToQueue";
import { processDeleteDatasets } from "./processDeleteDatasets";
import { prisma } from "@langfuse/shared/src/db";
import { env } from "../../env";
import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";

const PAGE_SIZE = 999;
const WRITE_CHUNK_SIZE = 500;
const HISTORICAL_DISPATCH_CHUNK_SIZE = 100;

type BatchActionJobData = TQueueJobTypes[QueueName.BatchActionQueue];
type BatchActionPayload = BatchActionJobData["payload"];

function normalizeFilters(query: BatchActionQuery): FilterState {
  return (query.filter ?? []).map((filter) =>
    filter.type === "datetime"
      ? { ...filter, value: new Date(filter.value) }
      : filter,
  );
}

function withCutoff(
  query: BatchActionQuery,
  column: string,
  cutoffCreatedAt: Date,
): FilterState {
  return [
    ...normalizeFilters(query),
    {
      type: "datetime",
      column,
      operator: "<",
      value: cutoffCreatedAt,
    },
  ];
}

function requireTargetId(payload: BatchActionPayload): string {
  if (!("targetId" in payload) || !payload.targetId) {
    throw new Error(`Target ID is required for ${payload.actionId}`);
  }
  return payload.targetId;
}

async function listObservationIds(
  payload: Extract<
    BatchActionPayload,
    { actionId: "observation-add-to-annotation-queue" }
  >,
): Promise<string[]> {
  const ids: string[] = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const rows = await getObservationsWithModelDataFromEventsTable({
      projectId: payload.projectId,
      filter: withCutoff(
        payload.query,
        "startTime",
        new Date(payload.cutoffCreatedAt),
      ),
      orderBy: payload.query.orderBy,
      searchQuery: payload.query.searchQuery,
      searchType: payload.query.searchType,
      limit: PAGE_SIZE,
      offset,
    });
    ids.push(...rows.map(({ id }) => id));
    if (rows.length < PAGE_SIZE) return ids;
  }
}

async function listTraceIds(
  payload: Extract<
    BatchActionPayload,
    { actionId: "trace-add-to-annotation-queue" }
  >,
): Promise<string[]> {
  const ids: string[] = [];
  for (let page = 0; ; page += 1) {
    const rows = await getTraceIdentifiers({
      projectId: payload.projectId,
      filter: withCutoff(
        payload.query,
        "timestamp",
        new Date(payload.cutoffCreatedAt),
      ),
      orderBy: payload.query.orderBy,
      searchQuery: payload.query.searchQuery,
      searchType: payload.query.searchType,
      limit: PAGE_SIZE,
      page,
    });
    ids.push(...rows.map(({ id }) => id));
    if (rows.length < PAGE_SIZE) return ids;
  }
}

async function listSessionIds(
  payload: Extract<
    BatchActionPayload,
    { actionId: "session-add-to-annotation-queue" }
  >,
): Promise<string[]> {
  const ids: string[] = [];
  for (let page = 0; ; page += 1) {
    const rows = await getSessionsTable({
      projectId: payload.projectId,
      filter: withCutoff(
        payload.query,
        "createdAt",
        new Date(payload.cutoffCreatedAt),
      ),
      orderBy: payload.query.orderBy,
      limit: PAGE_SIZE,
      page,
    });
    ids.push(...rows.map(({ session_id }) => session_id));
    if (rows.length < PAGE_SIZE) return ids;
  }
}

async function processAnnotationQueueAction(
  payload: Extract<
    BatchActionPayload,
    {
      actionId:
        | "trace-add-to-annotation-queue"
        | "session-add-to-annotation-queue"
        | "observation-add-to-annotation-queue";
    }
  >,
): Promise<void> {
  const targetId = requireTargetId(payload);
  const [objectType, objectIds] =
    payload.actionId === ActionId.TraceAddToAnnotationQueue
      ? [AnnotationQueueObjectType.TRACE, await listTraceIds(payload)]
      : payload.actionId === ActionId.SessionAddToAnnotationQueue
        ? [AnnotationQueueObjectType.SESSION, await listSessionIds(payload)]
        : [
            AnnotationQueueObjectType.OBSERVATION,
            await listObservationIds(payload),
          ];

  for (let offset = 0; offset < objectIds.length; offset += WRITE_CHUNK_SIZE) {
    await processAddToAnnotationQueue({
      projectId: payload.projectId,
      objectIds: objectIds.slice(offset, offset + WRITE_CHUNK_SIZE),
      objectType,
      targetId,
    });
  }
}

async function processObservationAddToDataset(
  payload: Extract<
    BatchActionPayload,
    { actionId: "observation-add-to-dataset" }
  >,
): Promise<void> {
  const observations = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const rows = await getObservationsWithModelDataFromEventsTable({
      projectId: payload.projectId,
      filter: withCutoff(
        payload.query,
        "startTime",
        new Date(payload.cutoffCreatedAt),
      ),
      orderBy: payload.query.orderBy,
      searchQuery: payload.query.searchQuery,
      searchType: payload.query.searchType,
      limit: PAGE_SIZE,
      offset,
      selectIOAndMetadata: true,
    });
    observations.push(
      ...rows.map((row) => {
        if (!row.traceId) {
          throw new Error(
            `Observation ${row.id} cannot be added to a dataset without a trace ID`,
          );
        }
        return {
          id: row.id,
          traceId: row.traceId,
          input: row.input,
          output: row.output,
          metadata: row.metadata,
        };
      }),
    );
    if (rows.length < PAGE_SIZE) break;
  }

  await processAddObservationsToDataset({
    projectId: payload.projectId,
    batchActionId: payload.batchActionId,
    config: ObservationAddToDatasetConfigSchema.parse(payload.config),
    observations,
  });
}

async function processScoreDelete(
  payload: Extract<BatchActionPayload, { actionId: "score-delete" }>,
): Promise<void> {
  const scoreIds: string[] = [];
  for (let offset = 0; ; offset += PAGE_SIZE) {
    const rows = await getScoresUiTableFromEvents({
      projectId: payload.projectId,
      filter: withCutoff(
        payload.query,
        "timestamp",
        new Date(payload.cutoffCreatedAt),
      ),
      orderBy: payload.query.orderBy,
      limit: PAGE_SIZE,
      offset,
    });
    scoreIds.push(...rows.map(({ id }) => id));
    if (rows.length < PAGE_SIZE) break;
  }
  for (let offset = 0; offset < scoreIds.length; offset += WRITE_CHUNK_SIZE) {
    await processAnalyticsScoreDelete(
      payload.projectId,
      scoreIds.slice(offset, offset + WRITE_CHUNK_SIZE),
    );
  }
}

async function persistHistoricalDispatchTargets(
  projectId: string,
  targets: readonly HistoricalAnalyticsEvaluationTargetInput[],
): Promise<{ created: number; missing: number }> {
  const admissionContext = getWorkerAnalyticsAdmissionContext();
  if (!admissionContext) {
    throw new Error("Historical evaluation runtime is not admitted");
  }
  let created = 0;
  let missing = 0;
  for (
    let offset = 0;
    offset < targets.length;
    offset += HISTORICAL_DISPATCH_CHUNK_SIZE
  ) {
    const result = await createHistoricalAnalyticsEvaluationDispatches({
      admissionContext,
      projectId,
      targets: targets.slice(offset, offset + HISTORICAL_DISPATCH_CHUNK_SIZE),
    });
    created += result.created;
    missing += result.missing;
  }
  return { created, missing };
}

async function processHistoricalEvalCreate(
  jobData: BatchActionJobData,
  payload: Extract<BatchActionPayload, { actionId: "eval-create" }>,
): Promise<void> {
  const config = await prisma.jobConfiguration.findFirst({
    where: {
      id: payload.configId,
      projectId: payload.projectId,
      jobType: "EVAL",
      evalTemplateId: { not: null },
    },
    select: {
      id: true,
      evalTemplate: { select: { type: true } },
    },
  });
  if (!config) {
    throw new Error("Historical evaluator configuration is unavailable");
  }
  if (config.evalTemplate?.type !== EvalTemplateType.LLM_AS_JUDGE) {
    logger.info("Skipping non-LLM trace/dataset historical evaluator", {
      projectId: payload.projectId,
      configId: payload.configId,
      templateType: config.evalTemplate?.type ?? null,
    });
    return;
  }
  const requestId = `${jobData.id}:${config.id}`;
  const limit = env.LANGFUSE_MAX_HISTORIC_EVAL_CREATION_LIMIT;
  let created = 0;
  let missing = 0;

  if (payload.targetObject === EvalTargetObject.TRACE) {
    const traceQuery = await buildDorisTraceReadQuery(payload.projectId, [
      ...normalizeFilters(payload.query),
      {
        type: "datetime",
        column: "timestamp",
        operator: "<",
        value: new Date(payload.cutoffCreatedAt),
      },
    ]);
    if (!traceQuery.impossible) {
      const buffer: HistoricalAnalyticsEvaluationTargetInput[] = [];
      for await (const trace of getDorisTelemetryRepositories().traces.scanEvaluationTargets(
        {
          projectId: payload.projectId,
          range: traceQuery.range!,
          filters: traceQuery.filters,
          search: payload.query.searchQuery
            ? {
                query: payload.query.searchQuery,
                searchType: payload.query.searchType,
              }
            : undefined,
          limit,
        },
      )) {
        buffer.push({
          requestId,
          jobConfigurationId: config.id,
          targetId: trace.id,
          traceId: trace.id,
          observationId: null,
          datasetItemId: null,
          datasetRunItemId: null,
          targetTimestamp: trace.timestamp,
          traceEnvironment: trace.environment,
        });
        if (buffer.length === HISTORICAL_DISPATCH_CHUNK_SIZE) {
          const result = await persistHistoricalDispatchTargets(
            payload.projectId,
            buffer.splice(0),
          );
          created += result.created;
          missing += result.missing;
        }
      }
      if (buffer.length > 0) {
        const result = await persistHistoricalDispatchTargets(
          payload.projectId,
          buffer,
        );
        created += result.created;
        missing += result.missing;
      }
    }
  } else {
    const runItems = await prisma.datasetRunItems.findMany({
      where: {
        projectId: payload.projectId,
        createdAt: { lt: new Date(payload.cutoffCreatedAt) },
      },
      select: {
        id: true,
        datasetItemId: true,
        traceId: true,
        observationId: true,
        createdAt: true,
      },
      orderBy: [{ createdAt: "asc" }, { id: "asc" }],
      take: limit,
    });
    const result = await persistHistoricalDispatchTargets(
      payload.projectId,
      runItems.map((item) => ({
        requestId,
        jobConfigurationId: config.id,
        targetId: item.id,
        traceId: item.traceId,
        observationId: item.observationId,
        datasetItemId: item.datasetItemId,
        datasetRunItemId: item.id,
        targetTimestamp: item.createdAt,
        traceEnvironment: null,
      })),
    );
    created += result.created;
    missing += result.missing;
  }

  logger.info("Created Doris historical evaluation dispatches", {
    projectId: payload.projectId,
    configId: config.id,
    created,
    missing,
  });
}

async function processHistoricalObservationEvaluation(
  jobData: BatchActionJobData,
  payload: Extract<
    BatchActionPayload,
    { actionId: "observation-run-batched-evaluation" }
  >,
): Promise<void> {
  const evaluatorIds = [...new Set(payload.evaluatorIds)];
  const sourceTable = payload.sourceTable ?? BatchEvalSourceTable.EVENTS;
  const targetObject =
    sourceTable === BatchEvalSourceTable.EVENTS ? "EVENT" : "EXPERIMENT";
  const evaluators = await prisma.jobConfiguration.findMany({
    where: {
      id: { in: evaluatorIds },
      projectId: payload.projectId,
      jobType: "EVAL",
      evalTemplateId: { not: null },
      targetObject,
    },
    select: { id: true },
  });
  if (evaluators.length !== evaluatorIds.length) {
    throw new Error("Selected historical evaluators are unavailable");
  }
  await prisma.batchAction.update({
    where: { id: payload.batchActionId, projectId: payload.projectId },
    data: {
      status: BatchActionStatus.Processing,
      totalCount: 0,
      processedCount: 0,
      failedCount: 0,
      log: null,
    },
  });
  const query = buildDorisObservationReadQuery([
    ...normalizeFilters(payload.query),
    {
      type: "datetime",
      column: "startTime",
      operator: "<",
      value: new Date(payload.cutoffCreatedAt),
    },
  ]);
  const historicalRange = query.range ?? {
    from: new Date(0),
    to: new Date(payload.cutoffCreatedAt),
  };
  const repository = getDorisTelemetryRepositories().observations;
  const maxRows = env.LANGFUSE_MAX_HISTORIC_EVAL_CREATION_LIMIT;
  let cursor: string | undefined;
  let totalCount = 0;
  let processedCount = 0;
  let failedCount = 0;
  do {
    const page = await repository.scan({
      projectId: payload.projectId,
      range: historicalRange,
      filters: query.filters,
      search: payload.query.searchQuery
        ? {
            query: payload.query.searchQuery,
            searchType: payload.query.searchType,
          }
        : undefined,
      cursor,
      limit: Math.min(PAGE_SIZE, maxRows - totalCount),
      includeFullContent: false,
    });
    if (page.items.length === 0) break;
    for (const observation of page.items) {
      const result = await persistHistoricalDispatchTargets(
        payload.projectId,
        evaluators.map(({ id }) => ({
          requestId: `${jobData.id}:${id}`,
          jobConfigurationId: id,
          targetId: observation.id,
          traceId: observation.traceId,
          observationId: observation.id,
          datasetItemId: null,
          datasetRunItemId: null,
          targetTimestamp: observation.startTime,
          traceEnvironment: observation.environment,
        })),
      );
      totalCount += 1;
      if (result.missing === 0) processedCount += 1;
      else failedCount += 1;
      if (totalCount >= maxRows) break;
    }
    cursor = totalCount >= maxRows ? undefined : (page.nextCursor ?? undefined);
    await prisma.batchAction.update({
      where: { id: payload.batchActionId, projectId: payload.projectId },
      data: { totalCount, processedCount, failedCount },
    });
  } while (cursor);

  await prisma.batchAction.update({
    where: { id: payload.batchActionId, projectId: payload.projectId },
    data: {
      status:
        failedCount === 0
          ? BatchActionStatus.Completed
          : processedCount === 0
            ? BatchActionStatus.Failed
            : BatchActionStatus.Partial,
      finishedAt: new Date(),
      totalCount,
      processedCount,
      failedCount,
      log:
        failedCount > 0
          ? `${failedCount} observations no longer had a visible Doris target`
          : null,
    },
  });
}

export async function handleBatchActionJob(
  jobData: Job<BatchActionJobData>["data"],
): Promise<void> {
  const payload = jobData.payload;
  switch (payload.actionId) {
    case ActionId.TraceAddToAnnotationQueue:
    case ActionId.SessionAddToAnnotationQueue:
    case ActionId.ObservationAddToAnnotationQueue:
      await processAnnotationQueueAction(payload);
      break;
    case ActionId.ObservationAddToDataset:
      await processObservationAddToDataset(payload);
      break;
    case ActionId.ScoreDelete:
      await processScoreDelete(payload);
      break;
    case ActionId.DatasetDelete: {
      const datasets = await findDatasetIdsForBatchDeletion({
        projectId: payload.projectId,
        cutoffCreatedAt: new Date(payload.cutoffCreatedAt),
        query: payload.query,
      });
      for (
        let offset = 0;
        offset < datasets.length;
        offset += WRITE_CHUNK_SIZE
      ) {
        await processDeleteDatasets(
          payload.projectId,
          datasets.slice(offset, offset + WRITE_CHUNK_SIZE).map(({ id }) => id),
        );
      }
      break;
    }
    case "eval-create":
      await processHistoricalEvalCreate(jobData, payload);
      break;
    case ActionId.ObservationBatchEvaluation:
      await processHistoricalObservationEvaluation(jobData, payload);
      break;
    default:
      throw new Error(
        `Batch action ${payload.actionId} is unavailable for the Doris backend`,
      );
  }
  logger.info("Doris batch action completed", {
    projectId: payload.projectId,
    actionId: payload.actionId,
  });
}
