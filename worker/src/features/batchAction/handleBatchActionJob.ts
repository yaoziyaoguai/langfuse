import {
  ActionId,
  AnnotationQueueObjectType,
  ObservationAddToDatasetConfigSchema,
  type BatchActionQuery,
  type FilterState,
} from "@langfuse/shared";
import type { Job } from "bullmq";
import {
  findDatasetIdsForBatchDeletion,
  getObservationsWithModelDataFromEventsTable,
  getScoresUiTableFromEvents,
  getSessionsTable,
  getTraceIdentifiers,
  logger,
  QueueName,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";

import { processAnalyticsScoreDelete } from "../scores/processAnalyticsScoreDelete";
import { processAddObservationsToDataset } from "./processAddObservationsToDataset";
import { processAddToAnnotationQueue } from "./processAddToQueue";
import { processDeleteDatasets } from "./processDeleteDatasets";

const PAGE_SIZE = 999;
const WRITE_CHUNK_SIZE = 500;

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
    default:
      throw new Error(
        `Batch action ${payload.actionId} is unavailable in Doris R1A`,
      );
  }
  logger.info("Doris R1A batch action completed", {
    projectId: payload.projectId,
    actionId: payload.actionId,
  });
}
