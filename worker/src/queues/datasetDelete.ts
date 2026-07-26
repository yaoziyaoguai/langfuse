import { Job, Processor } from "bullmq";
import {
  analyticsDatasetDeletionReferenceMatches,
  claimAnalyticsDatasetDeletionOperation,
  completeAnalyticsDatasetDeletionOperation,
  findAnalyticsDatasetDeletionOperation,
  markAnalyticsDatasetDeletionBarrierVisible,
  QueueName,
  TQueueJobTypes,
  type DatasetQueueEventType,
} from "@langfuse/shared/src/server";
import { processClickhouseDatasetDelete } from "../features/datasets/processClickhouseDatasetDelete";
import { processDatasetDelete } from "../features/datasets/processDatasetDelete";
import { isAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { env } from "../env";

async function processManagedClickhouseDatasetDelete(
  payload: DatasetQueueEventType,
): Promise<void> {
  if (!payload.analyticsDeletion) {
    await processClickhouseDatasetDelete(payload);
    return;
  }
  const operation = await findAnalyticsDatasetDeletionOperation({
    operationId: payload.analyticsDeletion.operationId,
    projectId: payload.projectId,
  });
  if (
    !operation ||
    operation.datasetId !== payload.datasetId ||
    operation.scope !==
      (payload.deletionType === "dataset" ? "DATASET" : "DATASET_RUNS") ||
    !analyticsDatasetDeletionReferenceMatches({
      operation,
      reference: payload.analyticsDeletion,
    }) ||
    (payload.deletionType === "dataset-runs" &&
      [...payload.datasetRunIds].sort().join("\0") !==
        [...operation.datasetRunIds].sort().join("\0"))
  ) {
    throw new Error("Analytics dataset deletion contract mismatch");
  }
  if (operation.status === "COMPLETED") return;
  const owner = `clickhouse-dataset-delete-${process.pid}-${operation.id}`;
  const claimed = await claimAnalyticsDatasetDeletionOperation({
    operationId: operation.id,
    projectId: operation.projectId,
    owner,
  });
  if (!claimed) throw new Error("Analytics dataset deletion lease unavailable");
  const lease = { owner, fence: claimed.workerFence };
  await processClickhouseDatasetDelete(payload);
  if (
    !claimed.logicallyInvisible &&
    !(await markAnalyticsDatasetDeletionBarrierVisible({
      operationId: claimed.id,
      projectId: claimed.projectId,
      lease,
    }))
  ) {
    throw new Error("Analytics dataset deletion barrier fence was lost");
  }
  if (
    !(await completeAnalyticsDatasetDeletionOperation({
      operationId: claimed.id,
      projectId: claimed.projectId,
      lease,
    }))
  ) {
    throw new Error("Analytics dataset deletion completion fence was lost");
  }
}

export const datasetDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.DatasetDelete]>,
): Promise<void> => {
  if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "doris")) {
    await processDatasetDelete(job.data.payload);
  } else {
    await processManagedClickhouseDatasetDelete(job.data.payload);
  }
};
