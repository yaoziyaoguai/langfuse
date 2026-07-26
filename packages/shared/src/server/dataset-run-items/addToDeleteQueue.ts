import { DatasetDeleteQueue } from "../redis/datasetDelete";
import { QueueJobs } from "../queues";
import { redis } from "../redis/redis";
import { randomUUID } from "crypto";
import { markAnalyticsDatasetDeletionOutboxPublished } from "../repositories/analyticsDatasetDeletionOperations";

type DatasetDeletionType = "dataset" | "dataset-runs";

type DatasetDeletionPayload = {
  deletionType: DatasetDeletionType;
  projectId: string;
  datasetId: string;
  datasetRunIds?: string[];
  analyticsDeletion?: {
    operationId: string;
    datasetGeneration: string | null;
    runGenerations: Readonly<Record<string, string>>;
  };
};

export const addToDeleteDatasetQueue = async ({
  deletionType,
  projectId,
  datasetId,
  datasetRunIds = [],
  analyticsDeletion,
}: DatasetDeletionPayload) => {
  if (redis) {
    const queue = DatasetDeleteQueue.getInstance();
    if (!queue) return false;
    await queue.add(
      QueueJobs.DatasetDelete,
      {
        payload: {
          deletionType,
          projectId,
          datasetId,
          datasetRunIds,
          analyticsDeletion,
        },
        id: randomUUID(),
        timestamp: new Date(),
        name: QueueJobs.DatasetDelete,
      },
      analyticsDeletion ? { jobId: analyticsDeletion.operationId } : undefined,
    );
    if (analyticsDeletion) {
      await markAnalyticsDatasetDeletionOutboxPublished({
        operationId: analyticsDeletion.operationId,
      });
    }
    return true;
  }
  return false;
};
