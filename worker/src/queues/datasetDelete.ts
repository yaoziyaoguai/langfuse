import { Job, Processor } from "bullmq";
import { QueueName, TQueueJobTypes } from "@langfuse/shared/src/server";
import { processClickhouseDatasetDelete } from "../features/datasets/processClickhouseDatasetDelete";
import { processDatasetDelete } from "../features/datasets/processDatasetDelete";
import { isAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { env } from "../env";

export const datasetDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.DatasetDelete]>,
): Promise<void> => {
  if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "doris")) {
    await processDatasetDelete(job.data.payload);
  } else {
    await processClickhouseDatasetDelete(job.data.payload);
  }
};
