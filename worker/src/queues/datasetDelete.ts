import { Job, Processor } from "bullmq";
import { QueueName, TQueueJobTypes } from "@langfuse/shared/src/server";
import { processDatasetDelete } from "../features/datasets/processDatasetDelete";

export const datasetDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.DatasetDelete]>,
): Promise<void> => {
  await processDatasetDelete(job.data.payload);
};
