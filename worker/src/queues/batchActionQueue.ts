import type { Job, Processor } from "bullmq";
import { QueueName, type TQueueJobTypes } from "@langfuse/shared/src/server";

import { handleBatchActionJob } from "../features/batchAction/handleBatchActionJob";

export const batchActionQueueProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.BatchActionQueue]>,
): Promise<void> => {
  await handleBatchActionJob(job.data);
};
