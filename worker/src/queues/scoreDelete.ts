import { Job, Processor } from "bullmq";
import {
  QueueName,
  shouldSkipDeletionFor,
  TQueueJobTypes,
} from "@langfuse/shared/src/server";

import { processAnalyticsScoreDelete } from "../features/scores/processAnalyticsScoreDelete";

export const scoreDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.ScoreDelete]>,
): Promise<void> => {
  const { scoreIds, projectId } = job.data.payload;

  if (await shouldSkipDeletionFor(projectId, scoreIds, "score")) {
    return;
  }

  await processAnalyticsScoreDelete(projectId, scoreIds);
};
