import { Job, Processor } from "bullmq";
import {
  QueueName,
  shouldSkipDeletionFor,
  TQueueJobTypes,
} from "@langfuse/shared/src/server";

import { processClickhouseScoreDelete } from "../features/scores/processClickhouseScoreDelete";
import { processAnalyticsScoreDelete } from "../features/scores/processAnalyticsScoreDelete";
import { isAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { env } from "../env";

export const scoreDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.ScoreDelete]>,
): Promise<void> => {
  const { scoreIds, projectId } = job.data.payload;

  if (await shouldSkipDeletionFor(projectId, scoreIds, "score")) {
    return;
  }

  if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "doris")) {
    await processAnalyticsScoreDelete(projectId, scoreIds);
  } else {
    await processClickhouseScoreDelete(projectId, scoreIds);
  }
};
