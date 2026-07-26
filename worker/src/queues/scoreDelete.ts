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
import { prisma } from "@langfuse/shared/src/db";
import { getWorkerAnalyticsAdmissionContext } from "../analyticsRuntime";
import { withAnalyticsDurableWorkFence } from "../features/analytics-deletion/analyticsDeletionWorkFence";
import { validateManagedScoreDeletionReference } from "../features/scores/managedScoreDeletionReference";

export const scoreDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.ScoreDelete]>,
): Promise<void> => {
  const {
    scoreIds,
    projectId,
    analyticsProvenance,
    deletionOperationId,
    deletionGeneration,
  } = job.data.payload;
  const selectedBackend = isAnalyticsBackend(
    env.LANGFUSE_ANALYTICS_BACKEND,
    "doris",
  )
    ? ("doris" as const)
    : ("clickhouse" as const);
  const resourceIdentity = String(job.id ?? job.data.id);
  const serializedProvenance = validateManagedScoreDeletionReference(
    { deletionOperationId, deletionGeneration, analyticsProvenance },
    resourceIdentity,
  );

  await withAnalyticsDurableWorkFence({
    client: prisma,
    serializedProvenance,
    admissionContext: getWorkerAnalyticsAdmissionContext(),
    selectedBackend,
    claimKind: "score-delete",
    resourceIdentity,
    run: async () => {
      if (await shouldSkipDeletionFor(projectId, scoreIds, "score")) return;
      if (selectedBackend === "doris") {
        await processAnalyticsScoreDelete(projectId, scoreIds);
      } else {
        await processClickhouseScoreDelete(projectId, scoreIds);
      }
    },
  });
};
