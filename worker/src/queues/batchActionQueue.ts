import { Job } from "bullmq";
import { traceException, logger } from "@langfuse/shared/src/server";
import { QueueName, TQueueJobTypes } from "@langfuse/shared/src/server";
import { handleBatchActionJob } from "../features/batchAction/handleBatchActionJob";
import { handleClickhouseBatchActionJob } from "../features/batchAction/handleClickhouseBatchActionJob";
import { isAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { env } from "../env";
import { prisma } from "@langfuse/shared/src/db";
import { getWorkerAnalyticsAdmissionContext } from "../analyticsRuntime";
import { withAnalyticsDurableWorkFence } from "../features/analytics-deletion/analyticsDeletionWorkFence";
import { validateManagedScoreDeletionReference } from "../features/scores/managedScoreDeletionReference";

export const batchActionQueueProcessor = async (
  job: Job<TQueueJobTypes[QueueName.BatchActionQueue]>,
) => {
  try {
    logger.info(
      `Executing Batch Action job ${JSON.stringify(job.data.payload.actionId)}`,
    );
    const selectedBackend = isAnalyticsBackend(
      env.LANGFUSE_ANALYTICS_BACKEND,
      "doris",
    )
      ? ("doris" as const)
      : ("clickhouse" as const);
    const processJob = () =>
      selectedBackend === "doris"
        ? handleBatchActionJob(job.data)
        : handleClickhouseBatchActionJob(job.data);
    if (job.data.payload.actionId === "score-delete") {
      const { deletionOperationId, deletionGeneration, analyticsProvenance } =
        job.data.payload;
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
        claimKind: "batch-score-delete",
        resourceIdentity,
        run: processJob,
      });
    } else {
      await processJob();
    }
    logger.info(
      `Finished Batch Action Job ${JSON.stringify(job.data.payload.actionId)}`,
    );

    return true;
  } catch (e) {
    logger.error(`Failed Batch Action job for id ${job.id}`, e);
    traceException(e);
    throw e;
  }
};
