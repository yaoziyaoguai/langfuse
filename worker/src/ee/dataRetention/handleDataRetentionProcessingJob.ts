import {
  deleteEventsOlderThanDays,
  deleteMediaFiles,
  deleteObservationsOlderThanDays,
  deleteScoresOlderThanDays,
  deleteTracesOlderThanDays,
  DataRetentionProcessingQueue,
  findExpiredMediaByProjectId,
  getS3MediaStorageClient,
  isDorisAnalyticsBackend,
  logger,
  QueueJobs,
  removeIngestionEventsFromS3AndDeleteClickhouseRefsForProject,
  getCurrentSpan,
} from "@langfuse/shared/src/server";
import { Job } from "bullmq";
import { prisma } from "@langfuse/shared/src/db";
import { randomUUID } from "node:crypto";
import { env, v4WritesToEventsTable } from "../../env";
import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";
import { deleteDorisProjectRetentionHeads } from "./deleteDorisProjectRetentionHeads";
import { processDorisProjectRetention } from "./processDorisProjectRetention";

export const handleDataRetentionProcessingJob = async (job: Job) => {
  const { projectId, retention } = job.data.payload;

  const span = getCurrentSpan();
  if (span) {
    span.setAttribute("messaging.bullmq.job.input.jobId", job.data.id);
    span.setAttribute("messaging.bullmq.job.input.projectId", projectId);
  }

  // CRITICAL FIX: Re-fetch current retention setting from database
  // This prevents stale queued jobs from deleting data after retention is disabled
  const project = await prisma.project.findUnique({
    where: { id: projectId },
    select: { retentionDays: true },
  });

  if (isDorisAnalyticsBackend()) {
    return processDorisProjectRetention(
      {
        projectId,
        queuedRetentionDays: retention,
        admissionContext: getWorkerAnalyticsAdmissionContext(),
      },
      {
        client: prisma,
        deleteDorisHeads: deleteDorisProjectRetentionHeads,
        onCutoffPublished: async ({
          projectId: retainedProjectId,
          cutoffDate,
        }) => {
          if (!env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET) return;
          const mediaFilesToDelete = await findExpiredMediaByProjectId({
            projectId: retainedProjectId,
            cutoffDate,
          });
          await deleteMediaFiles({
            projectId: retainedProjectId,
            mediaFiles: mediaFilesToDelete,
            storageClient: getS3MediaStorageClient(
              env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET,
            ),
          });
        },
        scheduleContinuation: async ({
          projectId: retainedProjectId,
          retentionDays,
          delayMs,
        }) => {
          const queue = DataRetentionProcessingQueue.getInstance();
          if (!queue) {
            throw new Error(
              "DataRetentionProcessingQueue not initialized for Doris continuation",
            );
          }
          await queue.add(
            QueueJobs.DataRetentionProcessingJob,
            {
              id: randomUUID(),
              name: QueueJobs.DataRetentionProcessingJob,
              timestamp: new Date(),
              payload: {
                projectId: retainedProjectId,
                retention: retentionDays,
              },
            },
            { delay: delayMs },
          );
        },
      },
    );
  }

  // Skip if project no longer exists, has no retention, or retention is set to 0 (indefinite)
  if (!project || !project.retentionDays || project.retentionDays === 0) {
    logger.info(
      `[Data Retention] Skipping project ${projectId} - retention disabled or set to 0`,
    );
    return;
  }

  // Use the CURRENT retention value from database, not the queued value
  const currentRetention = project.retentionDays;

  if (span) {
    span.setAttribute(
      "messaging.bullmq.job.input.retentionId",
      currentRetention,
    );
  }

  // Log if retention value changed since job was queued
  if (currentRetention !== retention) {
    logger.warn(
      `[Data Retention] Retention changed for project ${projectId}: queued=${retention} days, current=${currentRetention} days. Using current value.`,
    );
  }

  const cutoffDate = new Date(
    Date.now() - currentRetention * 24 * 60 * 60 * 1000,
  );

  // Delete media files if bucket is configured
  if (env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET) {
    logger.info(
      `[Data Retention] Deleting media files older than ${currentRetention} days for project ${projectId}`,
    );
    const mediaFilesToDelete = await findExpiredMediaByProjectId({
      projectId,
      cutoffDate,
    });
    const deletedMediaCount = await deleteMediaFiles({
      projectId,
      mediaFiles: mediaFilesToDelete,
      storageClient: getS3MediaStorageClient(
        env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET,
      ),
    });
    logger.info(
      `[Data Retention] Deleted ${deletedMediaCount} media files for project ${projectId}`,
    );
  }

  // Delete ClickHouse (TTL / Delete Queries)
  logger.info(
    `[Data Retention] Deleting ClickHouse and S3 data older than ${currentRetention} days for project ${projectId}`,
  );
  await Promise.all([
    env.LANGFUSE_ENABLE_BLOB_STORAGE_FILE_LOG === "true"
      ? removeIngestionEventsFromS3AndDeleteClickhouseRefsForProject(
          projectId,
          cutoffDate,
        )
      : Promise.resolve(),
    deleteTracesOlderThanDays(projectId, cutoffDate),
    deleteObservationsOlderThanDays(projectId, cutoffDate),
    deleteScoresOlderThanDays(projectId, cutoffDate),
    v4WritesToEventsTable(env)
      ? deleteEventsOlderThanDays(projectId, cutoffDate)
      : Promise.resolve(),
  ]);
  logger.info(
    `[Data Retention] Deleted ClickHouse and S3 data older than ${currentRetention} days for project ${projectId}`,
  );

  // Set S3 Lifecycle for deletion (Future)
};
