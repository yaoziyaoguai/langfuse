import { randomUUID } from "node:crypto";

import type { Job } from "bullmq";
import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  DataRetentionProcessingQueue,
  deleteEventsOlderThanDays,
  deleteMediaFiles,
  deleteObservationsOlderThanDays,
  deleteScoresOlderThanDays,
  deleteTracesOlderThanDays,
  findExpiredMediaByProjectId,
  getCurrentSpan,
  getS3MediaStorageClient,
  isDorisAnalyticsBackend,
  logger,
  QueueJobs,
  removeIngestionEventsFromS3AndDeleteClickhouseRefsForProject,
} from "@langfuse/shared/src/server";

import { getWorkerAnalyticsAdmissionContext } from "../../../analyticsRuntime";
import { env, v4WritesToEventsTable } from "../../../env";
import { processDorisProjectRetention } from "./dorisProjectRetention";

type RetentionPayload = {
  projectId: string;
  retention: number;
};

type ProcessingDependencies = {
  client?: PrismaClient;
  isDoris?: () => boolean;
  processDoris?: typeof processDorisProjectRetention;
  deleteDorisHeads?: NonNullable<
    Parameters<typeof processDorisProjectRetention>[1]
  >["deleteDorisHeads"];
  deleteMedia?: (projectId: string, cutoffDate: Date) => Promise<void>;
  deleteClickHouseData?: (projectId: string, cutoffDate: Date) => Promise<void>;
  now?: () => number;
};

function parsePayload(job: Job): RetentionPayload {
  const payload = job.data?.payload as Partial<RetentionPayload> | undefined;
  if (
    !payload ||
    typeof payload.projectId !== "string" ||
    !payload.projectId ||
    !Number.isSafeInteger(payload.retention) ||
    Number(payload.retention) < 3
  ) {
    throw new TypeError("Invalid Community data retention job");
  }
  return {
    projectId: payload.projectId,
    retention: Number(payload.retention),
  };
}

export async function processCommunityDataRetentionJob(
  job: Job,
  dependencies: ProcessingDependencies = {},
) {
  const { projectId, retention } = parsePayload(job);
  const span = getCurrentSpan();
  span?.setAttribute("messaging.bullmq.job.input.jobId", job.data.id);
  span?.setAttribute("messaging.bullmq.job.input.projectId", projectId);

  const client = dependencies.client ?? prisma;
  const isDoris = dependencies.isDoris ?? isDorisAnalyticsBackend;
  if (isDoris()) {
    return (dependencies.processDoris ?? processDorisProjectRetention)(
      {
        projectId,
        queuedRetentionDays: retention,
        admissionContext: getWorkerAnalyticsAdmissionContext(),
      },
      {
        client,
        deleteDorisHeads: dependencies.deleteDorisHeads,
        onCutoffPublished: async ({ cutoffDate }) => {
          await (dependencies.deleteMedia ?? deleteExpiredMedia)(
            projectId,
            cutoffDate,
          );
        },
        scheduleContinuation: async ({ retentionDays, delayMs }) => {
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
              payload: { projectId, retention: retentionDays },
            },
            { delay: delayMs },
          );
        },
      },
    );
  }

  const project = await client.project.findUnique({
    where: { id: projectId },
    select: { retentionDays: true },
  });
  if (!project?.retentionDays) {
    logger.info(
      `[Community Data Retention] Skipping project ${projectId}; retention is disabled`,
    );
    return { outcome: "idle" as const };
  }

  const retentionDays = project.retentionDays;
  if (!Number.isSafeInteger(retentionDays) || retentionDays < 3) {
    throw new TypeError("Invalid project retention period");
  }
  span?.setAttribute("messaging.bullmq.job.input.retentionId", retentionDays);
  if (retentionDays !== retention) {
    logger.warn(
      `[Community Data Retention] Project ${projectId} changed from ${retention} to ${retentionDays} days; using the current value`,
    );
  }

  const cutoffDate = new Date(
    (dependencies.now ?? Date.now)() - retentionDays * 24 * 60 * 60 * 1000,
  );
  await (dependencies.deleteMedia ?? deleteExpiredMedia)(projectId, cutoffDate);
  await (dependencies.deleteClickHouseData ?? deleteClickHouseData)(
    projectId,
    cutoffDate,
  );
  return { outcome: "completed" as const, cutoffDate };
}

async function deleteClickHouseData(
  projectId: string,
  cutoffDate: Date,
): Promise<void> {
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
}

async function deleteExpiredMedia(
  projectId: string,
  cutoffDate: Date,
): Promise<void> {
  if (!env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET) return;
  const mediaFiles = await findExpiredMediaByProjectId({
    projectId,
    cutoffDate,
  });
  await deleteMediaFiles({
    projectId,
    mediaFiles,
    storageClient: getS3MediaStorageClient(env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET),
  });
}
