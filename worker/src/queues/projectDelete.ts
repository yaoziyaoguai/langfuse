import { Job, Processor, UnrecoverableError } from "bullmq";
import {
  deleteEventsByProjectId,
  deleteMediaLinkRowsByProjectId,
  deleteMediaFiles,
  deleteObservationsByProjectId,
  deleteScoresByProjectId,
  deleteTracesByProjectId,
  deleteDatasetRunItemsByProjectId,
  findAllMediaByProjectId,
  getCurrentSpan,
  getS3MediaStorageClient,
  logger,
  isDorisAnalyticsBackend,
  ProjectQueueEventSchema,
  QueueName,
  removeIngestionEventsFromS3AndDeleteClickhouseRefsForProject,
  TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";
import { Prisma } from "@prisma/client";
import { env, v4WritesToEventsTable } from "../env";
import { processAnalyticsProjectDelete } from "../features/projects/processAnalyticsProjectDelete";
import { withAnalyticsDeletionWorkFence } from "../features/analytics-deletion/analyticsDeletionWorkFence";
import { getWorkerAnalyticsAdmissionContext } from "../analyticsRuntime";

async function processLegacyProjectDelete(input: {
  readonly projectId: string;
  readonly orgId: string;
}): Promise<void> {
  const { projectId, orgId } = input;
  await deleteMediaLinkRowsByProjectId({ projectId });

  if (env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET) {
    logger.info(`Deleting media for ${projectId} in org ${orgId}`);
    const mediaFilesToDelete = await findAllMediaByProjectId({ projectId });
    await deleteMediaFiles({
      projectId,
      mediaFiles: mediaFilesToDelete,
      storageClient: getS3MediaStorageClient(
        env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET,
      ),
    });
  }

  logger.info(
    `Deleting ClickHouse and S3 data for ${projectId} in org ${orgId}`,
  );
  await Promise.all([
    env.LANGFUSE_ENABLE_BLOB_STORAGE_FILE_LOG === "true"
      ? removeIngestionEventsFromS3AndDeleteClickhouseRefsForProject(
          projectId,
          undefined,
        )
      : Promise.resolve(),
    deleteTracesByProjectId(projectId),
    deleteObservationsByProjectId(projectId),
    deleteScoresByProjectId(projectId),
    v4WritesToEventsTable(env)
      ? deleteEventsByProjectId(projectId)
      : Promise.resolve(),
  ]);
  await deleteDatasetRunItemsByProjectId(projectId);

  logger.info(`Deleting PG data for project ${projectId} in org ${orgId}`);
  try {
    const existingProject = await prisma.project.findUnique({
      where: { id: projectId, orgId },
    });
    if (!existingProject) {
      logger.info(
        `Tried to delete project ${projectId} from PG, but it does not exist anymore.`,
      );
      return;
    }
    await prisma.project.delete({ where: { id: projectId, orgId } });
  } catch (error) {
    logger.error(
      `Error deleting project ${projectId} in org ${orgId}: ${error}`,
      { stack: error instanceof Error ? error.stack : undefined },
    );
    if (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      (error.code === "P2025" || error.code === "P2016")
    ) {
      logger.warn(
        `Tried to delete project ${projectId} in org ${orgId}, but it does not exist`,
      );
      return;
    }
    throw error;
  }

  logger.info(`Deleted ${projectId} in org ${orgId}`);
}

export const projectDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.ProjectDelete]>,
): Promise<void> => {
  const payload = ProjectQueueEventSchema.parse(job.data.payload);
  const { orgId, projectId } = payload;

  const span = getCurrentSpan();
  if (span) {
    span.setAttribute("messaging.bullmq.job.input.id", job.data.id);
    span.setAttribute(
      "messaging.bullmq.job.input.projectId",
      job.data.payload.projectId,
    );
    span.setAttribute(
      "messaging.bullmq.job.input.orgId",
      job.data.payload.orgId,
    );
  }

  logger.info(`Deleting ${projectId} in org ${orgId}`);

  const hasDurableReference =
    payload.deletionOperationId !== undefined ||
    payload.deletionGeneration !== undefined ||
    payload.analyticsProvenance !== undefined;
  const selectedBackend = isDorisAnalyticsBackend()
    ? ("doris" as const)
    : ("clickhouse" as const);
  if (selectedBackend === "clickhouse" && hasDurableReference) {
    throw new UnrecoverableError(
      "ClickHouse project deletion must use the legacy queue contract",
    );
  }
  if (selectedBackend === "doris" && !hasDurableReference) {
    throw new UnrecoverableError(
      "Doris project deletion requires a durable operation",
    );
  }
  if (hasDurableReference) {
    if (!payload.deletionOperationId || !payload.deletionGeneration) {
      throw new Error("Project deletion queue reference is incomplete");
    }
    await processAnalyticsProjectDelete({
      projectId,
      organizationId: orgId,
      reference: {
        operationId: payload.deletionOperationId,
        generation: BigInt(payload.deletionGeneration),
        analyticsProvenance: payload.analyticsProvenance,
      },
    });
    return;
  }

  await withAnalyticsDeletionWorkFence({
    client: prisma,
    operation: null,
    serializedProvenance: undefined,
    admissionContext: getWorkerAnalyticsAdmissionContext(),
    selectedBackend,
    claimKind: "analytics-deletion-operation",
    run: () => processLegacyProjectDelete({ projectId, orgId }),
  });
};
