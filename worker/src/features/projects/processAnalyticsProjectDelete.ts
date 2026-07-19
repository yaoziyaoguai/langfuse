import type { AnalyticsDeletionOperation } from "@prisma/client";
import { randomUUID } from "node:crypto";
import { prisma } from "@langfuse/shared/src/db";
import {
  completeDeletionOperation,
  claimDeletionOperation,
  deleteMediaFiles,
  deleteMediaLinkRowsByProjectId,
  findAllMediaByProjectId,
  findDeletionOperationForProject,
  getS3MediaStorageClient,
  hasPreBarrierIngestionWork,
  logger,
  markDeletionBarrierVisible,
  markDeletionOperationRetrying,
  markDeletionOperationPhase,
  renewDeletionOperationLease,
} from "@langfuse/shared/src/server";

import { env } from "../../env";
import {
  getDorisAnalyticsLifecycleRuntime,
  type DorisAnalyticsLifecycleRuntime,
} from "../../services/dorisAnalyticsLifecycle";

export type ProjectDeletionReference = {
  readonly operationId: string;
  readonly generation: bigint;
};

async function operationFor(input: {
  readonly projectId: string;
  readonly organizationId: string;
  readonly reference: ProjectDeletionReference;
}): Promise<AnalyticsDeletionOperation> {
  const operation = await findDeletionOperationForProject({
    projectId: input.projectId,
    operationId: input.reference.operationId,
  });
  if (
    !operation ||
    operation.scope !== "PROJECT" ||
    operation.organizationId !== input.organizationId ||
    operation.traceId !== null ||
    operation.generation !== input.reference.generation
  ) {
    throw new Error("Project deletion operation is invalid");
  }
  return operation;
}

async function deleteProjectMedia(projectId: string): Promise<void> {
  await deleteMediaLinkRowsByProjectId({ projectId });
  if (!env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET) return;
  const mediaFiles = await findAllMediaByProjectId({ projectId });
  await deleteMediaFiles({
    projectId,
    mediaFiles,
    storageClient: getS3MediaStorageClient(env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET),
  });
}

/**
 * Keeps the independent operation/generation rows while removing the entire
 * Project foreign-key scope. Raw/canonical objects remain on their lifecycle.
 */
export async function processAnalyticsProjectDelete(
  input: {
    readonly projectId: string;
    readonly organizationId: string;
    readonly reference: ProjectDeletionReference;
  },
  lifecycle: DorisAnalyticsLifecycleRuntime = getDorisAnalyticsLifecycleRuntime(),
): Promise<void> {
  const currentOperation = await operationFor(input);
  if (currentOperation.status === "COMPLETED") return;
  const owner = randomUUID();
  const operation = await claimDeletionOperation({
    operationId: input.reference.operationId,
    projectId: input.projectId,
    owner,
  });
  if (!operation) {
    throw new Error("Project deletion operation is already leased");
  }
  const lease = { owner, fence: operation.workerFence };
  try {
    if (!operation.logicallyInvisible) {
      const barrier = await lifecycle.store.publishProjectTombstone({
        operationId: operation.id,
        projectId: input.projectId,
        generation: input.reference.generation,
        createdAt: operation.createdAt,
      });
      if (!barrier.visible) {
        await markDeletionOperationRetrying({
          operationId: operation.id,
          projectId: input.projectId,
          phase: "visibility_barrier",
          reasonCode: "BARRIER_NOT_VISIBLE",
          logicallyInvisible: false,
          lease,
        });
        throw new Error("Project deletion barrier is not visible");
      }
      const recorded = await markDeletionBarrierVisible({
        operationId: operation.id,
        projectId: input.projectId,
        scope: "PROJECT",
        generation: input.reference.generation,
        barrierLabel: barrier.barrierLabel,
        lease,
      });
      if (!recorded) throw new Error("Project deletion barrier lost its fence");
    }

    if (
      await hasPreBarrierIngestionWork({
        projectId: input.projectId,
        barrierCreatedAt: operation.createdAt,
      })
    ) {
      await markDeletionOperationRetrying({
        operationId: operation.id,
        projectId: input.projectId,
        phase: "ingestion_drain",
        reasonCode: "PRE_BARRIER_INGESTION_ACTIVE",
        logicallyInvisible: true,
        lease,
      });
      throw new Error("Project deletion is waiting for ingestion drain");
    }

    const renewed = await renewDeletionOperationLease({
      operationId: operation.id,
      projectId: input.projectId,
      lease,
    });
    if (!renewed) throw new Error("Project deletion lost its worker lease");
    const phaseRecorded = await markDeletionOperationPhase({
      operationId: operation.id,
      projectId: input.projectId,
      phase: "materialized_cleanup",
      lease,
    });
    if (!phaseRecorded) {
      throw new Error("Project deletion lost its worker fence");
    }

    const heads = await prisma.analyticsEntityHead.findMany({
      where: { projectId: input.projectId },
    });
    await lifecycle.materializedDeletion.deleteHeads(operation.id, heads);
    await deleteProjectMedia(input.projectId);

    await prisma.project.deleteMany({
      where: {
        id: input.projectId,
        orgId: input.organizationId,
      },
    });
    const completed = await completeDeletionOperation({
      operationId: operation.id,
      projectId: input.projectId,
      scope: "PROJECT",
      generation: input.reference.generation,
      lease,
    });
    if (!completed)
      throw new Error("Project deletion completion lost its fence");
  } catch (error) {
    const current = await operationFor(input);
    if (current.status !== "COMPLETED") {
      await markDeletionOperationRetrying({
        operationId: current.id,
        projectId: input.projectId,
        phase: current.logicallyInvisible
          ? current.phase === "ingestion_drain"
            ? "ingestion_drain"
            : "materialized_cleanup"
          : "visibility_barrier",
        reasonCode: current.logicallyInvisible
          ? "CLEANUP_RETRY"
          : "BARRIER_RETRY",
        logicallyInvisible: current.logicallyInvisible,
        lease,
      });
    }
    logger.warn("Doris project deletion will retry", {
      projectId: input.projectId,
      organizationId: input.organizationId,
      deletionOperationId: operation.id,
      phase: current.phase,
    });
    throw error;
  }
}
