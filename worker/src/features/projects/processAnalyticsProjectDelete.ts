import type { AnalyticsDeletionOperation } from "@prisma/client";
import { randomUUID } from "node:crypto";
import { prisma } from "@langfuse/shared/src/db";
import {
  acquireAnalyticsMutationPermit,
  finalizeProjectDeletionOperation,
  claimDeletionOperation,
  deleteDatasetRunItemsByProjectId,
  deleteEventsByProjectId,
  deleteMediaFiles,
  deleteMediaLinkRowsByProjectId,
  deleteObservationsByProjectId,
  deleteScoresByProjectId,
  deleteTracesByProjectId,
  findAllMediaByProjectId,
  findDeletionOperationForProject,
  getS3MediaStorageClient,
  hasPreBarrierIngestionWork,
  isDorisAnalyticsBackend,
  logger,
  markDeletionBarrierVisible,
  markDeletionOperationRetrying,
  markDeletionOperationPhase,
  removeIngestionEventsFromS3AndDeleteClickhouseRefsForProject,
  renewDeletionOperationLease,
  type AnalyticsRuntimeAdmissionContext,
  type SerializedAnalyticsDurableProvenance,
} from "@langfuse/shared/src/server";

import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";
import { env, v4WritesToEventsTable } from "../../env";
import { withAnalyticsDeletionWorkFence } from "../analytics-deletion/analyticsDeletionWorkFence";
import {
  getDorisAnalyticsLifecycleRuntime,
  type DorisAnalyticsLifecycleRuntime,
} from "../../services/dorisAnalyticsLifecycle";

export type ProjectDeletionReference = {
  readonly operationId: string;
  readonly generation: bigint;
  readonly analyticsProvenance?: SerializedAnalyticsDurableProvenance;
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

async function deleteClickhouseProjectAnalytics(
  projectId: string,
): Promise<void> {
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
  lifecycle?: DorisAnalyticsLifecycleRuntime,
  admissionContext: AnalyticsRuntimeAdmissionContext | null = getWorkerAnalyticsAdmissionContext(),
): Promise<void> {
  const currentOperation = await operationFor(input);
  const selectedBackend = isDorisAnalyticsBackend()
    ? ("doris" as const)
    : ("clickhouse" as const);
  await withAnalyticsDeletionWorkFence({
    client: prisma,
    operation: currentOperation,
    serializedProvenance: input.reference.analyticsProvenance,
    admissionContext,
    selectedBackend,
    claimKind: "analytics-deletion-operation",
    run: () =>
      currentOperation.status === "COMPLETED"
        ? Promise.resolve()
        : processFencedAnalyticsProjectDelete({
            ...input,
            lifecycle:
              currentOperation.analyticsBackend === "DORIS"
                ? (lifecycle ?? getDorisAnalyticsLifecycleRuntime())
                : lifecycle,
          }),
  });
}

async function processFencedAnalyticsProjectDelete(input: {
  readonly projectId: string;
  readonly organizationId: string;
  readonly reference: ProjectDeletionReference;
  readonly lifecycle?: DorisAnalyticsLifecycleRuntime;
}): Promise<void> {
  const { lifecycle } = input;
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
    const mutationPermit = await acquireAnalyticsMutationPermit({
      mutation: {
        kind: "deletion",
        checkpointGeneration: operation.checkpointGeneration,
        createdAt: operation.createdAt,
      },
    });
    if (mutationPermit.outcome === "held") {
      throw new Error("Project deletion is held by the analytics checkpoint");
    }
    if (!operation.logicallyInvisible) {
      const barrier =
        operation.analyticsBackend !== "DORIS"
          ? await deleteClickhouseProjectAnalytics(input.projectId).then(
              () => ({
                visible: true,
                barrierLabel: `clickhouse-deletion-${operation.id}`,
              }),
            )
          : await (
              lifecycle ?? getDorisAnalyticsLifecycleRuntime()
            ).store.publishProjectTombstone({
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
        barrierAcceptanceSequence: operation.ingestionBarrierSequence,
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
    if (operation.analyticsBackend === "DORIS") {
      await (
        lifecycle ?? getDorisAnalyticsLifecycleRuntime()
      ).materializedDeletion.deleteHeads(operation.id, heads);
    }
    await deleteProjectMedia(input.projectId);

    const completed = await finalizeProjectDeletionOperation({
      projectOperationId: operation.id,
      projectId: input.projectId,
      organizationId: input.organizationId,
      projectGeneration: input.reference.generation,
      lease,
    });
    if (!completed) {
      throw new Error("Project deletion could not finalize atomically");
    }
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
    logger.warn("Analytics project deletion will retry", {
      projectId: input.projectId,
      organizationId: input.organizationId,
      deletionOperationId: operation.id,
      phase: current.phase,
    });
    throw error;
  }
}
