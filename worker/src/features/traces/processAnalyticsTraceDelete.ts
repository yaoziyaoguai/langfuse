import type { AnalyticsDeletionOperation } from "@prisma/client";
import { randomUUID } from "node:crypto";
import { prisma } from "@langfuse/shared/src/db";
import {
  acquireAnalyticsMutationPermit,
  completeDeletionOperation,
  claimDeletionOperation,
  findDeletionOperationForProject,
  hasPreBarrierIngestionWork,
  logger,
  markDeletionBarrierVisible,
  markDeletionOperationRetrying,
  markDeletionOperationPhase,
  renewDeletionOperationLease,
} from "@langfuse/shared/src/server";

import {
  getDorisAnalyticsLifecycleRuntime,
  type DorisAnalyticsLifecycleRuntime,
} from "../../services/dorisAnalyticsLifecycle";
import { deleteMediaItemsForTraces } from "./deleteTraceMedia";
import { processPostgresTraceDelete } from "./processPostgresTraceDelete";

export type TraceDeletionReference = {
  readonly operationId: string;
  readonly traceId: string;
  readonly generation: bigint;
};

async function operationFor(
  projectId: string,
  reference: TraceDeletionReference,
): Promise<AnalyticsDeletionOperation> {
  const operation = await findDeletionOperationForProject({
    projectId,
    operationId: reference.operationId,
  });
  if (
    !operation ||
    operation.scope !== "TRACE" ||
    operation.traceId !== reference.traceId ||
    operation.generation !== reference.generation
  ) {
    throw new Error("Trace deletion operation is invalid");
  }
  return operation;
}

/**
 * Makes a trace invisible first, then converges known Doris keys and attributable
 * Postgres/media state. Raw multi-trace ingestion objects remain lifecycle-owned.
 */
export async function processAnalyticsTraceDelete(
  projectId: string,
  reference: TraceDeletionReference,
  lifecycle: DorisAnalyticsLifecycleRuntime = getDorisAnalyticsLifecycleRuntime(),
): Promise<void> {
  const currentOperation = await operationFor(projectId, reference);
  if (currentOperation.status === "COMPLETED") return;
  const owner = randomUUID();
  const operation = await claimDeletionOperation({
    operationId: reference.operationId,
    projectId,
    owner,
  });
  if (!operation) throw new Error("Trace deletion operation is already leased");
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
      throw new Error("Trace deletion is held by the analytics checkpoint");
    }
    if (!operation.logicallyInvisible) {
      const barrier = await lifecycle.store.publishTraceTombstone({
        operationId: operation.id,
        projectId,
        traceId: reference.traceId,
        generation: reference.generation,
        createdAt: operation.createdAt,
      });
      if (!barrier.visible) {
        await markDeletionOperationRetrying({
          operationId: operation.id,
          projectId,
          phase: "visibility_barrier",
          reasonCode: "BARRIER_NOT_VISIBLE",
          logicallyInvisible: false,
          lease,
        });
        throw new Error("Trace deletion barrier is not visible");
      }
      const recorded = await markDeletionBarrierVisible({
        operationId: operation.id,
        projectId,
        scope: "TRACE",
        traceId: reference.traceId,
        generation: reference.generation,
        barrierLabel: barrier.barrierLabel,
        lease,
      });
      if (!recorded) throw new Error("Trace deletion barrier lost its fence");
    }

    if (
      await hasPreBarrierIngestionWork({
        projectId,
        barrierCreatedAt: operation.createdAt,
      })
    ) {
      await markDeletionOperationRetrying({
        operationId: operation.id,
        projectId,
        phase: "ingestion_drain",
        reasonCode: "PRE_BARRIER_INGESTION_ACTIVE",
        logicallyInvisible: true,
        lease,
      });
      throw new Error("Trace deletion is waiting for ingestion drain");
    }

    const renewed = await renewDeletionOperationLease({
      operationId: operation.id,
      projectId,
      lease,
    });
    if (!renewed) throw new Error("Trace deletion lost its worker lease");
    const phaseRecorded = await markDeletionOperationPhase({
      operationId: operation.id,
      projectId,
      phase: "materialized_cleanup",
      lease,
    });
    if (!phaseRecorded) throw new Error("Trace deletion lost its worker fence");

    const heads = await prisma.analyticsEntityHead.findMany({
      where: { projectId, owningTraceId: reference.traceId },
    });
    await lifecycle.materializedDeletion.deleteHeads(operation.id, heads);
    await deleteMediaItemsForTraces(projectId, [reference.traceId]);
    await processPostgresTraceDelete(projectId, [reference.traceId]);
    await prisma.traceControlState.deleteMany({
      where: { projectId, traceId: reference.traceId },
    });
    const completed = await completeDeletionOperation({
      operationId: operation.id,
      projectId,
      scope: "TRACE",
      traceId: reference.traceId,
      generation: reference.generation,
      lease,
    });
    if (!completed) throw new Error("Trace deletion completion lost its fence");
  } catch (error) {
    const current = await operationFor(projectId, reference);
    if (current.status !== "COMPLETED") {
      await markDeletionOperationRetrying({
        operationId: operation.id,
        projectId,
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
    logger.warn("Doris trace deletion will retry", {
      projectId,
      deletionOperationId: operation.id,
      phase: current.phase,
    });
    throw error;
  }
}
