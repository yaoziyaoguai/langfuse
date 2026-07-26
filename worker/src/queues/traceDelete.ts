import { Job, Processor, UnrecoverableError } from "bullmq";
import {
  getCurrentSpan,
  isDorisAnalyticsBackend,
  logger,
  QueueName,
  shouldSkipDeletionFor,
  TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import { processClickhouseTraceDelete } from "../features/traces/processClickhouseTraceDelete";
import { processPostgresTraceDelete } from "../features/traces/processPostgresTraceDelete";
import { processAnalyticsTraceDeletionBatch } from "../features/traces/processAnalyticsTraceDeletionBatch";
import { withAnalyticsDeletionWorkFence } from "../features/analytics-deletion/analyticsDeletionWorkFence";
import { getWorkerAnalyticsAdmissionContext } from "../analyticsRuntime";
import { env } from "../env";

export const traceDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.TraceDelete]>,
): Promise<void> => {
  const projectId = job.data.payload.projectId;
  const eventTraceIds =
    "traceIds" in job.data.payload
      ? job.data.payload.traceIds
      : [job.data.payload.traceId];

  const span = getCurrentSpan();

  const [toBeDeletedTraces, pendingEventTraceIds] = await Promise.all([
    prisma.pendingDeletion.findMany({
      where: {
        projectId,
        object: "trace",
        isDeleted: false,
      },
      select: {
        objectId: true,
      },
    }),
    prisma.pendingDeletion.findMany({
      where: {
        projectId,
        object: "trace",
        objectId: {
          in: eventTraceIds,
        },
      },
    }),
  ]);

  // TraceIds from the event body might be deleted already or do not exist in the pending_deletions table
  // as we go live with this feature with a full trace deletion queue. At the same time, we do not want to delete
  // twice, as we might have already deleted them in a previous job and want to spare CH resources.
  // -> Filter out traces that are already deleted
  // -> Keep traces that are not in the pending_deletions table at all.
  const toBeDeletedEventTraceIds = eventTraceIds.filter(
    (traceId) =>
      !pendingEventTraceIds.some((t) => t.objectId === traceId && t.isDeleted),
  );

  // Combine valid event traces with pending deletions
  const allTraceIds = Array.from(
    new Set([
      ...toBeDeletedTraces.map((t) => t.objectId),
      ...toBeDeletedEventTraceIds,
    ]),
  );

  const deletionOperations = job.data.payload.deletionOperations;
  const selectedBackend = isDorisAnalyticsBackend()
    ? ("doris" as const)
    : ("clickhouse" as const);
  if (selectedBackend === "clickhouse" && deletionOperations) {
    throw new UnrecoverableError(
      "ClickHouse trace deletion must use the legacy queue contract",
    );
  }
  if (selectedBackend === "doris" && !deletionOperations) {
    throw new UnrecoverableError(
      "Doris trace deletion requires a durable operation",
    );
  }
  if (allTraceIds.length === 0 && !deletionOperations) {
    logger.debug(`No traces to delete for project ${projectId}`);
    return;
  }

  logger.debug(
    `Batch deleting ${allTraceIds.length} traces for project ${projectId}`,
  );

  const deletionOperationsToDelete = deletionOperations?.slice(
    0,
    env.LANGFUSE_DELETE_BATCH_SIZE,
  );
  const traceIdsToDelete = deletionOperationsToDelete
    ? deletionOperationsToDelete.map(({ traceId }) => traceId)
    : allTraceIds.slice(0, env.LANGFUSE_DELETE_BATCH_SIZE);

  // Add all trace IDs to span attributes for observability
  if (span) {
    span.setAttribute(
      "messaging.bullmq.job.computed.totalTraceCount",
      traceIdsToDelete.length,
    );
    span.setAttribute(
      "messaging.bullmq.job.computed.eventTraceCount",
      eventTraceIds.length,
    );
    span.setAttribute(
      "messaging.bullmq.job.computed.pendingTraceCount",
      toBeDeletedTraces.length,
    );
  }

  try {
    if (await shouldSkipDeletionFor(projectId, traceIdsToDelete, "trace")) {
      return;
    }

    if (deletionOperationsToDelete) {
      await processAnalyticsTraceDeletionBatch({
        projectId,
        traceIds: traceIdsToDelete,
        deletionOperations: deletionOperationsToDelete,
      });
    } else {
      await withAnalyticsDeletionWorkFence({
        client: prisma,
        operation: null,
        serializedProvenance: undefined,
        admissionContext: getWorkerAnalyticsAdmissionContext(),
        selectedBackend,
        claimKind: "analytics-deletion-operation",
        run: () =>
          Promise.all([
            processPostgresTraceDelete(projectId, traceIdsToDelete),
            processClickhouseTraceDelete(projectId, traceIdsToDelete),
          ]).then(() => undefined),
      });
    }

    // Mark only the pending traces as deleted (not the ones from the event, as they might be legacy)
    if (toBeDeletedTraces.length > 0) {
      await prisma.pendingDeletion.updateMany({
        where: {
          projectId,
          object: "trace",
          objectId: {
            in: traceIdsToDelete,
          },
          isDeleted: false,
        },
        data: {
          isDeleted: true,
        },
      });
    }

    logger.debug(
      `Successfully batch deleted ${allTraceIds.length} traces and marked them as deleted in pending_deletions table`,
    );
  } catch (error) {
    logger.error(
      `Failed to batch delete traces for project ${projectId}:`,
      error,
    );
    throw error;
  }
};
