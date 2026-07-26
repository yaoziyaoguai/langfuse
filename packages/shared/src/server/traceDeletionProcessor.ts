import { randomUUID } from "crypto";
import { prisma } from "../db";
import { TraceDeleteQueue } from "./redis/traceDelete";
import { QueueJobs } from "./queues";
import { logger } from "./logger";
import { env } from "../env";
import { shouldSkipDeletionFor } from "./deletionGuard";
import {
  scheduleTraceDeletionOperations,
  type AnalyticsDeletionRequester,
} from "./repositories/analyticsDeletionOperations";
import { isDorisAnalyticsBackend } from "./repositories/telemetry/doris/runtime";
import {
  analyticsDurableProvenanceFromRecord,
  serializeAnalyticsDurableProvenance,
  type AnalyticsRuntimeAdmissionContext,
} from "./analytics-persistence";

export interface TraceDeletionProcessorOptions {
  delayMs?: number; // Default from LANGFUSE_TRACE_DELETE_DELAY_MS env var
  organizationId?: string;
  requester?: AnalyticsDeletionRequester;
  analyticsAdmissionContext?: AnalyticsRuntimeAdmissionContext | null;
}

export type TraceDeletionDispatch = {
  readonly deletionOperationId: string;
  readonly traceId: string;
  readonly status: "scheduled" | "retrying" | "needs_attention" | "completed";
  readonly logicallyInvisible: boolean;
};

/**
 * Efficient trace deletion processor that batches deletions for better performance.
 *
 * This function:
 * 1. Creates a record in the pending_deletions table for each trace
 * 2. Sends a deletion event to the queue with a configurable delay
 * 3. The worker will batch delete all pending traces from ClickHouse
 * 4. Sets the is_deleted flag to true after successful deletion
 *
 * @param projectId - The project ID
 * @param traceIds - Array of trace IDs to delete
 * @param options - Configuration options including delay
 */
export async function traceDeletionProcessor(
  projectId: string,
  traceIds: string[],
  options: TraceDeletionProcessorOptions = {},
): Promise<readonly TraceDeletionDispatch[]> {
  const { delayMs = env.LANGFUSE_TRACE_DELETE_DELAY_MS } = options;

  if (traceIds.length === 0) {
    logger.warn("traceDeletionProcessor called with empty traceIds array", {
      projectId,
    });
    return [];
  }

  logger.info(
    `Processing trace deletion for ${traceIds.length} traces in project ${projectId}`,
    {
      projectId,
      traceIds,
      delayMs,
    },
  );

  if (await shouldSkipDeletionFor(projectId, traceIds, "trace")) {
    return []; // Early return - don't create pending_deletions or queue job
  }

  try {
    const isDoris = isDorisAnalyticsBackend();
    if (isDoris && !options.analyticsAdmissionContext) {
      throw new Error(
        "Doris analytics deletion requires managed runtime admission",
      );
    }
    const scheduled = isDoris
      ? await (async () => {
          const project = await prisma.project.findUniqueOrThrow({
            where: { id: projectId },
            select: { orgId: true },
          });
          return scheduleTraceDeletionOperations({
            projectId,
            organizationId: options.organizationId ?? project.orgId,
            traceIds,
            requester: options.requester ?? {
              principalType: "system",
              principalId: "trace-deletion-processor",
            },
            analyticsAdmissionContext: options.analyticsAdmissionContext,
          });
        })()
      : [];

    // Create pending deletion records for all traces
    await prisma.pendingDeletion.createMany({
      data: traceIds.map((traceId) => ({
        projectId,
        object: "trace",
        objectId: traceId,
        isDeleted: false,
      })),
      skipDuplicates: true, // Avoid conflicts if trace is already pending deletion
    });

    // Get the trace delete queue
    const traceDeleteQueue = TraceDeleteQueue.getInstance();
    if (!traceDeleteQueue) {
      throw new Error("TraceDeleteQueue not available");
    }

    // Send deletion event with delay
    await traceDeleteQueue.add(
      QueueJobs.TraceDelete,
      {
        timestamp: new Date(),
        id: randomUUID(),
        name: QueueJobs.TraceDelete,
        payload: {
          projectId,
          traceIds,
          ...(scheduled.length > 0
            ? {
                deletionOperations: scheduled.map(
                  ({ operation, traceId, generation }) => {
                    const provenance =
                      analyticsDurableProvenanceFromRecord(operation);
                    return {
                      operationId: operation.id,
                      traceId,
                      generation: generation.toString(),
                      ...(provenance
                        ? {
                            analyticsProvenance:
                              serializeAnalyticsDurableProvenance(provenance),
                          }
                        : {}),
                    };
                  },
                ),
              }
            : {}),
        },
      },
      {
        delay: delayMs,
      },
    );
    return isDoris
      ? scheduled.map(({ operation, traceId }) => ({
          deletionOperationId: operation.id,
          traceId,
          status:
            operation.status.toLowerCase() as TraceDeletionDispatch["status"],
          logicallyInvisible: operation.logicallyInvisible,
        }))
      : [];
  } catch (error) {
    logger.error(`Failed to process trace deletion for project ${projectId}`, {
      projectId,
      traceIds,
      error,
    });
    throw error;
  }
}
