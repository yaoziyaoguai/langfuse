import type { AnalyticsIngestionOperation, PrismaClient } from "@prisma/client";
import {
  UnrecoverableError,
  type Job,
  type JobsOptions,
  type Processor,
} from "bullmq";
import {
  AnalyticsIngestionQueue,
  AnalyticsIngestionQueueEventSchema,
  AnalyticsPersistenceError,
  claimAnalyticsIngestionOutbox,
  findAnalyticsIngestionOperationForProject,
  markAnalyticsIngestionOutboxPublished,
  markAnalyticsIngestionTerminalFailure,
  resolveAnalyticsIngestionAttemptFailure,
  logger,
  QueueJobs,
  QueueName,
  recordIncrement,
  type AnalyticsBatchSink,
  type CanonicalAnalyticsBatch,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

interface AnalyticsIngestionQueueProducer {
  add(
    name: QueueJobs.AnalyticsIngestionJob,
    data: TQueueJobTypes[QueueName.AnalyticsIngestionQueue],
    options: JobsOptions,
  ): Promise<{
    getState(): Promise<string>;
    retry(state: "failed"): Promise<void>;
  }>;
}

function recordAttemptResolution(
  resolution: "requeued" | "terminalized" | "unchanged",
): void {
  recordIncrement("langfuse.analytics.ingestion.queue", 1, {
    status:
      resolution === "requeued"
        ? "outbox_requeued"
        : resolution === "terminalized"
          ? "terminalized"
          : "stale_delivery",
  });
}

export async function publishAnalyticsIngestionOutboxBatch(input: {
  readonly client?: PrismaClient;
  readonly queue?: AnalyticsIngestionQueueProducer;
  readonly workerId: string;
  readonly now?: Date;
  readonly limit?: number;
  readonly lockMs?: number;
  readonly claimOutbox?: typeof claimAnalyticsIngestionOutbox;
  readonly markPublished?: typeof markAnalyticsIngestionOutboxPublished;
}): Promise<number> {
  const queue = input.queue ?? AnalyticsIngestionQueue.getInstance();
  if (!queue) {
    throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
      tags: { phase: "outbox_queue" },
    });
  }
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const claimOutbox = input.claimOutbox ?? claimAnalyticsIngestionOutbox;
  const markPublished =
    input.markPublished ?? markAnalyticsIngestionOutboxPublished;
  const claimed = await claimOutbox({
    client,
    workerId: input.workerId,
    now,
    lockedUntil: new Date(now.getTime() + (input.lockMs ?? 60_000)),
    limit: input.limit ?? 100,
  });

  let published = 0;
  for (const outbox of claimed) {
    const delivery = await queue.add(
      QueueJobs.AnalyticsIngestionJob,
      {
        timestamp: now,
        id: outbox.operationId,
        payload: {
          operationId: outbox.operationId,
          projectId: outbox.operation.projectId,
          generation: outbox.generation,
        },
        name: QueueJobs.AnalyticsIngestionJob,
      },
      {
        jobId: `${outbox.operationId}-g${outbox.generation}`,
        attempts: 1,
      },
    );
    if ((await delivery.getState()) === "failed") {
      await delivery.retry("failed");
    }
    const marked = await markPublished({
      client,
      operationId: outbox.operationId,
      generation: outbox.generation,
      workerId: input.workerId,
      now,
    });
    if (!marked) {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: {
          operationId: outbox.operationId,
          phase: "outbox_publish_fence",
        },
      });
    }
    published += 1;
    recordIncrement("langfuse.analytics.ingestion.outbox", 1, {
      status: "published",
    });
  }
  return published;
}

type CanonicalizationOperation = Pick<
  AnalyticsIngestionOperation,
  | "id"
  | "projectId"
  | "sourceOperationId"
  | "sourceChecksum"
  | "rawObjectKey"
  | "acceptedAtNanos"
  | "canonicalizerVersion"
  | "schemaVersion"
>;

export function analyticsIngestionQueueProcessorBuilder(input: {
  readonly sink: AnalyticsBatchSink;
  readonly canonicalize: (
    operation: CanonicalizationOperation,
  ) => Promise<CanonicalAnalyticsBatch>;
  readonly client?: PrismaClient;
  readonly findOperation?: typeof findAnalyticsIngestionOperationForProject;
  readonly markTerminalFailure?: typeof markAnalyticsIngestionTerminalFailure;
  readonly resolveAttemptFailure?: typeof resolveAnalyticsIngestionAttemptFailure;
  readonly reconcileUnresolved?: (input: {
    readonly operationId: string;
    readonly projectId: string;
  }) => Promise<boolean>;
  readonly assertReady?: () => Promise<void>;
  readonly withOperationLock?: (
    operation: CanonicalizationOperation,
    run: () => Promise<void>,
  ) => Promise<void>;
}): Processor<TQueueJobTypes[QueueName.AnalyticsIngestionQueue]> {
  return async (
    job: Job<TQueueJobTypes[QueueName.AnalyticsIngestionQueue]>,
  ): Promise<void> => {
    const payload = AnalyticsIngestionQueueEventSchema.parse(job.data.payload);
    const operation = await (
      input.findOperation ?? findAnalyticsIngestionOperationForProject
    )({
      client: input.client ?? prisma,
      operationId: payload.operationId,
      projectId: payload.projectId,
    });
    if (!operation) {
      throw new AnalyticsPersistenceError("ANALYTICS_NOT_FOUND", false);
    }
    const generation = payload.generation;
    if (operation.outboxV2?.generation !== generation) return;
    if (operation.terminalAt) {
      if (
        operation.status === "VISIBLE" ||
        operation.status === "CANCELLED_BY_DELETION" ||
        operation.status === "COMPLETED_WITH_CANCELLATIONS"
      ) {
        return;
      }
      const terminalError = new AnalyticsPersistenceError(
        operation.status === "UNRECOVERABLE"
          ? "ANALYTICS_UNRECOVERABLE"
          : "ANALYTICS_QUARANTINED",
        false,
        { tags: { operationId: operation.id, phase: "queue_terminal" } },
      );
      throw new UnrecoverableError(terminalError.message);
    }

    try {
      await input.assertReady?.();
      const run = async () => {
        if (
          await input.reconcileUnresolved?.({
            operationId: operation.id,
            projectId: operation.projectId,
          })
        ) {
          recordIncrement("langfuse.analytics.ingestion.queue", 1, {
            status: "terminal",
          });
          return;
        }
        const batch = await input.canonicalize(operation);
        await input.sink.persist(batch);
        recordIncrement("langfuse.analytics.ingestion.queue", 1, {
          status: "terminal",
        });
      };
      if (input.withOperationLock) {
        await input.withOperationLock(operation, run);
      } else {
        await run();
      }
    } catch (error) {
      const persistenceError =
        error instanceof AnalyticsPersistenceError
          ? error
          : new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
              tags: {
                operationId: operation.id,
                phase: "unexpected_worker_error",
              },
            });
      if (!(error instanceof AnalyticsPersistenceError)) {
        logger.error("Unexpected Doris analytics ingestion worker error", {
          operationId: operation.id,
          projectId: operation.projectId,
          error,
        });
      }
      if (!persistenceError.retryable) {
        const terminalized = await (
          input.markTerminalFailure ?? markAnalyticsIngestionTerminalFailure
        )({
          client: input.client ?? prisma,
          operationId: operation.id,
          projectId: operation.projectId,
          status:
            persistenceError.code === "ANALYTICS_QUARANTINED" ||
            persistenceError.code === "ANALYTICS_CONFLICT"
              ? "QUARANTINED"
              : "UNRECOVERABLE",
          reasonCode: persistenceError.code,
          expectedGeneration: generation,
        });
        if (!terminalized) {
          const resolution = await (
            input.resolveAttemptFailure ??
            resolveAnalyticsIngestionAttemptFailure
          )({
            client: input.client ?? prisma,
            operationId: operation.id,
            projectId: operation.projectId,
            reasonCode: persistenceError.code,
            expectedGeneration: generation,
          });
          recordAttemptResolution(resolution);
        }
        throw new UnrecoverableError(persistenceError.message);
      }
      const resolution = await (
        input.resolveAttemptFailure ?? resolveAnalyticsIngestionAttemptFailure
      )({
        client: input.client ?? prisma,
        operationId: operation.id,
        projectId: operation.projectId,
        reasonCode: persistenceError.code,
        expectedGeneration: generation,
      });
      recordAttemptResolution(resolution);
      throw new UnrecoverableError(persistenceError.message);
    }
  };
}
