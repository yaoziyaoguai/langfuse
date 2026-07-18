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
  markAnalyticsIngestionRetrying,
  markAnalyticsIngestionTerminalFailure,
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
  ): Promise<unknown>;
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
    await queue.add(
      QueueJobs.AnalyticsIngestionJob,
      {
        timestamp: now,
        id: outbox.operationId,
        payload: {
          operationId: outbox.operationId,
          projectId: outbox.operation.projectId,
        },
        name: QueueJobs.AnalyticsIngestionJob,
      },
      { jobId: outbox.operationId },
    );
    const marked = await markPublished({
      client,
      operationId: outbox.operationId,
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
  readonly markRetrying?: typeof markAnalyticsIngestionRetrying;
  readonly markTerminalFailure?: typeof markAnalyticsIngestionTerminalFailure;
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
    if (operation.terminalAt) {
      if (
        operation.status === "VISIBLE" ||
        operation.status === "CANCELLED_BY_DELETION" ||
        operation.status === "COMPLETED_WITH_CANCELLATIONS"
      ) {
        return;
      }
      throw new AnalyticsPersistenceError(
        operation.status === "UNRECOVERABLE"
          ? "ANALYTICS_UNRECOVERABLE"
          : "ANALYTICS_QUARANTINED",
        false,
        { tags: { operationId: operation.id, phase: "queue_terminal" } },
      );
    }

    try {
      const batch = await input.canonicalize(operation);
      await input.sink.persist(batch);
      recordIncrement("langfuse.analytics.ingestion.queue", 1, {
        status: "terminal",
      });
    } catch (error) {
      if (!(error instanceof AnalyticsPersistenceError)) throw error;
      if (!error.retryable) {
        await (
          input.markTerminalFailure ?? markAnalyticsIngestionTerminalFailure
        )({
          client: input.client ?? prisma,
          operationId: operation.id,
          projectId: operation.projectId,
          status:
            error.code === "ANALYTICS_QUARANTINED" ||
            error.code === "ANALYTICS_CONFLICT"
              ? "QUARANTINED"
              : "UNRECOVERABLE",
          reasonCode: error.code,
        });
        throw new UnrecoverableError(error.message);
      }
      const attempts = job.opts.attempts ?? 1;
      const exhausted = job.attemptsMade + 1 >= attempts;
      await (input.markRetrying ?? markAnalyticsIngestionRetrying)({
        client: input.client ?? prisma,
        operationId: operation.id,
        projectId: operation.projectId,
        reasonCode: exhausted ? "MAX_RETRIES_EXHAUSTED" : error.code,
      });
      recordIncrement("langfuse.analytics.ingestion.queue", 1, {
        status: exhausted ? "retry_exhausted" : "retrying",
      });
      throw error;
    }
  };
}
