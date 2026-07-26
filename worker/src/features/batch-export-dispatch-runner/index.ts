import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  BatchExportQueue,
  BatchExportProvenanceError,
  deferBatchExportDispatch,
  findPendingBatchExportDispatchIds,
  logger,
  publishBatchExportDispatch,
  quarantineBatchExportDispatch,
  QueueJobs,
  QueueName,
  type AnalyticsRuntimeAdmissionContext,
  type BatchExportJobType,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";

import { PeriodicRunner } from "../../utils/PeriodicRunner";

type BatchExportQueueProducer = {
  add(
    name: QueueJobs.BatchExportJob,
    data: TQueueJobTypes[QueueName.BatchExport],
    options: { readonly jobId: string },
  ): Promise<{
    getState(): Promise<string>;
    retry(state: "failed"): Promise<void>;
  }>;
};

type FindPending = typeof findPendingBatchExportDispatchIds;
type PublishDispatch = typeof publishBatchExportDispatch;

export async function publishBatchExportDispatchBatch(input: {
  readonly client?: PrismaClient;
  readonly queue?: BatchExportQueueProducer | null;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly now?: Date;
  readonly limit?: number;
  readonly findPending?: FindPending;
  readonly publishDispatch?: PublishDispatch;
  readonly quarantineDispatch?: typeof quarantineBatchExportDispatch;
  readonly recordFailure?: typeof deferBatchExportDispatch;
}): Promise<number> {
  const client = input.client ?? prisma;
  const queue = input.queue ?? BatchExportQueue.getInstance();
  if (!queue) throw new Error("Batch export dispatch queue is unavailable");
  if (!input.admissionContext) {
    throw new Error("Batch export recovery runtime is not admitted");
  }
  const now = input.now ?? new Date();
  const pending = await (
    input.findPending ?? findPendingBatchExportDispatchIds
  )({ client, now, limit: input.limit ?? 50 });
  let published = 0;

  for (const candidate of pending) {
    try {
      const didPublish = await (
        input.publishDispatch ?? publishBatchExportDispatch
      )({
        client,
        admissionContext: input.admissionContext,
        action: "recovery",
        batchExportId: candidate.batchExportId,
        expectedGeneration: candidate.generation,
        publish: async (payload: BatchExportJobType) => {
          const jobId = `${candidate.batchExportId}-g${candidate.generation}`;
          const delivery = await queue.add(
            QueueJobs.BatchExportJob,
            {
              id: jobId,
              name: QueueJobs.BatchExportJob,
              timestamp: now,
              payload,
            },
            { jobId },
          );
          if ((await delivery.getState()) === "failed") {
            await delivery.retry("failed");
          }
        },
      });
      if (didPublish) published += 1;
    } catch (error) {
      if (error instanceof BatchExportProvenanceError) {
        await (input.quarantineDispatch ?? quarantineBatchExportDispatch)({
          client,
          batchExportId: candidate.batchExportId,
          failureCode: "BATCH_EXPORT_DISPATCH_PROVENANCE_MISMATCH",
          log: error.message,
        });
        logger.error("Quarantined invalid Doris batch export dispatch", {
          batchExportId: candidate.batchExportId,
          error,
        });
      } else {
        await (input.recordFailure ?? deferBatchExportDispatch)({
          client,
          batchExportId: candidate.batchExportId,
          expectedGeneration: candidate.generation,
        });
        logger.error("Deferred failed Doris batch export dispatch", {
          batchExportId: candidate.batchExportId,
          error,
        });
      }
    }
  }
  return published;
}

export class BatchExportDispatchRunner extends PeriodicRunner {
  protected readonly name = "BatchExportDispatchRunner";

  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
      readonly publishBatch?: typeof publishBatchExportDispatchBatch;
    },
  ) {
    super();
    if (
      !Number.isSafeInteger(dependencies.intervalMs) ||
      dependencies.intervalMs < 100 ||
      !Number.isSafeInteger(dependencies.batchSize) ||
      dependencies.batchSize < 1 ||
      dependencies.batchSize > 100
    ) {
      throw new TypeError("Invalid batch export dispatch runner configuration");
    }
  }

  protected get defaultIntervalMs(): number {
    return this.dependencies.intervalMs;
  }

  public processBatch(): Promise<number | void> {
    return this.execute();
  }

  protected async execute(): Promise<number | void> {
    const published = await (
      this.dependencies.publishBatch ?? publishBatchExportDispatchBatch
    )({
      admissionContext: this.dependencies.getAdmissionContext(),
      limit: this.dependencies.batchSize,
    });
    if (published > 0) {
      logger.debug("Published Doris batch export dispatch outbox rows", {
        published,
      });
    }
    return published === this.dependencies.batchSize ? 0 : undefined;
  }
}
