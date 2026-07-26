import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  AnalyticsEvaluationDispatchProvenanceError,
  AnalyticsEvaluationDispatchQueue,
  deferAnalyticsEvaluationDispatch,
  findPendingAnalyticsEvaluationDispatches,
  logger,
  publishAnalyticsEvaluationDispatch,
  quarantineAnalyticsEvaluationDispatch,
  QueueJobs,
  type AnalyticsEvaluationDispatchEventType,
  type AnalyticsRuntimeAdmissionContext,
  type TQueueJobTypes,
  QueueName,
} from "@langfuse/shared/src/server";

import { PeriodicRunner } from "../../utils/PeriodicRunner";

type EvaluationDispatchQueueProducer = {
  add(
    name: QueueJobs.AnalyticsEvaluationDispatch,
    data: TQueueJobTypes[QueueName.AnalyticsEvaluationDispatch],
    options: { readonly jobId: string },
  ): Promise<{
    getState(): Promise<string>;
    retry(state: "failed"): Promise<void>;
  }>;
};

export async function publishAnalyticsEvaluationDispatchBatch(input: {
  readonly client?: PrismaClient;
  readonly queue?: EvaluationDispatchQueueProducer | null;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly now?: Date;
  readonly limit?: number;
  readonly findPending?: typeof findPendingAnalyticsEvaluationDispatches;
  readonly publishDispatch?: typeof publishAnalyticsEvaluationDispatch;
  readonly quarantineDispatch?: typeof quarantineAnalyticsEvaluationDispatch;
  readonly deferDispatch?: typeof deferAnalyticsEvaluationDispatch;
}): Promise<number> {
  const client = input.client ?? prisma;
  const queue = input.queue ?? AnalyticsEvaluationDispatchQueue.getInstance();
  if (!queue) throw new Error("Evaluation dispatch queue is unavailable");
  if (!input.admissionContext) {
    throw new Error("Evaluation dispatch recovery runtime is not admitted");
  }
  const now = input.now ?? new Date();
  const pending = await (
    input.findPending ?? findPendingAnalyticsEvaluationDispatches
  )({ client, now, limit: input.limit ?? 50 });
  let published = 0;

  for (const candidate of pending) {
    try {
      const didPublish = await (
        input.publishDispatch ?? publishAnalyticsEvaluationDispatch
      )({
        client,
        admissionContext: input.admissionContext,
        dispatchId: candidate.id,
        expectedGeneration: candidate.dispatchGeneration,
        publish: async (payload: AnalyticsEvaluationDispatchEventType) => {
          const jobId = `${candidate.id}-g${candidate.dispatchGeneration}`;
          const delivery = await queue.add(
            QueueJobs.AnalyticsEvaluationDispatch,
            {
              id: jobId,
              name: QueueJobs.AnalyticsEvaluationDispatch,
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
      if (error instanceof AnalyticsEvaluationDispatchProvenanceError) {
        await (
          input.quarantineDispatch ?? quarantineAnalyticsEvaluationDispatch
        )({
          client,
          dispatchId: candidate.id,
          expectedGeneration: candidate.dispatchGeneration,
          failureCode: "EVALUATION_DISPATCH_PROVENANCE_MISMATCH",
        });
        logger.error("Quarantined invalid Doris evaluation dispatch", {
          dispatchId: candidate.id,
          errorCode: "EVALUATION_DISPATCH_PROVENANCE_MISMATCH",
        });
      } else {
        await (input.deferDispatch ?? deferAnalyticsEvaluationDispatch)({
          client,
          dispatchId: candidate.id,
          expectedGeneration: candidate.dispatchGeneration,
        });
        logger.error("Deferred failed Doris evaluation dispatch", {
          dispatchId: candidate.id,
          errorKind: error instanceof Error ? error.name : "UnknownError",
        });
      }
    }
  }
  return published;
}

export class AnalyticsEvaluationDispatchRunner extends PeriodicRunner {
  protected readonly name = "AnalyticsEvaluationDispatchRunner";

  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
      readonly publishBatch?: typeof publishAnalyticsEvaluationDispatchBatch;
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
      throw new TypeError("Invalid evaluation dispatch runner configuration");
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
      this.dependencies.publishBatch ?? publishAnalyticsEvaluationDispatchBatch
    )({
      admissionContext: this.dependencies.getAdmissionContext(),
      limit: this.dependencies.batchSize,
    });
    if (published > 0) {
      logger.debug("Published Doris evaluation dispatch rows", { published });
    }
    return published === this.dependencies.batchSize ? 0 : undefined;
  }
}
