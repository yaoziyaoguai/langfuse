import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  deferExperimentExecutionDispatch,
  ExperimentCreateQueue,
  ExperimentExecutionProvenanceError,
  findPendingExperimentExecutionDispatches,
  logger,
  publishExperimentExecutionDispatch,
  quarantineExperimentExecutionDispatch,
  QueueJobs,
  QueueName,
  type AnalyticsRuntimeAdmissionContext,
  type ExperimentCreateEventType,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";

import { PeriodicRunner } from "../../utils/PeriodicRunner";

type ExperimentQueueProducer = {
  add(
    name: QueueName.ExperimentCreate,
    data: TQueueJobTypes[QueueName.ExperimentCreate],
    options: { readonly jobId: string },
  ): Promise<{
    getState(): Promise<string>;
    retry(state: "failed"): Promise<void>;
  }>;
};

export async function publishExperimentExecutionDispatchBatch(input: {
  readonly client?: PrismaClient;
  readonly queue?: ExperimentQueueProducer | null;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly now?: Date;
  readonly limit?: number;
  readonly findPending?: typeof findPendingExperimentExecutionDispatches;
  readonly publishDispatch?: typeof publishExperimentExecutionDispatch;
  readonly quarantineDispatch?: typeof quarantineExperimentExecutionDispatch;
  readonly deferDispatch?: typeof deferExperimentExecutionDispatch;
}): Promise<number> {
  const client = input.client ?? prisma;
  const queue = input.queue ?? ExperimentCreateQueue.getInstance();
  if (!queue) throw new Error("Experiment dispatch queue is unavailable");
  if (!input.admissionContext) {
    throw new Error("Experiment dispatch recovery runtime is not admitted");
  }
  const now = input.now ?? new Date();
  const pending = await (
    input.findPending ?? findPendingExperimentExecutionDispatches
  )({ client, now, limit: input.limit ?? 50 });
  let published = 0;

  for (const candidate of pending) {
    try {
      const didPublish = await (
        input.publishDispatch ?? publishExperimentExecutionDispatch
      )({
        client,
        admissionContext: input.admissionContext,
        action: "recovery",
        projectId: candidate.projectId,
        runId: candidate.runId,
        expectedGeneration: candidate.generation,
        publish: async (payload: ExperimentCreateEventType) => {
          const jobId = `${candidate.runId}-g${candidate.generation}`;
          const delivery = await queue.add(
            QueueName.ExperimentCreate,
            {
              id: jobId,
              name: QueueJobs.ExperimentCreateJob,
              timestamp: now,
              payload,
              retryBaggage: {
                originalJobTimestamp: now,
                attempt: 0,
              },
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
      if (error instanceof ExperimentExecutionProvenanceError) {
        await (
          input.quarantineDispatch ?? quarantineExperimentExecutionDispatch
        )({
          client,
          projectId: candidate.projectId,
          runId: candidate.runId,
          expectedGeneration: candidate.generation,
          failureCode: "EXPERIMENT_DISPATCH_PROVENANCE_MISMATCH",
        });
        logger.error("Quarantined invalid Doris experiment dispatch", {
          projectId: candidate.projectId,
          runId: candidate.runId,
        });
      } else {
        await (input.deferDispatch ?? deferExperimentExecutionDispatch)({
          client,
          projectId: candidate.projectId,
          runId: candidate.runId,
          expectedGeneration: candidate.generation,
        });
        logger.error("Deferred failed Doris experiment dispatch", {
          projectId: candidate.projectId,
          runId: candidate.runId,
          errorKind: error instanceof Error ? error.name : "UnknownError",
        });
      }
    }
  }
  return published;
}

export class ExperimentExecutionDispatchRunner extends PeriodicRunner {
  protected readonly name = "ExperimentExecutionDispatchRunner";

  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
      readonly publishBatch?: typeof publishExperimentExecutionDispatchBatch;
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
      throw new TypeError(
        "Invalid experiment execution dispatch runner configuration",
      );
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
      this.dependencies.publishBatch ?? publishExperimentExecutionDispatchBatch
    )({
      admissionContext: this.dependencies.getAdmissionContext(),
      limit: this.dependencies.batchSize,
    });
    if (published > 0) {
      logger.debug("Published Doris experiment dispatch outbox rows", {
        published,
      });
    }
    return published === this.dependencies.batchSize ? 0 : undefined;
  }
}
