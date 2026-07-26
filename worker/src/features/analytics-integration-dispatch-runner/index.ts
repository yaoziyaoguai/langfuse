import type { PrismaClient } from "@prisma/client";
import type { Queue } from "bullmq";
import { randomUUID } from "node:crypto";

import { prisma } from "@langfuse/shared/src/db";
import {
  AnalyticsIntegrationExecutionProvenanceError,
  BlobStorageIntegrationProcessingQueue,
  deferAnalyticsIntegrationExecutionPublication,
  findPublishableAnalyticsIntegrationExecutions,
  logger,
  MixpanelIntegrationProcessingQueue,
  PostHogIntegrationProcessingQueue,
  publishAnalyticsIntegrationExecution,
  quarantineAnalyticsIntegrationExecution,
  QueueJobs,
  recoverExpiredAnalyticsIntegrationExecutions,
  type AnalyticsIntegrationExecutionEnvelope,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";

import { PeriodicRunner } from "../../utils/PeriodicRunner";

function destination(envelope: AnalyticsIntegrationExecutionEnvelope): {
  readonly queue: Queue | null;
  readonly jobName:
    | typeof QueueJobs.PostHogIntegrationProcessingJob
    | typeof QueueJobs.MixpanelIntegrationProcessingJob
    | typeof QueueJobs.BlobStorageIntegrationProcessingJob;
} {
  switch (envelope.integrationType) {
    case "POSTHOG":
      return {
        queue: PostHogIntegrationProcessingQueue.getInstance(),
        jobName: QueueJobs.PostHogIntegrationProcessingJob,
      };
    case "MIXPANEL":
      return {
        queue: MixpanelIntegrationProcessingQueue.getInstance(),
        jobName: QueueJobs.MixpanelIntegrationProcessingJob,
      };
    case "BLOB_STORAGE":
      return {
        queue: BlobStorageIntegrationProcessingQueue.getInstance(),
        jobName: QueueJobs.BlobStorageIntegrationProcessingJob,
      };
  }
}

export async function publishAnalyticsIntegrationExecutionBatch(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly now?: Date;
  readonly limit?: number;
  readonly queueFor?: typeof destination;
}): Promise<number> {
  if (!input.admissionContext) {
    throw new Error("Analytics integration recovery runtime is not admitted");
  }
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  await recoverExpiredAnalyticsIntegrationExecutions({ client, now });
  const pending = await findPublishableAnalyticsIntegrationExecutions({
    client,
    now,
    limit: input.limit ?? 50,
  });
  let published = 0;
  for (const envelope of pending) {
    try {
      const target = (input.queueFor ?? destination)(envelope);
      if (!target.queue) {
        throw new Error(
          "Analytics integration processing queue is unavailable",
        );
      }
      const didPublish = await publishAnalyticsIntegrationExecution({
        client,
        admissionContext: input.admissionContext,
        envelope,
        queueJobId: envelope.executionId,
        now,
        publish: async (authoritativeEnvelope) => {
          const job = await target.queue!.add(
            target.jobName,
            {
              id: randomUUID(),
              name: target.jobName,
              timestamp: now,
              payload: authoritativeEnvelope,
            },
            { jobId: authoritativeEnvelope.executionId, removeOnFail: true },
          );
          if ((await job.getState()) === "failed") {
            await job.retry("failed");
          }
        },
      });
      if (didPublish) published += 1;
    } catch (error) {
      if (error instanceof AnalyticsIntegrationExecutionProvenanceError) {
        await quarantineAnalyticsIntegrationExecution({
          client,
          executionId: envelope.executionId,
          failureCode: "INTEGRATION_EXECUTION_PROVENANCE_MISMATCH",
          now,
        });
        logger.error("Quarantined invalid analytics integration execution", {
          executionId: envelope.executionId,
        });
      } else {
        await deferAnalyticsIntegrationExecutionPublication({
          client,
          executionId: envelope.executionId,
          failureCode: "INTEGRATION_QUEUE_PUBLICATION_FAILED",
          now,
        });
        logger.error("Deferred analytics integration publication", {
          executionId: envelope.executionId,
          errorKind: error instanceof Error ? error.name : "UnknownError",
        });
      }
    }
  }
  return published;
}

export class AnalyticsIntegrationDispatchRunner extends PeriodicRunner {
  protected readonly name = "AnalyticsIntegrationDispatchRunner";

  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
      readonly publishBatch?: typeof publishAnalyticsIntegrationExecutionBatch;
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
        "Invalid analytics integration dispatch runner configuration",
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
      this.dependencies.publishBatch ??
      publishAnalyticsIntegrationExecutionBatch
    )({
      admissionContext: this.dependencies.getAdmissionContext(),
      limit: this.dependencies.batchSize,
    });
    return published === this.dependencies.batchSize ? 0 : undefined;
  }
}
