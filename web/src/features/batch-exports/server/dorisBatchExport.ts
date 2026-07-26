import type {
  BatchExport,
  BatchExportDispatchOutbox,
  Prisma,
  PrismaClient,
} from "@langfuse/shared/src/db";
import {
  BatchExportQueue,
  createDorisBatchExportIntent,
  logger,
  publishBatchExportDispatch,
  QueueJobs,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";

import { CommunityCapabilityUnavailableError } from "@/src/features/capabilities/communityAvailability";

type ManagedBatchExport = BatchExport & {
  readonly dispatchOutbox: BatchExportDispatchOutbox;
};

export async function createAdmittedDorisBatchExport(input: {
  readonly client: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly projectId: string;
  readonly userId: string;
  readonly name: string;
  readonly format: string;
  readonly query: Prisma.InputJsonValue;
}): Promise<ManagedBatchExport> {
  const activation =
    await input.client.analyticsCapabilityActivation.findUnique({
      where: { capability: "CORE_BATCH_EXPORTS" },
    });
  if (
    !input.admissionContext ||
    !activation ||
    activation.status !== "ACTIVE" ||
    activation.backend !== "DORIS" ||
    activation.deploymentGeneration !==
      input.admissionContext.deploymentGeneration
  ) {
    throw new CommunityCapabilityUnavailableError("batchExports");
  }

  return createDorisBatchExportIntent({
    client: input.client,
    admissionContext: input.admissionContext,
    projectId: input.projectId,
    userId: input.userId,
    name: input.name,
    format: input.format,
    query: input.query,
  });
}

export async function dispatchDorisBatchExport(input: {
  readonly client: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly batchExport: ManagedBatchExport;
}): Promise<void> {
  try {
    await publishBatchExportDispatch({
      client: input.client,
      admissionContext: input.admissionContext,
      action: "externalProducer",
      batchExportId: input.batchExport.id,
      expectedGeneration: input.batchExport.dispatchOutbox.generation,
      publish: async (payload) => {
        const queue = BatchExportQueue.getInstance();
        if (!queue) throw new Error("Batch export queue is unavailable");
        const jobId = `${input.batchExport.id}-g${input.batchExport.dispatchOutbox.generation}`;
        const delivery = await queue.add(
          QueueJobs.BatchExportJob,
          {
            id: jobId,
            name: QueueJobs.BatchExportJob,
            timestamp: new Date(),
            payload,
          },
          { jobId },
        );
        if ((await delivery.getState()) === "failed") {
          await delivery.retry("failed");
        }
      },
    });
  } catch (error) {
    logger.warn(
      "[BATCH EXPORT] Immediate dispatch failed; durable recovery will retry",
      { batchExportId: input.batchExport.id, error },
    );
  }
}
