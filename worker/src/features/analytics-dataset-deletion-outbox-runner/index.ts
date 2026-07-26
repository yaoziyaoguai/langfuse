import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  addToDeleteDatasetQueue,
  analyticsDatasetDeletionQueueReference,
  decodeAnalyticsDatasetRunGenerations,
  deferAnalyticsDatasetDeletionOutbox,
  listPendingAnalyticsDatasetDeletionOutbox,
  logger,
} from "@langfuse/shared/src/server";

import { PeriodicRunner } from "../../utils/PeriodicRunner";

export async function publishAnalyticsDatasetDeletionOutboxBatch(input: {
  readonly client?: PrismaClient;
  readonly now?: Date;
  readonly limit?: number;
  readonly publish?: typeof addToDeleteDatasetQueue;
}): Promise<number> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const pending = await listPendingAnalyticsDatasetDeletionOutbox({
    client,
    now,
    limit: input.limit,
  });
  let published = 0;
  for (const row of pending) {
    const operation = row.operation;
    const scheduled = {
      operation,
      datasetGeneration: operation.datasetGeneration,
      runGenerations: decodeAnalyticsDatasetRunGenerations(
        operation.runGenerations,
      ),
    };
    try {
      const didPublish = await (input.publish ?? addToDeleteDatasetQueue)({
        deletionType:
          operation.scope === "DATASET" ? "dataset" : "dataset-runs",
        projectId: operation.projectId,
        datasetId: operation.datasetId,
        datasetRunIds: operation.datasetRunIds,
        analyticsDeletion: analyticsDatasetDeletionQueueReference(scheduled),
      });
      if (!didPublish) throw new Error("Dataset deletion queue is unavailable");
      published += 1;
    } catch (error) {
      await deferAnalyticsDatasetDeletionOutbox({
        client,
        operationId: operation.id,
        now,
      });
      logger.error("Deferred failed analytics dataset deletion dispatch", {
        operationId: operation.id,
        error,
      });
    }
  }
  return published;
}

export class AnalyticsDatasetDeletionOutboxRunner extends PeriodicRunner {
  protected readonly name = "AnalyticsDatasetDeletionOutboxRunner";

  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly publishBatch?: typeof publishAnalyticsDatasetDeletionOutboxBatch;
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
        "Invalid analytics dataset deletion outbox runner configuration",
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
      publishAnalyticsDatasetDeletionOutboxBatch
    )({ limit: this.dependencies.batchSize });
    return published === this.dependencies.batchSize ? 0 : undefined;
  }
}
