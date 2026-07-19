import { logger } from "@langfuse/shared/src/server";

import { publishAnalyticsIngestionOutboxBatch } from "../../queues/analyticsIngestionQueue";
import { PeriodicRunner } from "../../utils/PeriodicRunner";

export class AnalyticsIngestionOutboxRunner extends PeriodicRunner {
  protected readonly name = "AnalyticsIngestionOutboxRunner";

  constructor(
    private readonly dependencies: {
      readonly workerId: string;
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly publishBatch?: typeof publishAnalyticsIngestionOutboxBatch;
    },
  ) {
    super();
    if (
      !Number.isSafeInteger(dependencies.intervalMs) ||
      dependencies.intervalMs < 100 ||
      !Number.isSafeInteger(dependencies.batchSize) ||
      dependencies.batchSize < 1 ||
      dependencies.batchSize > 1_000
    ) {
      throw new TypeError("Invalid analytics outbox runner configuration");
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
      this.dependencies.publishBatch ?? publishAnalyticsIngestionOutboxBatch
    )({
      workerId: this.dependencies.workerId,
      limit: this.dependencies.batchSize,
    });
    if (published > 0) {
      logger.debug("Published Doris analytics ingestion outbox rows", {
        published,
      });
    }
    return published === this.dependencies.batchSize ? 0 : undefined;
  }
}
