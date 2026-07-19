import {
  handoffLegacyAnalyticsIngestionOutbox,
  logger,
  recordIncrement,
  recoverStalePublishedAnalyticsIngestionOutbox,
} from "@langfuse/shared/src/server";

import { publishAnalyticsIngestionOutboxBatch } from "../../queues/analyticsIngestionQueue";
import { PeriodicRunner } from "../../utils/PeriodicRunner";

// Load and canonicalization leases last one minute. Recovery waits twice that
// long and also requires the operation itself to have made no progress.
const MIN_PUBLISHED_STALE_MS = 120_000;

export class AnalyticsIngestionOutboxRunner extends PeriodicRunner {
  protected readonly name = "AnalyticsIngestionOutboxRunner";

  constructor(
    private readonly dependencies: {
      readonly workerId: string;
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly publishBatch?: typeof publishAnalyticsIngestionOutboxBatch;
      readonly recoverStale?: typeof recoverStalePublishedAnalyticsIngestionOutbox;
      readonly handoffLegacy?: typeof handoffLegacyAnalyticsIngestionOutbox;
      readonly assertReady?: () => Promise<void>;
      readonly now?: () => Date;
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
    await this.dependencies.assertReady?.();
    const now = this.dependencies.now?.() ?? new Date();
    const handedOff = await (this.dependencies.handoffLegacy?.({
      now,
      limit: this.dependencies.batchSize,
    }) ?? Promise.resolve(0));
    const recovered = await (
      this.dependencies.recoverStale ??
      recoverStalePublishedAnalyticsIngestionOutbox
    )({
      now,
      updatedBefore: new Date(
        now.getTime() -
          Math.max(this.dependencies.intervalMs, MIN_PUBLISHED_STALE_MS),
      ),
      limit: this.dependencies.batchSize,
    });
    const published = await (
      this.dependencies.publishBatch ?? publishAnalyticsIngestionOutboxBatch
    )({
      workerId: this.dependencies.workerId,
      limit: this.dependencies.batchSize,
    });
    if (handedOff > 0 || recovered > 0 || published > 0) {
      logger.debug("Published Doris analytics ingestion outbox rows", {
        handedOff,
        recovered,
        published,
      });
    }
    if (handedOff > 0) {
      recordIncrement(
        "langfuse.analytics.ingestion.legacy_handoff",
        handedOff,
        { unit: "records" },
      );
    }
    return handedOff === this.dependencies.batchSize ||
      recovered === this.dependencies.batchSize ||
      published === this.dependencies.batchSize
      ? 0
      : undefined;
  }
}
