import {
  clearExpiredAnalyticsIntegrationScratchLease,
  isAnalyticsIntegrationScratchLeaseLive,
  logger,
} from "@langfuse/shared/src/server";

import { ParquetScratchManager } from "../blobstorage/ParquetScratchManager";
import { PeriodicRunner } from "../../utils/PeriodicRunner";
import { WORKER_HOST_ID } from "../../utils/hostId";

export class AnalyticsIntegrationScratchCleaner extends PeriodicRunner {
  protected readonly name = "AnalyticsIntegrationScratchCleaner";
  private readonly manager: ParquetScratchManager;

  constructor(
    private readonly intervalMs: number,
    manager?: ParquetScratchManager,
  ) {
    super();
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1_000) {
      throw new TypeError(
        "Invalid analytics integration scratch cleanup interval",
      );
    }
    this.manager = manager ?? new ParquetScratchManager();
  }

  protected get defaultIntervalMs(): number {
    return this.intervalMs;
  }

  public processBatch(): Promise<number | void> {
    return this.execute();
  }

  protected async execute(): Promise<number | void> {
    const now = new Date();
    const removed = await this.manager.cleanupOrphans({
      isLive: (executionId, relativeDirectory) =>
        isAnalyticsIntegrationScratchLeaseLive({
          executionId,
          hostId: WORKER_HOST_ID,
          relativePath: relativeDirectory,
          now,
        }),
      onRemove: async (executionId, relativeDirectory) => {
        await clearExpiredAnalyticsIntegrationScratchLease({
          executionId,
          hostId: WORKER_HOST_ID,
          relativePath: relativeDirectory,
          now,
        });
      },
    });
    if (removed > 0) {
      logger.info("Removed orphaned Doris Parquet scratch directories", {
        removed,
      });
    }
    return undefined;
  }
}
