import { logger, recordIncrement } from "@langfuse/shared/src/server";

import { PeriodicRunner } from "../../utils/PeriodicRunner";

type ReconciliationResult = {
  readonly scanned: number;
  readonly recovered: number;
  readonly existing: number;
  readonly invalid: number;
  readonly nextCursor?: string;
};

export class AnalyticsIngestionRawReconciler extends PeriodicRunner {
  protected readonly name = "AnalyticsIngestionRawReconciler";
  private cursor: string | undefined;

  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly assertReady: () => Promise<void>;
      readonly reconcile: (
        limit: number,
        cursor?: string,
      ) => Promise<ReconciliationResult>;
    },
  ) {
    super();
    if (
      !Number.isSafeInteger(dependencies.intervalMs) ||
      dependencies.intervalMs < 1_000 ||
      !Number.isSafeInteger(dependencies.batchSize) ||
      dependencies.batchSize < 1 ||
      dependencies.batchSize > 1_000
    ) {
      throw new TypeError("Invalid analytics raw reconciler configuration");
    }
  }

  protected get defaultIntervalMs(): number {
    return this.dependencies.intervalMs;
  }

  public processBatch(): Promise<number | void> {
    return this.execute();
  }

  protected async execute(): Promise<number | void> {
    await this.dependencies.assertReady();
    const result = await this.dependencies.reconcile(
      this.dependencies.batchSize,
      this.cursor,
    );
    this.cursor = result.nextCursor;
    if (result.recovered > 0 || result.invalid > 0) {
      logger.info("Reconciled raw Doris analytics ingestion objects", {
        scanned: result.scanned,
        recovered: result.recovered,
        existing: result.existing,
        invalid: result.invalid,
      });
    }
    if (result.recovered > 0) {
      recordIncrement(
        "langfuse.analytics.ingestion.raw_reconciliation",
        result.recovered,
        { status: "recovered" },
      );
    }
    if (result.invalid > 0) {
      recordIncrement(
        "langfuse.analytics.ingestion.raw_reconciliation",
        result.invalid,
        { status: "invalid" },
      );
    }
    return result.nextCursor !== undefined && result.invalid === 0
      ? 0
      : undefined;
  }
}
