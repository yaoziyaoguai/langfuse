import type { AnalyticsDeletionOperation } from "@prisma/client";
import { logger } from "@langfuse/shared/src/server";

import { PeriodicRunner } from "../../utils/PeriodicRunner";

// Project deletion jobs intentionally wait one minute; recovery only owns work
// that has stayed untouched beyond the normal queue's scheduling window.
const MIN_RECOVERY_STALE_MS = 120_000;

type FindRecoverableOperations = (input: {
  readonly updatedBefore: Date;
  readonly leaseExpiredBefore: Date;
  readonly limit: number;
}) => Promise<readonly AnalyticsDeletionOperation[]>;

export class AnalyticsDeletionRecoveryRunner extends PeriodicRunner {
  protected readonly name = "AnalyticsDeletionRecoveryRunner";

  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly batchSize: number;
      readonly findRecoverableOperations: FindRecoverableOperations;
      readonly processOperation: (
        operation: AnalyticsDeletionOperation,
      ) => Promise<void>;
      readonly assertReady?: () => Promise<void>;
      readonly now?: () => Date;
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
      throw new TypeError("Invalid analytics deletion recovery configuration");
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
    const operations = await this.dependencies.findRecoverableOperations({
      updatedBefore: new Date(
        now.getTime() -
          Math.max(this.dependencies.intervalMs, MIN_RECOVERY_STALE_MS),
      ),
      leaseExpiredBefore: now,
      limit: this.dependencies.batchSize,
    });
    let failures = 0;
    for (const operation of operations) {
      try {
        await this.dependencies.processOperation(operation);
      } catch (error) {
        failures += 1;
        logger.warn("Doris analytics deletion recovery will retry", {
          deletionOperationId: operation.id,
          projectId: operation.projectId,
          scope: operation.scope,
          error,
        });
      }
    }
    if (operations.length > 0) {
      logger.info("Processed Doris analytics deletion recovery batch", {
        operations: operations.length,
        failures,
      });
    }
    return operations.length === this.dependencies.batchSize && failures === 0
      ? 0
      : undefined;
  }
}
