import type {
  AnalyticsEntityHead,
  AnalyticsEntityType,
  PrismaClient,
} from "@prisma/client";
import {
  advanceAnalyticsRetentionRun,
  completeAnalyticsRetentionRun,
  countUnresolvedAnalyticsLoadsBefore,
  deleteAnalyticsEntityHeadsForRetention,
  findAnalyticsEntityHeadsForRetention,
  logger,
  recordAnalyticsRetentionFailure,
  startOrResumeAnalyticsRetention,
  type ActiveAnalyticsRetentionRun,
  type AnalyticsRetentionPhase,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import { PeriodicExclusiveRunner } from "../../utils/PeriodicExclusiveRunner";
import { getDorisAnalyticsLifecycleRuntime } from "../../services/dorisAnalyticsLifecycle";

const NEXT_PHASE: Readonly<
  Partial<Record<AnalyticsRetentionPhase, AnalyticsRetentionPhase>>
> = {
  DRAIN: "EVENTS",
  EVENTS: "SCORES",
  SCORES: "BLOB_REFERENCES",
  BLOB_REFERENCES: "COMPLETE",
};

const PHASE_ENTITY_TYPE: Readonly<
  Partial<Record<AnalyticsRetentionPhase, AnalyticsEntityType>>
> = {
  EVENTS: "EVENT",
  SCORES: "SCORE",
  BLOB_REFERENCES: "FILE_REFERENCE",
};

function cutoffDate(now: Date, retentionDays: number): Date {
  if (
    !Number.isFinite(now.getTime()) ||
    !Number.isSafeInteger(retentionDays) ||
    retentionDays < 3
  ) {
    throw new TypeError("Invalid Doris global retention configuration");
  }
  return new Date(
    Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate() - retentionDays,
    ),
  );
}

export type DorisGlobalRetentionStepResult =
  | { readonly outcome: "idle" }
  | {
      readonly outcome: "waiting" | "processed" | "advanced" | "completed";
      readonly runId: string;
      readonly phase: AnalyticsRetentionPhase;
      readonly cutoffDate: Date;
    };

type RetentionDependencies = {
  readonly client?: PrismaClient;
  readonly startOrResume?: typeof startOrResumeAnalyticsRetention;
  readonly advance?: typeof advanceAnalyticsRetentionRun;
  readonly complete?: typeof completeAnalyticsRetentionRun;
  readonly countUnresolvedLoads?: typeof countUnresolvedAnalyticsLoadsBefore;
  readonly recordFailure?: typeof recordAnalyticsRetentionFailure;
  readonly findHeads?: typeof findAnalyticsEntityHeadsForRetention;
  readonly deleteHeads?: typeof deleteAnalyticsEntityHeadsForRetention;
  readonly deleteDorisHeads?: (
    operationId: string,
    heads: readonly AnalyticsEntityHead[],
  ) => Promise<void>;
};

export async function processDorisGlobalRetentionStep(input: {
  readonly retentionDays: number;
  readonly drainMs: number;
  readonly batchSize: number;
  readonly now?: Date;
  readonly dependencies?: RetentionDependencies;
}): Promise<DorisGlobalRetentionStepResult> {
  const now = input.now ?? new Date();
  if (!Number.isSafeInteger(input.drainMs) || input.drainMs < 60_000) {
    throw new TypeError("Invalid Doris retention drain window");
  }
  if (
    !Number.isSafeInteger(input.batchSize) ||
    input.batchSize < 1 ||
    input.batchSize > 5_000
  ) {
    throw new TypeError("Invalid Doris retention batch size");
  }
  const dependencies = input.dependencies ?? {};
  const client = dependencies.client ?? prisma;
  const startOrResume =
    dependencies.startOrResume ?? startOrResumeAnalyticsRetention;
  const run = await startOrResume({
    client,
    cutoffDate: cutoffDate(now, input.retentionDays),
    now,
  });
  if (!run) return { outcome: "idle" };

  try {
    if (run.phase === "DRAIN") {
      if (now.getTime() - run.startedAt.getTime() < input.drainMs) {
        return result("waiting", run);
      }
      const unresolvedLoads = await (
        dependencies.countUnresolvedLoads ?? countUnresolvedAnalyticsLoadsBefore
      )({ client, cutoffDate: run.cutoffDate });
      if (unresolvedLoads > 0) return result("waiting", run);
    }
    if (run.phase === "COMPLETE") {
      const completed = await (
        dependencies.complete ?? completeAnalyticsRetentionRun
      )({ client, runId: run.id, now });
      if (!completed) {
        throw new Error("Analytics retention completion lost its active fence");
      }
      return result("completed", run);
    }

    if (run.phase !== "DRAIN") {
      const entityType = PHASE_ENTITY_TYPE[run.phase];
      if (!entityType) {
        throw new Error(`Missing retention entity type for ${run.phase}`);
      }
      const heads = await (
        dependencies.findHeads ?? findAnalyticsEntityHeadsForRetention
      )({
        client,
        cutoffDate: run.cutoffDate,
        entityType,
        limit: input.batchSize,
      });
      if (heads.length > 0) {
        await (
          dependencies.deleteDorisHeads ??
          ((operationId, selected) =>
            getDorisAnalyticsLifecycleRuntime().materializedDeletion.deleteHeads(
              operationId,
              selected,
            ))
        )(`${run.id}-${run.phase}`, heads);
        const deleted = await (
          dependencies.deleteHeads ?? deleteAnalyticsEntityHeadsForRetention
        )({
          client,
          cutoffDate: run.cutoffDate,
          entityType,
          headIds: heads.map(({ id }) => id),
        });
        if (deleted !== heads.length) {
          throw new Error("Analytics retention entity-head fence was lost");
        }
        return result("processed", run);
      }
    }

    const nextPhase = NEXT_PHASE[run.phase];
    if (!nextPhase)
      throw new Error(`Missing retention phase after ${run.phase}`);
    const advanced = await (
      dependencies.advance ?? advanceAnalyticsRetentionRun
    )({
      client,
      runId: run.id,
      expectedPhase: run.phase,
      nextPhase,
    });
    if (!advanced) {
      throw new Error("Analytics retention phase transition lost its fence");
    }
    return result("advanced", run);
  } catch (error) {
    await (dependencies.recordFailure ?? recordAnalyticsRetentionFailure)({
      client,
      runId: run.id,
      phase: run.phase,
      reasonCode: "RETENTION_STEP_FAILED",
    });
    throw error;
  }
}

function result(
  outcome: "waiting" | "processed" | "advanced" | "completed",
  run: ActiveAnalyticsRetentionRun,
): DorisGlobalRetentionStepResult {
  return {
    outcome,
    runId: run.id,
    phase: run.phase,
    cutoffDate: run.cutoffDate,
  };
}

export class DorisGlobalRetentionRunner extends PeriodicExclusiveRunner {
  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly retentionDays: number;
      readonly drainMs: number;
      readonly batchSize: number;
      readonly assertReady?: () => Promise<void>;
      readonly processStep?: typeof processDorisGlobalRetentionStep;
    },
  ) {
    super({
      name: "DorisGlobalRetentionRunner",
      lockKey: "langfuse:doris-global-retention",
      lockTtlSeconds: 2 * 60 * 60,
      onUnavailable: "fail",
    });
    if (
      !Number.isSafeInteger(dependencies.intervalMs) ||
      dependencies.intervalMs < 1_000
    ) {
      throw new TypeError("Invalid Doris retention interval");
    }
  }

  protected get defaultIntervalMs(): number {
    return this.dependencies.intervalMs;
  }

  protected async execute(): Promise<void> {
    await this.withLock(async () => {
      await this.dependencies.assertReady?.();
      const result = await (
        this.dependencies.processStep ?? processDorisGlobalRetentionStep
      )({
        retentionDays: this.dependencies.retentionDays,
        drainMs: this.dependencies.drainMs,
        batchSize: this.dependencies.batchSize,
      });
      if (result.outcome !== "idle" && result.outcome !== "waiting") {
        logger.info("Advanced Doris global retention", result);
      }
    });
  }
}
