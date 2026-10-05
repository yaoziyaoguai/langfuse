import type {
  AnalyticsEntityHead,
  AnalyticsEntityType,
  PrismaClient,
} from "@prisma/client";
import {
  advanceAnalyticsRetentionRun,
  analyticsProjectRetentionStateId,
  analyticsDurableProvenanceFromRecord,
  completeAnalyticsRetentionRun,
  countUnresolvedAnalyticsLoadsBefore,
  createAnalyticsBackendClaimLease,
  deleteAnalyticsEntityHeadsForRetention,
  findAnalyticsEntityHeadsForRetention,
  getAnalyticsRetentionDatabaseClock,
  lockAnalyticsBackendClaimLeaseForIo,
  lockLegacyAnalyticsAdmission,
  logger,
  recordAnalyticsRetentionFailure,
  releaseAnalyticsBackendClaimLease,
  startOrResumeAnalyticsRetention,
  type ActiveAnalyticsRetentionRun,
  type AnalyticsBackend,
  type AnalyticsRetentionClient,
  type AnalyticsRetentionPhase,
  type AnalyticsRuntimeAdmissionContext,
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
  readonly withWorkFence?: RetentionWorkFence;
  readonly getDatabaseNow?: typeof getAnalyticsRetentionDatabaseClock;
};

type RetentionWorkFence = <T>(input: {
  readonly client: PrismaClient;
  readonly run: ActiveAnalyticsRetentionRun;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly execute: (client: AnalyticsRetentionClient) => Promise<T>;
}) => Promise<T>;

const RETENTION_CLAIM_MS = 30 * 60_000;
const RETENTION_FENCE_TIMEOUT_MS = 35 * 60_000;

const withAnalyticsRetentionWorkFence: RetentionWorkFence = async (input) => {
  const provenance = analyticsDurableProvenanceFromRecord(input.run);
  if (!provenance) {
    if (input.admissionContext) {
      throw new Error(
        "Legacy analytics retention cannot use managed admission",
      );
    }
    return input.client.$transaction(
      async (transaction) => {
        await lockLegacyAnalyticsAdmission(transaction);
        return input.execute(transaction);
      },
      { timeout: RETENTION_FENCE_TIMEOUT_MS },
    );
  }

  const expectedBackend: AnalyticsBackend =
    provenance.analyticsBackend === "DORIS" ? "doris" : "clickhouse";
  if (
    !input.admissionContext ||
    input.admissionContext.backend !== expectedBackend ||
    input.admissionContext.deploymentGeneration !==
      provenance.deploymentGeneration
  ) {
    throw new Error("Analytics retention runtime is not admitted");
  }
  const fence = {
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend,
    expectedDeploymentGeneration: provenance.deploymentGeneration,
    expectedWorkloadEpochFingerprint: provenance.workloadEpochFingerprint,
    expectedRuntimeContractVersion: provenance.runtimeContractVersion,
    action: "foundation" as const,
  };
  const claim = await createAnalyticsBackendClaimLease({
    client: input.client,
    ...fence,
    claimKind: "analytics-retention",
    resourceIdentity: input.run.id,
    leaseMs: RETENTION_CLAIM_MS,
  });
  if (!claim) throw new Error("Analytics retention work is already claimed");

  try {
    return await input.client.$transaction(
      async (transaction) => {
        await lockAnalyticsBackendClaimLeaseForIo({
          transaction,
          claimLeaseId: claim.id,
          fence,
        });
        return input.execute(transaction);
      },
      { timeout: RETENTION_FENCE_TIMEOUT_MS },
    );
  } finally {
    await releaseAnalyticsBackendClaimLease({
      client: input.client,
      claimLeaseId: claim.id,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    });
  }
};

export async function processDorisGlobalRetentionStep(input: {
  readonly retentionDays: number;
  readonly drainMs: number;
  readonly batchSize: number;
  readonly projectId?: string;
  readonly stateId?: string;
  readonly onCutoffPublished?: (cutoffDate: Date) => Promise<void>;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext | null;
  readonly dependencies?: RetentionDependencies;
}): Promise<DorisGlobalRetentionStepResult> {
  if (
    (input.projectId &&
      input.stateId !== analyticsProjectRetentionStateId(input.projectId)) ||
    (!input.projectId && input.stateId !== undefined)
  ) {
    throw new TypeError("Retention scope does not match its durable state");
  }
  if (!Number.isSafeInteger(input.retentionDays) || input.retentionDays < 3) {
    throw new TypeError("Invalid Doris global retention configuration");
  }
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
    retentionDays: input.retentionDays,
    admissionContext: input.admissionContext ?? null,
    ...(input.stateId ? { stateId: input.stateId } : {}),
  });
  if (!run) return { outcome: "idle" };

  try {
    return await (
      dependencies.withWorkFence ?? withAnalyticsRetentionWorkFence
    )({
      client,
      run,
      admissionContext: input.admissionContext ?? null,
      execute: async (fencedClient) => {
        await input.onCutoffPublished?.(run.cutoffDate);
        const now = await (
          dependencies.getDatabaseNow ?? getAnalyticsRetentionDatabaseClock
        )({ client: fencedClient });
        if (run.phase === "DRAIN") {
          if (now.getTime() - run.startedAt.getTime() < input.drainMs) {
            return result("waiting", run);
          }
          const unresolvedLoads = await (
            dependencies.countUnresolvedLoads ??
            countUnresolvedAnalyticsLoadsBefore
          )({
            client: fencedClient,
            cutoffDate: run.cutoffDate,
            ...(input.projectId ? { projectId: input.projectId } : {}),
          });
          if (unresolvedLoads > 0) return result("waiting", run);
        }
        if (run.phase === "COMPLETE") {
          const completed = await (
            dependencies.complete ?? completeAnalyticsRetentionRun
          )({ client: fencedClient, runId: run.id });
          if (!completed) {
            throw new Error(
              "Analytics retention completion lost its active fence",
            );
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
            client: fencedClient,
            cutoffDate: run.cutoffDate,
            entityType,
            limit: input.batchSize,
            ...(input.projectId ? { projectId: input.projectId } : {}),
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
              client: fencedClient,
              cutoffDate: run.cutoffDate,
              entityType,
              headIds: heads.map(({ id }) => id),
              ...(input.projectId ? { projectId: input.projectId } : {}),
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
          client: fencedClient,
          runId: run.id,
          expectedPhase: run.phase,
          nextPhase,
        });
        if (!advanced) {
          throw new Error(
            "Analytics retention phase transition lost its fence",
          );
        }
        return result("advanced", run);
      },
    });
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
      readonly getAdmissionContext?: () => AnalyticsRuntimeAdmissionContext | null;
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
        admissionContext: this.dependencies.getAdmissionContext?.() ?? null,
      });
      if (result.outcome !== "idle" && result.outcome !== "waiting") {
        logger.info("Advanced Doris global retention", result);
      }
    });
  }
}
