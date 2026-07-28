import type { AnalyticsEntityHead, PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  analyticsProjectRetentionStateId,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";

import {
  processDorisGlobalRetentionStep,
  type DorisGlobalRetentionStepResult,
} from "../../features/doris-global-retention";

const PROJECT_RETENTION_DRAIN_MS = 60_000;
const PROJECT_RETENTION_BATCH_SIZE = 5_000;

type ProcessDorisProjectRetentionDependencies = {
  readonly client?: PrismaClient;
  readonly processStep?: typeof processDorisGlobalRetentionStep;
  readonly deleteDorisHeads?: (
    operationId: string,
    heads: readonly AnalyticsEntityHead[],
    context: {
      readonly cutoffDate: Date;
      readonly projectId?: string;
    },
  ) => Promise<void>;
  readonly onCutoffPublished?: (input: {
    readonly projectId: string;
    readonly cutoffDate: Date;
  }) => Promise<void>;
  readonly scheduleContinuation: (input: {
    readonly projectId: string;
    readonly retentionDays: number;
    readonly delayMs: number;
  }) => Promise<void>;
};

export async function processDorisProjectRetention(
  input: {
    readonly projectId: string;
    readonly queuedRetentionDays: number;
    readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  },
  dependencies: ProcessDorisProjectRetentionDependencies,
): Promise<DorisGlobalRetentionStepResult> {
  if (!input.projectId) {
    throw new TypeError("Invalid Doris project retention job");
  }
  const client = dependencies.client ?? prisma;
  const stateId = analyticsProjectRetentionStateId(input.projectId);
  const [project, state] = await Promise.all([
    client.project.findUnique({
      where: { id: input.projectId },
      select: { retentionDays: true },
    }),
    client.analyticsRetentionState.findUnique({
      where: { id: stateId },
      select: {
        activeRunId: true,
        activeCutoff: true,
        activeRun: { select: { phase: true } },
      },
    }),
  ]);
  if (!project) return { outcome: "idle" };

  const activeRun =
    state?.activeRunId !== null && state?.activeRunId !== undefined;
  const currentRetentionDays = project.retentionDays;
  if (
    !activeRun &&
    (!currentRetentionDays ||
      !Number.isSafeInteger(currentRetentionDays) ||
      currentRetentionDays < 3)
  ) {
    return { outcome: "idle" };
  }
  const retentionDays =
    currentRetentionDays && currentRetentionDays >= 3
      ? currentRetentionDays
      : input.queuedRetentionDays;
  if (!Number.isSafeInteger(retentionDays) || retentionDays < 3) {
    throw new TypeError("Invalid active Doris project retention period");
  }
  if (activeRun && state?.activeRun?.phase === "DRAIN") {
    if (!state.activeCutoff) {
      throw new Error("Active Doris project retention cutoff is missing");
    }
    await dependencies.onCutoffPublished?.({
      projectId: input.projectId,
      cutoffDate: state.activeCutoff,
    });
  }
  const result = await (
    dependencies.processStep ?? processDorisGlobalRetentionStep
  )({
    retentionDays,
    drainMs: PROJECT_RETENTION_DRAIN_MS,
    batchSize: PROJECT_RETENTION_BATCH_SIZE,
    scope: {
      stateId,
      projectId: input.projectId,
    },
    admissionContext: input.admissionContext,
    dependencies: {
      client,
      ...(dependencies.deleteDorisHeads
        ? {
            deleteDorisHeads: dependencies.deleteDorisHeads,
          }
        : {}),
    },
  });

  if (!activeRun && result.outcome !== "idle" && result.phase === "DRAIN") {
    await dependencies.onCutoffPublished?.({
      projectId: input.projectId,
      cutoffDate: result.cutoffDate,
    });
  }
  if (result.outcome !== "idle" && result.outcome !== "completed") {
    await dependencies.scheduleContinuation({
      projectId: input.projectId,
      retentionDays,
      delayMs: result.outcome === "waiting" ? PROJECT_RETENTION_DRAIN_MS : 0,
    });
  }
  return result;
}
