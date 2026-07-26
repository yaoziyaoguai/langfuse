import { randomUUID } from "node:crypto";

import {
  Prisma,
  type AnalyticsEntityHead,
  type AnalyticsEntityType,
  type AnalyticsRetentionRun,
  type PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import {
  lockAnalyticsAdmission,
  lockLegacyAnalyticsAdmission,
  type AnalyticsRuntimeAdmissionContext,
} from "../analytics-persistence/analyticsBackendAdmission";
import {
  analyticsDurableProvenanceFromRecord,
  analyticsProducerProvenanceFromAdmission,
  type AnalyticsDurableProvenance,
} from "../analytics-persistence/analyticsDurableProvenance";
import { acquireAnalyticsRetentionMutationPermit } from "./analyticsCheckpoints";

export const ANALYTICS_RETENTION_STATE_ID = "global";

export const ANALYTICS_RETENTION_PHASES = [
  "DRAIN",
  "EVENTS",
  "SCORES",
  "BLOB_REFERENCES",
  "COMPLETE",
] as const;

export type AnalyticsRetentionPhase =
  (typeof ANALYTICS_RETENTION_PHASES)[number];

export type AnalyticsRetentionClient = PrismaClient | Prisma.TransactionClient;

async function databaseClock(
  transaction: Prisma.TransactionClient,
): Promise<Date> {
  const [row] = await transaction.$queryRaw<readonly { now: Date }[]>(
    Prisma.sql`SELECT clock_timestamp() AS now`,
  );
  if (!row || !Number.isFinite(row.now.getTime())) {
    throw new Error("Postgres did not return its analytics retention clock");
  }
  return row.now;
}

function utcDate(value: Date): Date {
  if (Number.isNaN(value.getTime())) {
    throw new TypeError("Invalid analytics retention date");
  }
  return new Date(
    Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()),
  );
}

function retentionCutoff(now: Date, retentionDays: number): Date {
  if (!Number.isSafeInteger(retentionDays) || retentionDays < 3) {
    throw new TypeError("Invalid analytics retention period");
  }
  return new Date(
    Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate() - retentionDays,
    ),
  );
}

function phase(value: string): AnalyticsRetentionPhase {
  if (!ANALYTICS_RETENTION_PHASES.includes(value as AnalyticsRetentionPhase)) {
    throw new Error(`Unknown analytics retention phase: ${value}`);
  }
  return value as AnalyticsRetentionPhase;
}

export type ActiveAnalyticsRetentionRun = Omit<
  AnalyticsRetentionRun,
  "phase"
> & { readonly phase: AnalyticsRetentionPhase };

function activeRun(run: AnalyticsRetentionRun): ActiveAnalyticsRetentionRun {
  if (run.status !== "RUNNING") {
    throw new Error("Active analytics retention run is not running");
  }
  return { ...run, phase: phase(run.phase) };
}

async function lockRetentionAdmission(
  transaction: Prisma.TransactionClient,
  admissionContext: AnalyticsRuntimeAdmissionContext | null,
): Promise<AnalyticsDurableProvenance | null> {
  if (!admissionContext) {
    await lockLegacyAnalyticsAdmission(transaction);
    return null;
  }
  const admission = await lockAnalyticsAdmission({
    transaction,
    runtimeLeaseId: admissionContext.runtimeLeaseId,
    expectedBackend: admissionContext.backend,
    expectedDeploymentGeneration: admissionContext.deploymentGeneration,
    action: "foundation",
  });
  return analyticsProducerProvenanceFromAdmission(admission);
}

function assertRetentionProvenanceMatchesAdmission(
  run: AnalyticsRetentionRun,
  admission: AnalyticsDurableProvenance | null,
): void {
  const provenance = analyticsDurableProvenanceFromRecord(run);
  if ((provenance === null) !== (admission === null)) {
    throw new Error("Analytics retention provenance does not match deployment");
  }
  if (
    provenance &&
    admission &&
    (provenance.analyticsBackend !== admission.analyticsBackend ||
      provenance.deploymentGeneration !== admission.deploymentGeneration ||
      provenance.workloadEpochFingerprint !==
        admission.workloadEpochFingerprint ||
      provenance.runtimeContractVersion !== admission.runtimeContractVersion)
  ) {
    throw new Error("Analytics retention durable provenance changed");
  }
}

async function assertRetentionMutationAllowed(
  transaction: Prisma.TransactionClient,
): Promise<void> {
  const permit = await acquireAnalyticsRetentionMutationPermit({ transaction });
  if (permit.outcome === "held") {
    throw new Error("Analytics retention is held by the analytics checkpoint");
  }
  const integrationReplay =
    await transaction.analyticsCapabilityActivation.findUnique({
      where: { capability: "ANALYTICS_INTEGRATIONS" },
      select: {
        captureRequired: true,
        rescanRequired: true,
      },
    });
  if (integrationReplay?.captureRequired || integrationReplay?.rescanRequired) {
    throw new Error(
      "Analytics retention is held by analytics integration replay",
    );
  }
}

export async function startOrResumeAnalyticsRetention(input: {
  readonly client?: PrismaClient;
  readonly retentionDays: number;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext | null;
}): Promise<ActiveAnalyticsRetentionRun | null> {
  const client = input.client ?? prisma;
  if (!Number.isSafeInteger(input.retentionDays) || input.retentionDays < 3) {
    throw new TypeError("Invalid analytics retention period");
  }

  return client.$transaction(async (transaction) => {
    const producerProvenance = await lockRetentionAdmission(
      transaction,
      input.admissionContext ?? null,
    );
    await assertRetentionMutationAllowed(transaction);
    const now = await databaseClock(transaction);
    const cutoffDate = retentionCutoff(now, input.retentionDays);
    const state = await transaction.analyticsRetentionState.upsert({
      where: { id: ANALYTICS_RETENTION_STATE_ID },
      create: { id: ANALYTICS_RETENTION_STATE_ID },
      update: {},
    });
    if (state.activeRunId) {
      const run = await transaction.analyticsRetentionRun.findUniqueOrThrow({
        where: { id: state.activeRunId },
      });
      assertRetentionProvenanceMatchesAdmission(run, producerProvenance);
      return activeRun(run);
    }
    if (state.purgedBefore && cutoffDate <= state.purgedBefore) return null;

    const run = await transaction.analyticsRetentionRun.create({
      data: {
        id: randomUUID(),
        cutoffDate,
        phase: "DRAIN",
        status: "RUNNING",
        analyticsBackend: producerProvenance?.analyticsBackend ?? null,
        deploymentGeneration: producerProvenance?.deploymentGeneration ?? null,
        workloadEpochFingerprint:
          producerProvenance?.workloadEpochFingerprint ?? null,
        runtimeContractVersion:
          producerProvenance?.runtimeContractVersion ?? null,
        producerRuntimeLeaseId:
          producerProvenance?.producerRuntimeLeaseId ?? null,
        startedAt: now,
      },
    });
    const claimed = await transaction.analyticsRetentionState.updateMany({
      where: {
        id: ANALYTICS_RETENTION_STATE_ID,
        activeRunId: null,
        OR: [{ purgedBefore: null }, { purgedBefore: { lt: cutoffDate } }],
      },
      data: {
        activeRunId: run.id,
        activeCutoff: cutoffDate,
      },
    });
    if (claimed.count === 1) return activeRun(run);

    await transaction.analyticsRetentionRun.delete({ where: { id: run.id } });
    const winner = await transaction.analyticsRetentionState.findUniqueOrThrow({
      where: { id: ANALYTICS_RETENTION_STATE_ID },
    });
    if (!winner.activeRunId) return null;
    const winningRun =
      await transaction.analyticsRetentionRun.findUniqueOrThrow({
        where: { id: winner.activeRunId },
      });
    assertRetentionProvenanceMatchesAdmission(winningRun, producerProvenance);
    return activeRun(winningRun);
  });
}

export async function advanceAnalyticsRetentionRun(input: {
  readonly client?: AnalyticsRetentionClient;
  readonly runId: string;
  readonly expectedPhase: AnalyticsRetentionPhase;
  readonly nextPhase: AnalyticsRetentionPhase;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const expectedIndex = ANALYTICS_RETENTION_PHASES.indexOf(input.expectedPhase);
  if (
    !input.runId ||
    expectedIndex < 0 ||
    ANALYTICS_RETENTION_PHASES[expectedIndex + 1] !== input.nextPhase
  ) {
    throw new TypeError("Invalid analytics retention phase transition");
  }
  const updated = await client.analyticsRetentionRun.updateMany({
    where: {
      id: input.runId,
      status: "RUNNING",
      phase: input.expectedPhase,
    },
    data: { phase: input.nextPhase, lastErrorCode: null },
  });
  return updated.count === 1;
}

export async function recordAnalyticsRetentionFailure(input: {
  readonly client?: AnalyticsRetentionClient;
  readonly runId: string;
  readonly phase: AnalyticsRetentionPhase;
  readonly reasonCode: string;
}): Promise<void> {
  if (!/^[A-Z0-9_]{1,64}$/.test(input.reasonCode)) {
    throw new TypeError("Invalid analytics retention failure reason");
  }
  const client = input.client ?? prisma;
  await client.analyticsRetentionRun.updateMany({
    where: { id: input.runId, status: "RUNNING", phase: input.phase },
    data: { lastErrorCode: input.reasonCode },
  });
}

export async function completeAnalyticsRetentionRun(input: {
  readonly client?: AnalyticsRetentionClient;
  readonly runId: string;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const complete = async (transaction: Prisma.TransactionClient) => {
    await assertRetentionMutationAllowed(transaction);
    const now = await databaseClock(transaction);
    const state = await transaction.analyticsRetentionState.findUnique({
      where: { id: ANALYTICS_RETENTION_STATE_ID },
    });
    if (state?.activeRunId !== input.runId) return false;
    const run = await transaction.analyticsRetentionRun.findUniqueOrThrow({
      where: { id: input.runId },
    });
    if (run.status !== "RUNNING" || run.phase !== "COMPLETE") return false;
    if (state.purgedBefore && run.cutoffDate < state.purgedBefore) {
      throw new Error("Analytics retention watermark would move backwards");
    }
    await transaction.analyticsRetentionRun.update({
      where: { id: run.id },
      data: { status: "COMPLETED", completedAt: now, lastErrorCode: null },
    });
    await transaction.analyticsRetentionState.update({
      where: { id: ANALYTICS_RETENTION_STATE_ID },
      data: {
        purgedBefore: run.cutoffDate,
        activeRunId: null,
        activeCutoff: null,
      },
    });
    return true;
  };
  return "$transaction" in client
    ? client.$transaction(complete)
    : complete(client);
}

export async function getAnalyticsRetentionDatabaseClock(input: {
  readonly client?: AnalyticsRetentionClient;
}): Promise<Date> {
  const client = input.client ?? prisma;
  return "$transaction" in client
    ? client.$transaction(databaseClock)
    : databaseClock(client);
}

export async function getAnalyticsRetentionBarrier(
  input: {
    readonly client?: AnalyticsRetentionClient;
  } = {},
): Promise<Date | null> {
  const client = input.client ?? prisma;
  const state = await client.analyticsRetentionState.findUnique({
    where: { id: ANALYTICS_RETENTION_STATE_ID },
    select: { purgedBefore: true, activeCutoff: true },
  });
  if (!state) return null;
  if (!state.purgedBefore) return state.activeCutoff;
  if (!state.activeCutoff) return state.purgedBefore;
  return state.activeCutoff > state.purgedBefore
    ? state.activeCutoff
    : state.purgedBefore;
}

export async function countUnresolvedAnalyticsLoadsBefore(input: {
  readonly client?: AnalyticsRetentionClient;
  readonly cutoffDate: Date;
}): Promise<number> {
  const client = input.client ?? prisma;
  return client.analyticsLoadBatch.count({
    where: {
      status: { in: ["LOADING", "UNKNOWN"] },
      partitionDate: { lt: utcDate(input.cutoffDate) },
    },
  });
}

export async function findAnalyticsEntityHeadsForRetention(input: {
  readonly client?: AnalyticsRetentionClient;
  readonly cutoffDate: Date;
  readonly entityType: AnalyticsEntityType;
  readonly limit: number;
}): Promise<AnalyticsEntityHead[]> {
  if (
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 5_000
  ) {
    throw new TypeError("Invalid analytics retention batch size");
  }
  // 这是 deployment-wide retention 的全局扫描，不是用户发起的 project 查询。
  const client = input.client;
  if (!client || "$transaction" in client) {
    throw new TypeError(
      "Analytics retention batch selection requires an existing transaction",
    );
  }
  await assertRetentionMutationAllowed(client);
  return client.analyticsEntityHead.findMany({
    where: {
      entityType: input.entityType,
      partitionDate: { lt: utcDate(input.cutoffDate) },
    },
    orderBy: [{ partitionDate: "asc" }, { id: "asc" }],
    take: input.limit,
  });
}

export async function deleteAnalyticsEntityHeadsForRetention(input: {
  readonly client?: AnalyticsRetentionClient;
  readonly cutoffDate: Date;
  readonly entityType: AnalyticsEntityType;
  readonly headIds: readonly string[];
}): Promise<number> {
  if (input.headIds.length === 0) return 0;
  const client = input.client ?? prisma;
  const deleted = await client.analyticsEntityHead.deleteMany({
    where: {
      id: { in: input.headIds.concat() },
      entityType: input.entityType,
      partitionDate: { lt: utcDate(input.cutoffDate) },
    },
  });
  return deleted.count;
}
