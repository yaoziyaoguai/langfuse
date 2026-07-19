import { randomUUID } from "node:crypto";

import type {
  AnalyticsEntityHead,
  AnalyticsEntityType,
  AnalyticsRetentionRun,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";

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

function utcDate(value: Date): Date {
  if (Number.isNaN(value.getTime())) {
    throw new TypeError("Invalid analytics retention date");
  }
  return new Date(
    Date.UTC(value.getUTCFullYear(), value.getUTCMonth(), value.getUTCDate()),
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

export async function startOrResumeAnalyticsRetention(input: {
  readonly client?: PrismaClient;
  readonly cutoffDate: Date;
  readonly now?: Date;
}): Promise<ActiveAnalyticsRetentionRun | null> {
  const client = input.client ?? prisma;
  const cutoffDate = utcDate(input.cutoffDate);
  const now = input.now ?? new Date();
  if (!Number.isFinite(now.getTime())) {
    throw new TypeError("Invalid analytics retention start time");
  }

  return client.$transaction(async (transaction) => {
    const state = await transaction.analyticsRetentionState.upsert({
      where: { id: ANALYTICS_RETENTION_STATE_ID },
      create: { id: ANALYTICS_RETENTION_STATE_ID },
      update: {},
    });
    if (state.activeRunId) {
      return activeRun(
        await transaction.analyticsRetentionRun.findUniqueOrThrow({
          where: { id: state.activeRunId },
        }),
      );
    }
    if (state.purgedBefore && cutoffDate <= state.purgedBefore) return null;

    const run = await transaction.analyticsRetentionRun.create({
      data: {
        id: randomUUID(),
        cutoffDate,
        phase: "DRAIN",
        status: "RUNNING",
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
    return activeRun(
      await transaction.analyticsRetentionRun.findUniqueOrThrow({
        where: { id: winner.activeRunId },
      }),
    );
  });
}

export async function advanceAnalyticsRetentionRun(input: {
  readonly client?: PrismaClient;
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
  readonly client?: PrismaClient;
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
  readonly client?: PrismaClient;
  readonly runId: string;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  return client.$transaction(async (transaction) => {
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
  });
}

export async function getAnalyticsRetentionBarrier(
  input: {
    readonly client?: PrismaClient;
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
  readonly client?: PrismaClient;
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
  readonly client?: PrismaClient;
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
  const client = input.client ?? prisma;
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
  readonly client?: PrismaClient;
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
