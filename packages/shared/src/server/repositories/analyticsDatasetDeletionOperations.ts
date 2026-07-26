import { randomUUID } from "node:crypto";

import type {
  AnalyticsDatasetDeletionOperation,
  AnalyticsDatasetDeletionScope,
  Prisma,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";

type AnalyticsControlClient = PrismaClient | Prisma.TransactionClient;

export type AnalyticsDatasetDeletionLease = {
  readonly owner: string;
  readonly fence: bigint;
};

export type ScheduledAnalyticsDatasetDeletion = {
  readonly operation: AnalyticsDatasetDeletionOperation;
  readonly datasetGeneration: bigint | null;
  readonly runGenerations: Readonly<Record<string, bigint>>;
};

export function analyticsDatasetDeletionQueueReference(
  scheduled: ScheduledAnalyticsDatasetDeletion,
): {
  readonly operationId: string;
  readonly datasetGeneration: string | null;
  readonly runGenerations: Readonly<Record<string, string>>;
} {
  return {
    operationId: scheduled.operation.id,
    datasetGeneration: scheduled.datasetGeneration?.toString() ?? null,
    runGenerations: Object.fromEntries(
      Object.entries(scheduled.runGenerations).map(([runId, generation]) => [
        runId,
        generation.toString(),
      ]),
    ),
  };
}

export function analyticsDatasetDeletionReferenceMatches(input: {
  readonly operation: Pick<
    AnalyticsDatasetDeletionOperation,
    "id" | "datasetGeneration" | "runGenerations"
  >;
  readonly reference: {
    readonly operationId: string;
    readonly datasetGeneration: string | null;
    readonly runGenerations: Readonly<Record<string, string>>;
  };
}): boolean {
  if (input.reference.operationId !== input.operation.id) return false;
  try {
    const referencedDatasetGeneration =
      input.reference.datasetGeneration === null
        ? null
        : BigInt(input.reference.datasetGeneration);
    const persistedRunGenerations = decodeAnalyticsDatasetRunGenerations(
      input.operation.runGenerations,
    );
    const referencedEntries = Object.entries(input.reference.runGenerations)
      .map(([runId, generation]) => [runId, generation] as const)
      .sort(([left], [right]) => left.localeCompare(right));
    const persistedEntries = Object.entries(persistedRunGenerations)
      .map(([runId, generation]) => [runId, generation.toString()] as const)
      .sort(([left], [right]) => left.localeCompare(right));
    return (
      referencedDatasetGeneration === input.operation.datasetGeneration &&
      JSON.stringify(referencedEntries) === JSON.stringify(persistedEntries)
    );
  } catch {
    return false;
  }
}

function assertDeletionInput(input: {
  readonly scope: AnalyticsDatasetDeletionScope;
  readonly projectId: string;
  readonly datasetId: string;
  readonly datasetRunIds?: readonly string[];
}): void {
  if (
    !input.projectId ||
    !input.datasetId ||
    input.datasetRunIds?.some((runId) => !runId) ||
    (input.scope === "DATASET_RUNS" && (input.datasetRunIds?.length ?? 0) === 0)
  ) {
    throw new TypeError("Invalid analytics dataset deletion request");
  }
}

function encodeRunGenerations(
  generations: Readonly<Record<string, bigint>>,
): Prisma.InputJsonObject {
  return Object.fromEntries(
    Object.entries(generations).map(([runId, generation]) => [
      runId,
      generation.toString(),
    ]),
  );
}

export function decodeAnalyticsDatasetRunGenerations(
  value: Prisma.JsonValue,
): Readonly<Record<string, bigint>> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("Invalid analytics dataset run generations");
  }
  const result: Record<string, bigint> = {};
  for (const [runId, generation] of Object.entries(value)) {
    if (
      !runId ||
      typeof generation !== "string" ||
      !/^(0|[1-9][0-9]*)$/.test(generation)
    ) {
      throw new TypeError("Invalid analytics dataset run generations");
    }
    result[runId] = BigInt(generation);
  }
  return result;
}

/**
 * 调用方必须把此函数放在删除 Dataset/DatasetRuns 的同一个事务中，并且先创建
 * intent 再删业务行。这样即使 Redis 或 Worker 故障，generation fence 也不会丢。
 */
export async function createAnalyticsDatasetDeletionIntent(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly scope: AnalyticsDatasetDeletionScope;
  readonly projectId: string;
  readonly datasetId: string;
  readonly datasetRunIds?: readonly string[];
}): Promise<ScheduledAnalyticsDatasetDeletion> {
  assertDeletionInput(input);
  const suppliedRunIds = input.datasetRunIds;
  const ownedRunIds =
    input.scope === "DATASET" &&
    (!suppliedRunIds || suppliedRunIds.length === 0)
      ? (
          await input.transaction.datasetRuns.findMany({
            where: {
              projectId: input.projectId,
              datasetId: input.datasetId,
            },
            select: { id: true },
          })
        ).map(({ id }) => id)
      : (suppliedRunIds ?? []);
  const datasetRunIds = [...new Set(ownedRunIds)].sort();

  const datasetGeneration =
    input.scope === "DATASET"
      ? (
          await input.transaction.analyticsDatasetDeletionGeneration.upsert({
            where: {
              projectId_datasetId: {
                projectId: input.projectId,
                datasetId: input.datasetId,
              },
            },
            create: {
              projectId: input.projectId,
              datasetId: input.datasetId,
              generation: 1n,
            },
            update: { generation: { increment: 1n } },
            select: { generation: true },
          })
        ).generation
      : null;

  const runGenerations: Record<string, bigint> = {};
  for (const datasetRunId of datasetRunIds) {
    const state =
      await input.transaction.analyticsDatasetRunDeletionGeneration.upsert({
        where: {
          projectId_datasetRunId: {
            projectId: input.projectId,
            datasetRunId,
          },
        },
        create: {
          projectId: input.projectId,
          datasetId: input.datasetId,
          datasetRunId,
          generation: 1n,
        },
        update: { generation: { increment: 1n } },
        select: { datasetId: true, generation: true },
      });
    if (state.datasetId !== input.datasetId) {
      throw new Error(
        "Analytics dataset run generation dataset ownership changed",
      );
    }
    runGenerations[datasetRunId] = state.generation;
  }

  const operation =
    await input.transaction.analyticsDatasetDeletionOperation.create({
      data: {
        id: randomUUID(),
        scope: input.scope,
        projectId: input.projectId,
        datasetId: input.datasetId,
        datasetRunIds,
        datasetGeneration,
        runGenerations: encodeRunGenerations(runGenerations),
        outbox: { create: {} },
      },
    });
  return { operation, datasetGeneration, runGenerations };
}

export async function getAnalyticsDatasetDeletionGenerations(input: {
  readonly client?: AnalyticsControlClient;
  readonly projectId: string;
  readonly datasetId: string;
  readonly datasetRunIds: readonly string[];
}): Promise<{
  readonly datasetGeneration: bigint;
  readonly runGenerations: Readonly<Record<string, bigint>>;
}> {
  const client = input.client ?? prisma;
  const [dataset, runs] = await Promise.all([
    client.analyticsDatasetDeletionGeneration.findUnique({
      where: {
        projectId_datasetId: {
          projectId: input.projectId,
          datasetId: input.datasetId,
        },
      },
      select: { generation: true },
    }),
    client.analyticsDatasetRunDeletionGeneration.findMany({
      where: {
        projectId: input.projectId,
        datasetRunId: { in: [...new Set(input.datasetRunIds)] },
      },
      select: { datasetRunId: true, generation: true },
    }),
  ]);
  return {
    datasetGeneration: dataset?.generation ?? 0n,
    runGenerations: Object.fromEntries(
      runs.map(({ datasetRunId, generation }) => [datasetRunId, generation]),
    ),
  };
}

export async function claimAnalyticsDatasetDeletionOperation(input: {
  readonly client?: PrismaClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly owner: string;
  readonly leaseMs?: number;
  readonly now?: Date;
}): Promise<AnalyticsDatasetDeletionOperation | null> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  if (!input.operationId || !input.projectId || !input.owner) {
    throw new TypeError("Invalid analytics dataset deletion claim");
  }
  return client.$transaction(async (transaction) => {
    const claimed =
      await transaction.analyticsDatasetDeletionOperation.updateMany({
        where: {
          id: input.operationId,
          projectId: input.projectId,
          status: { not: "COMPLETED" },
          OR: [
            { leaseOwner: null },
            { leaseExpiresAt: null },
            { leaseExpiresAt: { lte: now } },
            { leaseOwner: input.owner },
          ],
        },
        data: {
          workerFence: { increment: 1n },
          leaseOwner: input.owner,
          leaseExpiresAt: new Date(
            now.getTime() + (input.leaseMs ?? 5 * 60_000),
          ),
        },
      });
    if (claimed.count !== 1) return null;
    return transaction.analyticsDatasetDeletionOperation.findFirst({
      where: {
        id: input.operationId,
        projectId: input.projectId,
        leaseOwner: input.owner,
      },
    });
  });
}

export async function markAnalyticsDatasetDeletionBarrierVisible(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly lease: AnalyticsDatasetDeletionLease;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const updated = await client.analyticsDatasetDeletionOperation.updateMany({
    where: {
      id: input.operationId,
      projectId: input.projectId,
      status: { not: "COMPLETED" },
      workerFence: input.lease.fence,
      leaseOwner: input.lease.owner,
    },
    data: {
      phase: "physical_cleanup",
      logicallyInvisible: true,
      status: "RETRYING",
      lastErrorCode: null,
    },
  });
  return updated.count === 1;
}

export async function completeAnalyticsDatasetDeletionOperation(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly lease: AnalyticsDatasetDeletionLease;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const updated = await client.analyticsDatasetDeletionOperation.updateMany({
    where: {
      id: input.operationId,
      projectId: input.projectId,
      logicallyInvisible: true,
      workerFence: input.lease.fence,
      leaseOwner: input.lease.owner,
      status: { not: "COMPLETED" },
    },
    data: {
      status: "COMPLETED",
      phase: "completed",
      completedAt: now,
      leaseOwner: null,
      leaseExpiresAt: null,
      lastErrorCode: null,
    },
  });
  return updated.count === 1;
}

export function findAnalyticsDatasetDeletionOperation(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly projectId: string;
}): Promise<AnalyticsDatasetDeletionOperation | null> {
  const client = input.client ?? prisma;
  return client.analyticsDatasetDeletionOperation.findFirst({
    where: { id: input.operationId, projectId: input.projectId },
  });
}

export async function markAnalyticsDatasetDeletionOutboxPublished(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const updated = await client.analyticsDatasetDeletionOutbox.updateMany({
    where: { operationId: input.operationId, status: { not: "PUBLISHED" } },
    data: {
      status: "PUBLISHED",
      publishedAt: input.now ?? new Date(),
      lockedBy: null,
      lockedUntil: null,
    },
  });
  return updated.count === 1;
}

export function listPendingAnalyticsDatasetDeletionOutbox(input: {
  readonly client?: AnalyticsControlClient;
  readonly now?: Date;
  readonly limit?: number;
}) {
  const client = input.client ?? prisma;
  return client.analyticsDatasetDeletionOutbox.findMany({
    where: {
      status: "PENDING",
      nextAttemptAt: { lte: input.now ?? new Date() },
      operation: { status: { not: "COMPLETED" } },
    },
    include: { operation: true },
    orderBy: [{ nextAttemptAt: "asc" }, { id: "asc" }],
    take: input.limit ?? 100,
  });
}

export async function deferAnalyticsDatasetDeletionOutbox(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const current = await client.analyticsDatasetDeletionOutbox.findUnique({
    where: { operationId: input.operationId },
    select: { attempts: true },
  });
  if (!current) return false;
  const delayMs = Math.min(60_000, 1_000 * 2 ** Math.min(current.attempts, 6));
  const updated = await client.analyticsDatasetDeletionOutbox.updateMany({
    where: { operationId: input.operationId, status: "PENDING" },
    data: {
      attempts: { increment: 1 },
      nextAttemptAt: new Date(now.getTime() + delayMs),
      lockedBy: null,
      lockedUntil: null,
    },
  });
  return updated.count === 1;
}
