import { randomUUID } from "node:crypto";

import type {
  AnalyticsDeletionOperation,
  AnalyticsDeletionScope,
  Prisma,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import type { AnalyticsDeletionProgress } from "../analytics-persistence";
import { getActiveCheckpointGenerationForAcceptance } from "./analyticsCheckpoints";

type AnalyticsControlClient = PrismaClient | Prisma.TransactionClient;

export type AnalyticsDeletionLease = {
  readonly owner: string;
  readonly fence: bigint;
};

const STATUS_RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;
const ACTIVE_INGESTION_STATUSES = [
  "ACCEPTED",
  "QUEUED",
  "PERSISTED",
  "RETRYING",
] as const;

export type AnalyticsDeletionRequester = {
  readonly principalType: "user" | "api_key" | "system";
  readonly principalId: string;
};

export type ScheduledTraceDeletion = {
  readonly operation: AnalyticsDeletionOperation;
  readonly traceId: string;
  readonly generation: bigint;
};

export class AnalyticsProjectDeletionInProgressError extends Error {
  constructor() {
    super("Trace deletion is superseded by project deletion");
    this.name = "AnalyticsProjectDeletionInProgressError";
  }
}

export async function claimDeletionOperation(input: {
  readonly client?: PrismaClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly owner: string;
  readonly leaseMs?: number;
  readonly now?: Date;
}): Promise<AnalyticsDeletionOperation | null> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const leaseExpiresAt = new Date(
    now.getTime() + (input.leaseMs ?? 5 * 60_000),
  );
  return client.$transaction(async (transaction) => {
    const claimed = await transaction.analyticsDeletionOperation.updateMany({
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
        workerFence: { increment: 1 },
        leaseOwner: input.owner,
        leaseExpiresAt,
      },
    });
    if (claimed.count !== 1) return null;
    return transaction.analyticsDeletionOperation.findFirst({
      where: {
        id: input.operationId,
        projectId: input.projectId,
        leaseOwner: input.owner,
      },
    });
  });
}

export async function renewDeletionOperationLease(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly lease: AnalyticsDeletionLease;
  readonly leaseMs?: number;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const renewed = await client.analyticsDeletionOperation.updateMany({
    where: {
      id: input.operationId,
      projectId: input.projectId,
      workerFence: input.lease.fence,
      leaseOwner: input.lease.owner,
      status: { not: "COMPLETED" },
    },
    data: {
      leaseExpiresAt: new Date(now.getTime() + (input.leaseMs ?? 5 * 60_000)),
    },
  });
  return renewed.count === 1;
}

function statusExpiry(createdAt: Date): Date {
  return new Date(createdAt.getTime() + STATUS_RETENTION_MS);
}

function isRetryableTransactionError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error.code === "P2034" || error.code === "P2002")
  );
}

async function serializable<T>(
  client: PrismaClient,
  callback: (transaction: Prisma.TransactionClient) => Promise<T>,
): Promise<T> {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    try {
      return await client.$transaction(callback, {
        isolationLevel: "Serializable",
      });
    } catch (error) {
      if (!isRetryableTransactionError(error) || attempt === 2) throw error;
    }
  }
  throw new Error("Unreachable deletion transaction state");
}

export async function scheduleTraceDeletionOperations(input: {
  readonly client?: PrismaClient;
  readonly projectId: string;
  readonly organizationId: string;
  readonly traceIds: readonly string[];
  readonly requester: AnalyticsDeletionRequester;
  readonly now?: Date;
}): Promise<readonly ScheduledTraceDeletion[]> {
  if (
    !input.projectId ||
    !input.organizationId ||
    input.traceIds.length === 0
  ) {
    throw new TypeError("Invalid trace deletion request");
  }
  const traceIds = [...new Set(input.traceIds)];
  if (traceIds.some((traceId) => !traceId)) {
    throw new TypeError("Invalid trace deletion request");
  }
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();

  return serializable(client, async (transaction) => {
    const checkpointGeneration =
      await getActiveCheckpointGenerationForAcceptance({ transaction, now });
    const projectDeletion =
      await transaction.analyticsProjectDeletionGeneration.findUnique({
        where: { projectId: input.projectId },
        select: { generation: true },
      });
    if (projectDeletion) {
      throw new AnalyticsProjectDeletionInProgressError();
    }
    await transaction.project.findFirstOrThrow({
      where: {
        id: input.projectId,
        orgId: input.organizationId,
        deletedAt: null,
      },
      select: { id: true },
    });
    const scheduled: ScheduledTraceDeletion[] = [];
    for (const traceId of traceIds) {
      const current = await transaction.analyticsDeletionTombstone.findUnique({
        where: {
          projectId_traceId: { projectId: input.projectId, traceId },
        },
      });
      const existing = current
        ? await transaction.analyticsDeletionOperation.findFirst({
            where: {
              projectId: input.projectId,
              traceId,
              scope: "TRACE",
              generation: current.generation,
            },
            orderBy: { createdAt: "desc" },
          })
        : null;
      if (current && existing) {
        scheduled.push({
          operation: existing,
          traceId,
          generation: current.generation,
        });
        continue;
      }

      const generation = current?.generation ?? 1n;
      if (!current) {
        await transaction.analyticsDeletionTombstone.create({
          data: {
            projectId: input.projectId,
            traceId,
            generation,
            status: "RETRYING",
          },
        });
      }
      const operation = await transaction.analyticsDeletionOperation.create({
        data: {
          id: randomUUID(),
          scope: "TRACE",
          organizationId: input.organizationId,
          projectId: input.projectId,
          traceId,
          generation,
          checkpointGeneration,
          requesterPrincipalType: input.requester.principalType,
          requesterPrincipalId: input.requester.principalId,
          status:
            current?.barrierVisibleAt && current.status !== "RETRYING"
              ? current.status
              : "RETRYING",
          phase: current?.barrierVisibleAt
            ? "materialized_cleanup"
            : "visibility_barrier",
          logicallyInvisible: Boolean(current?.barrierVisibleAt),
          statusExpiresAt: statusExpiry(now),
          createdAt: now,
        },
      });
      scheduled.push({ operation, traceId, generation });
    }
    return scheduled;
  });
}

export async function scheduleProjectDeletionOperation(input: {
  readonly client?: PrismaClient;
  readonly projectId: string;
  readonly organizationId: string;
  readonly requester: AnalyticsDeletionRequester;
  readonly now?: Date;
}): Promise<AnalyticsDeletionOperation> {
  if (!input.projectId || !input.organizationId) {
    throw new TypeError("Invalid project deletion request");
  }
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  return serializable(client, async (transaction) => {
    const checkpointGeneration =
      await getActiveCheckpointGenerationForAcceptance({ transaction, now });
    await transaction.project.findFirstOrThrow({
      where: { id: input.projectId, orgId: input.organizationId },
      select: { id: true },
    });
    const current =
      await transaction.analyticsProjectDeletionGeneration.findUnique({
        where: { projectId: input.projectId },
      });
    const existing = current
      ? await transaction.analyticsDeletionOperation.findFirst({
          where: {
            projectId: input.projectId,
            scope: "PROJECT",
            generation: current.generation,
          },
          orderBy: { createdAt: "desc" },
        })
      : null;
    if (existing) return existing;

    const generation = current?.generation ?? 1n;
    if (!current) {
      await transaction.analyticsProjectDeletionGeneration.create({
        data: { projectId: input.projectId, generation },
      });
    }
    return transaction.analyticsDeletionOperation.create({
      data: {
        id: randomUUID(),
        scope: "PROJECT",
        organizationId: input.organizationId,
        projectId: input.projectId,
        generation,
        checkpointGeneration,
        requesterPrincipalType: input.requester.principalType,
        requesterPrincipalId: input.requester.principalId,
        status: "RETRYING",
        phase: "visibility_barrier",
        logicallyInvisible: false,
        statusExpiresAt: statusExpiry(now),
        createdAt: now,
      },
    });
  });
}

export async function markDeletionBarrierVisible(input: {
  readonly client?: PrismaClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly scope: AnalyticsDeletionScope;
  readonly traceId?: string;
  readonly generation: bigint;
  readonly barrierLabel: string;
  readonly lease?: AnalyticsDeletionLease;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  return client.$transaction(async (transaction) => {
    const operation = await transaction.analyticsDeletionOperation.updateMany({
      where: {
        id: input.operationId,
        projectId: input.projectId,
        scope: input.scope,
        generation: input.generation,
        ...(input.lease
          ? {
              workerFence: input.lease.fence,
              leaseOwner: input.lease.owner,
            }
          : {}),
      },
      data: {
        status: "SCHEDULED",
        phase: "ingestion_drain",
        logicallyInvisible: true,
        cancellationReasonCode: null,
      },
    });
    if (operation.count !== 1) return false;

    if (input.scope === "TRACE") {
      if (!input.traceId)
        throw new TypeError("Trace deletion requires traceId");
      const tombstone = await transaction.analyticsDeletionTombstone.updateMany(
        {
          where: {
            projectId: input.projectId,
            traceId: input.traceId,
            generation: input.generation,
          },
          data: {
            status: "SCHEDULED",
            barrierLabel: input.barrierLabel,
            barrierVisibleAt: now,
          },
        },
      );
      if (tombstone.count !== 1) {
        throw new Error("Trace deletion tombstone disappeared during barrier");
      }
    }
    return true;
  });
}

export async function markDeletionOperationRetrying(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly phase: string;
  readonly reasonCode: string;
  readonly logicallyInvisible: boolean;
  readonly lease?: AnalyticsDeletionLease;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const result = await client.analyticsDeletionOperation.updateMany({
    where: {
      id: input.operationId,
      projectId: input.projectId,
      status: { not: "COMPLETED" },
      ...(input.lease
        ? {
            workerFence: input.lease.fence,
            leaseOwner: input.lease.owner,
          }
        : {}),
    },
    data: {
      status: "RETRYING",
      phase: input.phase,
      logicallyInvisible: input.logicallyInvisible,
      cancellationReasonCode: input.reasonCode,
      leaseOwner: null,
      leaseExpiresAt: null,
    },
  });
  return result.count === 1;
}

export async function markDeletionOperationPhase(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly phase: string;
  readonly lease: AnalyticsDeletionLease;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const result = await client.analyticsDeletionOperation.updateMany({
    where: {
      id: input.operationId,
      projectId: input.projectId,
      workerFence: input.lease.fence,
      leaseOwner: input.lease.owner,
      status: { not: "COMPLETED" },
      logicallyInvisible: true,
    },
    data: {
      status: "SCHEDULED",
      phase: input.phase,
      cancellationReasonCode: null,
    },
  });
  return result.count === 1;
}

export async function completeDeletionOperation(input: {
  readonly client?: PrismaClient;
  readonly operationId: string;
  readonly projectId: string;
  readonly scope: AnalyticsDeletionScope;
  readonly traceId?: string;
  readonly generation: bigint;
  readonly lease?: AnalyticsDeletionLease;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  return client.$transaction(async (transaction) => {
    const result = await transaction.analyticsDeletionOperation.updateMany({
      where: {
        id: input.operationId,
        projectId: input.projectId,
        scope: input.scope,
        generation: input.generation,
        logicallyInvisible: true,
        ...(input.lease
          ? {
              workerFence: input.lease.fence,
              leaseOwner: input.lease.owner,
            }
          : {}),
      },
      data: {
        status: "COMPLETED",
        phase: "completed",
        completedAt: now,
        cancellationReasonCode: null,
        leaseOwner: null,
        leaseExpiresAt: null,
      },
    });
    if (result.count !== 1) return false;

    if (input.scope === "TRACE") {
      if (!input.traceId)
        throw new TypeError("Trace deletion requires traceId");
      const tombstone = await transaction.analyticsDeletionTombstone.updateMany(
        {
          where: {
            projectId: input.projectId,
            traceId: input.traceId,
            generation: input.generation,
          },
          data: { status: "COMPLETED", completedAt: now },
        },
      );
      if (tombstone.count !== 1) {
        throw new Error(
          "Trace deletion tombstone disappeared during completion",
        );
      }
    }
    return true;
  });
}

export async function completeTraceDeletionsSupersededByProject(input: {
  readonly client?: PrismaClient;
  readonly projectOperationId: string;
  readonly projectId: string;
  readonly projectGeneration: bigint;
  readonly lease: AnalyticsDeletionLease;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  if (
    !input.projectOperationId ||
    !input.projectId ||
    input.projectGeneration <= 0n ||
    !input.lease.owner ||
    input.lease.fence <= 0n ||
    !Number.isFinite(now.getTime())
  ) {
    throw new TypeError("Invalid project deletion supersession");
  }
  return client.$transaction(async (transaction) => {
    const projectOperation =
      await transaction.analyticsDeletionOperation.findFirst({
        where: {
          id: input.projectOperationId,
          projectId: input.projectId,
          scope: "PROJECT",
          generation: input.projectGeneration,
          workerFence: input.lease.fence,
          leaseOwner: input.lease.owner,
          logicallyInvisible: true,
          status: { not: "COMPLETED" },
        },
        select: { id: true },
      });
    if (!projectOperation) return false;

    await transaction.analyticsDeletionOperation.updateMany({
      where: {
        projectId: input.projectId,
        scope: "TRACE",
        status: { not: "COMPLETED" },
      },
      data: {
        status: "COMPLETED",
        phase: "completed_by_project_deletion",
        logicallyInvisible: true,
        cancellationReasonCode: "SUPERSEDED_BY_PROJECT_DELETION",
        completedAt: now,
        leaseOwner: null,
        leaseExpiresAt: null,
      },
    });
    await transaction.analyticsDeletionTombstone.updateMany({
      where: { projectId: input.projectId, status: { not: "COMPLETED" } },
      data: { status: "COMPLETED", completedAt: now },
    });
    return true;
  });
}

export async function hasPreBarrierIngestionWork(input: {
  readonly client?: AnalyticsControlClient;
  readonly projectId: string;
  readonly barrierCreatedAt: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const count = await client.analyticsIngestionOperation.count({
    where: {
      projectId: input.projectId,
      acceptedAt: { lte: input.barrierCreatedAt },
      status: { in: [...ACTIVE_INGESTION_STATUSES] },
    },
  });
  return count > 0;
}

export function findDeletionOperationForProject(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly projectId: string;
}): Promise<AnalyticsDeletionOperation | null> {
  const client = input.client ?? prisma;
  return client.analyticsDeletionOperation.findFirst({
    where: { id: input.operationId, projectId: input.projectId },
  });
}

export function findLatestProjectDeletionOperation(input: {
  readonly client?: AnalyticsControlClient;
  readonly projectId: string;
  readonly organizationId: string;
}): Promise<AnalyticsDeletionOperation | null> {
  const client = input.client ?? prisma;
  return client.analyticsDeletionOperation.findFirst({
    where: {
      projectId: input.projectId,
      organizationId: input.organizationId,
      scope: "PROJECT",
    },
    orderBy: [{ generation: "desc" }, { createdAt: "desc" }],
  });
}

export function findRecoverableDeletionOperations(input: {
  readonly client?: AnalyticsControlClient;
  readonly scopes: readonly AnalyticsDeletionScope[];
  readonly updatedBefore: Date;
  readonly leaseExpiredBefore: Date;
  readonly limit: number;
}): Promise<readonly AnalyticsDeletionOperation[]> {
  if (
    input.scopes.length === 0 ||
    Number.isNaN(input.updatedBefore.getTime()) ||
    Number.isNaN(input.leaseExpiredBefore.getTime()) ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 1_000
  ) {
    throw new TypeError("Invalid deletion recovery query");
  }
  const client = input.client ?? prisma;
  return client.analyticsDeletionOperation.findMany({
    where: {
      scope: { in: [...input.scopes] },
      status: { in: ["RETRYING", "SCHEDULED"] },
      completedAt: null,
      updatedAt: { lte: input.updatedBefore },
      OR: [
        { leaseOwner: null },
        { leaseExpiresAt: null },
        { leaseExpiresAt: { lte: input.leaseExpiredBefore } },
      ],
    },
    orderBy: [{ updatedAt: "asc" }, { id: "asc" }],
    take: input.limit,
  });
}

export function getDeletionProgressForProject(input: {
  readonly client?: AnalyticsControlClient;
  readonly operationId: string;
  readonly projectId: string;
}): Promise<AnalyticsDeletionProgress | null> {
  return findDeletionOperationForProject(input).then((operation) =>
    operation
      ? {
          operationId: operation.id,
          projectId: operation.projectId,
          phase: operation.phase,
          logicallyInvisible: operation.logicallyInvisible,
        }
      : null,
  );
}
