import { Prisma } from "@prisma/client";
import type {
  AnalyticsCheckpointGeneration,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";

type AnalyticsCheckpointClient = PrismaClient | Prisma.TransactionClient;

// All checkpoint creation and Doris mutation admission crosses this transaction
// lock. A receipt committed before the lock is part of the cut; a receipt that
// commits after it is tagged with the active generation and remains queued.
const ANALYTICS_CHECKPOINT_ADVISORY_LOCK = 7_681_221_833_480_511n;
const EPOCH = new Date(0);

const LOAD_TERMINAL_STATUSES = [
  "VISIBLE",
  "FAILED",
  "CANCELLED_BY_DELETION",
] as const;

export class AnalyticsCheckpointBusyError extends Error {
  constructor() {
    super("Analytics checkpoint is already active");
    this.name = "AnalyticsCheckpointBusyError";
  }
}

export class AnalyticsCheckpointReconciliationRequiredError extends Error {
  constructor() {
    super("Analytics checkpoint anchor reconciliation is required");
    this.name = "AnalyticsCheckpointReconciliationRequiredError";
  }
}

export async function acquireAnalyticsCheckpointTransactionLock(
  transaction: Prisma.TransactionClient,
): Promise<void> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock(${ANALYTICS_CHECKPOINT_ADVISORY_LOCK})::text AS locked`,
  );
}

async function findActiveCheckpoint(
  client: AnalyticsCheckpointClient,
  now: Date,
): Promise<AnalyticsCheckpointGeneration | null> {
  return client.analyticsCheckpointGeneration.findFirst({
    where: { status: "PREPARING", leaseExpiresAt: { gt: now } },
    orderBy: { generation: "desc" },
  });
}

export async function getActiveCheckpointGenerationForAcceptance(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly now: Date;
}): Promise<bigint> {
  await acquireAnalyticsCheckpointTransactionLock(input.transaction);
  const active = await findActiveCheckpoint(input.transaction, input.now);
  return active?.generation ?? 0n;
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
  throw new Error("Unreachable checkpoint transaction state");
}

export async function beginAnalyticsCheckpoint(input: {
  readonly client?: PrismaClient;
  readonly leaseOwner: string;
  readonly leaseMs: number;
  readonly now?: Date;
}): Promise<AnalyticsCheckpointGeneration> {
  if (
    !input.leaseOwner ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 10_000
  ) {
    throw new TypeError("Invalid analytics checkpoint lease");
  }
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  return serializable(client, async (transaction) => {
    await acquireAnalyticsCheckpointTransactionLock(transaction);
    await transaction.analyticsCheckpointGeneration.updateMany({
      where: {
        status: "PREPARING",
        leaseExpiresAt: { lte: now },
        manifestHash: null,
      },
      data: {
        status: "ABORTED",
        abortedAt: now,
        abortReasonCode: "CHECKPOINT_LEASE_EXPIRED",
      },
    });
    if (await findActiveCheckpoint(transaction, now)) {
      throw new AnalyticsCheckpointBusyError();
    }
    if (
      await transaction.analyticsCheckpointGeneration.findFirst({
        where: { status: "PREPARING" },
        select: { generation: true },
      })
    ) {
      throw new AnalyticsCheckpointReconciliationRequiredError();
    }

    const [generation, operation, load, deletion, predecessor] =
      await Promise.all([
        transaction.analyticsCheckpointGeneration.aggregate({
          _max: { generation: true },
        }),
        transaction.analyticsIngestionOperation.aggregate({
          _max: { acceptedAt: true, acceptedAtNanos: true },
        }),
        transaction.analyticsLoadBatch.aggregate({
          _max: { createdAt: true },
        }),
        transaction.analyticsDeletionOperation.aggregate({
          _max: { createdAt: true },
        }),
        transaction.analyticsCheckpointGeneration.findFirst({
          where: { status: "SEALED" },
          orderBy: { generation: "desc" },
          select: { manifestHash: true },
        }),
      ]);

    return transaction.analyticsCheckpointGeneration.create({
      data: {
        generation: (generation._max.generation ?? 0n) + 1n,
        status: "PREPARING",
        leaseOwner: input.leaseOwner,
        leaseExpiresAt: new Date(now.getTime() + input.leaseMs),
        operationHighWatermarkAcceptedAt: operation._max.acceptedAt ?? EPOCH,
        operationHighWatermarkAcceptedAtNanos:
          operation._max.acceptedAtNanos ?? 0n,
        loadHighWatermarkCreatedAt: load._max.createdAt ?? EPOCH,
        deletionHighWatermarkCreatedAt: deletion._max.createdAt ?? EPOCH,
        predecessorHash: predecessor?.manifestHash ?? null,
        createdAt: now,
      },
    });
  });
}

export async function findAnalyticsCheckpointPendingAnchor(input: {
  readonly client?: PrismaClient;
}): Promise<AnalyticsCheckpointGeneration | null> {
  return (input.client ?? prisma).analyticsCheckpointGeneration.findFirst({
    where: {
      status: "PREPARING",
      manifestHash: { not: null },
      signature: { not: null },
    },
    orderBy: { generation: "asc" },
  });
}

export async function claimAnalyticsCheckpointAnchorReconciliation(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly leaseMs: number;
  readonly now?: Date;
}): Promise<AnalyticsCheckpointGeneration | null> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  if (!input.leaseOwner || input.leaseMs < 10_000) {
    throw new TypeError("Invalid checkpoint anchor reconciliation lease");
  }
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsCheckpointTransactionLock(transaction);
    const claimed = await transaction.analyticsCheckpointGeneration.updateMany({
      where: {
        generation: input.generation,
        status: "PREPARING",
        manifestHash: { not: null },
        signature: { not: null },
        OR: [
          { leaseOwner: input.leaseOwner },
          { leaseExpiresAt: { lte: now } },
        ],
      },
      data: {
        leaseOwner: input.leaseOwner,
        leaseExpiresAt: new Date(now.getTime() + input.leaseMs),
      },
    });
    if (claimed.count !== 1) return null;
    return transaction.analyticsCheckpointGeneration.findUniqueOrThrow({
      where: { generation: input.generation },
    });
  });
}

export async function renewAnalyticsCheckpointLease(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly leaseMs: number;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  if (
    input.generation <= 0n ||
    !input.leaseOwner ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 10_000
  ) {
    throw new TypeError("Invalid analytics checkpoint renewal");
  }
  const renewed = await client.analyticsCheckpointGeneration.updateMany({
    where: {
      generation: input.generation,
      status: "PREPARING",
      leaseOwner: input.leaseOwner,
      leaseExpiresAt: { gt: now },
    },
    data: { leaseExpiresAt: new Date(now.getTime() + input.leaseMs) },
  });
  return renewed.count === 1;
}

export type AnalyticsMutationPermit =
  | {
      readonly outcome: "allowed";
      readonly checkpointGeneration: bigint | null;
    }
  | {
      readonly outcome: "held";
      readonly checkpointGeneration: bigint;
      readonly reasonCode: "CHECKPOINT_FENCE";
    };

export async function acquireAnalyticsMutationPermit(input: {
  readonly client?: PrismaClient;
  readonly mutation:
    | {
        readonly kind: "ingestion";
        readonly checkpointGeneration: bigint;
        readonly operationAcceptedAtNanos: bigint;
      }
    | {
        readonly kind: "deletion";
        readonly checkpointGeneration: bigint;
        readonly createdAt: Date;
      };
  readonly now?: Date;
}): Promise<AnalyticsMutationPermit> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsCheckpointTransactionLock(transaction);
    const active = await findActiveCheckpoint(transaction, now);
    if (!active) {
      return { outcome: "allowed", checkpointGeneration: null } as const;
    }
    const isPreCut =
      input.mutation.checkpointGeneration < active.generation &&
      (input.mutation.kind === "ingestion"
        ? input.mutation.operationAcceptedAtNanos <=
          active.operationHighWatermarkAcceptedAtNanos
        : input.mutation.createdAt <= active.deletionHighWatermarkCreatedAt);
    return isPreCut
      ? ({
          outcome: "allowed",
          checkpointGeneration: active.generation,
        } as const)
      : ({
          outcome: "held",
          checkpointGeneration: active.generation,
          reasonCode: "CHECKPOINT_FENCE",
        } as const);
  });
}

export async function getAnalyticsCheckpointDrainState(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
}): Promise<{
  readonly nonterminalOperations: number;
  readonly nonterminalLoads: number;
  readonly nonterminalDeletions: number;
  readonly drained: boolean;
}> {
  const client = input.client ?? prisma;
  const checkpoint =
    await client.analyticsCheckpointGeneration.findUniqueOrThrow({
      where: { generation: input.generation },
    });
  const operationCut = {
    checkpointGeneration: { lt: checkpoint.generation },
    acceptedAtNanos: {
      lte: checkpoint.operationHighWatermarkAcceptedAtNanos,
    },
  } as const;
  const [nonterminalOperations, nonterminalLoads, nonterminalDeletions] =
    await Promise.all([
      client.analyticsIngestionOperation.count({
        where: { ...operationCut, terminalAt: null },
      }),
      client.analyticsLoadBatch.count({
        where: {
          status: { notIn: [...LOAD_TERMINAL_STATUSES] },
          operation: operationCut,
        },
      }),
      client.analyticsDeletionOperation.count({
        where: {
          checkpointGeneration: { lt: checkpoint.generation },
          createdAt: { lte: checkpoint.deletionHighWatermarkCreatedAt },
          status: { not: "COMPLETED" },
        },
      }),
    ]);
  return {
    nonterminalOperations,
    nonterminalLoads,
    nonterminalDeletions,
    drained:
      nonterminalOperations === 0 &&
      nonterminalLoads === 0 &&
      nonterminalDeletions === 0,
  };
}

export async function recordAnalyticsCheckpointArtifacts(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly postgresSnapshotId: string;
  readonly postgresWalLsn: string;
  readonly dorisSnapshotId: string;
  readonly artifactDigests: Prisma.InputJsonValue;
  readonly manifest: Prisma.InputJsonValue;
  readonly keyId: string;
  readonly manifestHash: string;
  readonly signature: string;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const updated = await client.analyticsCheckpointGeneration.updateMany({
    where: {
      generation: input.generation,
      status: "PREPARING",
      leaseOwner: input.leaseOwner,
      leaseExpiresAt: { gt: now },
      manifestHash: null,
    },
    data: {
      postgresSnapshotId: input.postgresSnapshotId,
      postgresWalLsn: input.postgresWalLsn,
      dorisSnapshotId: input.dorisSnapshotId,
      artifactDigests: input.artifactDigests,
      manifest: input.manifest,
      keyId: input.keyId,
      manifestHash: input.manifestHash,
      signature: input.signature,
    },
  });
  return updated.count === 1;
}

export async function sealAnalyticsCheckpoint(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly manifestHash: string;
  readonly externalAnchorRef: string;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const updated = await client.analyticsCheckpointGeneration.updateMany({
    where: {
      generation: input.generation,
      status: "PREPARING",
      leaseOwner: input.leaseOwner,
      leaseExpiresAt: { gt: now },
      manifestHash: input.manifestHash,
      signature: { not: null },
    },
    data: {
      status: "SEALED",
      externalAnchorRef: input.externalAnchorRef,
      sealedAt: now,
    },
  });
  return updated.count === 1;
}

export async function abortAnalyticsCheckpoint(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly reasonCode: string;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const updated = await client.analyticsCheckpointGeneration.updateMany({
    where: {
      generation: input.generation,
      status: "PREPARING",
      leaseOwner: input.leaseOwner,
    },
    data: {
      status: "ABORTED",
      abortedAt: now,
      abortReasonCode: input.reasonCode,
    },
  });
  return updated.count === 1;
}
