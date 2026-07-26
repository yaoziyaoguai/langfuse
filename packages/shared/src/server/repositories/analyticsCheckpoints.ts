import { Prisma } from "@prisma/client";
import type {
  AnalyticsCheckpointGeneration,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import {
  lockAnalyticsAdmission,
  lockLegacyAnalyticsAdmission,
  type AnalyticsRuntimeAdmissionContext,
} from "../analytics-persistence/analyticsBackendAdmission";
import {
  analyticsDurableProvenanceFromRecord,
  analyticsDurableProvenanceMatches,
  analyticsProducerProvenanceFromAdmission,
  deserializeAnalyticsDurableProvenance,
  type AnalyticsDurableProvenance,
} from "../analytics-persistence/analyticsDurableProvenance";
import { acquireAnalyticsDeploymentSharedLock } from "./analyticsBackendDeployment";

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

async function acquireAnalyticsCheckpointTransactionSharedLock(
  transaction: Prisma.TransactionClient,
): Promise<void> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock_shared(${ANALYTICS_CHECKPOINT_ADVISORY_LOCK})::text AS locked`,
  );
}

async function databaseClock(
  transaction: Prisma.TransactionClient,
): Promise<Date> {
  const [row] = await transaction.$queryRaw<readonly { now: Date }[]>(
    Prisma.sql`SELECT clock_timestamp() AS now`,
  );
  if (!row) throw new Error("Postgres did not return its current timestamp");
  return row.now;
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

async function lockAnalyticsCheckpointGeneration(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly generation: bigint;
  readonly mode: "SHARE" | "UPDATE";
}): Promise<AnalyticsCheckpointGeneration | null> {
  const lock =
    input.mode === "SHARE" ? Prisma.sql`FOR SHARE` : Prisma.sql`FOR UPDATE`;
  await input.transaction.$queryRaw(
    Prisma.sql`SELECT generation FROM analytics_checkpoint_generations WHERE generation = ${input.generation} ${lock}`,
  );
  return input.transaction.analyticsCheckpointGeneration.findUnique({
    where: { generation: input.generation },
  });
}

function checkpointProvenance(
  checkpoint: AnalyticsCheckpointGeneration,
): AnalyticsDurableProvenance | null {
  return analyticsDurableProvenanceFromRecord(checkpoint);
}

async function assertCheckpointProvenanceIsCurrent(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly checkpoint: AnalyticsCheckpointGeneration;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly now?: Date;
}): Promise<void> {
  const originalProvenance = checkpointProvenance(input.checkpoint);
  if (!originalProvenance) {
    await lockLegacyAnalyticsAdmission(input.transaction);
    return;
  }
  if (!input.admissionContext) {
    throw new Error("Managed analytics checkpoint requires runtime admission");
  }
  const admission = await lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend: input.admissionContext.backend,
    expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
    action: "foundation",
    now: input.now,
  });
  const currentProvenance = analyticsProducerProvenanceFromAdmission(admission);
  if (
    currentProvenance.analyticsBackend !==
      originalProvenance.analyticsBackend ||
    currentProvenance.deploymentGeneration !==
      originalProvenance.deploymentGeneration ||
    currentProvenance.workloadEpochFingerprint !==
      originalProvenance.workloadEpochFingerprint ||
    currentProvenance.runtimeContractVersion !==
      originalProvenance.runtimeContractVersion
  ) {
    throw new Error("Analytics checkpoint provenance changed");
  }
}

async function captureCheckpointProvenance(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly now: Date;
}): Promise<AnalyticsDurableProvenance | null> {
  if (!input.admissionContext) {
    await lockLegacyAnalyticsAdmission(input.transaction);
    return null;
  }
  return analyticsProducerProvenanceFromAdmission(
    await lockAnalyticsAdmission({
      transaction: input.transaction,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
      expectedBackend: input.admissionContext.backend,
      expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
      action: "foundation",
      now: input.now,
    }),
  );
}

function assertManifestProvenance(input: {
  readonly checkpoint: AnalyticsCheckpointGeneration;
  readonly manifest: Prisma.InputJsonValue;
}): void {
  const originalProvenance = checkpointProvenance(input.checkpoint);
  if (
    typeof input.manifest !== "object" ||
    input.manifest === null ||
    Array.isArray(input.manifest)
  ) {
    throw new TypeError("Analytics checkpoint manifest must be an object");
  }
  const manifest = input.manifest as Record<string, unknown>;
  if (!originalProvenance) {
    if (
      manifest.analyticsProvenance !== undefined &&
      manifest.analyticsProvenance !== null
    ) {
      throw new Error("Legacy checkpoint manifest cannot add provenance");
    }
    return;
  }
  const manifestProvenance = deserializeAnalyticsDurableProvenance(
    manifest.analyticsProvenance,
  );
  if (
    !analyticsDurableProvenanceMatches(originalProvenance, manifestProvenance)
  ) {
    throw new Error("Analytics checkpoint manifest provenance changed");
  }
}

export async function getActiveCheckpointGenerationForAcceptance(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly now: Date;
}): Promise<bigint> {
  await acquireAnalyticsDeploymentSharedLock(input.transaction);
  await acquireAnalyticsCheckpointTransactionLock(input.transaction);
  const active = await findActiveCheckpoint(
    input.transaction,
    await databaseClock(input.transaction),
  );
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
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
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
  return serializable(client, async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireAnalyticsCheckpointTransactionLock(transaction);
    const now = await databaseClock(transaction);
    const expired = await transaction.analyticsCheckpointGeneration.findMany({
      where: {
        status: "PREPARING",
        leaseExpiresAt: { lte: now },
        manifestHash: null,
      },
    });
    for (const checkpoint of expired) {
      const locked = await lockAnalyticsCheckpointGeneration({
        transaction,
        generation: checkpoint.generation,
        mode: "UPDATE",
      });
      if (!locked) continue;
      await assertCheckpointProvenanceIsCurrent({
        transaction,
        checkpoint: locked,
        admissionContext: input.admissionContext,
        now,
      });
      await transaction.analyticsCheckpointGeneration.updateMany({
        where: {
          generation: locked.generation,
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
    }
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
    const provenance = await captureCheckpointProvenance({
      transaction,
      admissionContext: input.admissionContext,
      now,
    });

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
        analyticsBackend: provenance?.analyticsBackend,
        deploymentGeneration: provenance?.deploymentGeneration,
        workloadEpochFingerprint: provenance?.workloadEpochFingerprint,
        runtimeContractVersion: provenance?.runtimeContractVersion,
        producerRuntimeLeaseId: provenance?.producerRuntimeLeaseId,
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
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly now?: Date;
}): Promise<AnalyticsCheckpointGeneration | null> {
  const client = input.client ?? prisma;
  if (!input.leaseOwner || input.leaseMs < 10_000) {
    throw new TypeError("Invalid checkpoint anchor reconciliation lease");
  }
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireAnalyticsCheckpointTransactionLock(transaction);
    const checkpoint = await lockAnalyticsCheckpointGeneration({
      transaction,
      generation: input.generation,
      mode: "UPDATE",
    });
    const now = await databaseClock(transaction);
    if (
      !checkpoint ||
      checkpoint.status !== "PREPARING" ||
      checkpoint.manifestHash === null ||
      checkpoint.signature === null ||
      (checkpoint.leaseOwner !== input.leaseOwner &&
        checkpoint.leaseExpiresAt > now)
    ) {
      return null;
    }
    await assertCheckpointProvenanceIsCurrent({
      transaction,
      checkpoint,
      admissionContext: input.admissionContext,
      now,
    });
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
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  if (
    input.generation <= 0n ||
    !input.leaseOwner ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 10_000
  ) {
    throw new TypeError("Invalid analytics checkpoint renewal");
  }
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const checkpoint = await lockAnalyticsCheckpointGeneration({
      transaction,
      generation: input.generation,
      mode: "UPDATE",
    });
    const now = await databaseClock(transaction);
    if (
      !checkpoint ||
      checkpoint.status !== "PREPARING" ||
      checkpoint.leaseOwner !== input.leaseOwner ||
      checkpoint.leaseExpiresAt <= now
    ) {
      return false;
    }
    await assertCheckpointProvenanceIsCurrent({
      transaction,
      checkpoint,
      admissionContext: input.admissionContext,
      now,
    });
    const renewed = await transaction.analyticsCheckpointGeneration.updateMany({
      where: {
        generation: input.generation,
        status: "PREPARING",
        leaseOwner: input.leaseOwner,
        leaseExpiresAt: { gt: now },
      },
      data: { leaseExpiresAt: new Date(now.getTime() + input.leaseMs) },
    });
    return renewed.count === 1;
  });
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
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireAnalyticsCheckpointTransactionLock(transaction);
    // 生产调用方的事件时间可能来自偏斜的进程时钟，不能用于判断全局 fence。
    const now = await databaseClock(transaction);
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

/**
 * Retention 没有可供 checkpoint drain 的独立 batch ledger，因此锁必须由
 * 调用方所在事务一直持有到 Doris mutation visible 且 Postgres 状态提交。
 */
export async function acquireAnalyticsRetentionMutationPermit(input: {
  readonly transaction: Prisma.TransactionClient;
}): Promise<AnalyticsMutationPermit> {
  await acquireAnalyticsDeploymentSharedLock(input.transaction);
  await acquireAnalyticsCheckpointTransactionLock(input.transaction);
  const active = await findActiveCheckpoint(
    input.transaction,
    await databaseClock(input.transaction),
  );
  return active
    ? {
        outcome: "held",
        checkpointGeneration: active.generation,
        reasonCode: "CHECKPOINT_FENCE",
      }
    : { outcome: "allowed", checkpointGeneration: null };
}

export async function getAnalyticsCheckpointDrainState(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly now?: Date;
}): Promise<{
  readonly nonterminalOperations: number;
  readonly nonterminalLoads: number;
  readonly nonterminalDeletions: number;
  readonly drained: boolean;
}> {
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const checkpoint = await lockAnalyticsCheckpointGeneration({
      transaction,
      generation: input.generation,
      mode: "SHARE",
    });
    if (!checkpoint) throw new Error("Analytics checkpoint does not exist");
    const now = await databaseClock(transaction);
    await assertCheckpointProvenanceIsCurrent({
      transaction,
      checkpoint,
      admissionContext: input.admissionContext,
      now,
    });
    const operationCut = {
      checkpointGeneration: { lt: checkpoint.generation },
      acceptedAtNanos: {
        lte: checkpoint.operationHighWatermarkAcceptedAtNanos,
      },
    } as const;
    const [nonterminalOperations, nonterminalLoads, nonterminalDeletions] =
      await Promise.all([
        transaction.analyticsIngestionOperation.count({
          where: { ...operationCut, terminalAt: null },
        }),
        transaction.analyticsLoadBatch.count({
          where: {
            status: { notIn: [...LOAD_TERMINAL_STATUSES] },
            operation: operationCut,
          },
        }),
        transaction.analyticsDeletionOperation.count({
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
  });
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
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const checkpoint = await lockAnalyticsCheckpointGeneration({
      transaction,
      generation: input.generation,
      mode: "UPDATE",
    });
    const now = await databaseClock(transaction);
    if (
      !checkpoint ||
      checkpoint.status !== "PREPARING" ||
      checkpoint.leaseOwner !== input.leaseOwner ||
      checkpoint.leaseExpiresAt <= now ||
      checkpoint.manifestHash !== null
    ) {
      return false;
    }
    await assertCheckpointProvenanceIsCurrent({
      transaction,
      checkpoint,
      admissionContext: input.admissionContext,
      now,
    });
    assertManifestProvenance({ checkpoint, manifest: input.manifest });
    const updated = await transaction.analyticsCheckpointGeneration.updateMany({
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
  });
}

export async function sealAnalyticsCheckpoint(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly manifestHash: string;
  readonly externalAnchorRef: string;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const checkpoint = await lockAnalyticsCheckpointGeneration({
      transaction,
      generation: input.generation,
      mode: "UPDATE",
    });
    const now = await databaseClock(transaction);
    if (
      !checkpoint ||
      checkpoint.status !== "PREPARING" ||
      checkpoint.leaseOwner !== input.leaseOwner ||
      checkpoint.leaseExpiresAt <= now ||
      checkpoint.manifestHash !== input.manifestHash ||
      checkpoint.signature === null
    ) {
      return false;
    }
    await assertCheckpointProvenanceIsCurrent({
      transaction,
      checkpoint,
      admissionContext: input.admissionContext,
      now,
    });
    const updated = await transaction.analyticsCheckpointGeneration.updateMany({
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
  });
}

export async function abortAnalyticsCheckpoint(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly reasonCode: string;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly now?: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const checkpoint = await lockAnalyticsCheckpointGeneration({
      transaction,
      generation: input.generation,
      mode: "UPDATE",
    });
    const now = await databaseClock(transaction);
    if (
      !checkpoint ||
      checkpoint.status !== "PREPARING" ||
      checkpoint.leaseOwner !== input.leaseOwner
    ) {
      return false;
    }
    await assertCheckpointProvenanceIsCurrent({
      transaction,
      checkpoint,
      admissionContext: input.admissionContext,
      now,
    });
    const updated = await transaction.analyticsCheckpointGeneration.updateMany({
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
  });
}

export async function withAnalyticsCheckpointIoFence<T>(input: {
  readonly client?: PrismaClient;
  readonly generation: bigint;
  readonly leaseOwner: string;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext;
  readonly transactionTimeoutMs: number;
  readonly execute: () => Promise<T>;
  readonly now?: Date;
}): Promise<T> {
  if (
    input.generation <= 0n ||
    !input.leaseOwner ||
    !Number.isSafeInteger(input.transactionTimeoutMs) ||
    input.transactionTimeoutMs < 1_000
  ) {
    throw new TypeError("Invalid analytics checkpoint IO fence");
  }
  const client = input.client ?? prisma;
  return client.$transaction(
    async (transaction) => {
      await acquireAnalyticsDeploymentSharedLock(transaction);
      // 整组 artifact capture 在同一事务内持有 shared lock；retention mutation
      // 需要 exclusive lock，因此不会插入不同 artifact 的采集间隙。
      await acquireAnalyticsCheckpointTransactionSharedLock(transaction);
      const checkpoint = await lockAnalyticsCheckpointGeneration({
        transaction,
        generation: input.generation,
        mode: "SHARE",
      });
      const now = await databaseClock(transaction);
      if (
        !checkpoint ||
        checkpoint.status !== "PREPARING" ||
        checkpoint.leaseOwner !== input.leaseOwner ||
        checkpoint.leaseExpiresAt <= now
      ) {
        throw new Error("Analytics checkpoint IO lease is not active");
      }
      await assertCheckpointProvenanceIsCurrent({
        transaction,
        checkpoint,
        admissionContext: input.admissionContext,
        now,
      });
      return input.execute();
    },
    { timeout: input.transactionTimeoutMs },
  );
}
