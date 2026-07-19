import { createHash, randomUUID } from "node:crypto";

import type {
  AnalyticsCandidateDisposition,
  AnalyticsEntityType,
  AnalyticsIngestionOperation,
  AnalyticsIngestionOperationStatus,
  Prisma,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import { getActiveCheckpointGenerationForAcceptance } from "./analyticsCheckpoints";
import {
  findAndLockAnalyticsIngestionOperation,
  lockAnalyticsIngestionOperation,
} from "./analyticsIngestionLock";

const SHA256_HEX = /^[a-f0-9]{64}$/;
const RETRY_BASE_DELAY_MS = 5_000;
const RETRY_MAX_DELAY_MS = 30 * 60_000;
const LEGACY_HANDOFF_ADVISORY_LOCK_KEY = 181_865_275_000_002n;
const LEGACY_HANDOFF_LOCK_PREFIX = "doris-handoff:";
const LEGACY_HANDOFF_LOCK_MS = 5 * 60_000;

async function tryAcquireLegacyHandoffLock(
  transaction: Prisma.TransactionClient,
): Promise<boolean> {
  const [lock] = await transaction.$queryRaw<
    Array<{ acquired: boolean }>
  >`SELECT pg_try_advisory_xact_lock(${LEGACY_HANDOFF_ADVISORY_LOCK_KEY}) AS acquired`;
  return lock?.acquired === true;
}

export function calculateAnalyticsIngestionRetryDelayMs(input: {
  readonly operationId: string;
  readonly generation: number;
}): number {
  if (
    !input.operationId ||
    !Number.isSafeInteger(input.generation) ||
    input.generation < 1
  ) {
    throw new TypeError("Invalid analytics ingestion retry generation");
  }
  const nominal = Math.min(
    RETRY_MAX_DELAY_MS,
    RETRY_BASE_DELAY_MS * 2 ** Math.min(input.generation - 1, 20),
  );
  const entropy = createHash("sha256")
    .update(`${input.operationId}:${input.generation}`)
    .digest()
    .readUInt32BE(0);
  const jitter = 0.75 + (entropy / 0xffffffff) * 0.5;
  return Math.max(
    1_000,
    Math.min(RETRY_MAX_DELAY_MS, Math.round(nominal * jitter)),
  );
}

function isUniqueConstraintError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "P2002"
  );
}

export class AnalyticsIngestionReceiptConflictError extends Error {
  constructor() {
    super("Ingestion source operation conflicts with its receipt");
    this.name = "AnalyticsIngestionReceiptConflictError";
  }
}

type CreateAnalyticsIngestionReceiptInput = {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  sourceOperationId: string;
  sourceChecksum: string;
  rawObjectKey: string;
  acceptedAt: Date;
  acceptedAtNanos: bigint;
  canonicalizerVersion: string;
  schemaVersion: number;
  recoverableUntil: Date;
  statusExpiresAt: Date;
};

function receiptMatches(
  operation: AnalyticsIngestionOperation,
  input: CreateAnalyticsIngestionReceiptInput,
): boolean {
  return (
    operation.projectId === input.projectId &&
    operation.sourceOperationId === input.sourceOperationId &&
    operation.sourceChecksum === input.sourceChecksum &&
    operation.rawObjectKey === input.rawObjectKey &&
    operation.acceptedAt.getTime() === input.acceptedAt.getTime() &&
    operation.acceptedAtNanos === input.acceptedAtNanos &&
    operation.canonicalizerVersion === input.canonicalizerVersion &&
    operation.schemaVersion === input.schemaVersion &&
    operation.recoverableUntil.getTime() === input.recoverableUntil.getTime() &&
    operation.statusExpiresAt.getTime() === input.statusExpiresAt.getTime()
  );
}

export async function createAnalyticsIngestionReceipt(
  input: CreateAnalyticsIngestionReceiptInput,
): Promise<{
  readonly operation: AnalyticsIngestionOperation;
  readonly created: boolean;
}> {
  const client = input.client ?? prisma;
  if (
    !input.operationId ||
    !input.projectId ||
    !input.sourceOperationId ||
    !SHA256_HEX.test(input.sourceChecksum) ||
    !input.rawObjectKey ||
    !Number.isFinite(input.acceptedAt.getTime()) ||
    input.acceptedAtNanos < 0n ||
    input.acceptedAtNanos / 1_000_000n !== BigInt(input.acceptedAt.getTime()) ||
    !input.canonicalizerVersion ||
    !Number.isSafeInteger(input.schemaVersion) ||
    input.schemaVersion <= 0 ||
    input.recoverableUntil <= input.acceptedAt ||
    input.statusExpiresAt <= input.recoverableUntil
  ) {
    throw new TypeError("Invalid analytics ingestion receipt");
  }

  try {
    const operation = await client.$transaction(async (transaction) => {
      const checkpointGeneration =
        await getActiveCheckpointGenerationForAcceptance({
          transaction,
          now: input.acceptedAt,
        });
      const created = await transaction.analyticsIngestionOperation.create({
        data: {
          id: input.operationId,
          projectId: input.projectId,
          sourceOperationId: input.sourceOperationId,
          sourceChecksum: input.sourceChecksum,
          rawObjectKey: input.rawObjectKey,
          acceptedAt: input.acceptedAt,
          acceptedAtNanos: input.acceptedAtNanos,
          canonicalizerVersion: input.canonicalizerVersion,
          schemaVersion: input.schemaVersion,
          checkpointGeneration,
          recoverableUntil: input.recoverableUntil,
          statusExpiresAt: input.statusExpiresAt,
        },
      });
      await transaction.analyticsIngestionOutboxV2.create({
        data: {
          operationId: created.id,
          status: "PENDING",
          nextAttemptAt: input.acceptedAt,
        },
      });
      return created;
    });
    return { operation, created: true };
  } catch (error) {
    if (!isUniqueConstraintError(error)) throw error;
    const operation = await client.analyticsIngestionOperation.findFirst({
      where: {
        projectId: input.projectId,
        sourceOperationId: input.sourceOperationId,
      },
    });
    if (!operation || !receiptMatches(operation, input)) {
      throw new AnalyticsIngestionReceiptConflictError();
    }
    return { operation, created: false };
  }
}

export type CanonicalizationFenceReservation =
  | {
      readonly outcome: "reserved";
      readonly fence: bigint;
      readonly reservedObjectKey: string;
    }
  | {
      readonly outcome:
        | "already_published"
        | "leased"
        | "reconciliation_required"
        | "stale_fence";
      readonly operation: AnalyticsIngestionOperation;
    };

export async function reserveCanonicalizationFence(input: {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  expectedFence: bigint;
  nextFence: bigint;
  leaseOwner: string;
  leaseUntil: Date;
  now: Date;
  reservedObjectKey: string;
  confirmedAbsentObjectKey: string | null;
}): Promise<CanonicalizationFenceReservation> {
  const client = input.client ?? prisma;
  if (
    input.nextFence !== input.expectedFence + 1n ||
    !input.leaseOwner ||
    input.leaseUntil <= input.now ||
    !input.reservedObjectKey
  ) {
    throw new TypeError("Invalid canonicalization fence reservation");
  }

  const operation = await client.analyticsIngestionOperation.findFirstOrThrow({
    where: { id: input.operationId, projectId: input.projectId },
  });
  if (operation.terminalAt) {
    return { outcome: "stale_fence", operation };
  }
  if (operation.canonicalObjectKey) {
    return { outcome: "already_published", operation };
  }
  if (operation.canonicalizationFence !== input.expectedFence) {
    return { outcome: "stale_fence", operation };
  }
  if (
    operation.canonicalizationLeaseOwner !== input.leaseOwner &&
    operation.canonicalizationLeaseUntil &&
    operation.canonicalizationLeaseUntil > input.now
  ) {
    return { outcome: "leased", operation };
  }
  if (
    operation.reservedCanonicalObjectKey !== null &&
    operation.reservedCanonicalObjectKey !== input.confirmedAbsentObjectKey
  ) {
    return { outcome: "reconciliation_required", operation };
  }

  const updated = await client.analyticsIngestionOperation.updateMany({
    where: {
      id: input.operationId,
      projectId: input.projectId,
      canonicalizationFence: input.expectedFence,
      canonicalObjectKey: null,
      manifestState: "PENDING",
      terminalAt: null,
      reservedCanonicalObjectKey: operation.reservedCanonicalObjectKey,
      OR: [
        { canonicalizationLeaseOwner: input.leaseOwner },
        { canonicalizationLeaseUntil: null },
        { canonicalizationLeaseUntil: { lte: input.now } },
      ],
    },
    data: {
      canonicalizationFence: input.nextFence,
      canonicalizationLeaseOwner: input.leaseOwner,
      canonicalizationLeaseUntil: input.leaseUntil,
      canonicalizationAttempts: { increment: 1 },
      reservedCanonicalObjectKey: input.reservedObjectKey,
      status: "QUEUED",
    },
  });
  if (updated.count !== 1) {
    return {
      outcome: "stale_fence",
      operation: await client.analyticsIngestionOperation.findFirstOrThrow({
        where: { id: input.operationId, projectId: input.projectId },
      }),
    };
  }
  return {
    outcome: "reserved",
    fence: input.nextFence,
    reservedObjectKey: input.reservedObjectKey,
  };
}

export async function claimAnalyticsIngestionOutbox(input: {
  client?: PrismaClient;
  workerId: string;
  now: Date;
  lockedUntil: Date;
  limit: number;
}) {
  const client = input.client ?? prisma;
  if (
    !input.workerId ||
    input.lockedUntil <= input.now ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 100
  ) {
    throw new TypeError("Invalid analytics ingestion outbox claim");
  }

  return client.$transaction(async (transaction) => {
    const available = await transaction.analyticsIngestionOutboxV2.findMany({
      where: {
        status: "PENDING",
        nextAttemptAt: { lte: input.now },
        OR: [{ lockedUntil: null }, { lockedUntil: { lte: input.now } }],
      },
      orderBy: [{ nextAttemptAt: "asc" }, { createdAt: "asc" }],
      take: input.limit,
    });
    const claimed = [];
    for (const row of available) {
      const updated = await transaction.analyticsIngestionOutboxV2.updateMany({
        where: {
          id: row.id,
          status: "PENDING",
          nextAttemptAt: { lte: input.now },
          OR: [{ lockedUntil: null }, { lockedUntil: { lte: input.now } }],
        },
        data: {
          lockedBy: input.workerId,
          lockedUntil: input.lockedUntil,
          attempts: { increment: 1 },
        },
      });
      if (updated.count === 1) {
        claimed.push(
          await transaction.analyticsIngestionOutboxV2.findUniqueOrThrow({
            where: { id: row.id },
            include: { operation: true },
          }),
        );
      }
    }
    return claimed;
  });
}

export async function markAnalyticsIngestionOutboxPublished(input: {
  client?: PrismaClient;
  operationId: string;
  generation: number;
  workerId: string;
  now: Date;
}): Promise<boolean> {
  if (
    !input.operationId ||
    !Number.isSafeInteger(input.generation) ||
    input.generation < 1 ||
    !input.workerId ||
    !Number.isFinite(input.now.getTime())
  ) {
    throw new TypeError("Invalid analytics ingestion outbox publication");
  }
  const client = input.client ?? prisma;
  const updated = await client.analyticsIngestionOutboxV2.updateMany({
    where: {
      operationId: input.operationId,
      generation: input.generation,
      status: "PENDING",
      lockedBy: input.workerId,
      lockedUntil: { gt: input.now },
    },
    data: {
      status: "PUBLISHED",
      publishedAt: input.now,
      lockedBy: null,
      lockedUntil: null,
    },
  });
  return updated.count === 1;
}

export async function recoverStalePublishedAnalyticsIngestionOutbox(input: {
  client?: PrismaClient;
  now: Date;
  updatedBefore: Date;
  limit: number;
}): Promise<number> {
  if (
    !Number.isFinite(input.now.getTime()) ||
    !Number.isFinite(input.updatedBefore.getTime()) ||
    input.updatedBefore >= input.now ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 100
  ) {
    throw new TypeError("Invalid stale analytics ingestion outbox recovery");
  }
  const client = input.client ?? prisma;
  const candidates = await client.analyticsIngestionOutboxV2.findMany({
    where: {
      status: "PUBLISHED",
      updatedAt: { lte: input.updatedBefore },
      operation: {
        terminalAt: null,
        updatedAt: { lte: input.updatedBefore },
        loadBatches: {
          none: {
            OR: [
              { updatedAt: { gt: input.updatedBefore } },
              { leaseExpiresAt: { gt: input.now } },
            ],
          },
        },
      },
    },
    orderBy: [{ updatedAt: "asc" }, { id: "asc" }],
    take: input.limit,
    select: {
      operationId: true,
      generation: true,
      operation: { select: { projectId: true } },
    },
  });

  let recovered = 0;
  for (const candidate of candidates) {
    const reset = await client.$transaction(async (transaction) => {
      const operation = await findAndLockAnalyticsIngestionOperation(
        transaction,
        {
          operationId: candidate.operationId,
          projectId: candidate.operation.projectId,
        },
      );
      if (
        !operation ||
        operation.terminalAt !== null ||
        operation.updatedAt > input.updatedBefore
      ) {
        return false;
      }
      const activeLoadProgress = await transaction.analyticsLoadBatch.count({
        where: {
          operationId: candidate.operationId,
          projectId: candidate.operation.projectId,
          OR: [
            { updatedAt: { gt: input.updatedBefore } },
            { leaseExpiresAt: { gt: input.now } },
          ],
        },
      });
      if (activeLoadProgress > 0) return false;
      const updated = await transaction.analyticsIngestionOutboxV2.updateMany({
        where: {
          operationId: candidate.operationId,
          generation: candidate.generation,
          status: "PUBLISHED",
          updatedAt: { lte: input.updatedBefore },
        },
        data: {
          status: "PENDING",
          nextAttemptAt: input.now,
          lockedBy: null,
          lockedUntil: null,
          publishedAt: null,
        },
      });
      return updated.count === 1;
    });
    if (reset) recovered += 1;
  }
  return recovered;
}

export async function handoffLegacyAnalyticsIngestionOutbox(input: {
  client?: PrismaClient;
  now: Date;
  limit: number;
}): Promise<number> {
  if (
    !Number.isFinite(input.now.getTime()) ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 100
  ) {
    throw new TypeError("Invalid legacy analytics ingestion outbox handoff");
  }
  const client = input.client ?? prisma;
  const claimId = `${LEGACY_HANDOFF_LOCK_PREFIX}${randomUUID()}`;
  const claimUntil = new Date(input.now.getTime() + LEGACY_HANDOFF_LOCK_MS);
  const candidates = await client.$transaction(
    async (transaction) => {
      // The advisory lock serializes only the short batch claim. Row leases
      // then distribute work across replicas and recover after a crash.
      if (!(await tryAcquireLegacyHandoffLock(transaction))) return [];
      const available = await transaction.analyticsIngestionOutbox.findMany({
        where: {
          operation: { terminalAt: null },
          OR: [
            { lockedBy: null },
            {
              lockedBy: {
                not: { startsWith: LEGACY_HANDOFF_LOCK_PREFIX },
              },
            },
            { lockedUntil: null },
            { lockedUntil: { lte: input.now } },
          ],
        },
        orderBy: [{ updatedAt: "asc" }, { id: "asc" }],
        take: input.limit,
        select: {
          id: true,
          operationId: true,
          operation: { select: { projectId: true } },
        },
      });
      if (available.length === 0) return [];
      await transaction.analyticsIngestionOutbox.updateMany({
        where: { id: { in: available.map(({ id }) => id) } },
        data: { lockedBy: claimId, lockedUntil: claimUntil },
      });
      return available;
    },
    { timeout: 10_000 },
  );

  let handedOff = 0;
  for (const candidate of candidates) {
    const moved = await client.$transaction(
      async (transaction) => {
        const operation = await findAndLockAnalyticsIngestionOperation(
          transaction,
          {
            operationId: candidate.operationId,
            projectId: candidate.operation.projectId,
          },
        );
        if (!operation || operation.terminalAt !== null) return false;

        const legacy = await transaction.analyticsIngestionOutbox.findUnique({
          where: { operationId: candidate.operationId },
          select: { id: true, attempts: true, lockedBy: true },
        });
        if (!legacy || legacy.lockedBy !== claimId) return false;

        const current = await transaction.analyticsIngestionOutboxV2.findUnique(
          {
            where: { operationId: candidate.operationId },
            select: { id: true },
          },
        );
        if (!current) {
          await transaction.analyticsIngestionOutboxV2.create({
            data: {
              operationId: candidate.operationId,
              status: "PENDING",
              generation: 1,
              attempts: legacy.attempts,
              nextAttemptAt: input.now,
            },
          });
        }

        const deleted = await transaction.analyticsIngestionOutbox.deleteMany({
          where: { id: legacy.id, operationId: candidate.operationId },
        });
        return deleted.count === 1;
      },
      { timeout: 10_000 },
    );
    if (moved) handedOff += 1;
  }
  return handedOff;
}

export type AnalyticsIngestionCandidateInput = {
  readonly candidateKey: string;
  readonly entityType: AnalyticsEntityType;
  readonly entityKey: string;
  readonly owningTraceId: string | null;
  readonly partitionDate: Date;
  readonly sourceVersion: bigint;
  readonly canonicalPayloadHash: string;
  readonly traceDeletionGeneration: bigint;
  readonly projectDeletionGeneration: bigint;
};

export async function publishCanonicalArtifact(input: {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  fence: bigint;
  leaseOwner: string;
  canonicalObjectKey: string;
  artifactChecksum: string;
  candidates: readonly AnalyticsIngestionCandidateInput[];
}): Promise<{
  readonly outcome: "published" | "already_published" | "stale_fence";
  readonly operation: AnalyticsIngestionOperation;
}> {
  const client = input.client ?? prisma;
  const candidateKeys = input.candidates.map(
    ({ candidateKey }) => candidateKey,
  );
  if (
    input.fence <= 0n ||
    !input.leaseOwner ||
    !input.canonicalObjectKey ||
    !SHA256_HEX.test(input.artifactChecksum) ||
    input.candidates.length === 0 ||
    new Set(candidateKeys).size !== candidateKeys.length ||
    input.candidates.some(
      (candidate) =>
        !candidate.candidateKey ||
        !candidate.entityKey ||
        !SHA256_HEX.test(candidate.canonicalPayloadHash) ||
        candidate.traceDeletionGeneration < 0n ||
        candidate.projectDeletionGeneration < 0n,
    )
  ) {
    throw new TypeError("Invalid canonical artifact publication");
  }

  return client.$transaction(async (transaction) => {
    const current = await lockAnalyticsIngestionOperation(transaction, {
      operationId: input.operationId,
      projectId: input.projectId,
    });
    if (current.canonicalObjectKey !== null) {
      if (
        current.canonicalObjectKey !== input.canonicalObjectKey ||
        current.canonicalArtifactChecksum !== input.artifactChecksum
      ) {
        throw new AnalyticsIngestionReceiptConflictError();
      }
      return { outcome: "already_published" as const, operation: current };
    }
    if (current.terminalAt) {
      return { outcome: "stale_fence" as const, operation: current };
    }

    const updated = await transaction.analyticsIngestionOperation.updateMany({
      where: {
        id: input.operationId,
        projectId: input.projectId,
        canonicalizationFence: input.fence,
        canonicalizationLeaseOwner: input.leaseOwner,
        reservedCanonicalObjectKey: input.canonicalObjectKey,
        canonicalObjectKey: null,
        manifestState: "PENDING",
        terminalAt: null,
      },
      data: {
        canonicalObjectKey: input.canonicalObjectKey,
        canonicalArtifactChecksum: input.artifactChecksum,
        candidateManifest: {
          count: input.candidates.length,
          keys: [...candidateKeys].sort(),
        },
        manifestState: "CANDIDATE_PUBLISHED",
        canonicalizationLeaseOwner: null,
        canonicalizationLeaseUntil: null,
        status: "QUEUED",
      },
    });
    if (updated.count !== 1) {
      return { outcome: "stale_fence" as const, operation: current };
    }

    await transaction.analyticsIngestionCandidate.createMany({
      data: input.candidates.map((candidate) => ({
        operationId: input.operationId,
        projectId: input.projectId,
        candidateKey: candidate.candidateKey,
        entityType: candidate.entityType,
        entityKey: candidate.entityKey,
        owningTraceId: candidate.owningTraceId,
        partitionDate: candidate.partitionDate,
        sourceVersion: candidate.sourceVersion,
        canonicalPayloadHash: candidate.canonicalPayloadHash,
        traceDeletionGeneration: candidate.traceDeletionGeneration,
        projectDeletionGeneration: candidate.projectDeletionGeneration,
      })),
    });

    return {
      outcome: "published" as const,
      operation: await transaction.analyticsIngestionOperation.findFirstOrThrow(
        {
          where: { id: input.operationId, projectId: input.projectId },
        },
      ),
    };
  });
}

export function findAnalyticsIngestionOperationForProject(input: {
  client?: PrismaClient | Prisma.TransactionClient;
  operationId: string;
  projectId: string;
}) {
  const client = input.client ?? prisma;
  return client.analyticsIngestionOperation.findFirst({
    where: { id: input.operationId, projectId: input.projectId },
    include: {
      outboxV2: true,
      candidates: { orderBy: { candidateKey: "asc" } },
      loadBatches: { orderBy: { logicalBatchId: "asc" } },
    },
  });
}

export async function getAnalyticsIngestionStatusForProject(input: {
  client?: PrismaClient | Prisma.TransactionClient;
  operationId: string;
  projectId: string;
}) {
  const client = input.client ?? prisma;
  const operation = await client.analyticsIngestionOperation.findFirst({
    where: { id: input.operationId, projectId: input.projectId },
    select: {
      id: true,
      projectId: true,
      status: true,
      manifestState: true,
      acceptedAt: true,
      recoverableUntil: true,
      statusExpiresAt: true,
      visibleAt: true,
      terminalAt: true,
      cancellationReasonCode: true,
      lastErrorCode: true,
      outbox: { select: { status: true } },
      outboxV2: { select: { status: true } },
      candidates: {
        orderBy: { candidateKey: "asc" },
        select: {
          candidateKey: true,
          entityType: true,
          entityKey: true,
          owningTraceId: true,
          disposition: true,
          loadBatchId: true,
          reasonCode: true,
        },
      },
      loadBatches: {
        orderBy: [{ targetTable: "asc" }, { logicalBatchId: "asc" }],
        select: {
          id: true,
          targetTable: true,
          status: true,
          totalRows: true,
          filteredRows: true,
          lastErrorCode: true,
          visibleAt: true,
        },
      },
    },
  });
  if (!operation) return null;

  return {
    projectId: operation.projectId,
    operationId: operation.id,
    status: operation.status,
    manifest: operation.manifestState,
    outbox: operation.outboxV2?.status ?? operation.outbox?.status ?? "PENDING",
    acceptedAt: operation.acceptedAt,
    recoverableUntil: operation.recoverableUntil,
    statusExpiresAt: operation.statusExpiresAt,
    visibleAt: operation.visibleAt,
    terminalAt: operation.terminalAt,
    reasonCode:
      operation.cancellationReasonCode ?? operation.lastErrorCode ?? null,
    candidates: operation.candidates,
    loads: operation.loadBatches,
  };
}

async function terminalizeAnalyticsIngestion(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly operation: AnalyticsIngestionOperation;
  readonly fallbackStatus: "QUARANTINED" | "UNRECOVERABLE";
  readonly reasonCode: string;
  readonly now: Date;
}): Promise<boolean> {
  const terminalLoads = await input.transaction.analyticsLoadBatch.findMany({
    where: {
      operationId: input.operation.id,
      projectId: input.operation.projectId,
      status: { in: ["PENDING", "FAILED"] },
    },
    select: { id: true },
  });
  const terminalLoadIds = terminalLoads.map(({ id }) => id);
  await input.transaction.analyticsIngestionCandidate.updateMany({
    where: {
      operationId: input.operation.id,
      projectId: input.operation.projectId,
      disposition: { in: ["PENDING", "LOAD_REQUIRED"] },
      ...(terminalLoadIds.length > 0
        ? {
            OR: [
              { loadBatchId: null },
              { loadBatchId: { in: terminalLoadIds } },
            ],
          }
        : { loadBatchId: null }),
    },
    data: {
      disposition: "QUARANTINED",
      reasonCode: input.reasonCode,
      quarantineExpiresAt: input.operation.recoverableUntil,
    },
  });
  await input.transaction.analyticsLoadBatch.updateMany({
    where: {
      operationId: input.operation.id,
      projectId: input.operation.projectId,
      status: "PENDING",
    },
    data: {
      status: "FAILED",
      lastErrorCode: input.reasonCode,
      leaseOwner: null,
      leaseExpiresAt: null,
    },
  });

  let status: AnalyticsIngestionOperationStatus = input.fallbackStatus;
  let visibleAt: Date | null = null;
  if (input.operation.manifestState === "FROZEN") {
    const [candidates, visibleLoads] = await Promise.all([
      input.transaction.analyticsIngestionCandidate.findMany({
        where: {
          operationId: input.operation.id,
          projectId: input.operation.projectId,
        },
        select: { disposition: true, loadBatchId: true },
      }),
      input.transaction.analyticsLoadBatch.findMany({
        where: {
          operationId: input.operation.id,
          projectId: input.operation.projectId,
          status: "VISIBLE",
        },
        select: { id: true },
      }),
    ]);
    const visibleLoadIds = new Set(visibleLoads.map(({ id }) => id));
    const required = candidates.filter(
      ({ disposition }) => disposition === "LOAD_REQUIRED",
    );
    if (
      candidates.some(({ disposition }) => disposition === "PENDING") ||
      required.some(
        ({ loadBatchId }) =>
          loadBatchId === null || !visibleLoadIds.has(loadBatchId),
      )
    ) {
      throw new Error("Analytics ingestion terminal state is inconsistent");
    }
    const hasVisible = required.length > 0;
    const hasQuarantine = candidates.some(
      ({ disposition }) => disposition === "QUARANTINED",
    );
    const hasCancellation = candidates.some(
      ({ disposition }) => disposition === "CANCELLED_BY_DELETION",
    );
    status = hasQuarantine
      ? hasVisible
        ? "PARTIAL_FAILED"
        : input.fallbackStatus
      : hasCancellation
        ? hasVisible
          ? "COMPLETED_WITH_CANCELLATIONS"
          : "CANCELLED_BY_DELETION"
        : "VISIBLE";
    if (
      status === "VISIBLE" ||
      status === "PARTIAL_FAILED" ||
      status === "COMPLETED_WITH_CANCELLATIONS"
    ) {
      visibleAt = input.now;
    }
  }

  const updated =
    await input.transaction.analyticsIngestionOperation.updateMany({
      where: {
        id: input.operation.id,
        projectId: input.operation.projectId,
        terminalAt: null,
      },
      data: {
        status,
        lastErrorCode:
          status === "QUARANTINED" ||
          status === "UNRECOVERABLE" ||
          status === "PARTIAL_FAILED"
            ? input.reasonCode
            : null,
        visibleAt,
        terminalAt: input.now,
      },
    });
  return updated.count === 1;
}

export async function markAnalyticsIngestionTerminalFailure(input: {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  status: "QUARANTINED" | "UNRECOVERABLE";
  reasonCode: string;
  expectedGeneration: number;
  now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  if (
    !input.operationId ||
    !input.projectId ||
    !/^[A-Z0-9_]{1,64}$/.test(input.reasonCode) ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1 ||
    !Number.isFinite(now.getTime())
  ) {
    throw new TypeError("Invalid analytics ingestion terminal failure");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    const operation = await lockAnalyticsIngestionOperation(transaction, {
      operationId: input.operationId,
      projectId: input.projectId,
    });
    if (operation.terminalAt) return false;
    const outbox =
      await transaction.analyticsIngestionOutboxV2.findUniqueOrThrow({
        where: { operationId: input.operationId },
        select: { generation: true },
      });
    if (outbox.generation !== input.expectedGeneration) {
      return false;
    }
    const unresolvedLoads = await transaction.analyticsLoadBatch.count({
      where: {
        operationId: input.operationId,
        projectId: input.projectId,
        status: { in: ["LOADING", "UNKNOWN"] },
      },
    });
    if (unresolvedLoads > 0) return false;
    return terminalizeAnalyticsIngestion({
      transaction,
      operation,
      fallbackStatus: input.status,
      reasonCode: input.reasonCode,
      now,
    });
  });
}

export async function resolveAnalyticsIngestionAttemptFailure(input: {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  reasonCode: string;
  expectedGeneration: number;
  now?: Date;
  retryDelayMs?: number;
}): Promise<"requeued" | "terminalized" | "unchanged"> {
  const now = input.now ?? new Date();
  if (
    !input.operationId ||
    !input.projectId ||
    !/^[A-Z0-9_]{1,64}$/.test(input.reasonCode) ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1 ||
    !Number.isFinite(now.getTime()) ||
    (input.retryDelayMs !== undefined &&
      (!Number.isSafeInteger(input.retryDelayMs) || input.retryDelayMs < 1_000))
  ) {
    throw new TypeError("Invalid analytics ingestion attempt failure");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    const operation = await lockAnalyticsIngestionOperation(transaction, {
      operationId: input.operationId,
      projectId: input.projectId,
    });
    if (operation.terminalAt) return "unchanged" as const;
    const outbox =
      await transaction.analyticsIngestionOutboxV2.findUniqueOrThrow({
        where: { operationId: input.operationId },
        select: { generation: true },
      });
    if (outbox.generation !== input.expectedGeneration) {
      return "unchanged" as const;
    }
    const unresolvedLoads = await transaction.analyticsLoadBatch.count({
      where: {
        operationId: input.operationId,
        projectId: input.projectId,
        status: { in: ["LOADING", "UNKNOWN"] },
      },
    });
    if (unresolvedLoads > 0 || now < operation.recoverableUntil) {
      const updated = await transaction.analyticsIngestionOperation.updateMany({
        where: {
          id: input.operationId,
          projectId: input.projectId,
          terminalAt: null,
        },
        data: {
          status: "RETRYING",
          lastErrorCode: input.reasonCode,
        },
      });
      if (updated.count !== 1) return "unchanged" as const;
      const retryDelayMs =
        input.retryDelayMs ??
        calculateAnalyticsIngestionRetryDelayMs({
          operationId: input.operationId,
          generation: outbox.generation,
        });
      const requeued = await transaction.analyticsIngestionOutboxV2.updateMany({
        where: {
          operationId: input.operationId,
          generation: outbox.generation,
        },
        data: {
          generation: { increment: 1 },
          status: "PENDING",
          nextAttemptAt: new Date(now.getTime() + retryDelayMs),
          lockedBy: null,
          lockedUntil: null,
          publishedAt: null,
        },
      });
      if (requeued.count !== 1) {
        throw new Error("Analytics ingestion outbox disappeared during retry");
      }
      return "requeued" as const;
    }

    const terminalized = await terminalizeAnalyticsIngestion({
      transaction,
      operation,
      fallbackStatus: "UNRECOVERABLE",
      reasonCode: input.reasonCode,
      now,
    });
    return terminalized ? ("terminalized" as const) : ("unchanged" as const);
  });
}

type AnalyticsCandidateDispositionInput = {
  readonly candidateKey: string;
  readonly disposition: AnalyticsCandidateDisposition;
  readonly loadBatchId: string | null;
  readonly reasonCode: string | null;
  readonly quarantineExpiresAt: Date | null;
};

type AnalyticsLoadBatchManifestInput = {
  readonly id: string;
  readonly databaseName: string;
  readonly targetTable: string;
  readonly logicalBatchId: string;
  readonly attempt: number;
  readonly label: string;
  readonly payloadHash: string;
  readonly partitionDate: Date | null;
  readonly candidateKeys: readonly string[];
};

export class AnalyticsManifestInvariantError extends Error {
  constructor() {
    super("Analytics ingestion manifest is incomplete or inconsistent");
    this.name = "AnalyticsManifestInvariantError";
  }
}

const DORIS_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;
const DORIS_LABEL = /^[A-Za-z0-9_-]{1,128}$/;

export async function freezeAnalyticsIngestionManifest(input: {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  fence: bigint;
  canonicalObjectKey: string;
  dispositions: readonly AnalyticsCandidateDispositionInput[];
  loadBatches: readonly AnalyticsLoadBatchManifestInput[];
}): Promise<{
  readonly outcome: "frozen" | "already_frozen" | "stale_fence";
  readonly operation: AnalyticsIngestionOperation;
}> {
  const client = input.client ?? prisma;
  const dispositionKeys = input.dispositions.map(
    ({ candidateKey }) => candidateKey,
  );
  const loadBatchIds = input.loadBatches.map(({ id }) => id);
  if (
    input.fence <= 0n ||
    !input.canonicalObjectKey ||
    input.dispositions.length === 0 ||
    new Set(dispositionKeys).size !== dispositionKeys.length ||
    new Set(loadBatchIds).size !== loadBatchIds.length ||
    input.loadBatches.some(
      (batch) =>
        !batch.id ||
        !DORIS_IDENTIFIER.test(batch.databaseName) ||
        !DORIS_IDENTIFIER.test(batch.targetTable) ||
        !batch.logicalBatchId ||
        !Number.isSafeInteger(batch.attempt) ||
        batch.attempt < 0 ||
        !DORIS_LABEL.test(batch.label) ||
        !SHA256_HEX.test(batch.payloadHash) ||
        batch.candidateKeys.length === 0 ||
        new Set(batch.candidateKeys).size !== batch.candidateKeys.length,
    )
  ) {
    throw new AnalyticsManifestInvariantError();
  }

  const batchById = new Map(
    input.loadBatches.map((batch) => [batch.id, batch]),
  );
  const dispositionByKey = new Map(
    input.dispositions.map((disposition) => [
      disposition.candidateKey,
      disposition,
    ]),
  );
  for (const disposition of input.dispositions) {
    if (
      (disposition.disposition === "LOAD_REQUIRED") !==
        (disposition.loadBatchId !== null) ||
      (disposition.loadBatchId !== null &&
        !batchById.has(disposition.loadBatchId)) ||
      (disposition.disposition === "QUARANTINED" &&
        disposition.quarantineExpiresAt === null)
    ) {
      throw new AnalyticsManifestInvariantError();
    }
  }
  for (const batch of input.loadBatches) {
    if (
      batch.candidateKeys.some(
        (candidateKey) =>
          dispositionByKey.get(candidateKey)?.loadBatchId !== batch.id,
      )
    ) {
      throw new AnalyticsManifestInvariantError();
    }
  }
  const requiredKeys = input.dispositions
    .filter(({ disposition }) => disposition === "LOAD_REQUIRED")
    .map(({ candidateKey }) => candidateKey)
    .sort();
  const batchedKeys = input.loadBatches
    .flatMap(({ candidateKeys }) => candidateKeys)
    .sort();
  if (
    requiredKeys.length !== batchedKeys.length ||
    requiredKeys.some((key, index) => key !== batchedKeys[index])
  ) {
    throw new AnalyticsManifestInvariantError();
  }

  return client.$transaction(async (transaction) => {
    const current = await lockAnalyticsIngestionOperation(transaction, {
      operationId: input.operationId,
      projectId: input.projectId,
    });
    if (current.manifestState === "FROZEN") {
      return { outcome: "already_frozen" as const, operation: current };
    }
    if (
      current.terminalAt !== null ||
      current.canonicalizationFence !== input.fence ||
      current.canonicalObjectKey !== input.canonicalObjectKey ||
      current.manifestState !== "CANDIDATE_PUBLISHED"
    ) {
      return { outcome: "stale_fence" as const, operation: current };
    }

    const persistedCandidates =
      await transaction.analyticsIngestionCandidate.findMany({
        where: { operationId: input.operationId },
        orderBy: { candidateKey: "asc" },
      });
    const persistedKeys = persistedCandidates.map(
      ({ candidateKey }) => candidateKey,
    );
    if (
      persistedKeys.length !== dispositionKeys.length ||
      persistedKeys.some((key) => !dispositionByKey.has(key))
    ) {
      throw new AnalyticsManifestInvariantError();
    }

    await transaction.analyticsLoadBatch.createMany({
      data: input.loadBatches.map((batch) => ({
        id: batch.id,
        operationId: input.operationId,
        projectId: input.projectId,
        databaseName: batch.databaseName,
        targetTable: batch.targetTable,
        logicalBatchId: batch.logicalBatchId,
        attempt: batch.attempt,
        fenceGeneration: input.fence,
        label: batch.label,
        payloadHash: batch.payloadHash,
        canonicalObjectKey: input.canonicalObjectKey,
        partitionDate: batch.partitionDate,
      })),
    });

    for (const disposition of input.dispositions) {
      const updated = await transaction.analyticsIngestionCandidate.updateMany({
        where: {
          operationId: input.operationId,
          candidateKey: disposition.candidateKey,
          disposition: "PENDING",
        },
        data: {
          disposition: disposition.disposition,
          loadBatchId: disposition.loadBatchId,
          reasonCode: disposition.reasonCode,
          quarantineExpiresAt: disposition.quarantineExpiresAt,
        },
      });
      if (updated.count !== 1) throw new AnalyticsManifestInvariantError();
    }

    const frozenManifest = {
      candidates: [...input.dispositions]
        .sort((left, right) =>
          left.candidateKey < right.candidateKey
            ? -1
            : left.candidateKey > right.candidateKey
              ? 1
              : 0,
        )
        .map((disposition) => ({
          candidateKey: disposition.candidateKey,
          disposition: disposition.disposition,
          loadBatchId: disposition.loadBatchId,
          reasonCode: disposition.reasonCode,
        })),
      loadBatches: [...input.loadBatches]
        .sort((left, right) =>
          left.logicalBatchId < right.logicalBatchId
            ? -1
            : left.logicalBatchId > right.logicalBatchId
              ? 1
              : 0,
        )
        .map((batch) => ({
          id: batch.id,
          databaseName: batch.databaseName,
          targetTable: batch.targetTable,
          logicalBatchId: batch.logicalBatchId,
          attempt: batch.attempt,
          label: batch.label,
          payloadHash: batch.payloadHash,
          candidateKeys: [...batch.candidateKeys].sort(),
        })),
    } satisfies Prisma.InputJsonObject;

    const frozen = await transaction.analyticsIngestionOperation.updateMany({
      where: {
        id: input.operationId,
        projectId: input.projectId,
        canonicalizationFence: input.fence,
        canonicalObjectKey: input.canonicalObjectKey,
        manifestState: "CANDIDATE_PUBLISHED",
        terminalAt: null,
      },
      data: {
        manifestState: "FROZEN",
        frozenManifest,
        status: "PERSISTED",
      },
    });
    if (frozen.count !== 1) throw new AnalyticsManifestInvariantError();

    return {
      outcome: "frozen" as const,
      operation: await transaction.analyticsIngestionOperation.findFirstOrThrow(
        {
          where: { id: input.operationId, projectId: input.projectId },
        },
      ),
    };
  });
}
