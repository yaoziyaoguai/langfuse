import type {
  AnalyticsCandidateDisposition,
  AnalyticsEntityType,
  AnalyticsIngestionOperation,
  Prisma,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";

const SHA256_HEX = /^[a-f0-9]{64}$/;

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
          recoverableUntil: input.recoverableUntil,
          statusExpiresAt: input.statusExpiresAt,
        },
      });
      await transaction.analyticsIngestionOutbox.create({
        data: { operationId: created.id, nextAttemptAt: input.acceptedAt },
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
    const available = await transaction.analyticsIngestionOutbox.findMany({
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
      const updated = await transaction.analyticsIngestionOutbox.updateMany({
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
          await transaction.analyticsIngestionOutbox.findUniqueOrThrow({
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
  workerId: string;
  now: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  const updated = await client.analyticsIngestionOutbox.updateMany({
    where: {
      operationId: input.operationId,
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
    const current =
      await transaction.analyticsIngestionOperation.findFirstOrThrow({
        where: { id: input.operationId, projectId: input.projectId },
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

    const updated = await transaction.analyticsIngestionOperation.updateMany({
      where: {
        id: input.operationId,
        projectId: input.projectId,
        canonicalizationFence: input.fence,
        canonicalizationLeaseOwner: input.leaseOwner,
        reservedCanonicalObjectKey: input.canonicalObjectKey,
        canonicalObjectKey: null,
        manifestState: "PENDING",
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
    outbox: operation.outbox?.status ?? "PENDING",
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

export async function markAnalyticsIngestionRetrying(input: {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  reasonCode: string;
}): Promise<boolean> {
  if (
    !input.operationId ||
    !input.projectId ||
    !/^[A-Z0-9_]{1,64}$/.test(input.reasonCode)
  ) {
    throw new TypeError("Invalid analytics ingestion retry state");
  }
  const client = input.client ?? prisma;
  const updated = await client.analyticsIngestionOperation.updateMany({
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
  return updated.count === 1;
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
    const current =
      await transaction.analyticsIngestionOperation.findFirstOrThrow({
        where: { id: input.operationId, projectId: input.projectId },
      });
    if (current.manifestState === "FROZEN") {
      return { outcome: "already_frozen" as const, operation: current };
    }
    if (
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
