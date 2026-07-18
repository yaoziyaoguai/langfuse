import type {
  AnalyticsDeletionOperation,
  AnalyticsDeletionStatus,
  AnalyticsEntityHead,
  AnalyticsEntityType,
  AnalyticsIngestionOperationStatus,
  Prisma,
  PrismaClient,
  TraceControlState,
} from "@prisma/client";

import { prisma } from "../../db";

type AnalyticsControlClient = PrismaClient | Prisma.TransactionClient;

type EntityHeadClaimOutcome =
  | "won"
  | "noop"
  | "superseded"
  | "conflict"
  | "partition_conflict"
  | "stale_fence";

export type AnalyticsEntityHeadClaimResult = {
  outcome: EntityHeadClaimOutcome;
  head: AnalyticsEntityHead;
};

type ClaimAnalyticsEntityHeadInput = {
  client?: AnalyticsControlClient;
  projectId: string;
  operationId: string;
  entityType: AnalyticsEntityType;
  entityKey: string;
  owningTraceId: string | null;
  expectedSourceVersion: bigint | null;
  sourceVersion: bigint;
  canonicalPayloadHash: string;
  partitionDate: Date;
  canonicalizerVersion: string;
  fenceGeneration: bigint;
  traceDeletionGeneration: bigint;
  projectDeletionGeneration: bigint;
};

function isUniqueConstraintError(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    error.code === "P2002"
  );
}

function samePartition(left: Date, right: Date): boolean {
  return left.toISOString().slice(0, 10) === right.toISOString().slice(0, 10);
}

function classifyEntityHeadClaim(
  current: AnalyticsEntityHead,
  candidate: ClaimAnalyticsEntityHeadInput,
): AnalyticsEntityHeadClaimResult {
  if (current.sourceVersion > candidate.sourceVersion) {
    return { outcome: "superseded", head: current };
  }

  if (
    samePartition(current.partitionDate, candidate.partitionDate) &&
    current.sourceVersion === candidate.sourceVersion &&
    current.canonicalPayloadHash === candidate.canonicalPayloadHash
  ) {
    return { outcome: "noop", head: current };
  }

  if (
    current.fenceGeneration > candidate.fenceGeneration ||
    current.traceDeletionGeneration > candidate.traceDeletionGeneration ||
    current.projectDeletionGeneration > candidate.projectDeletionGeneration
  ) {
    return { outcome: "stale_fence", head: current };
  }

  if (!samePartition(current.partitionDate, candidate.partitionDate)) {
    return { outcome: "partition_conflict", head: current };
  }

  return { outcome: "conflict", head: current };
}

async function findEntityHeadOrThrow(
  client: AnalyticsControlClient,
  input: Pick<
    ClaimAnalyticsEntityHeadInput,
    "projectId" | "entityType" | "entityKey"
  >,
): Promise<AnalyticsEntityHead> {
  return client.analyticsEntityHead.findUniqueOrThrow({
    where: {
      projectId_entityType_entityKey: {
        projectId: input.projectId,
        entityType: input.entityType,
        entityKey: input.entityKey,
      },
    },
  });
}

export async function claimAnalyticsEntityHead(
  input: ClaimAnalyticsEntityHeadInput,
): Promise<AnalyticsEntityHeadClaimResult> {
  const client = input.client ?? prisma;

  const advanceExistingHead = () =>
    client.analyticsEntityHead.updateMany({
      where: {
        projectId: input.projectId,
        entityType: input.entityType,
        entityKey: input.entityKey,
        sourceVersion: { lt: input.sourceVersion },
        partitionDate: input.partitionDate,
        fenceGeneration: { lte: input.fenceGeneration },
        traceDeletionGeneration: { lte: input.traceDeletionGeneration },
        projectDeletionGeneration: {
          lte: input.projectDeletionGeneration,
        },
      },
      data: {
        operationId: input.operationId,
        owningTraceId: input.owningTraceId,
        sourceVersion: input.sourceVersion,
        canonicalPayloadHash: input.canonicalPayloadHash,
        canonicalizerVersion: input.canonicalizerVersion,
        fenceGeneration: input.fenceGeneration,
        traceDeletionGeneration: input.traceDeletionGeneration,
        projectDeletionGeneration: input.projectDeletionGeneration,
      },
    });

  let updated = await advanceExistingHead();

  if (updated.count === 1) {
    return {
      outcome: "won",
      head: await findEntityHeadOrThrow(client, input),
    };
  }

  if (input.expectedSourceVersion === null) {
    try {
      const head = await client.analyticsEntityHead.create({
        data: {
          projectId: input.projectId,
          operationId: input.operationId,
          entityType: input.entityType,
          entityKey: input.entityKey,
          owningTraceId: input.owningTraceId,
          sourceVersion: input.sourceVersion,
          canonicalPayloadHash: input.canonicalPayloadHash,
          partitionDate: input.partitionDate,
          canonicalizerVersion: input.canonicalizerVersion,
          fenceGeneration: input.fenceGeneration,
          traceDeletionGeneration: input.traceDeletionGeneration,
          projectDeletionGeneration: input.projectDeletionGeneration,
        },
      });
      return { outcome: "won", head };
    } catch (error) {
      if (!isUniqueConstraintError(error)) throw error;
      updated = await advanceExistingHead();
      if (updated.count === 1) {
        return {
          outcome: "won",
          head: await findEntityHeadOrThrow(client, input),
        };
      }
    }
  }

  const current = await findEntityHeadOrThrow(client, input);
  return classifyEntityHeadClaim(current, input);
}

export async function advanceAnalyticsIngestionOperation({
  client = prisma,
  operationId,
  projectId,
  expectedFence,
  nextFence,
  status,
}: {
  client?: AnalyticsControlClient;
  operationId: string;
  projectId: string;
  expectedFence: bigint;
  nextFence: bigint;
  status: AnalyticsIngestionOperationStatus;
}): Promise<boolean> {
  if (nextFence <= expectedFence) {
    throw new RangeError("nextFence must be greater than expectedFence");
  }

  const result = await client.analyticsIngestionOperation.updateMany({
    where: {
      id: operationId,
      projectId,
      canonicalizationFence: expectedFence,
    },
    data: { canonicalizationFence: nextFence, status },
  });
  return result.count === 1;
}

export async function advanceAnalyticsDeletionOperation({
  client = prisma,
  operationId,
  organizationId,
  expectedFence,
  nextFence,
  status,
  phase,
}: {
  client?: AnalyticsControlClient;
  operationId: string;
  organizationId: string;
  expectedFence: bigint;
  nextFence: bigint;
  status: AnalyticsDeletionStatus;
  phase: string;
}): Promise<boolean> {
  if (nextFence <= expectedFence) {
    throw new RangeError("nextFence must be greater than expectedFence");
  }

  const result = await client.analyticsDeletionOperation.updateMany({
    where: {
      id: operationId,
      organizationId,
      workerFence: expectedFence,
    },
    data: { workerFence: nextFence, status, phase },
  });
  return result.count === 1;
}

export async function advanceTraceDeletionTombstone({
  client = prisma,
  projectId,
  traceId,
  generation,
  status,
  barrierLabel,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  traceId: string;
  generation: bigint;
  status: AnalyticsDeletionStatus;
  barrierLabel?: string;
}): Promise<{ advanced: boolean; generation: bigint }> {
  for (;;) {
    const updated = await client.analyticsDeletionTombstone.updateMany({
      where: { projectId, traceId, generation: { lt: generation } },
      data: { generation, status, barrierLabel },
    });
    if (updated.count === 1) return { advanced: true, generation };

    const current = await client.analyticsDeletionTombstone.findUnique({
      where: { projectId_traceId: { projectId, traceId } },
    });
    if (current) {
      if (current.generation >= generation) {
        return { advanced: false, generation: current.generation };
      }
      continue;
    }

    try {
      await client.analyticsDeletionTombstone.create({
        data: { projectId, traceId, generation, status, barrierLabel },
      });
      return { advanced: true, generation };
    } catch (error) {
      if (!isUniqueConstraintError(error)) throw error;
    }
  }
}

export async function advanceProjectDeletionGeneration({
  client = prisma,
  projectId,
  generation,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  generation: bigint;
}): Promise<{ advanced: boolean; generation: bigint }> {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const updated = await client.analyticsProjectDeletionGeneration.updateMany({
      where: { projectId, generation: { lt: generation } },
      data: { generation },
    });
    if (updated.count === 1) return { advanced: true, generation };

    const current = await client.analyticsProjectDeletionGeneration.findUnique({
      where: { projectId },
    });
    if (current) {
      if (current.generation >= generation) {
        return { advanced: false, generation: current.generation };
      }
      continue;
    }

    try {
      await client.analyticsProjectDeletionGeneration.create({
        data: { projectId, generation },
      });
      return { advanced: true, generation };
    } catch (error) {
      if (!isUniqueConstraintError(error)) throw error;
    }
  }

  const current =
    await client.analyticsProjectDeletionGeneration.findUniqueOrThrow({
      where: { projectId },
    });
  return { advanced: false, generation: current.generation };
}

export async function getProjectDeletionGeneration({
  client = prisma,
  projectId,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
}): Promise<bigint> {
  const current = await client.analyticsProjectDeletionGeneration.findUnique({
    where: { projectId },
  });
  return current?.generation ?? 0n;
}

export function findDeletionOperationForOrganization({
  client = prisma,
  operationId,
  organizationId,
}: {
  client?: AnalyticsControlClient;
  operationId: string;
  organizationId: string;
}): Promise<AnalyticsDeletionOperation | null> {
  return client.analyticsDeletionOperation.findFirst({
    where: { id: operationId, organizationId },
  });
}

export async function initializeTraceControlState({
  client = prisma,
  projectId,
  traceId,
  initializedByOperationId,
  bookmarked,
  public: isPublic,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  traceId: string;
  initializedByOperationId: string;
  bookmarked: boolean;
  public: boolean;
}): Promise<{ created: boolean; state: TraceControlState }> {
  try {
    const state = await client.traceControlState.create({
      data: {
        projectId,
        traceId,
        initializedByOperationId,
        bookmarked,
        public: isPublic,
      },
    });
    return { created: true, state };
  } catch (error) {
    if (!isUniqueConstraintError(error)) throw error;
    const state = await client.traceControlState.findUniqueOrThrow({
      where: { projectId_traceId: { projectId, traceId } },
    });
    return { created: false, state };
  }
}

export async function mutateTraceControlState({
  client = prisma,
  projectId,
  traceId,
  expectedRevision,
  bookmarked,
  public: isPublic,
  mutationSource,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  traceId: string;
  expectedRevision: bigint;
  bookmarked: boolean;
  public: boolean;
  mutationSource: string;
}): Promise<boolean> {
  const result = await client.traceControlState.updateMany({
    where: { projectId, traceId, revision: expectedRevision },
    data: {
      bookmarked,
      public: isPublic,
      revision: { increment: 1n },
      lastMutationSource: mutationSource,
    },
  });
  return result.count === 1;
}

export function getTraceControlState({
  client = prisma,
  projectId,
  traceId,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  traceId: string;
}): Promise<TraceControlState | null> {
  return client.traceControlState.findUnique({
    where: { projectId_traceId: { projectId, traceId } },
  });
}
