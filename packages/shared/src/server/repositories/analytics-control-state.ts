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
  | "locator_conflict"
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
  lookupId: string;
  owningTraceId: string | null;
  owningDatasetId?: string | null;
  owningDatasetRunId?: string | null;
  expectedSourceVersion: bigint | null;
  sourceVersion: bigint;
  canonicalPayloadHash: string;
  partitionDate: Date;
  canonicalizerVersion: string;
  fenceGeneration: bigint;
  traceDeletionGeneration: bigint;
  projectDeletionGeneration: bigint;
  datasetDeletionGeneration?: bigint;
  runDeletionGeneration?: bigint;
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
  if (current.lookupId !== null && current.lookupId !== candidate.lookupId) {
    return { outcome: "locator_conflict", head: current };
  }
  if (current.sourceVersion > candidate.sourceVersion) {
    return { outcome: "superseded", head: current };
  }

  if (
    current.fenceGeneration > candidate.fenceGeneration ||
    current.traceDeletionGeneration > candidate.traceDeletionGeneration ||
    current.projectDeletionGeneration > candidate.projectDeletionGeneration ||
    current.datasetDeletionGeneration >
      (candidate.datasetDeletionGeneration ?? 0n) ||
    current.runDeletionGeneration > (candidate.runDeletionGeneration ?? 0n)
  ) {
    return { outcome: "stale_fence", head: current };
  }

  if (
    samePartition(current.partitionDate, candidate.partitionDate) &&
    current.sourceVersion === candidate.sourceVersion &&
    current.canonicalPayloadHash === candidate.canonicalPayloadHash
  ) {
    return { outcome: "noop", head: current };
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
        OR: [{ lookupId: input.lookupId }, { lookupId: null }],
        sourceVersion: { lt: input.sourceVersion },
        partitionDate: input.partitionDate,
        fenceGeneration: { lte: input.fenceGeneration },
        traceDeletionGeneration: { lte: input.traceDeletionGeneration },
        projectDeletionGeneration: {
          lte: input.projectDeletionGeneration,
        },
        datasetDeletionGeneration: {
          lte: input.datasetDeletionGeneration ?? 0n,
        },
        runDeletionGeneration: { lte: input.runDeletionGeneration ?? 0n },
      },
      data: {
        operationId: input.operationId,
        lookupId: input.lookupId,
        owningTraceId: input.owningTraceId,
        owningDatasetId: input.owningDatasetId ?? null,
        owningDatasetRunId: input.owningDatasetRunId ?? null,
        sourceVersion: input.sourceVersion,
        canonicalPayloadHash: input.canonicalPayloadHash,
        canonicalizerVersion: input.canonicalizerVersion,
        fenceGeneration: input.fenceGeneration,
        traceDeletionGeneration: input.traceDeletionGeneration,
        projectDeletionGeneration: input.projectDeletionGeneration,
        datasetDeletionGeneration: input.datasetDeletionGeneration ?? 0n,
        runDeletionGeneration: input.runDeletionGeneration ?? 0n,
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
          lookupId: input.lookupId,
          owningTraceId: input.owningTraceId,
          owningDatasetId: input.owningDatasetId ?? null,
          owningDatasetRunId: input.owningDatasetRunId ?? null,
          sourceVersion: input.sourceVersion,
          canonicalPayloadHash: input.canonicalPayloadHash,
          partitionDate: input.partitionDate,
          canonicalizerVersion: input.canonicalizerVersion,
          fenceGeneration: input.fenceGeneration,
          traceDeletionGeneration: input.traceDeletionGeneration,
          projectDeletionGeneration: input.projectDeletionGeneration,
          datasetDeletionGeneration: input.datasetDeletionGeneration ?? 0n,
          runDeletionGeneration: input.runDeletionGeneration ?? 0n,
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

  await client.analyticsEntityHead.updateMany({
    where: {
      projectId: input.projectId,
      entityType: input.entityType,
      entityKey: input.entityKey,
      lookupId: null,
    },
    data: { lookupId: input.lookupId },
  });

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

export async function getTraceDeletionGeneration({
  client = prisma,
  projectId,
  traceId,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  traceId: string;
}): Promise<bigint> {
  const current = await client.analyticsDeletionTombstone.findUnique({
    where: { projectId_traceId: { projectId, traceId } },
  });
  return current?.generation ?? 0n;
}

export async function getDatasetDeletionGeneration({
  client = prisma,
  projectId,
  datasetId,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  datasetId: string;
}): Promise<bigint> {
  const current = await client.analyticsDatasetDeletionGeneration.findUnique({
    where: { projectId_datasetId: { projectId, datasetId } },
    select: { generation: true },
  });
  return current?.generation ?? 0n;
}

export async function getDatasetRunDeletionState({
  client = prisma,
  projectId,
  datasetRunId,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  datasetRunId: string;
}): Promise<{
  readonly datasetId: string;
  readonly generation: bigint;
} | null> {
  return client.analyticsDatasetRunDeletionGeneration.findUnique({
    where: { projectId_datasetRunId: { projectId, datasetRunId } },
    select: { datasetId: true, generation: true },
  });
}

export async function getDatasetRunDeletionGeneration(input: {
  client?: AnalyticsControlClient;
  projectId: string;
  datasetRunId: string;
}): Promise<bigint> {
  return (await getDatasetRunDeletionState(input))?.generation ?? 0n;
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

export async function updateTraceControlState({
  client = prisma,
  projectId,
  traceId,
  initialState,
  updates,
  mutationSource,
}: {
  client?: AnalyticsControlClient;
  projectId: string;
  traceId: string;
  initialState: { bookmarked: boolean; public: boolean };
  updates: { bookmarked?: boolean; public?: boolean };
  mutationSource: string;
}): Promise<void> {
  if (updates.bookmarked === undefined && updates.public === undefined) {
    return;
  }

  for (let attempt = 0; attempt < 10; attempt += 1) {
    const current = await getTraceControlState({ client, projectId, traceId });
    if (!current) {
      try {
        await client.traceControlState.create({
          data: {
            projectId,
            traceId,
            bookmarked: updates.bookmarked ?? initialState.bookmarked,
            public: updates.public ?? initialState.public,
            revision: 1n,
            lastMutationSource: mutationSource,
          },
        });
        return;
      } catch (error) {
        if (!isUniqueConstraintError(error)) throw error;
        continue;
      }
    }

    const updated = await mutateTraceControlState({
      client,
      projectId,
      traceId,
      expectedRevision: current.revision,
      bookmarked: updates.bookmarked ?? current.bookmarked,
      public: updates.public ?? current.public,
      mutationSource,
    });
    if (updated) return;
  }

  throw new Error("Trace control state changed too frequently");
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
