import type {
  AnalyticsIngestionOperationStatus,
  AnalyticsLoadBatch,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";

export type AnalyticsLoadBatchClaim =
  | {
      readonly outcome: "claimed";
      readonly fence: bigint;
      readonly loadBatch: AnalyticsLoadBatch;
    }
  | {
      readonly outcome:
        | "already_visible"
        | "leased"
        | "reconciliation_required"
        | "stale_fence"
        | "terminal";
      readonly loadBatch: AnalyticsLoadBatch;
    };

export async function claimAnalyticsLoadBatch(input: {
  client?: PrismaClient;
  loadBatchId: string;
  projectId: string;
  expectedFence: bigint;
  nextFence: bigint;
  leaseOwner: string;
  leaseUntil: Date;
  now: Date;
}): Promise<AnalyticsLoadBatchClaim> {
  const client = input.client ?? prisma;
  if (
    input.nextFence !== input.expectedFence + 1n ||
    !input.leaseOwner ||
    input.leaseUntil <= input.now
  ) {
    throw new TypeError("Invalid analytics load-batch claim");
  }

  const batch = await client.analyticsLoadBatch.findFirstOrThrow({
    where: { id: input.loadBatchId, projectId: input.projectId },
  });
  if (batch.status === "VISIBLE") {
    return { outcome: "already_visible", loadBatch: batch };
  }
  if (batch.status === "UNKNOWN") {
    return { outcome: "reconciliation_required", loadBatch: batch };
  }
  if (batch.status === "LOADING") {
    if (
      batch.leaseOwner !== input.leaseOwner &&
      batch.leaseExpiresAt &&
      batch.leaseExpiresAt > input.now
    ) {
      return { outcome: "leased", loadBatch: batch };
    }
    return { outcome: "reconciliation_required", loadBatch: batch };
  }
  if (batch.status !== "PENDING") {
    return { outcome: "terminal", loadBatch: batch };
  }
  if (batch.fenceGeneration !== input.expectedFence) {
    return { outcome: "stale_fence", loadBatch: batch };
  }

  const updated = await client.analyticsLoadBatch.updateMany({
    where: {
      id: input.loadBatchId,
      projectId: input.projectId,
      status: "PENDING",
      fenceGeneration: input.expectedFence,
    },
    data: {
      status: "LOADING",
      fenceGeneration: input.nextFence,
      leaseOwner: input.leaseOwner,
      leaseExpiresAt: input.leaseUntil,
    },
  });
  if (updated.count !== 1) {
    return {
      outcome: "stale_fence",
      loadBatch: await client.analyticsLoadBatch.findFirstOrThrow({
        where: { id: input.loadBatchId, projectId: input.projectId },
      }),
    };
  }
  return {
    outcome: "claimed",
    fence: input.nextFence,
    loadBatch: await client.analyticsLoadBatch.findFirstOrThrow({
      where: { id: input.loadBatchId, projectId: input.projectId },
    }),
  };
}

export async function recordAnalyticsLoadOutcome(input: {
  client?: PrismaClient;
  loadBatchId: string;
  projectId: string;
  fence: bigint;
  leaseOwner: string;
  outcome: "VISIBLE" | "UNKNOWN" | "FAILED";
  transactionId: string | null;
  totalRows: number | null;
  filteredRows: number | null;
  errorCode: string | null;
  now: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  if (
    input.fence <= 0n ||
    !input.leaseOwner ||
    (input.outcome === "VISIBLE" &&
      (!Number.isSafeInteger(input.totalRows) ||
        input.totalRows === null ||
        input.totalRows < 0 ||
        input.filteredRows !== 0)) ||
    (input.outcome === "FAILED" && !input.errorCode)
  ) {
    throw new TypeError("Invalid analytics load outcome");
  }

  const updated = await client.analyticsLoadBatch.updateMany({
    where: {
      id: input.loadBatchId,
      projectId: input.projectId,
      fenceGeneration: input.fence,
      leaseOwner: input.leaseOwner,
      status: "LOADING",
    },
    data: {
      status: input.outcome,
      transactionId: input.transactionId,
      totalRows: input.totalRows,
      filteredRows: input.filteredRows,
      lastErrorCode: input.errorCode,
      visibleAt: input.outcome === "VISIBLE" ? input.now : null,
      leaseOwner: null,
      leaseExpiresAt: null,
    },
  });
  return updated.count === 1;
}

export async function recordAnalyticsLoadReconciliation(input: {
  client?: PrismaClient;
  loadBatchId: string;
  projectId: string;
  fence: bigint;
  status: "UNKNOWN" | "PREPARE" | "COMMITTED" | "VISIBLE" | "ABORTED";
  transactionId: string | null;
  totalRows: number | null;
  filteredRows: number | null;
  now: Date;
}): Promise<boolean> {
  const client = input.client ?? prisma;
  if (
    input.fence <= 0n ||
    (input.status === "VISIBLE" &&
      (!Number.isSafeInteger(input.totalRows) ||
        input.totalRows === null ||
        input.totalRows < 0 ||
        input.filteredRows !== 0))
  ) {
    throw new TypeError("Invalid analytics load reconciliation");
  }

  const status =
    input.status === "VISIBLE"
      ? "VISIBLE"
      : input.status === "ABORTED"
        ? "FAILED"
        : "UNKNOWN";
  const updated = await client.analyticsLoadBatch.updateMany({
    where: {
      id: input.loadBatchId,
      projectId: input.projectId,
      fenceGeneration: input.fence,
      status: { in: ["LOADING", "UNKNOWN"] },
    },
    data: {
      status,
      transactionId: input.transactionId,
      totalRows: input.totalRows,
      filteredRows: input.filteredRows,
      lastErrorCode: input.status === "ABORTED" ? "LOAD_ABORTED" : null,
      visibleAt: input.status === "VISIBLE" ? input.now : null,
      leaseOwner: null,
      leaseExpiresAt: null,
    },
  });
  return updated.count === 1;
}

export async function completeAnalyticsIngestionOperation(input: {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  now: Date;
}): Promise<{
  readonly outcome: "completed" | "already_completed" | "pending";
  readonly status: AnalyticsIngestionOperationStatus;
}> {
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    const operation =
      await transaction.analyticsIngestionOperation.findFirstOrThrow({
        where: { id: input.operationId, projectId: input.projectId },
        include: { candidates: true, loadBatches: true },
      });
    if (operation.terminalAt) {
      return {
        outcome: "already_completed" as const,
        status: operation.status,
      };
    }
    if (
      operation.manifestState !== "FROZEN" ||
      operation.candidates.some(({ disposition }) => disposition === "PENDING")
    ) {
      return { outcome: "pending" as const, status: operation.status };
    }

    const loadBatchById = new Map(
      operation.loadBatches.map((batch) => [batch.id, batch]),
    );
    const required = operation.candidates.filter(
      ({ disposition }) => disposition === "LOAD_REQUIRED",
    );
    if (
      required.some(({ loadBatchId }) => {
        const batch = loadBatchId ? loadBatchById.get(loadBatchId) : undefined;
        return !batch || batch.status !== "VISIBLE" || batch.filteredRows !== 0;
      })
    ) {
      return { outcome: "pending" as const, status: operation.status };
    }

    const hasVisible = required.length > 0;
    const hasQuarantine = operation.candidates.some(
      ({ disposition }) => disposition === "QUARANTINED",
    );
    const hasCancellation = operation.candidates.some(
      ({ disposition }) => disposition === "CANCELLED_BY_DELETION",
    );
    const status: AnalyticsIngestionOperationStatus = hasQuarantine
      ? hasVisible
        ? "PARTIAL_FAILED"
        : "QUARANTINED"
      : hasCancellation
        ? hasVisible
          ? "COMPLETED_WITH_CANCELLATIONS"
          : "CANCELLED_BY_DELETION"
        : "VISIBLE";

    const updated = await transaction.analyticsIngestionOperation.updateMany({
      where: {
        id: input.operationId,
        projectId: input.projectId,
        manifestState: "FROZEN",
        terminalAt: null,
      },
      data: {
        status,
        visibleAt: hasVisible || status === "VISIBLE" ? input.now : null,
        terminalAt: input.now,
      },
    });
    if (updated.count !== 1) {
      const current =
        await transaction.analyticsIngestionOperation.findFirstOrThrow({
          where: { id: input.operationId, projectId: input.projectId },
        });
      return {
        outcome: current.terminalAt
          ? ("already_completed" as const)
          : ("pending" as const),
        status: current.status,
      };
    }
    return { outcome: "completed" as const, status };
  });
}
