import { createHash } from "node:crypto";

import type {
  AnalyticsIngestionOperationStatus,
  AnalyticsLoadBatch,
  Prisma,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import type { AnalyticsRuntimeAdmissionContext } from "../analytics-persistence/analyticsBackendAdmission";
import {
  captureAnalyticsEvaluationDispatches,
  type AnalyticsEvaluationDispatchTargetInput,
} from "./analyticsEvaluationDispatches";
import {
  captureAnalyticsIntegrationDeliveries,
  type AnalyticsIntegrationDeliveryTargetInput,
} from "./analyticsIntegrationDeliveries";
import { lockAnalyticsIngestionOperation } from "./analyticsIngestionLock";
import { getAnalyticsRetentionBarrier } from "./analyticsRetention";

export function analyticsLoadBatchIdentity(input: {
  readonly projectId: string;
  readonly operationId: string;
  readonly logicalBatchId: string;
  readonly attempt: number;
}): { readonly id: string; readonly label: string } {
  if (
    !input.projectId ||
    !input.operationId ||
    !input.logicalBatchId ||
    !Number.isSafeInteger(input.attempt) ||
    input.attempt < 0
  ) {
    throw new TypeError("Invalid analytics load-batch identity");
  }
  const digest = createHash("sha256")
    .update(
      [
        "langfuse-doris-load-v1",
        input.projectId,
        input.operationId,
        input.logicalBatchId,
        String(input.attempt),
      ].join("\0"),
      "utf8",
    )
    .digest("hex");
  return {
    id: `alb_${digest.slice(0, 28)}`,
    label: `lf_${digest}`,
  };
}

async function quarantineLoadCandidates(input: {
  transaction: Prisma.TransactionClient;
  loadBatchId: string;
  projectId: string;
  reasonCode: string;
}): Promise<void> {
  const loadBatch = await input.transaction.analyticsLoadBatch.findFirstOrThrow(
    {
      where: { id: input.loadBatchId, projectId: input.projectId },
      select: { operation: { select: { recoverableUntil: true } } },
    },
  );
  await input.transaction.analyticsIngestionCandidate.updateMany({
    where: {
      projectId: input.projectId,
      loadBatchId: input.loadBatchId,
      disposition: "LOAD_REQUIRED",
    },
    data: {
      disposition: "QUARANTINED",
      reasonCode: input.reasonCode,
      quarantineExpiresAt: loadBatch.operation.recoverableUntil,
    },
  });
}

export type AnalyticsLoadDeletionRevalidation = {
  readonly outcome:
    | "current"
    | "cancelled"
    | "terminal"
    | "reconciliation_required"
    | "mixed_generation"
    | "lost_race";
};

/**
 * 将 deletion generation 的最后一次 control-plane 检查与仍为 PENDING 的
 * load/candidate 取消放在同一事务中。load batch 按 owning trace 隔离，因而正常
 * 情况下不会出现只取消一个 batch 中部分 rows 的语义。
 */
export async function cancelAnalyticsLoadBatchIfDeleted(input: {
  client?: PrismaClient;
  loadBatchId: string;
  projectId: string;
  claimedFence?: bigint;
  leaseOwner?: string;
}): Promise<AnalyticsLoadDeletionRevalidation> {
  const client = input.client ?? prisma;
  if (
    !input.loadBatchId ||
    !input.projectId ||
    (input.claimedFence === undefined) !== (input.leaseOwner === undefined) ||
    (input.claimedFence !== undefined && input.claimedFence <= 0n) ||
    input.leaseOwner === ""
  ) {
    throw new TypeError("Invalid analytics load deletion revalidation");
  }

  return client.$transaction(async (transaction) => {
    const loadBatch = await transaction.analyticsLoadBatch.findFirstOrThrow({
      where: { id: input.loadBatchId, projectId: input.projectId },
    });
    if (loadBatch.status === "UNKNOWN") {
      return { outcome: "reconciliation_required" as const };
    }
    const ownsClaimedLoad =
      loadBatch.status === "LOADING" &&
      input.claimedFence === loadBatch.fenceGeneration &&
      input.leaseOwner === loadBatch.leaseOwner;
    if (loadBatch.status === "LOADING" && !ownsClaimedLoad) {
      return { outcome: "reconciliation_required" as const };
    }
    if (loadBatch.status !== "PENDING" && !ownsClaimedLoad) {
      return { outcome: "terminal" as const };
    }

    const candidates = await transaction.analyticsIngestionCandidate.findMany({
      where: {
        projectId: input.projectId,
        loadBatchId: input.loadBatchId,
        disposition: "LOAD_REQUIRED",
      },
    });
    if (candidates.length === 0) {
      throw new TypeError("Analytics load batch has no required candidates");
    }
    const traceIds = [
      ...new Set(
        candidates.flatMap(({ owningTraceId }) =>
          owningTraceId ? [owningTraceId] : [],
        ),
      ),
    ];
    const datasetIds = [
      ...new Set(
        candidates.flatMap(({ owningDatasetId }) =>
          owningDatasetId ? [owningDatasetId] : [],
        ),
      ),
    ];
    const datasetRunIds = [
      ...new Set(
        candidates.flatMap(({ owningDatasetRunId }) =>
          owningDatasetRunId ? [owningDatasetRunId] : [],
        ),
      ),
    ];
    const [
      projectGeneration,
      traceTombstones,
      datasetGenerations,
      runGenerations,
    ] = await Promise.all([
      transaction.analyticsProjectDeletionGeneration.findUnique({
        where: { projectId: input.projectId },
        select: { generation: true },
      }),
      transaction.analyticsDeletionTombstone.findMany({
        where: {
          projectId: input.projectId,
          traceId: { in: traceIds },
        },
        select: { traceId: true, generation: true },
      }),
      transaction.analyticsDatasetDeletionGeneration.findMany({
        where: {
          projectId: input.projectId,
          datasetId: { in: datasetIds },
        },
        select: { datasetId: true, generation: true },
      }),
      transaction.analyticsDatasetRunDeletionGeneration.findMany({
        where: {
          projectId: input.projectId,
          datasetRunId: { in: datasetRunIds },
        },
        select: { datasetRunId: true, generation: true },
      }),
    ]);
    const traceGenerationById = new Map(
      traceTombstones.map(({ traceId, generation }) => [traceId, generation]),
    );
    const datasetGenerationById = new Map(
      datasetGenerations.map(({ datasetId, generation }) => [
        datasetId,
        generation,
      ]),
    );
    const runGenerationById = new Map(
      runGenerations.map(({ datasetRunId, generation }) => [
        datasetRunId,
        generation,
      ]),
    );
    // Tombstones are permanent anti-resurrection barriers. A candidate that
    // was canonicalized after a barrier must be cancelled as well as one that
    // raced with it; equality is therefore not permission to write.
    const stale = candidates.map(
      ({ owningTraceId, owningDatasetId, owningDatasetRunId }) =>
        (projectGeneration?.generation ?? 0n) !== 0n ||
        (owningTraceId
          ? (traceGenerationById.get(owningTraceId) ?? 0n) !== 0n
          : false) ||
        (owningDatasetId
          ? (datasetGenerationById.get(owningDatasetId) ?? 0n) !== 0n
          : false) ||
        (owningDatasetRunId
          ? (runGenerationById.get(owningDatasetRunId) ?? 0n) !== 0n
          : false),
    );
    if (!stale.some(Boolean)) return { outcome: "current" as const };
    if (!stale.every(Boolean)) {
      return { outcome: "mixed_generation" as const };
    }

    const cancelled = await transaction.analyticsLoadBatch.updateMany({
      where: {
        id: input.loadBatchId,
        projectId: input.projectId,
        status: ownsClaimedLoad ? "LOADING" : "PENDING",
        fenceGeneration: loadBatch.fenceGeneration,
        ...(ownsClaimedLoad ? { leaseOwner: input.leaseOwner } : {}),
      },
      data: {
        status: "CANCELLED_BY_DELETION",
        lastErrorCode: "DELETION_BARRIER",
        leaseOwner: null,
        leaseExpiresAt: null,
      },
    });
    if (cancelled.count !== 1) return { outcome: "lost_race" as const };
    const cancelledCandidates =
      await transaction.analyticsIngestionCandidate.updateMany({
        where: {
          projectId: input.projectId,
          loadBatchId: input.loadBatchId,
          disposition: "LOAD_REQUIRED",
        },
        data: {
          disposition: "CANCELLED_BY_DELETION",
          reasonCode: "DELETION_BARRIER",
        },
      });
    if (cancelledCandidates.count !== candidates.length) {
      throw new Error("Analytics load candidate cancellation was incomplete");
    }
    return { outcome: "cancelled" as const };
  });
}

/**
 * Retention publishes its cutoff before the drain phase. Revalidate both the
 * pending and claimed load so an old replay cannot recreate a purged partition.
 */
export async function cancelAnalyticsLoadBatchIfRetained(input: {
  client?: PrismaClient;
  loadBatchId: string;
  projectId: string;
  claimedFence?: bigint;
  leaseOwner?: string;
}): Promise<AnalyticsLoadDeletionRevalidation> {
  const client = input.client ?? prisma;
  if (
    !input.loadBatchId ||
    !input.projectId ||
    (input.claimedFence === undefined) !== (input.leaseOwner === undefined) ||
    (input.claimedFence !== undefined && input.claimedFence <= 0n) ||
    input.leaseOwner === ""
  ) {
    throw new TypeError("Invalid analytics load retention revalidation");
  }

  return client.$transaction(async (transaction) => {
    const loadBatch = await transaction.analyticsLoadBatch.findFirstOrThrow({
      where: { id: input.loadBatchId, projectId: input.projectId },
    });
    if (loadBatch.status === "UNKNOWN") {
      return { outcome: "reconciliation_required" as const };
    }
    const ownsClaimedLoad =
      loadBatch.status === "LOADING" &&
      input.claimedFence === loadBatch.fenceGeneration &&
      input.leaseOwner === loadBatch.leaseOwner;
    if (loadBatch.status === "LOADING" && !ownsClaimedLoad) {
      return { outcome: "reconciliation_required" as const };
    }
    if (loadBatch.status !== "PENDING" && !ownsClaimedLoad) {
      return { outcome: "terminal" as const };
    }
    if (!loadBatch.partitionDate) return { outcome: "current" as const };

    const cutoff = await getAnalyticsRetentionBarrier({
      client: transaction,
      projectId: input.projectId,
    });
    if (!cutoff || loadBatch.partitionDate >= cutoff) {
      return { outcome: "current" as const };
    }

    const candidates = await transaction.analyticsIngestionCandidate.findMany({
      where: {
        projectId: input.projectId,
        loadBatchId: input.loadBatchId,
        disposition: "LOAD_REQUIRED",
      },
      select: { id: true },
    });
    if (candidates.length === 0) {
      throw new TypeError("Analytics load batch has no required candidates");
    }
    const cancelled = await transaction.analyticsLoadBatch.updateMany({
      where: {
        id: input.loadBatchId,
        projectId: input.projectId,
        status: ownsClaimedLoad ? "LOADING" : "PENDING",
        fenceGeneration: loadBatch.fenceGeneration,
        ...(ownsClaimedLoad ? { leaseOwner: input.leaseOwner } : {}),
      },
      data: {
        status: "CANCELLED_BY_DELETION",
        lastErrorCode: "RETENTION_BARRIER",
        leaseOwner: null,
        leaseExpiresAt: null,
      },
    });
    if (cancelled.count !== 1) return { outcome: "lost_race" as const };
    const cancelledCandidates =
      await transaction.analyticsIngestionCandidate.updateMany({
        where: {
          projectId: input.projectId,
          loadBatchId: input.loadBatchId,
          disposition: "LOAD_REQUIRED",
        },
        data: {
          disposition: "CANCELLED_BY_DELETION",
          reasonCode: "RETENTION_BARRIER",
        },
      });
    if (cancelledCandidates.count !== candidates.length) {
      throw new Error("Analytics retention cancellation was incomplete");
    }
    return { outcome: "cancelled" as const };
  });
}

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

  return client.$transaction(async (transaction) => {
    const operation = await lockAnalyticsIngestionOperation(transaction, {
      operationId: (
        await transaction.analyticsLoadBatch.findFirstOrThrow({
          where: { id: input.loadBatchId, projectId: input.projectId },
          select: { operationId: true },
        })
      ).operationId,
      projectId: input.projectId,
    });
    const batch = await transaction.analyticsLoadBatch.findFirstOrThrow({
      where: { id: input.loadBatchId, projectId: input.projectId },
    });
    if (operation.terminalAt || batch.status !== "PENDING") {
      if (batch.status === "VISIBLE") {
        return { outcome: "already_visible" as const, loadBatch: batch };
      }
      if (batch.status === "UNKNOWN") {
        return {
          outcome: "reconciliation_required" as const,
          loadBatch: batch,
        };
      }
      if (batch.status === "LOADING") {
        if (
          batch.leaseOwner !== input.leaseOwner &&
          batch.leaseExpiresAt &&
          batch.leaseExpiresAt > input.now
        ) {
          return { outcome: "leased" as const, loadBatch: batch };
        }
        return {
          outcome: "reconciliation_required" as const,
          loadBatch: batch,
        };
      }
      return { outcome: "terminal" as const, loadBatch: batch };
    }
    if (batch.fenceGeneration !== input.expectedFence) {
      return { outcome: "stale_fence" as const, loadBatch: batch };
    }

    const updated = await transaction.analyticsLoadBatch.updateMany({
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
        outcome: "stale_fence" as const,
        loadBatch: await transaction.analyticsLoadBatch.findFirstOrThrow({
          where: { id: input.loadBatchId, projectId: input.projectId },
        }),
      };
    }
    return {
      outcome: "claimed" as const,
      fence: input.nextFence,
      loadBatch: await transaction.analyticsLoadBatch.findFirstOrThrow({
        where: { id: input.loadBatchId, projectId: input.projectId },
      }),
    };
  });
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

  return client.$transaction(async (transaction) => {
    const updated = await transaction.analyticsLoadBatch.updateMany({
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
    if (updated.count !== 1) return false;
    if (input.outcome === "FAILED") {
      await quarantineLoadCandidates({
        transaction,
        loadBatchId: input.loadBatchId,
        projectId: input.projectId,
        reasonCode: input.errorCode!,
      });
    }
    return true;
  });
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

  if (input.status === "ABORTED") {
    return client.$transaction(async (transaction) => {
      const batch = await transaction.analyticsLoadBatch.findFirstOrThrow({
        where: { id: input.loadBatchId, projectId: input.projectId },
      });
      if (
        batch.fenceGeneration !== input.fence ||
        (batch.status !== "LOADING" && batch.status !== "UNKNOWN")
      ) {
        return false;
      }
      const requiredCandidates =
        await transaction.analyticsIngestionCandidate.count({
          where: {
            projectId: input.projectId,
            loadBatchId: input.loadBatchId,
            disposition: "LOAD_REQUIRED",
          },
        });
      const failed = await transaction.analyticsLoadBatch.updateMany({
        where: {
          id: input.loadBatchId,
          projectId: input.projectId,
          fenceGeneration: input.fence,
          status: { in: ["LOADING", "UNKNOWN"] },
        },
        data: {
          status: "FAILED",
          transactionId: input.transactionId,
          totalRows: input.totalRows,
          filteredRows: input.filteredRows,
          lastErrorCode: "LOAD_ABORTED",
          leaseOwner: null,
          leaseExpiresAt: null,
        },
      });
      if (failed.count !== 1) return false;
      if (requiredCandidates === 0) return true;

      const nextAttempt = batch.attempt + 1;
      const identity = analyticsLoadBatchIdentity({
        projectId: batch.projectId,
        operationId: batch.operationId,
        logicalBatchId: batch.logicalBatchId,
        attempt: nextAttempt,
      });
      await transaction.analyticsLoadBatch.create({
        data: {
          id: identity.id,
          operationId: batch.operationId,
          projectId: batch.projectId,
          databaseName: batch.databaseName,
          targetTable: batch.targetTable,
          logicalBatchId: batch.logicalBatchId,
          attempt: nextAttempt,
          fenceGeneration: 0n,
          label: identity.label,
          payloadHash: batch.payloadHash,
          canonicalObjectKey: batch.canonicalObjectKey,
          partitionDate: batch.partitionDate,
        },
      });
      const rebound = await transaction.analyticsIngestionCandidate.updateMany({
        where: {
          projectId: input.projectId,
          loadBatchId: input.loadBatchId,
          disposition: "LOAD_REQUIRED",
        },
        data: { loadBatchId: identity.id, reasonCode: "LOAD_ABORTED_RETRY" },
      });
      if (rebound.count !== requiredCandidates) {
        throw new Error("Analytics load retry candidate rebind was incomplete");
      }
      return true;
    });
  }

  const status = input.status === "VISIBLE" ? "VISIBLE" : "UNKNOWN";
  return client.$transaction(async (transaction) => {
    const updated = await transaction.analyticsLoadBatch.updateMany({
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
        lastErrorCode: null,
        visibleAt: input.status === "VISIBLE" ? input.now : null,
        leaseOwner: null,
        leaseExpiresAt: null,
      },
    });
    if (updated.count !== 1) return false;
    return true;
  });
}

export async function completeAnalyticsIngestionOperation(input: {
  client?: PrismaClient;
  operationId: string;
  projectId: string;
  now: Date;
  evaluationCapture?: {
    readonly admissionContext: AnalyticsRuntimeAdmissionContext;
    readonly targets: readonly AnalyticsEvaluationDispatchTargetInput[];
  };
  integrationCapture?: {
    readonly admissionContext: AnalyticsRuntimeAdmissionContext;
    readonly targets: readonly AnalyticsIntegrationDeliveryTargetInput[];
  };
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
    if (input.evaluationCapture && (hasVisible || status === "VISIBLE")) {
      await captureAnalyticsEvaluationDispatches({
        transaction,
        operation,
        admissionContext: input.evaluationCapture.admissionContext,
        targets: input.evaluationCapture.targets,
        now: input.now,
      });
    }
    if (input.integrationCapture && (hasVisible || status === "VISIBLE")) {
      await captureAnalyticsIntegrationDeliveries({
        transaction,
        operation,
        admissionContext: input.integrationCapture.admissionContext,
        targets: input.integrationCapture.targets,
        now: input.now,
      });
    }
    return { outcome: "completed" as const, status };
  });
}
