import { createHash } from "node:crypto";

import { Prisma } from "@prisma/client";
import type { AnalyticsEvaluationDispatch, PrismaClient } from "@prisma/client";

import { prisma } from "../../db";
import {
  lockAnalyticsAdmission,
  lockAnalyticsCapabilityCaptureIfEnabled,
  type AnalyticsRuntimeAdmissionContext,
} from "../analytics-persistence/analyticsBackendAdmission";
import type { AnalyticsEvaluationDispatchEventType } from "../queues";
import { sealAnalyticsEvaluationReplayCutoff } from "./analyticsEvaluationCapability";

export type AnalyticsEvaluationDispatchTargetInput = {
  readonly candidateKey: string;
  readonly targetType:
    | "TRACE_UPSERT"
    | "OBSERVATION_UPSERT"
    | "DATASET_RUN_ITEM_UPSERT"
    | "HISTORICAL";
  readonly targetId: string;
  readonly traceId: string;
  readonly observationId: string | null;
  readonly datasetItemId: string | null;
  readonly datasetItemValidFrom?: Date | null;
  readonly targetTimestamp: Date;
  readonly traceEnvironment: string | null;
};

type VisibleOperationForEvaluationCapture = {
  readonly id: string;
  readonly projectId: string;
  readonly candidates: readonly {
    readonly candidateKey: string;
    readonly disposition: string;
    readonly loadBatchId: string | null;
  }[];
  readonly loadBatches: readonly {
    readonly id: string;
    readonly status: string;
    readonly filteredRows: number | null;
  }[];
};

export function analyticsEvaluationDispatchIdentity(input: {
  readonly requestId: string;
  readonly targetType: AnalyticsEvaluationDispatchTargetInput["targetType"];
  readonly targetId: string;
}): string {
  const digest = createHash("sha256")
    .update(
      [
        "langfuse-doris-evaluation-dispatch-v1",
        input.requestId,
        input.targetType,
        input.targetId,
      ].join("\0"),
      "utf8",
    )
    .digest("hex");
  return `aed_${digest.slice(0, 28)}`;
}

function validateTarget(target: AnalyticsEvaluationDispatchTargetInput): void {
  if (
    !target.candidateKey ||
    !target.targetId ||
    !target.traceId ||
    !Number.isFinite(target.targetTimestamp.getTime()) ||
    (target.datasetItemValidFrom !== undefined &&
      target.datasetItemValidFrom !== null &&
      !Number.isFinite(target.datasetItemValidFrom.getTime())) ||
    (target.targetType === "TRACE_UPSERT" &&
      (target.targetId !== target.traceId ||
        target.observationId !== null ||
        target.datasetItemId !== null)) ||
    (target.targetType === "OBSERVATION_UPSERT" &&
      (target.observationId === null ||
        target.targetId !== target.observationId ||
        target.datasetItemId !== null)) ||
    (target.targetType === "DATASET_RUN_ITEM_UPSERT" &&
      target.datasetItemId === null)
  ) {
    throw new TypeError("Invalid analytics evaluation dispatch target");
  }
}

function sameTarget(
  left: AnalyticsEvaluationDispatchTargetInput,
  right: AnalyticsEvaluationDispatchTargetInput,
): boolean {
  return (
    left.targetType === right.targetType &&
    left.targetId === right.targetId &&
    left.traceId === right.traceId &&
    left.observationId === right.observationId &&
    left.datasetItemId === right.datasetItemId &&
    (left.datasetItemValidFrom?.getTime() ?? null) ===
      (right.datasetItemValidFrom?.getTime() ?? null) &&
    left.targetTimestamp.getTime() === right.targetTimestamp.getTime() &&
    left.traceEnvironment === right.traceEnvironment
  );
}

async function closeExpiredEvaluationCapture(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly activationGeneration: bigint;
  readonly deploymentGeneration: bigint;
  readonly now: Date;
  readonly failureCode:
    | "EVALUATION_CAPTURE_WINDOW_INVALID"
    | "EVALUATION_CAPTURE_WINDOW_EXPIRED"
    | "EVALUATION_CAPTURE_BUDGET_EXCEEDED";
}): Promise<void> {
  const cutoff = await sealAnalyticsEvaluationReplayCutoff({
    transaction: input.transaction,
    deploymentGeneration: input.deploymentGeneration,
    activationGeneration: input.activationGeneration,
    now: input.now,
  });
  await input.transaction.analyticsEvaluationDispatch.updateMany({
    where: {
      capabilityActivationGeneration: input.activationGeneration,
      deploymentGeneration: input.deploymentGeneration,
      status: "SUSPENDED",
    },
    data: {
      failureCode: input.failureCode,
    },
  });
  const closed =
    await input.transaction.analyticsCapabilityActivation.updateMany({
      where: {
        capability: "EVALUATIONS",
        generation: input.activationGeneration,
        deploymentGeneration: input.deploymentGeneration,
        status: "DARK",
        captureEnabled: true,
      },
      data: {
        status: "DISABLED",
        captureEnabled: false,
        captureStartedAt: null,
        captureExpiresAt: null,
        captureRowBudget: null,
        captureRequired: true,
        rescanRequired: true,
        cutoffState: cutoff.cutoffState,
        cutoffActivationGeneration: input.activationGeneration,
        cutoffDigest: cutoff.cutoffDigest,
        bootstrapCompletedGeneration: null,
        bootstrapEvidenceDigest: null,
        bootstrapCompletedAt: null,
        disabledAt: input.now,
        updatedAt: input.now,
      },
    });
  if (closed.count !== 1) {
    throw new Error("Evaluation capture window close was fenced");
  }
}

/**
 * Must run in the same transaction that terminalizes the ingestion operation.
 * Only candidates backed by a zero-filter VISIBLE load can create effects.
 */
export async function captureAnalyticsEvaluationDispatches(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly operation: VisibleOperationForEvaluationCapture;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly targets: readonly AnalyticsEvaluationDispatchTargetInput[];
  readonly now: Date;
}): Promise<number> {
  if (input.targets.length === 0) return 0;
  if (!Number.isFinite(input.now.getTime())) {
    throw new TypeError("Invalid analytics evaluation capture timestamp");
  }

  const capture = await lockAnalyticsCapabilityCaptureIfEnabled({
    transaction: input.transaction,
    admissionContext: input.admissionContext,
    capability: "evaluations",
    now: input.now,
  });
  if (!capture) return 0;
  if (capture.admission.analyticsBackend !== "DORIS") {
    throw new Error("Evaluation dispatch capture requires Doris provenance");
  }

  const visibleLoadIds = new Set(
    input.operation.loadBatches
      .filter(
        ({ status, filteredRows }) =>
          status === "VISIBLE" && filteredRows === 0,
      )
      .map(({ id }) => id),
  );
  const visibleCandidateKeys = new Set(
    input.operation.candidates
      .filter(
        ({ disposition, loadBatchId }) =>
          disposition === "LOAD_REQUIRED" &&
          loadBatchId !== null &&
          visibleLoadIds.has(loadBatchId),
      )
      .map(({ candidateKey }) => candidateKey),
  );

  const uniqueTargets = new Map<
    string,
    AnalyticsEvaluationDispatchTargetInput
  >();
  for (const target of input.targets) {
    validateTarget(target);
    if (!visibleCandidateKeys.has(target.candidateKey)) continue;
    const identity = `${target.targetType}\0${target.targetId}`;
    const existing = uniqueTargets.get(identity);
    if (existing && !sameTarget(existing, target)) {
      throw new Error("Evaluation dispatch target identity is inconsistent");
    }
    if (!existing) uniqueTargets.set(identity, target);
  }
  if (uniqueTargets.size === 0) return 0;

  if (capture.activationStatus === "DARK") {
    const failureCode =
      !capture.captureExpiresAt || capture.captureRowBudget === null
        ? "EVALUATION_CAPTURE_WINDOW_INVALID"
        : input.now >= capture.captureExpiresAt
          ? "EVALUATION_CAPTURE_WINDOW_EXPIRED"
          : capture.captureRows + BigInt(uniqueTargets.size) >
              BigInt(capture.captureRowBudget)
            ? "EVALUATION_CAPTURE_BUDGET_EXCEEDED"
            : null;
    if (failureCode) {
      await closeExpiredEvaluationCapture({
        transaction: input.transaction,
        activationGeneration: capture.admission.capabilityActivationGeneration,
        deploymentGeneration: capture.admission.deploymentGeneration,
        now: input.now,
        failureCode,
      });
      return 0;
    }
  }

  const status =
    capture.activationStatus === "ACTIVE" ? "PENDING" : "SUSPENDED";
  const created =
    await input.transaction.analyticsEvaluationDispatch.createMany({
      data: [...uniqueTargets.values()].map((target) => ({
        id: analyticsEvaluationDispatchIdentity({
          requestId: input.operation.id,
          targetType: target.targetType,
          targetId: target.targetId,
        }),
        operationId: input.operation.id,
        projectId: input.operation.projectId,
        sourceCandidateKey: target.candidateKey,
        requestId: input.operation.id,
        jobConfigurationId: null,
        targetType: target.targetType,
        targetId: target.targetId,
        traceId: target.traceId,
        observationId: target.observationId,
        datasetItemId: target.datasetItemId,
        datasetItemValidFrom: target.datasetItemValidFrom ?? null,
        targetTimestamp: target.targetTimestamp,
        traceEnvironment: target.traceEnvironment,
        analyticsBackend: capture.admission.analyticsBackend,
        deploymentGeneration: capture.admission.deploymentGeneration,
        workloadEpochFingerprint: capture.admission.workloadEpochFingerprint,
        runtimeContractVersion: capture.admission.runtimeContractVersion,
        captureRuntimeLeaseId: capture.admission.admittingRuntimeLeaseId,
        capabilityActivationGeneration:
          capture.admission.capabilityActivationGeneration,
        capabilityContractVersion: capture.admission.capabilityContractVersion,
        status,
        nextAttemptAt: input.now,
      })),
      skipDuplicates: true,
    });
  if (capture.activationStatus === "DARK" && created.count > 0) {
    const budgetRecorded =
      await input.transaction.analyticsCapabilityActivation.updateMany({
        where: {
          capability: "EVALUATIONS",
          generation: capture.admission.capabilityActivationGeneration,
          deploymentGeneration: capture.admission.deploymentGeneration,
          status: "DARK",
          captureEnabled: true,
          captureRows: capture.captureRows,
        },
        data: { captureRows: { increment: BigInt(created.count) } },
      });
    if (budgetRecorded.count !== 1) {
      throw new Error("Evaluation capture budget update was fenced");
    }
  }
  return created.count;
}

export async function replayVisibleAnalyticsEvaluationOperation(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly operationId: string;
  readonly projectId: string;
  readonly targets: readonly AnalyticsEvaluationDispatchTargetInput[];
  readonly now?: Date;
}): Promise<number> {
  if (!input.operationId || !input.projectId || input.targets.length === 0) {
    throw new TypeError("Invalid visible evaluation replay operation");
  }
  const now = input.now ?? new Date();
  return (input.client ?? prisma).$transaction(async (transaction) => {
    const operation =
      await transaction.analyticsIngestionOperation.findFirstOrThrow({
        where: {
          id: input.operationId,
          projectId: input.projectId,
          analyticsBackend: "DORIS",
          status: "VISIBLE",
        },
        select: {
          id: true,
          projectId: true,
          candidates: {
            select: {
              candidateKey: true,
              disposition: true,
              loadBatchId: true,
            },
          },
          loadBatches: {
            select: {
              id: true,
              status: true,
              filteredRows: true,
            },
          },
        },
      });
    return captureAnalyticsEvaluationDispatches({
      transaction,
      operation,
      admissionContext: input.admissionContext,
      targets: input.targets,
      now,
    });
  });
}

export type HistoricalAnalyticsEvaluationTargetInput = {
  readonly requestId: string;
  readonly jobConfigurationId: string;
  readonly targetId: string;
  readonly traceId: string;
  readonly observationId: string | null;
  readonly datasetItemId: string | null;
  readonly datasetRunItemId: string | null;
  readonly datasetItemValidFrom?: Date | null;
  readonly targetTimestamp: Date;
  readonly traceEnvironment: string | null;
};

async function findHistoricalTargetAnchor(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly projectId: string;
  readonly target: HistoricalAnalyticsEvaluationTargetInput;
}): Promise<{
  readonly operationId: string;
  readonly candidateKey: string;
} | null> {
  const head = await input.transaction.analyticsEntityHead.findFirst({
    where: {
      projectId: input.projectId,
      ...(input.target.datasetRunItemId
        ? {
            entityType: "DATASET_RUN_ITEM" as const,
            lookupId: input.target.datasetRunItemId,
          }
        : input.target.observationId
          ? {
              entityType: "EVENT" as const,
              lookupId: input.target.observationId,
              owningTraceId: input.target.traceId,
            }
          : {
              entityType: "EVENT" as const,
              owningTraceId: input.target.traceId,
            }),
    },
    select: { operationId: true, entityKey: true },
    orderBy: [{ sourceVersion: "desc" }, { entityKey: "asc" }],
  });
  if (!head) return null;
  const operation =
    await input.transaction.analyticsIngestionOperation.findFirst({
      where: {
        id: head.operationId,
        projectId: input.projectId,
        status: "VISIBLE",
      },
      select: {
        candidates: {
          where: {
            entityKey: head.entityKey,
            disposition: "LOAD_REQUIRED",
            loadBatchId: { not: null },
          },
          select: { candidateKey: true, loadBatchId: true },
          take: 1,
        },
        loadBatches: {
          where: { status: "VISIBLE", filteredRows: 0 },
          select: { id: true },
        },
      },
    });
  const candidate = operation?.candidates[0];
  if (
    !candidate?.loadBatchId ||
    !operation?.loadBatches.some(({ id }) => id === candidate.loadBatchId)
  ) {
    return null;
  }
  return {
    operationId: head.operationId,
    candidateKey: candidate.candidateKey,
  };
}

export async function createHistoricalAnalyticsEvaluationDispatches(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly targets: readonly HistoricalAnalyticsEvaluationTargetInput[];
  readonly now?: Date;
}): Promise<{ readonly created: number; readonly missing: number }> {
  if (
    !input.projectId ||
    input.targets.length === 0 ||
    input.targets.length > 500
  ) {
    throw new TypeError("Invalid historical evaluation dispatch batch");
  }
  const now = input.now ?? new Date();
  if (!Number.isFinite(now.getTime())) {
    throw new TypeError("Invalid historical evaluation dispatch timestamp");
  }
  const unique = new Map<string, HistoricalAnalyticsEvaluationTargetInput>();
  for (const target of input.targets) {
    if (
      !target.requestId ||
      !target.jobConfigurationId ||
      !target.targetId ||
      !target.traceId ||
      !Number.isFinite(target.targetTimestamp.getTime()) ||
      (target.datasetItemValidFrom !== undefined &&
        target.datasetItemValidFrom !== null &&
        !Number.isFinite(target.datasetItemValidFrom.getTime())) ||
      (target.datasetRunItemId === null) !== (target.datasetItemId === null)
    ) {
      throw new TypeError("Invalid historical evaluation target");
    }
    const identity = `${target.requestId}\0${target.targetId}`;
    const existing = unique.get(identity);
    if (existing && JSON.stringify(existing) !== JSON.stringify(target)) {
      throw new Error("Historical evaluation target identity is inconsistent");
    }
    unique.set(identity, target);
  }

  return (input.client ?? prisma).$transaction(
    async (transaction) => {
      const capture = await lockAnalyticsCapabilityCaptureIfEnabled({
        transaction,
        admissionContext: input.admissionContext,
        capability: "evaluations",
        now,
      });
      if (!capture || capture.activationStatus !== "ACTIVE") {
        throw new AnalyticsEvaluationDispatchProvenanceError(
          "Historical evaluation dispatch requires an active capability",
        );
      }
      const configIds = [
        ...new Set(
          [...unique.values()].map(
            ({ jobConfigurationId }) => jobConfigurationId,
          ),
        ),
      ];
      const configurations = await transaction.jobConfiguration.findMany({
        where: {
          id: { in: configIds },
          projectId: input.projectId,
          jobType: "EVAL",
          evalTemplateId: { not: null },
        },
        select: { id: true },
      });
      if (configurations.length !== configIds.length) {
        throw new AnalyticsEvaluationDispatchProvenanceError(
          "Historical evaluation configuration is invalid",
        );
      }

      let missing = 0;
      const rows = [];
      for (const target of unique.values()) {
        const anchor = await findHistoricalTargetAnchor({
          transaction,
          projectId: input.projectId,
          target,
        });
        if (!anchor) {
          missing += 1;
          continue;
        }
        rows.push({
          id: analyticsEvaluationDispatchIdentity({
            requestId: target.requestId,
            targetType: "HISTORICAL",
            targetId: target.targetId,
          }),
          operationId: anchor.operationId,
          projectId: input.projectId,
          sourceCandidateKey: anchor.candidateKey,
          requestId: target.requestId,
          jobConfigurationId: target.jobConfigurationId,
          targetType: "HISTORICAL" as const,
          targetId: target.targetId,
          traceId: target.traceId,
          observationId: target.observationId,
          datasetItemId: target.datasetItemId,
          datasetItemValidFrom: target.datasetItemValidFrom ?? null,
          targetTimestamp: target.targetTimestamp,
          traceEnvironment: target.traceEnvironment,
          analyticsBackend: capture.admission.analyticsBackend,
          deploymentGeneration: capture.admission.deploymentGeneration,
          workloadEpochFingerprint: capture.admission.workloadEpochFingerprint,
          runtimeContractVersion: capture.admission.runtimeContractVersion,
          captureRuntimeLeaseId: capture.admission.admittingRuntimeLeaseId,
          capabilityActivationGeneration:
            capture.admission.capabilityActivationGeneration,
          capabilityContractVersion:
            capture.admission.capabilityContractVersion,
          status: "PENDING" as const,
          nextAttemptAt: now,
        });
      }
      const created =
        rows.length === 0
          ? { count: 0 }
          : await transaction.analyticsEvaluationDispatch.createMany({
              data: rows,
              skipDuplicates: true,
            });
      return { created: created.count, missing };
    },
    { timeout: 30_000 },
  );
}

export class AnalyticsEvaluationDispatchProvenanceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AnalyticsEvaluationDispatchProvenanceError";
  }
}

function buildDispatchEnvelope(
  row: AnalyticsEvaluationDispatch,
): AnalyticsEvaluationDispatchEventType {
  return {
    dispatchId: row.id,
    dispatchGeneration: row.dispatchGeneration,
    projectId: row.projectId,
    operationId: row.operationId,
    targetType: row.targetType,
    targetId: row.targetId,
    analyticsBackend: "DORIS",
    deploymentGeneration: row.deploymentGeneration.toString(),
    workloadEpochFingerprint: row.workloadEpochFingerprint,
    runtimeContractVersion: row.runtimeContractVersion,
    capabilityActivationGeneration:
      row.capabilityActivationGeneration.toString(),
    capabilityContractVersion: row.capabilityContractVersion,
  };
}

function assertDispatchAdmission(
  admission: Awaited<ReturnType<typeof lockAnalyticsAdmission>>,
  row: AnalyticsEvaluationDispatch,
): void {
  if (
    admission.analyticsBackend !== row.analyticsBackend ||
    admission.deploymentGeneration !== row.deploymentGeneration ||
    admission.workloadEpochFingerprint !== row.workloadEpochFingerprint ||
    admission.runtimeContractVersion !== row.runtimeContractVersion ||
    admission.capabilityActivationGeneration !==
      row.capabilityActivationGeneration ||
    admission.capabilityContractVersion !== row.capabilityContractVersion
  ) {
    throw new AnalyticsEvaluationDispatchProvenanceError(
      "Evaluation dispatch runtime provenance does not match its record",
    );
  }
}

function assertEnvelopeMatchesDispatch(
  envelope: AnalyticsEvaluationDispatchEventType,
  row: AnalyticsEvaluationDispatch,
): void {
  const authoritative = buildDispatchEnvelope(row);
  if (
    envelope.dispatchId !== authoritative.dispatchId ||
    envelope.dispatchGeneration !== authoritative.dispatchGeneration ||
    envelope.projectId !== authoritative.projectId ||
    envelope.operationId !== authoritative.operationId ||
    envelope.targetType !== authoritative.targetType ||
    envelope.targetId !== authoritative.targetId ||
    envelope.analyticsBackend !== authoritative.analyticsBackend ||
    envelope.deploymentGeneration !== authoritative.deploymentGeneration ||
    envelope.workloadEpochFingerprint !==
      authoritative.workloadEpochFingerprint ||
    envelope.runtimeContractVersion !== authoritative.runtimeContractVersion ||
    envelope.capabilityActivationGeneration !==
      authoritative.capabilityActivationGeneration ||
    envelope.capabilityContractVersion !==
      authoritative.capabilityContractVersion
  ) {
    throw new AnalyticsEvaluationDispatchProvenanceError(
      "Evaluation dispatch envelope does not match its record",
    );
  }
}

export async function findPendingAnalyticsEvaluationDispatches(input: {
  readonly client?: PrismaClient;
  readonly now: Date;
  readonly limit: number;
}): Promise<readonly { id: string; dispatchGeneration: number }[]> {
  if (
    !Number.isFinite(input.now.getTime()) ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 100
  ) {
    throw new TypeError("Invalid analytics evaluation dispatch scan");
  }
  return (input.client ?? prisma).analyticsEvaluationDispatch.findMany({
    where: { status: "PENDING", nextAttemptAt: { lte: input.now } },
    select: { id: true, dispatchGeneration: true },
    orderBy: [{ nextAttemptAt: "asc" }, { createdAt: "asc" }],
    take: input.limit,
  });
}

export async function publishAnalyticsEvaluationDispatch(input: {
  readonly client: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly dispatchId: string;
  readonly expectedGeneration: number;
  readonly publish: (
    envelope: AnalyticsEvaluationDispatchEventType,
  ) => Promise<void>;
}): Promise<boolean> {
  if (
    !input.dispatchId ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1
  ) {
    throw new TypeError("Invalid analytics evaluation dispatch publication");
  }
  return input.client.$transaction(
    async (transaction) => {
      await transaction.$queryRaw(
        Prisma.sql`SELECT id FROM analytics_evaluation_dispatches WHERE id = ${input.dispatchId} FOR UPDATE`,
      );
      const row =
        await transaction.analyticsEvaluationDispatch.findUniqueOrThrow({
          where: { id: input.dispatchId },
        });
      if (row.dispatchGeneration !== input.expectedGeneration) {
        throw new AnalyticsEvaluationDispatchProvenanceError(
          "Evaluation dispatch generation is stale",
        );
      }
      if (row.status !== "PENDING") return false;

      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "evaluations",
        action: "recovery",
        expectedCapabilityActivationGeneration:
          row.capabilityActivationGeneration,
        expectedCapabilityContractVersion: row.capabilityContractVersion,
      });
      assertDispatchAdmission(admission, row);
      await input.publish(buildDispatchEnvelope(row));

      const updated = await transaction.analyticsEvaluationDispatch.updateMany({
        where: {
          id: row.id,
          status: "PENDING",
          dispatchGeneration: row.dispatchGeneration,
        },
        data: {
          status: "PUBLISHED",
          publishedAt: new Date(),
          attempts: { increment: 1 },
        },
      });
      if (updated.count !== 1) {
        throw new AnalyticsEvaluationDispatchProvenanceError(
          "Evaluation dispatch publication was fenced",
        );
      }
      return true;
    },
    { timeout: 30_000 },
  );
}

export async function deferAnalyticsEvaluationDispatch(input: {
  readonly client: PrismaClient;
  readonly dispatchId: string;
  readonly expectedGeneration: number;
  readonly now?: Date;
}): Promise<boolean> {
  if (
    !input.dispatchId ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1
  ) {
    throw new TypeError("Invalid analytics evaluation dispatch deferral");
  }
  const now = input.now ?? new Date();
  return input.client.$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM analytics_evaluation_dispatches WHERE id = ${input.dispatchId} FOR UPDATE`,
    );
    const row = await transaction.analyticsEvaluationDispatch.findUnique({
      where: { id: input.dispatchId },
    });
    if (
      !row ||
      row.status !== "PENDING" ||
      row.dispatchGeneration !== input.expectedGeneration
    ) {
      return false;
    }
    const attempts = row.attempts + 1;
    const delayMs = Math.min(
      10 * 60_000,
      10_000 * 2 ** Math.min(attempts - 1, 6),
    );
    const updated = await transaction.analyticsEvaluationDispatch.updateMany({
      where: {
        id: row.id,
        status: "PENDING",
        dispatchGeneration: input.expectedGeneration,
      },
      data: {
        attempts,
        nextAttemptAt: new Date(now.getTime() + delayMs),
      },
    });
    return updated.count === 1;
  });
}

export async function claimAnalyticsEvaluationDispatch(input: {
  readonly client: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly envelope: AnalyticsEvaluationDispatchEventType;
  readonly leaseOwner: string;
  readonly leaseMs: number;
  readonly now?: Date;
}): Promise<AnalyticsEvaluationDispatch | null> {
  if (
    !input.leaseOwner ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 1_000 ||
    input.leaseMs > 5 * 60_000
  ) {
    throw new TypeError("Invalid analytics evaluation dispatch claim");
  }
  const now = input.now ?? new Date();
  return input.client.$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM analytics_evaluation_dispatches WHERE id = ${input.envelope.dispatchId} FOR UPDATE`,
    );
    const row = await transaction.analyticsEvaluationDispatch.findUniqueOrThrow(
      {
        where: { id: input.envelope.dispatchId },
      },
    );
    assertEnvelopeMatchesDispatch(input.envelope, row);
    if (row.status === "COMPLETED" || row.status === "CANCELLED") return null;
    const reclaimable =
      row.status === "PROCESSING" &&
      row.processingLeaseExpiresAt !== null &&
      row.processingLeaseExpiresAt <= now;
    if (row.status !== "PUBLISHED" && !reclaimable) {
      throw new AnalyticsEvaluationDispatchProvenanceError(
        "Evaluation dispatch is not claimable",
      );
    }

    const admission = await lockAnalyticsAdmission({
      transaction,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
      expectedBackend: input.admissionContext.backend,
      expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
      capability: "evaluations",
      action: "claimExisting",
      expectedCapabilityActivationGeneration:
        row.capabilityActivationGeneration,
      expectedCapabilityContractVersion: row.capabilityContractVersion,
    });
    assertDispatchAdmission(admission, row);
    const updated = await transaction.analyticsEvaluationDispatch.updateMany({
      where: {
        id: row.id,
        dispatchGeneration: row.dispatchGeneration,
        status: reclaimable ? "PROCESSING" : "PUBLISHED",
      },
      data: {
        status: "PROCESSING",
        processingLeaseOwner: input.leaseOwner,
        processingLeaseExpiresAt: new Date(now.getTime() + input.leaseMs),
      },
    });
    if (updated.count !== 1) {
      throw new AnalyticsEvaluationDispatchProvenanceError(
        "Evaluation dispatch claim was fenced",
      );
    }
    return transaction.analyticsEvaluationDispatch.findUniqueOrThrow({
      where: { id: row.id },
    });
  });
}

export async function completeAnalyticsEvaluationDispatch(input: {
  readonly client: PrismaClient;
  readonly dispatchId: string;
  readonly expectedGeneration: number;
  readonly leaseOwner: string;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  const updated = await input.client.analyticsEvaluationDispatch.updateMany({
    where: {
      id: input.dispatchId,
      status: "PROCESSING",
      dispatchGeneration: input.expectedGeneration,
      processingLeaseOwner: input.leaseOwner,
      processingLeaseExpiresAt: { gt: now },
    },
    data: {
      status: "COMPLETED",
      completedAt: now,
      processingLeaseOwner: null,
      processingLeaseExpiresAt: null,
    },
  });
  return updated.count === 1;
}

export async function requeueAnalyticsEvaluationDispatch(input: {
  readonly client: PrismaClient;
  readonly dispatchId: string;
  readonly expectedGeneration: number;
  readonly leaseOwner: string;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  const updated = await input.client.analyticsEvaluationDispatch.updateMany({
    where: {
      id: input.dispatchId,
      status: "PROCESSING",
      dispatchGeneration: input.expectedGeneration,
      processingLeaseOwner: input.leaseOwner,
    },
    data: {
      status: "PENDING",
      dispatchGeneration: { increment: 1 },
      nextAttemptAt: new Date(now.getTime() + 10_000),
      publishedAt: null,
      processingLeaseOwner: null,
      processingLeaseExpiresAt: null,
    },
  });
  return updated.count === 1;
}

export async function quarantineAnalyticsEvaluationDispatch(input: {
  readonly client: PrismaClient;
  readonly dispatchId: string;
  readonly expectedGeneration: number;
  readonly leaseOwner?: string;
  readonly failureCode: string;
}): Promise<boolean> {
  if (
    !input.dispatchId ||
    !Number.isSafeInteger(input.expectedGeneration) ||
    input.expectedGeneration < 1 ||
    (input.leaseOwner !== undefined && !input.leaseOwner) ||
    !/^[A-Z0-9_]{1,64}$/.test(input.failureCode)
  ) {
    throw new TypeError("Invalid analytics evaluation dispatch quarantine");
  }
  const updated = await input.client.analyticsEvaluationDispatch.updateMany({
    where: {
      id: input.dispatchId,
      dispatchGeneration: input.expectedGeneration,
      status: input.leaseOwner
        ? "PROCESSING"
        : { in: ["PENDING", "PUBLISHED", "PROCESSING"] },
      ...(input.leaseOwner
        ? {
            processingLeaseOwner: input.leaseOwner,
          }
        : {}),
    },
    data: {
      status: "QUARANTINED",
      failureCode: input.failureCode,
      processingLeaseOwner: null,
      processingLeaseExpiresAt: null,
    },
  });
  return updated.count === 1;
}

export async function validateAnalyticsEvaluationExecution(input: {
  readonly client: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly envelope: AnalyticsEvaluationDispatchEventType;
  readonly jobExecutionId: string;
}): Promise<void> {
  if (!input.jobExecutionId) {
    throw new TypeError("Invalid managed evaluation execution");
  }
  await input.client.$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM analytics_evaluation_dispatches WHERE id = ${input.envelope.dispatchId} FOR SHARE`,
    );
    const row = await transaction.analyticsEvaluationDispatch.findUniqueOrThrow(
      {
        where: { id: input.envelope.dispatchId },
      },
    );
    assertEnvelopeMatchesDispatch(input.envelope, row);
    if (row.status !== "PROCESSING" && row.status !== "COMPLETED") {
      throw new AnalyticsEvaluationDispatchProvenanceError(
        "Evaluation dispatch is not executable",
      );
    }
    const job = await transaction.jobExecution.findFirst({
      where: {
        id: input.jobExecutionId,
        projectId: input.envelope.projectId,
        analyticsEvaluationDispatchId: row.id,
      },
      select: { id: true },
    });
    if (!job) {
      throw new AnalyticsEvaluationDispatchProvenanceError(
        "Evaluation execution is not linked to its dispatch",
      );
    }
    const admission = await lockAnalyticsAdmission({
      transaction,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
      expectedBackend: input.admissionContext.backend,
      expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
      capability: "evaluations",
      action: "claimExisting",
      expectedCapabilityActivationGeneration:
        row.capabilityActivationGeneration,
      expectedCapabilityContractVersion: row.capabilityContractVersion,
    });
    assertDispatchAdmission(admission, row);
  });
}
