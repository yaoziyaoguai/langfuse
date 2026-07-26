import { createHash } from "node:crypto";

import {
  Prisma,
  type AnalyticsIntegrationExecution,
  type AnalyticsIntegrationPendingDelivery,
  type AnalyticsIntegrationType,
  type PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import {
  lockAnalyticsAdmission,
  lockAnalyticsCapabilityCaptureIfEnabled,
  type AnalyticsRuntimeAdmissionContext,
} from "../analytics-persistence/analyticsBackendAdmission";

const MAX_PENDING_ROWS_PER_INTEGRATION = 100_000n;
const MAX_PENDING_BYTES_PER_INTEGRATION = 128n * 1024n * 1024n;
const MAX_PENDING_ROWS_PER_DEPLOYMENT = 1_000_000n;
const MAX_PENDING_BYTES_PER_DEPLOYMENT = 1024n * 1024n * 1024n;
const MAX_DELIVERY_ESTIMATED_BYTES = 16 * 1024 * 1024;
const MAX_EXECUTION_DELIVERIES = 500;
const MAX_EXECUTION_LEASE_MS = 15 * 60 * 1000;
const DEPLOYMENT_DELIVERY_QUOTA_LOCK_KEY = 181_865_275_000_009n;

export type AnalyticsIntegrationDeliveryTargetInput = {
  readonly candidateKey: string;
  readonly deliveryKind: "TRACE" | "GENERATION" | "OBSERVATION" | "SCORE";
  readonly entityKey: string;
  readonly estimatedBytes: number;
};

export type AnalyticsIntegrationExecutionManifestItem = {
  readonly deliveryKind: AnalyticsIntegrationDeliveryTargetInput["deliveryKind"];
  readonly entityKey: string;
  readonly deliveryIds: readonly string[];
};

export type AnalyticsIntegrationBootstrapIdentity = Pick<
  AnalyticsIntegrationExecutionManifestItem,
  "deliveryKind" | "entityKey"
>;

export type AnalyticsIntegrationExecutionManifest = {
  readonly version: 1;
  readonly integrationStateId: string;
  readonly integrationGeneration: string;
  readonly projectId: string;
  readonly items: readonly AnalyticsIntegrationExecutionManifestItem[];
};

export type AnalyticsIntegrationExecutionEnvelope = {
  readonly executionId: string;
  readonly projectId: string;
  readonly integrationType: AnalyticsIntegrationType;
  readonly integrationGeneration: string;
  readonly analyticsBackend: "DORIS";
  readonly deploymentGeneration: string;
  readonly workloadEpochFingerprint: string;
  readonly runtimeContractVersion: number;
  readonly capabilityActivationGeneration: string;
  readonly capabilityContractVersion: number;
  readonly manifestChecksum: string;
};

export type ClaimedAnalyticsIntegrationExecution = {
  readonly execution: AnalyticsIntegrationExecution;
  readonly manifest: AnalyticsIntegrationExecutionManifest;
  readonly deliveries: readonly AnalyticsIntegrationPendingDelivery[];
};

type VisibleOperationForIntegrationCapture = {
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

export class AnalyticsIntegrationExecutionProvenanceError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "AnalyticsIntegrationExecutionProvenanceError";
  }
}

export function analyticsIntegrationDeliveryIdentity(input: {
  readonly integrationStateId: string;
  readonly integrationGeneration: bigint;
  readonly operationId: string;
  readonly deliveryKind: AnalyticsIntegrationDeliveryTargetInput["deliveryKind"];
  readonly entityKey: string;
}): string {
  const digest = createHash("sha256")
    .update(
      [
        "langfuse-doris-analytics-integration-delivery-v1",
        input.integrationStateId,
        input.integrationGeneration.toString(),
        input.operationId,
        input.deliveryKind,
        input.entityKey,
      ].join("\0"),
      "utf8",
    )
    .digest("hex");
  return `aid_${digest.slice(0, 28)}`;
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function executionIdentity(input: {
  readonly integrationStateId: string;
  readonly integrationGeneration: bigint;
  readonly manifestChecksum: string;
}): string {
  return `aie_${sha256(
    [
      "langfuse-doris-analytics-integration-execution-v1",
      input.integrationStateId,
      input.integrationGeneration.toString(),
      input.manifestChecksum,
    ].join("\0"),
  ).slice(0, 28)}`;
}

function buildManifest(
  rows: readonly AnalyticsIntegrationPendingDelivery[],
): AnalyticsIntegrationExecutionManifest {
  const first = rows[0];
  if (!first) {
    throw new TypeError("Cannot seal an empty analytics integration manifest");
  }
  const items = new Map<
    string,
    {
      deliveryKind: AnalyticsIntegrationDeliveryTargetInput["deliveryKind"];
      entityKey: string;
      deliveryIds: string[];
    }
  >();
  for (const row of rows) {
    const key = `${row.deliveryKind}\0${row.entityKey}`;
    const item = items.get(key) ?? {
      deliveryKind: row.deliveryKind,
      entityKey: row.entityKey,
      deliveryIds: [],
    };
    item.deliveryIds.push(row.id);
    items.set(key, item);
  }
  return {
    version: 1,
    integrationStateId: first.integrationStateId,
    integrationGeneration: first.integrationGeneration.toString(),
    projectId: first.projectId,
    items: [...items.values()]
      .map((item) => ({
        ...item,
        deliveryIds: item.deliveryIds.sort(),
      }))
      .sort((left, right) => {
        const kind = left.deliveryKind.localeCompare(right.deliveryKind);
        return kind !== 0
          ? kind
          : left.entityKey.localeCompare(right.entityKey);
      }),
  };
}

function buildBootstrapManifest(input: {
  readonly integrationStateId: string;
  readonly integrationGeneration: bigint;
  readonly projectId: string;
  readonly identities: readonly AnalyticsIntegrationBootstrapIdentity[];
}): AnalyticsIntegrationExecutionManifest {
  const identities = new Map<string, AnalyticsIntegrationBootstrapIdentity>();
  for (const identity of input.identities) {
    if (
      !["TRACE", "GENERATION", "OBSERVATION", "SCORE"].includes(
        identity.deliveryKind,
      ) ||
      !identity.entityKey
    ) {
      throw new TypeError("Invalid analytics integration bootstrap identity");
    }
    identities.set(`${identity.deliveryKind}\0${identity.entityKey}`, identity);
  }
  return {
    version: 1,
    integrationStateId: input.integrationStateId,
    integrationGeneration: input.integrationGeneration.toString(),
    projectId: input.projectId,
    items: [...identities.values()]
      .map((identity) => ({ ...identity, deliveryIds: [] }))
      .sort((left, right) => {
        const kind = left.deliveryKind.localeCompare(right.deliveryKind);
        return kind !== 0
          ? kind
          : left.entityKey.localeCompare(right.entityKey);
      }),
  };
}

function parseManifest(
  value: Prisma.JsonValue,
): AnalyticsIntegrationExecutionManifest {
  if (
    typeof value !== "object" ||
    value === null ||
    Array.isArray(value) ||
    value.version !== 1 ||
    typeof value.integrationStateId !== "string" ||
    typeof value.integrationGeneration !== "string" ||
    typeof value.projectId !== "string" ||
    !Array.isArray(value.items)
  ) {
    throw new AnalyticsIntegrationExecutionProvenanceError(
      "Analytics integration execution manifest is invalid",
    );
  }
  const items = value.items.map((item) => {
    if (
      typeof item !== "object" ||
      item === null ||
      Array.isArray(item) ||
      !["TRACE", "GENERATION", "OBSERVATION", "SCORE"].includes(
        String(item.deliveryKind),
      ) ||
      typeof item.entityKey !== "string" ||
      !item.entityKey ||
      !Array.isArray(item.deliveryIds) ||
      item.deliveryIds.some(
        (deliveryId) => typeof deliveryId !== "string" || !deliveryId,
      )
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Analytics integration execution manifest item is invalid",
      );
    }
    return {
      deliveryKind:
        item.deliveryKind as AnalyticsIntegrationDeliveryTargetInput["deliveryKind"],
      entityKey: item.entityKey,
      deliveryIds: [...item.deliveryIds].sort() as string[],
    };
  });
  return {
    version: 1,
    integrationStateId: value.integrationStateId,
    integrationGeneration: value.integrationGeneration,
    projectId: value.projectId,
    items,
  };
}

function manifestJson(manifest: AnalyticsIntegrationExecutionManifest): string {
  return JSON.stringify(manifest);
}

function buildExecutionEnvelope(
  execution: AnalyticsIntegrationExecution,
): AnalyticsIntegrationExecutionEnvelope {
  if (execution.analyticsBackend !== "DORIS") {
    throw new AnalyticsIntegrationExecutionProvenanceError(
      "Analytics integration execution is not Doris work",
    );
  }
  return {
    executionId: execution.id,
    projectId: execution.projectId,
    integrationType: execution.integrationType,
    integrationGeneration: execution.integrationGeneration.toString(),
    analyticsBackend: "DORIS",
    deploymentGeneration: execution.deploymentGeneration.toString(),
    workloadEpochFingerprint: execution.workloadEpochFingerprint,
    runtimeContractVersion: execution.runtimeContractVersion,
    capabilityActivationGeneration:
      execution.capabilityActivationGeneration.toString(),
    capabilityContractVersion: execution.capabilityContractVersion,
    manifestChecksum: execution.manifestChecksum,
  };
}

function assertEnvelopeMatchesExecution(
  envelope: AnalyticsIntegrationExecutionEnvelope,
  execution: AnalyticsIntegrationExecution,
): void {
  const expected = buildExecutionEnvelope(execution);
  if (
    envelope.executionId !== expected.executionId ||
    envelope.projectId !== expected.projectId ||
    envelope.integrationType !== expected.integrationType ||
    envelope.integrationGeneration !== expected.integrationGeneration ||
    envelope.analyticsBackend !== expected.analyticsBackend ||
    envelope.deploymentGeneration !== expected.deploymentGeneration ||
    envelope.workloadEpochFingerprint !== expected.workloadEpochFingerprint ||
    envelope.runtimeContractVersion !== expected.runtimeContractVersion ||
    envelope.capabilityActivationGeneration !==
      expected.capabilityActivationGeneration ||
    envelope.capabilityContractVersion !== expected.capabilityContractVersion ||
    envelope.manifestChecksum !== expected.manifestChecksum
  ) {
    throw new AnalyticsIntegrationExecutionProvenanceError(
      "Analytics integration execution envelope was tampered",
    );
  }
}

function assertExecutionAdmission(
  admission: Awaited<ReturnType<typeof lockAnalyticsAdmission>>,
  execution: AnalyticsIntegrationExecution,
): void {
  if (
    admission.analyticsBackend !== execution.analyticsBackend ||
    admission.deploymentGeneration !== execution.deploymentGeneration ||
    admission.workloadEpochFingerprint !== execution.workloadEpochFingerprint ||
    admission.runtimeContractVersion !== execution.runtimeContractVersion ||
    admission.capabilityActivationGeneration !==
      execution.capabilityActivationGeneration ||
    admission.capabilityContractVersion !== execution.capabilityContractVersion
  ) {
    throw new AnalyticsIntegrationExecutionProvenanceError(
      "Analytics integration execution admission changed",
    );
  }
}

export async function lockAnalyticsIntegrationProject(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly projectId: string;
}): Promise<void> {
  if (!input.projectId) {
    throw new TypeError("Invalid analytics integration project lock");
  }
  await input.transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock(hashtextextended(${"analytics-integration:" + input.projectId}, 0))::text AS locked`,
  );
}

function validateTarget(target: AnalyticsIntegrationDeliveryTargetInput): void {
  if (
    !target.candidateKey ||
    !target.entityKey ||
    !Number.isSafeInteger(target.estimatedBytes) ||
    target.estimatedBytes < 0 ||
    target.estimatedBytes > MAX_DELIVERY_ESTIMATED_BYTES
  ) {
    throw new TypeError("Invalid analytics integration delivery target");
  }
}

function coalesceVisibleTargets(input: {
  readonly operation: VisibleOperationForIntegrationCapture;
  readonly targets: readonly AnalyticsIntegrationDeliveryTargetInput[];
}): readonly AnalyticsIntegrationDeliveryTargetInput[] {
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

  const coalesced = new Map<string, AnalyticsIntegrationDeliveryTargetInput>();
  for (const target of input.targets) {
    validateTarget(target);
    if (!visibleCandidateKeys.has(target.candidateKey)) continue;
    const identity = `${target.deliveryKind}\0${target.entityKey}`;
    const current = coalesced.get(identity);
    if (!current) {
      coalesced.set(identity, target);
      continue;
    }
    coalesced.set(identity, {
      ...target,
      candidateKey:
        current.candidateKey <= target.candidateKey
          ? current.candidateKey
          : target.candidateKey,
      estimatedBytes: Math.max(current.estimatedBytes, target.estimatedBytes),
    });
  }
  return [...coalesced.values()].sort((left, right) => {
    const kind = left.deliveryKind.localeCompare(right.deliveryKind);
    return kind !== 0 ? kind : left.entityKey.localeCompare(right.entityKey);
  });
}

async function closeInvalidDarkCapture(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly deploymentGeneration: bigint;
  readonly activationGeneration: bigint;
  readonly projectId: string;
  readonly now: Date;
  readonly failureCode:
    | "INTEGRATION_CAPTURE_WINDOW_INVALID"
    | "INTEGRATION_CAPTURE_WINDOW_EXPIRED"
    | "INTEGRATION_CAPTURE_BUDGET_EXCEEDED";
}): Promise<void> {
  await input.transaction.analyticsIntegrationState.updateMany({
    where: {
      projectId: input.projectId,
      status: {
        in: [
          "BOOTSTRAPPING_DARK",
          "BOOTSTRAPPING_ACTIVE",
          "ACTIVE",
          "DRAINING",
          "RESCANNING",
        ],
      },
    },
    data: {
      rescanRequired: true,
      lastErrorCode: input.failureCode,
    },
  });
  const closed =
    await input.transaction.analyticsCapabilityActivation.updateMany({
      where: {
        capability: "ANALYTICS_INTEGRATIONS",
        deploymentGeneration: input.deploymentGeneration,
        generation: input.activationGeneration,
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
        bootstrapCompletedGeneration: null,
        bootstrapEvidenceDigest: null,
        bootstrapCompletedAt: null,
        disabledAt: input.now,
        updatedAt: input.now,
      },
    });
  if (closed.count !== 1) {
    throw new Error("Analytics integration capture window close was fenced");
  }
}

/**
 * Runs in the same transaction that marks an analytics operation VISIBLE.
 * It records only zero-filter VISIBLE candidates and never performs analytics
 * reads or third-party effects.
 */
export async function captureAnalyticsIntegrationDeliveries(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly operation: VisibleOperationForIntegrationCapture;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly targets: readonly AnalyticsIntegrationDeliveryTargetInput[];
  readonly now: Date;
}): Promise<number> {
  if (input.targets.length === 0) return 0;
  if (!Number.isFinite(input.now.getTime())) {
    throw new TypeError("Invalid analytics integration capture timestamp");
  }

  const targets = coalesceVisibleTargets({
    operation: input.operation,
    targets: input.targets,
  });
  if (targets.length === 0) return 0;

  const capture = await lockAnalyticsCapabilityCaptureIfEnabled({
    transaction: input.transaction,
    admissionContext: input.admissionContext,
    capability: "analyticsIntegrations",
    now: input.now,
  });
  if (!capture) return 0;
  if (capture.admission.analyticsBackend !== "DORIS") {
    throw new Error(
      "Analytics integration delivery capture requires Doris provenance",
    );
  }

  await lockAnalyticsIntegrationProject({
    transaction: input.transaction,
    projectId: input.operation.projectId,
  });

  const states = await input.transaction.analyticsIntegrationState.findMany({
    where: {
      projectId: input.operation.projectId,
      status: {
        in: [
          "BOOTSTRAPPING_DARK",
          "BOOTSTRAPPING_ACTIVE",
          "ACTIVE",
          "DRAINING",
          "RESCANNING",
        ],
      },
    },
    orderBy: [{ integrationType: "asc" }, { id: "asc" }],
  });
  if (states.length === 0) return 0;

  // Counter updates are serialized deployment-wide so two projects cannot
  // both observe the last free quota and overcommit it.
  await input.transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock(${DEPLOYMENT_DELIVERY_QUOTA_LOCK_KEY})::text AS locked`,
  );
  const deploymentUsage =
    await input.transaction.analyticsIntegrationState.aggregate({
      _sum: {
        pendingRows: true,
        pendingEstimatedBytes: true,
      },
    });
  let deploymentPendingRows = deploymentUsage._sum.pendingRows ?? 0n;
  let deploymentPendingBytes = deploymentUsage._sum.pendingEstimatedBytes ?? 0n;

  const maximumNewRows = BigInt(states.length * targets.length);
  if (capture.activationStatus === "DARK") {
    const failureCode =
      !capture.captureExpiresAt || capture.captureRowBudget === null
        ? "INTEGRATION_CAPTURE_WINDOW_INVALID"
        : input.now >= capture.captureExpiresAt
          ? "INTEGRATION_CAPTURE_WINDOW_EXPIRED"
          : capture.captureRows + maximumNewRows >
              BigInt(capture.captureRowBudget)
            ? "INTEGRATION_CAPTURE_BUDGET_EXCEEDED"
            : null;
    if (failureCode) {
      await closeInvalidDarkCapture({
        transaction: input.transaction,
        deploymentGeneration: capture.admission.deploymentGeneration,
        activationGeneration: capture.admission.capabilityActivationGeneration,
        projectId: input.operation.projectId,
        now: input.now,
        failureCode,
      });
      return 0;
    }
  }

  let createdCount = 0;
  for (const state of states) {
    const status =
      capture.activationStatus === "ACTIVE" &&
      (state.status === "BOOTSTRAPPING_ACTIVE" ||
        state.status === "ACTIVE" ||
        state.status === "RESCANNING")
        ? ("PENDING" as const)
        : ("SUSPENDED" as const);
    const rows = targets.map((target) => ({
      id: analyticsIntegrationDeliveryIdentity({
        integrationStateId: state.id,
        integrationGeneration: state.generation,
        operationId: input.operation.id,
        deliveryKind: target.deliveryKind,
        entityKey: target.entityKey,
      }),
      integrationStateId: state.id,
      integrationType: state.integrationType,
      integrationGeneration: state.generation,
      operationId: input.operation.id,
      projectId: input.operation.projectId,
      sourceCandidateKey: target.candidateKey,
      deliveryKind: target.deliveryKind,
      entityKey: target.entityKey,
      estimatedBytes: target.estimatedBytes,
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
    }));
    const existingIds = new Set(
      (
        await input.transaction.analyticsIntegrationPendingDelivery.findMany({
          where: { id: { in: rows.map(({ id }) => id) } },
          select: { id: true },
        })
      ).map(({ id }) => id),
    );
    const newRows = rows.filter(({ id }) => !existingIds.has(id));
    if (newRows.length === 0) continue;
    const newEstimatedBytes = newRows.reduce(
      (sum, row) => sum + BigInt(row.estimatedBytes),
      0n,
    );
    if (
      state.pendingRows + BigInt(newRows.length) >
        MAX_PENDING_ROWS_PER_INTEGRATION ||
      state.pendingEstimatedBytes + newEstimatedBytes >
        MAX_PENDING_BYTES_PER_INTEGRATION ||
      deploymentPendingRows + BigInt(newRows.length) >
        MAX_PENDING_ROWS_PER_DEPLOYMENT ||
      deploymentPendingBytes + newEstimatedBytes >
        MAX_PENDING_BYTES_PER_DEPLOYMENT
    ) {
      const deploymentBudgetExceeded =
        deploymentPendingRows + BigInt(newRows.length) >
          MAX_PENDING_ROWS_PER_DEPLOYMENT ||
        deploymentPendingBytes + newEstimatedBytes >
          MAX_PENDING_BYTES_PER_DEPLOYMENT;
      const paused =
        await input.transaction.analyticsIntegrationState.updateMany({
          where: {
            id: state.id,
            generation: state.generation,
            status: state.status,
          },
          data: {
            status: "PAUSED_BACKLOG",
            rescanRequired: true,
            lastErrorCode: deploymentBudgetExceeded
              ? "DEPLOYMENT_PENDING_BUDGET_EXCEEDED"
              : "INTEGRATION_PENDING_BUDGET_EXCEEDED",
          },
        });
      if (paused.count !== 1) {
        throw new Error("Analytics integration backlog pause was fenced");
      }
      continue;
    }

    const created =
      await input.transaction.analyticsIntegrationPendingDelivery.createMany({
        data: newRows,
        skipDuplicates: true,
      });
    if (created.count !== newRows.length) {
      throw new Error("Analytics integration delivery capture raced");
    }
    const counted =
      await input.transaction.analyticsIntegrationState.updateMany({
        where: {
          id: state.id,
          generation: state.generation,
        },
        data: {
          pendingRows: { increment: BigInt(created.count) },
          pendingEstimatedBytes: { increment: newEstimatedBytes },
        },
      });
    if (counted.count !== 1) {
      throw new Error("Analytics integration delivery counter was fenced");
    }
    createdCount += created.count;
    deploymentPendingRows += BigInt(created.count);
    deploymentPendingBytes += newEstimatedBytes;
  }

  if (capture.activationStatus === "DARK" && createdCount > 0) {
    const recorded =
      await input.transaction.analyticsCapabilityActivation.updateMany({
        where: {
          capability: "ANALYTICS_INTEGRATIONS",
          deploymentGeneration: capture.admission.deploymentGeneration,
          generation: capture.admission.capabilityActivationGeneration,
          status: "DARK",
          captureEnabled: true,
          captureRows: capture.captureRows,
        },
        data: { captureRows: { increment: BigInt(createdCount) } },
      });
    if (recorded.count !== 1) {
      throw new Error("Analytics integration capture budget update was fenced");
    }
  }
  return createdCount;
}

export async function ensureDorisAnalyticsIntegrationState(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly integrationType: AnalyticsIntegrationType;
  readonly now?: Date;
}) {
  const now = input.now ?? new Date();
  if (!input.projectId || !Number.isFinite(now.getTime())) {
    throw new TypeError("Invalid Doris analytics integration state");
  }
  return (input.client ?? prisma).$transaction(async (transaction) => {
    await lockAnalyticsIntegrationProject({
      transaction,
      projectId: input.projectId,
    });
    const admission = await lockAnalyticsAdmission({
      transaction,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
      expectedBackend: input.admissionContext.backend,
      expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
      capability: "analyticsIntegrations",
      action: "internalBootstrap",
      now,
    });
    if (
      admission.analyticsBackend !== "DORIS" ||
      admission.capabilityActivationGeneration === undefined
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Doris analytics integration bootstrap admission is invalid",
      );
    }
    const activation =
      await transaction.analyticsCapabilityActivation.findUniqueOrThrow({
        where: { capability: "ANALYTICS_INTEGRATIONS" },
      });
    const nextStatus =
      activation.status === "DARK"
        ? ("BOOTSTRAPPING_DARK" as const)
        : ("BOOTSTRAPPING_ACTIVE" as const);
    const existing = await transaction.analyticsIntegrationState.findUnique({
      where: {
        projectId_integrationType: {
          projectId: input.projectId,
          integrationType: input.integrationType,
        },
      },
    });
    if (!existing) {
      return transaction.analyticsIntegrationState.create({
        data: {
          projectId: input.projectId,
          integrationType: input.integrationType,
          status: nextStatus,
        },
      });
    }
    if (existing.status !== "DISABLED") return existing;
    const [nonTerminalDeliveries, nonTerminalExecutions] = await Promise.all([
      transaction.analyticsIntegrationPendingDelivery.count({
        where: {
          integrationStateId: existing.id,
          integrationGeneration: existing.generation,
          status: { in: ["SUSPENDED", "PENDING", "CLAIMED"] },
        },
      }),
      transaction.analyticsIntegrationExecution.count({
        where: {
          integrationStateId: existing.id,
          integrationGeneration: existing.generation,
          status: {
            in: ["SEALED", "PUBLISHED", "RUNNING", "RETRYING"],
          },
        },
      }),
    ]);
    if (
      existing.pendingRows !== 0n ||
      existing.pendingEstimatedBytes !== 0n ||
      nonTerminalDeliveries !== 0 ||
      nonTerminalExecutions !== 0
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Disabled analytics integration still has durable work",
      );
    }
    return transaction.analyticsIntegrationState.update({
      where: { id: existing.id },
      data: {
        generation: { increment: 1n },
        status: nextStatus,
        pendingRows: 0n,
        pendingEstimatedBytes: 0n,
        rescanRequired: false,
        cutoffAcceptanceSequence: null,
        bootstrapManifest: Prisma.DbNull,
        bootstrapManifestKey: null,
        bootstrapManifestChecksum: null,
        bootstrapManifestRows: null,
        bootstrapSealedAt: null,
        lastErrorCode: null,
      },
    });
  });
}

export async function syncDorisAnalyticsIntegrationConfigState(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly integrationType: AnalyticsIntegrationType;
  readonly enabled: boolean;
  readonly now?: Date;
}) {
  const now = input.now ?? new Date();
  if (!input.projectId || !Number.isFinite(now.getTime())) {
    throw new TypeError("Invalid Doris analytics integration config mutation");
  }
  await lockAnalyticsIntegrationProject({
    transaction: input.transaction,
    projectId: input.projectId,
  });
  const admission = await lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend: input.admissionContext.backend,
    expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
    capability: "analyticsIntegrations",
    action: "externalProducer",
    now,
  });
  if (admission.analyticsBackend !== "DORIS") {
    throw new AnalyticsIntegrationExecutionProvenanceError(
      "Doris analytics integration config admission is invalid",
    );
  }
  const existing = await input.transaction.analyticsIntegrationState.findUnique(
    {
      where: {
        projectId_integrationType: {
          projectId: input.projectId,
          integrationType: input.integrationType,
        },
      },
    },
  );

  if (input.enabled) {
    if (!existing) {
      return input.transaction.analyticsIntegrationState.create({
        data: {
          projectId: input.projectId,
          integrationType: input.integrationType,
          status: "BOOTSTRAPPING_ACTIVE",
        },
      });
    }
    if (existing.status !== "DISABLED") return existing;
    const nonTerminal = await Promise.all([
      input.transaction.analyticsIntegrationPendingDelivery.count({
        where: {
          integrationStateId: existing.id,
          status: { in: ["SUSPENDED", "PENDING", "CLAIMED"] },
        },
      }),
      input.transaction.analyticsIntegrationExecution.count({
        where: {
          integrationStateId: existing.id,
          status: { in: ["SEALED", "PUBLISHED", "RUNNING", "RETRYING"] },
        },
      }),
    ]);
    if (
      existing.pendingRows !== 0n ||
      existing.pendingEstimatedBytes !== 0n ||
      nonTerminal.some((count) => count !== 0)
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Disabled analytics integration still has durable work",
      );
    }
    return input.transaction.analyticsIntegrationState.update({
      where: { id: existing.id },
      data: {
        generation: { increment: 1n },
        status: "BOOTSTRAPPING_ACTIVE",
        pendingRows: 0n,
        pendingEstimatedBytes: 0n,
        rescanRequired: false,
        cutoffAcceptanceSequence: null,
        bootstrapManifest: Prisma.DbNull,
        bootstrapManifestKey: null,
        bootstrapManifestChecksum: null,
        bootstrapManifestRows: null,
        bootstrapSealedAt: null,
        lastErrorCode: null,
      },
    });
  }

  if (!existing || existing.status === "DISABLED") return existing;
  const running = await input.transaction.analyticsIntegrationExecution.count({
    where: {
      integrationStateId: existing.id,
      integrationGeneration: existing.generation,
      status: "RUNNING",
    },
  });
  if (running !== 0) {
    throw new AnalyticsIntegrationExecutionProvenanceError(
      "Analytics integration has a running delivery; retry disable after its lease expires",
    );
  }
  await input.transaction.analyticsIntegrationPendingDelivery.updateMany({
    where: {
      integrationStateId: existing.id,
      integrationGeneration: existing.generation,
      status: { in: ["SUSPENDED", "PENDING", "CLAIMED"] },
    },
    data: {
      status: "QUARANTINED",
      completedAt: now,
      claimOwner: null,
      claimExpiresAt: null,
      failureCode: "INTEGRATION_CONFIG_DISABLED",
    },
  });
  await input.transaction.analyticsIntegrationExecution.updateMany({
    where: {
      integrationStateId: existing.id,
      integrationGeneration: existing.generation,
      status: { in: ["SEALED", "PUBLISHED", "RETRYING"] },
    },
    data: {
      status: "CANCELLED",
      completedAt: now,
      claimOwner: null,
      claimExpiresAt: null,
      lastErrorCode: "INTEGRATION_CONFIG_DISABLED",
    },
  });
  return input.transaction.analyticsIntegrationState.update({
    where: { id: existing.id },
    data: {
      status: "DISABLED",
      pendingRows: 0n,
      pendingEstimatedBytes: 0n,
      rescanRequired: false,
      cutoffAcceptanceSequence: null,
      bootstrapManifest: Prisma.DbNull,
      bootstrapManifestKey: null,
      bootstrapManifestChecksum: null,
      bootstrapManifestRows: null,
      bootstrapSealedAt: null,
      lastErrorCode: null,
    },
  });
}

export async function verifyDorisAnalyticsIntegrationBootstrap(input: {
  readonly transaction: Prisma.TransactionClient;
}): Promise<{ readonly bootstrapEvidenceDigest: string }> {
  const [posthog, mixpanel, blob, activation] = await Promise.all([
    input.transaction.posthogIntegration.findMany({
      where: { enabled: true },
      select: { projectId: true },
    }),
    input.transaction.mixpanelIntegration.findMany({
      where: { enabled: true },
      select: { projectId: true },
    }),
    input.transaction.blobStorageIntegration.findMany({
      where: { enabled: true },
      select: { projectId: true },
    }),
    input.transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: "ANALYTICS_INTEGRATIONS" },
    }),
  ]);
  const required = [
    ...posthog.map(({ projectId }) => ({
      projectId,
      integrationType: "POSTHOG" as const,
    })),
    ...mixpanel.map(({ projectId }) => ({
      projectId,
      integrationType: "MIXPANEL" as const,
    })),
    ...blob.map(({ projectId }) => ({
      projectId,
      integrationType: "BLOB_STORAGE" as const,
    })),
  ].sort((left, right) => {
    const project = left.projectId.localeCompare(right.projectId);
    return project !== 0
      ? project
      : left.integrationType.localeCompare(right.integrationType);
  });
  const evidence: string[] = [];
  for (const item of required) {
    const state =
      await input.transaction.analyticsIntegrationState.findUniqueOrThrow({
        where: {
          projectId_integrationType: {
            projectId: item.projectId,
            integrationType: item.integrationType,
          },
        },
      });
    if (
      state.status !== "BOOTSTRAPPING_DARK" ||
      !state.bootstrapManifest ||
      !state.bootstrapManifestChecksum ||
      state.bootstrapManifestRows === null ||
      !state.bootstrapSealedAt
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Enabled analytics integration has no sealed DARK bootstrap",
      );
    }
    const manifest = parseManifest(state.bootstrapManifest);
    if (
      manifest.integrationStateId !== state.id ||
      manifest.integrationGeneration !== state.generation.toString() ||
      manifest.projectId !== state.projectId ||
      BigInt(manifest.items.length) !== state.bootstrapManifestRows ||
      sha256(manifestJson(manifest)) !== state.bootstrapManifestChecksum
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Analytics integration bootstrap state manifest is invalid",
      );
    }
    const executions =
      await input.transaction.analyticsIntegrationExecution.findMany({
        where: {
          integrationStateId: state.id,
          integrationGeneration: state.generation,
          kind: "BOOTSTRAP",
          status: "SEALED",
        },
        orderBy: { id: "asc" },
      });
    const executionItems = executions
      .flatMap((execution) => {
        if (
          execution.analyticsBackend !== "DORIS" ||
          execution.deploymentGeneration !== activation.deploymentGeneration ||
          execution.capabilityActivationGeneration !== activation.generation ||
          execution.capabilityContractVersion !== activation.contractVersion ||
          sha256(manifestJson(parseManifest(execution.manifest))) !==
            execution.manifestChecksum
        ) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration bootstrap execution provenance is invalid",
          );
        }
        return parseManifest(execution.manifest).items;
      })
      .sort((left, right) => {
        const kind = left.deliveryKind.localeCompare(right.deliveryKind);
        return kind !== 0
          ? kind
          : left.entityKey.localeCompare(right.entityKey);
      });
    if (
      JSON.stringify(executionItems) !== JSON.stringify(manifest.items) ||
      executions.length === 0
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Analytics integration bootstrap execution manifest is incomplete",
      );
    }
    evidence.push(
      [
        state.projectId,
        state.integrationType,
        state.generation.toString(),
        state.bootstrapManifestChecksum,
        state.bootstrapManifestRows.toString(),
      ].join(":"),
    );
  }
  return {
    bootstrapEvidenceDigest: sha256(JSON.stringify(evidence)),
  };
}

export async function prepareDorisAnalyticsIntegrationDarkCapture(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly now: Date;
}): Promise<number> {
  const [posthog, mixpanel, blob, activation] = await Promise.all([
    input.transaction.posthogIntegration.findMany({
      where: { enabled: true },
      select: { projectId: true },
    }),
    input.transaction.mixpanelIntegration.findMany({
      where: { enabled: true },
      select: { projectId: true },
    }),
    input.transaction.blobStorageIntegration.findMany({
      where: { enabled: true },
      select: { projectId: true },
    }),
    input.transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: "ANALYTICS_INTEGRATIONS" },
    }),
  ]);
  const hasReplayCutoff =
    activation.rescanRequired &&
    activation.captureRequired &&
    activation.cutoffActivationGeneration === activation.generation - 1n &&
    typeof activation.cutoffState === "object" &&
    activation.cutoffState !== null &&
    !Array.isArray(activation.cutoffState) &&
    "kind" in activation.cutoffState &&
    activation.cutoffState.kind === "analytics_integration_operation_cutoff";
  const enabled = [
    ...posthog.map(({ projectId }) => ({
      projectId,
      integrationType: "POSTHOG" as const,
    })),
    ...mixpanel.map(({ projectId }) => ({
      projectId,
      integrationType: "MIXPANEL" as const,
    })),
    ...blob.map(({ projectId }) => ({
      projectId,
      integrationType: "BLOB_STORAGE" as const,
    })),
  ].sort((left, right) => {
    const project = left.projectId.localeCompare(right.projectId);
    return project !== 0
      ? project
      : left.integrationType.localeCompare(right.integrationType);
  });
  for (const config of enabled) {
    await lockAnalyticsIntegrationProject({
      transaction: input.transaction,
      projectId: config.projectId,
    });
    const state = await input.transaction.analyticsIntegrationState.findUnique({
      where: {
        projectId_integrationType: {
          projectId: config.projectId,
          integrationType: config.integrationType,
        },
      },
    });
    if (!state) {
      await input.transaction.analyticsIntegrationState.create({
        data: {
          projectId: config.projectId,
          integrationType: config.integrationType,
          status: "BOOTSTRAPPING_DARK",
        },
      });
      continue;
    }
    if (state.status === "BOOTSTRAPPING_DARK") continue;
    const running = await input.transaction.analyticsIntegrationExecution.count(
      {
        where: {
          integrationStateId: state.id,
          integrationGeneration: state.generation,
          status: "RUNNING",
        },
      },
    );
    if (running !== 0) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Analytics integration DARK replay is blocked by a running execution",
      );
    }
    if (!hasReplayCutoff) {
      await input.transaction.analyticsIntegrationPendingDelivery.updateMany({
        where: {
          integrationStateId: state.id,
          integrationGeneration: state.generation,
          status: { in: ["SUSPENDED", "PENDING", "CLAIMED"] },
        },
        data: {
          status: "QUARANTINED",
          completedAt: input.now,
          claimOwner: null,
          claimExpiresAt: null,
          failureCode: "INTEGRATION_GENERATION_REPLAYED",
        },
      });
    }
    await input.transaction.analyticsIntegrationExecution.updateMany({
      where: {
        integrationStateId: state.id,
        integrationGeneration: state.generation,
        status: { in: ["SEALED", "PUBLISHED", "RETRYING"] },
      },
      data: {
        status: "CANCELLED",
        completedAt: input.now,
        claimOwner: null,
        claimExpiresAt: null,
        lastErrorCode: "INTEGRATION_GENERATION_REPLAYED",
      },
    });
    await input.transaction.analyticsIntegrationState.update({
      where: { id: state.id },
      data: {
        generation: { increment: 1n },
        status: "BOOTSTRAPPING_DARK",
        pendingRows: 0n,
        pendingEstimatedBytes: 0n,
        rescanRequired: hasReplayCutoff,
        cutoffAcceptanceSequence: null,
        bootstrapManifest: Prisma.DbNull,
        bootstrapManifestKey: null,
        bootstrapManifestChecksum: null,
        bootstrapManifestRows: null,
        bootstrapSealedAt: null,
        lastErrorCode: null,
      },
    });
  }
  return enabled.length;
}

export async function replayDorisAnalyticsIntegrationDrainCapture(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly integrationType: AnalyticsIntegrationType;
  readonly now?: Date;
}): Promise<number> {
  const now = input.now ?? new Date();
  if (!input.projectId || !Number.isFinite(now.getTime())) {
    throw new TypeError("Invalid analytics integration replay handoff");
  }
  return (input.client ?? prisma).$transaction(
    async (transaction) => {
      await lockAnalyticsIntegrationProject({
        transaction,
        projectId: input.projectId,
      });
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "analyticsIntegrations",
        action: "internalBootstrap",
        now,
      });
      if (
        admission.analyticsBackend !== "DORIS" ||
        admission.capabilityActivationGeneration === undefined ||
        admission.capabilityContractVersion === undefined
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration replay admission is invalid",
        );
      }
      const [activation, state] = await Promise.all([
        transaction.analyticsCapabilityActivation.findUniqueOrThrow({
          where: { capability: "ANALYTICS_INTEGRATIONS" },
        }),
        transaction.analyticsIntegrationState.findUniqueOrThrow({
          where: {
            projectId_integrationType: {
              projectId: input.projectId,
              integrationType: input.integrationType,
            },
          },
        }),
      ]);
      if (!state.rescanRequired) return 0;
      if (
        (state.status !== "BOOTSTRAPPING_DARK" &&
          state.status !== "BOOTSTRAPPING_ACTIVE") ||
        activation.generation !== admission.capabilityActivationGeneration ||
        activation.cutoffActivationGeneration !== activation.generation - 1n ||
        !activation.rescanRequired ||
        !activation.captureRequired
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration replay cutoff changed",
        );
      }
      const oldDeliveries =
        await transaction.analyticsIntegrationPendingDelivery.findMany({
          where: {
            integrationStateId: state.id,
            integrationGeneration: state.generation - 1n,
            deploymentGeneration: admission.deploymentGeneration,
            capabilityActivationGeneration: activation.generation - 1n,
            status: "SUSPENDED",
          },
          orderBy: { id: "asc" },
        });
      const replayBytes = oldDeliveries.reduce(
        (sum, delivery) => sum + BigInt(delivery.estimatedBytes),
        0n,
      );
      if (
        BigInt(oldDeliveries.length) > MAX_PENDING_ROWS_PER_INTEGRATION ||
        replayBytes > MAX_PENDING_BYTES_PER_INTEGRATION ||
        activation.captureRowBudget === null ||
        activation.captureRows + BigInt(oldDeliveries.length) >
          BigInt(activation.captureRowBudget)
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration replay exceeds its durable budget",
        );
      }
      await transaction.$queryRaw(
        Prisma.sql`SELECT pg_advisory_xact_lock(${DEPLOYMENT_DELIVERY_QUOTA_LOCK_KEY})::text AS locked`,
      );
      const deploymentUsage =
        await transaction.analyticsIntegrationState.aggregate({
          _sum: {
            pendingRows: true,
            pendingEstimatedBytes: true,
          },
        });
      if (
        (deploymentUsage._sum.pendingRows ?? 0n) +
          BigInt(oldDeliveries.length) >
          MAX_PENDING_ROWS_PER_DEPLOYMENT ||
        (deploymentUsage._sum.pendingEstimatedBytes ?? 0n) + replayBytes >
          MAX_PENDING_BYTES_PER_DEPLOYMENT
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration replay exceeds the deployment budget",
        );
      }
      const rows = oldDeliveries.map((delivery) => ({
        id: analyticsIntegrationDeliveryIdentity({
          integrationStateId: state.id,
          integrationGeneration: state.generation,
          operationId: delivery.operationId,
          deliveryKind: delivery.deliveryKind,
          entityKey: delivery.entityKey,
        }),
        integrationStateId: state.id,
        integrationType: state.integrationType,
        integrationGeneration: state.generation,
        operationId: delivery.operationId,
        projectId: state.projectId,
        sourceCandidateKey: delivery.sourceCandidateKey,
        deliveryKind: delivery.deliveryKind,
        entityKey: delivery.entityKey,
        estimatedBytes: delivery.estimatedBytes,
        analyticsBackend: admission.analyticsBackend,
        deploymentGeneration: admission.deploymentGeneration,
        workloadEpochFingerprint: admission.workloadEpochFingerprint,
        runtimeContractVersion: admission.runtimeContractVersion,
        captureRuntimeLeaseId: admission.admittingRuntimeLeaseId,
        capabilityActivationGeneration:
          admission.capabilityActivationGeneration!,
        capabilityContractVersion: admission.capabilityContractVersion!,
        status: "SUSPENDED" as const,
        nextAttemptAt: now,
      }));
      const created =
        await transaction.analyticsIntegrationPendingDelivery.createMany({
          data: rows,
          skipDuplicates: true,
        });
      if (created.count !== rows.length) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration replay handoff raced",
        );
      }
      if (oldDeliveries.length > 0) {
        const retired =
          await transaction.analyticsIntegrationPendingDelivery.updateMany({
            where: {
              id: { in: oldDeliveries.map(({ id }) => id) },
              status: "SUSPENDED",
            },
            data: {
              status: "QUARANTINED",
              completedAt: now,
              failureCode: "INTEGRATION_GENERATION_REPLAYED",
            },
          });
        if (retired.count !== oldDeliveries.length) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration replay retirement was fenced",
          );
        }
      }
      const stateUpdated =
        await transaction.analyticsIntegrationState.updateMany({
          where: {
            id: state.id,
            generation: state.generation,
            rescanRequired: true,
            pendingRows: 0n,
            pendingEstimatedBytes: 0n,
          },
          data: {
            pendingRows: BigInt(rows.length),
            pendingEstimatedBytes: replayBytes,
            rescanRequired: false,
            lastErrorCode: null,
          },
        });
      if (stateUpdated.count !== 1) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration replay counters were fenced",
        );
      }
      if (rows.length > 0) {
        const captureUpdated =
          await transaction.analyticsCapabilityActivation.updateMany({
            where: {
              capability: "ANALYTICS_INTEGRATIONS",
              generation: activation.generation,
              status: "DARK",
              captureRows: activation.captureRows,
            },
            data: { captureRows: { increment: BigInt(rows.length) } },
          });
        if (captureUpdated.count !== 1) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration replay capture budget was fenced",
          );
        }
      }
      return rows.length;
    },
    { timeout: 30_000 },
  );
}

export async function verifyDorisAnalyticsIntegrationDrain(
  transaction: Prisma.TransactionClient,
  provenance: {
    readonly deploymentGeneration: bigint;
    readonly activationGeneration: bigint;
    readonly capabilityContractVersion: number;
  },
): Promise<void> {
  const nonTerminal = await transaction.analyticsIntegrationExecution.count({
    where: {
      analyticsBackend: "DORIS",
      deploymentGeneration: provenance.deploymentGeneration,
      capabilityActivationGeneration: provenance.activationGeneration,
      capabilityContractVersion: provenance.capabilityContractVersion,
      status: { in: ["SEALED", "PUBLISHED", "RUNNING", "RETRYING"] },
    },
  });
  if (nonTerminal !== 0) {
    throw new Error(
      "Analytics integration capability still has executable work",
    );
  }
}

export async function sealDorisAnalyticsIntegrationReplayCutoff(
  transaction: Prisma.TransactionClient,
): Promise<{
  readonly cutoffState: Prisma.InputJsonValue;
  readonly cutoffDigest: string;
}> {
  const maximum = await transaction.analyticsIngestionOperation.aggregate({
    _max: { acceptanceSequence: true },
  });
  const states = await transaction.analyticsIntegrationState.findMany({
    where: { status: "DRAINING" },
    select: {
      id: true,
      generation: true,
      pendingRows: true,
      pendingEstimatedBytes: true,
    },
    orderBy: { id: "asc" },
  });
  const cutoffState = {
    kind: "analytics_integration_operation_cutoff",
    acceptanceSequence: (maximum._max.acceptanceSequence ?? 0n).toString(),
    states: states.map((state) => ({
      id: state.id,
      generation: state.generation.toString(),
      pendingRows: state.pendingRows.toString(),
      pendingEstimatedBytes: state.pendingEstimatedBytes.toString(),
    })),
  } satisfies Prisma.InputJsonObject;
  return {
    cutoffState,
    cutoffDigest: sha256(JSON.stringify(cutoffState)),
  };
}

export async function sealDorisAnalyticsIntegrationBootstrapManifest(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly integrationType: AnalyticsIntegrationType;
  readonly identities: readonly AnalyticsIntegrationBootstrapIdentity[];
  readonly now?: Date;
}): Promise<readonly AnalyticsIntegrationExecutionEnvelope[]> {
  const now = input.now ?? new Date();
  if (
    !input.projectId ||
    !Number.isFinite(now.getTime()) ||
    input.identities.length > Number(MAX_PENDING_ROWS_PER_INTEGRATION)
  ) {
    throw new TypeError("Invalid analytics integration bootstrap manifest");
  }
  return (input.client ?? prisma).$transaction(
    async (transaction) => {
      await lockAnalyticsIntegrationProject({
        transaction,
        projectId: input.projectId,
      });
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "analyticsIntegrations",
        action: "internalBootstrap",
        now,
      });
      if (
        admission.analyticsBackend !== "DORIS" ||
        admission.capabilityActivationGeneration === undefined ||
        admission.capabilityContractVersion === undefined
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Doris analytics integration bootstrap admission is invalid",
        );
      }
      const state =
        await transaction.analyticsIntegrationState.findUniqueOrThrow({
          where: {
            projectId_integrationType: {
              projectId: input.projectId,
              integrationType: input.integrationType,
            },
          },
        });
      if (
        state.status !== "BOOTSTRAPPING_DARK" &&
        state.status !== "BOOTSTRAPPING_ACTIVE"
      ) {
        return [];
      }

      const manifest = buildBootstrapManifest({
        integrationStateId: state.id,
        integrationGeneration: state.generation,
        projectId: state.projectId,
        identities: input.identities,
      });
      const checksum = sha256(manifestJson(manifest));
      const key = `postgres-json://analytics-integration-bootstrap/${state.id}/${state.generation}`;
      if (state.bootstrapManifestChecksum) {
        if (
          state.bootstrapManifestChecksum !== checksum ||
          state.bootstrapManifestKey !== key ||
          state.bootstrapManifestRows !== BigInt(manifest.items.length) ||
          !state.bootstrapManifest ||
          sha256(manifestJson(parseManifest(state.bootstrapManifest))) !==
            checksum
        ) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration bootstrap manifest changed",
          );
        }
      } else {
        const sealed = await transaction.analyticsIntegrationState.updateMany({
          where: {
            id: state.id,
            generation: state.generation,
            bootstrapManifestChecksum: null,
          },
          data: {
            bootstrapManifest: manifest as unknown as Prisma.InputJsonValue,
            bootstrapManifestKey: key,
            bootstrapManifestChecksum: checksum,
            bootstrapManifestRows: BigInt(manifest.items.length),
            bootstrapSealedAt: now,
            lastErrorCode: null,
          },
        });
        if (sealed.count !== 1) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration bootstrap sealing raced",
          );
        }
      }

      const chunks =
        manifest.items.length === 0
          ? [[]]
          : Array.from(
              {
                length: Math.ceil(
                  manifest.items.length / MAX_EXECUTION_DELIVERIES,
                ),
              },
              (_, index) =>
                manifest.items.slice(
                  index * MAX_EXECUTION_DELIVERIES,
                  (index + 1) * MAX_EXECUTION_DELIVERIES,
                ),
            );
      const envelopes: AnalyticsIntegrationExecutionEnvelope[] = [];
      for (const items of chunks) {
        const executionManifest = { ...manifest, items };
        const manifestChecksum = sha256(manifestJson(executionManifest));
        const id = executionIdentity({
          integrationStateId: state.id,
          integrationGeneration: state.generation,
          manifestChecksum,
        });
        const execution =
          await transaction.analyticsIntegrationExecution.upsert({
            where: { id },
            create: {
              id,
              integrationStateId: state.id,
              integrationType: state.integrationType,
              integrationGeneration: state.generation,
              projectId: state.projectId,
              kind: "BOOTSTRAP",
              analyticsBackend: admission.analyticsBackend,
              deploymentGeneration: admission.deploymentGeneration,
              workloadEpochFingerprint: admission.workloadEpochFingerprint,
              runtimeContractVersion: admission.runtimeContractVersion,
              capabilityActivationGeneration:
                admission.capabilityActivationGeneration,
              capabilityContractVersion: admission.capabilityContractVersion,
              sealedRuntimeLeaseId: admission.admittingRuntimeLeaseId,
              manifest: executionManifest as unknown as Prisma.InputJsonValue,
              manifestChecksum,
              deliveryCount: 0,
              status: "SEALED",
              nextAttemptAt: now,
            },
            update: {},
          });
        assertExecutionAdmission(admission, execution);
        if (
          execution.kind !== "BOOTSTRAP" ||
          execution.integrationGeneration !== state.generation ||
          execution.manifestChecksum !== manifestChecksum
        ) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration bootstrap execution changed",
          );
        }
        envelopes.push(buildExecutionEnvelope(execution));
      }
      return envelopes;
    },
    { timeout: 30_000 },
  );
}

export async function sealAnalyticsIntegrationExecution(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly projectId: string;
  readonly integrationType: AnalyticsIntegrationType;
  readonly now?: Date;
  readonly limit?: number;
}): Promise<AnalyticsIntegrationExecutionEnvelope | null> {
  const now = input.now ?? new Date();
  const limit = input.limit ?? MAX_EXECUTION_DELIVERIES;
  if (
    !input.projectId ||
    !Number.isFinite(now.getTime()) ||
    !Number.isSafeInteger(limit) ||
    limit < 1 ||
    limit > MAX_EXECUTION_DELIVERIES
  ) {
    throw new TypeError("Invalid analytics integration execution seal");
  }
  return (input.client ?? prisma).$transaction(
    async (transaction) => {
      await lockAnalyticsIntegrationProject({
        transaction,
        projectId: input.projectId,
      });
      const state =
        await transaction.analyticsIntegrationState.findUniqueOrThrow({
          where: {
            projectId_integrationType: {
              projectId: input.projectId,
              integrationType: input.integrationType,
            },
          },
        });
      if (
        state.status !== "BOOTSTRAPPING_ACTIVE" &&
        state.status !== "ACTIVE" &&
        state.status !== "RESCANNING"
      ) {
        return null;
      }
      if (
        !state.bootstrapManifestChecksum ||
        (await transaction.analyticsIntegrationExecution.count({
          where: {
            integrationStateId: state.id,
            integrationGeneration: state.generation,
            kind: "BOOTSTRAP",
            status: {
              in: ["SEALED", "PUBLISHED", "RUNNING", "RETRYING"],
            },
          },
        })) > 0
      ) {
        return null;
      }
      const rows =
        await transaction.analyticsIntegrationPendingDelivery.findMany({
          where: {
            integrationStateId: state.id,
            integrationGeneration: state.generation,
            status: "PENDING",
            executionId: null,
            nextAttemptAt: { lte: now },
          },
          orderBy: [
            { nextAttemptAt: "asc" },
            { createdAt: "asc" },
            { id: "asc" },
          ],
          take: limit,
        });
      if (rows.length === 0) return null;

      const first = rows[0]!;
      if (
        rows.some(
          (row) =>
            row.projectId !== first.projectId ||
            row.integrationType !== first.integrationType ||
            row.integrationGeneration !== first.integrationGeneration ||
            row.analyticsBackend !== first.analyticsBackend ||
            row.deploymentGeneration !== first.deploymentGeneration ||
            row.workloadEpochFingerprint !== first.workloadEpochFingerprint ||
            row.runtimeContractVersion !== first.runtimeContractVersion ||
            row.capabilityActivationGeneration !==
              first.capabilityActivationGeneration ||
            row.capabilityContractVersion !== first.capabilityContractVersion,
        )
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration pending rows have mixed provenance",
        );
      }
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "analyticsIntegrations",
        action: "recovery",
        expectedCapabilityActivationGeneration:
          first.capabilityActivationGeneration,
        expectedCapabilityContractVersion: first.capabilityContractVersion,
        now,
      });
      if (
        admission.analyticsBackend !== first.analyticsBackend ||
        admission.deploymentGeneration !== first.deploymentGeneration ||
        admission.workloadEpochFingerprint !== first.workloadEpochFingerprint ||
        admission.runtimeContractVersion !== first.runtimeContractVersion ||
        admission.capabilityActivationGeneration !==
          first.capabilityActivationGeneration ||
        admission.capabilityContractVersion !== first.capabilityContractVersion
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration pending provenance is no longer admitted",
        );
      }

      const manifest = buildManifest(rows);
      const manifestChecksum = sha256(manifestJson(manifest));
      const executionId = executionIdentity({
        integrationStateId: state.id,
        integrationGeneration: state.generation,
        manifestChecksum,
      });
      const execution = await transaction.analyticsIntegrationExecution.create({
        data: {
          id: executionId,
          integrationStateId: state.id,
          integrationType: state.integrationType,
          integrationGeneration: state.generation,
          projectId: state.projectId,
          kind: state.status === "RESCANNING" ? "RESCAN" : "INCREMENTAL",
          analyticsBackend: admission.analyticsBackend,
          deploymentGeneration: admission.deploymentGeneration,
          workloadEpochFingerprint: admission.workloadEpochFingerprint,
          runtimeContractVersion: admission.runtimeContractVersion,
          capabilityActivationGeneration:
            admission.capabilityActivationGeneration!,
          capabilityContractVersion: admission.capabilityContractVersion!,
          sealedRuntimeLeaseId: admission.admittingRuntimeLeaseId,
          manifest: manifest as unknown as Prisma.InputJsonValue,
          manifestChecksum,
          deliveryCount: rows.length,
          status: "SEALED",
          nextAttemptAt: now,
        },
      });
      const assigned =
        await transaction.analyticsIntegrationPendingDelivery.updateMany({
          where: {
            id: { in: rows.map(({ id }) => id) },
            status: "PENDING",
            executionId: null,
          },
          data: {
            status: "CLAIMED",
            executionId: execution.id,
          },
        });
      if (assigned.count !== rows.length) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution delivery assignment raced",
        );
      }
      return buildExecutionEnvelope(execution);
    },
    { timeout: 30_000 },
  );
}

export async function findPublishableAnalyticsIntegrationExecutions(input: {
  readonly client?: PrismaClient;
  readonly now: Date;
  readonly limit: number;
}): Promise<readonly AnalyticsIntegrationExecutionEnvelope[]> {
  if (
    !Number.isFinite(input.now.getTime()) ||
    !Number.isSafeInteger(input.limit) ||
    input.limit < 1 ||
    input.limit > 100
  ) {
    throw new TypeError("Invalid analytics integration execution scan");
  }
  const rows = await (
    input.client ?? prisma
  ).analyticsIntegrationExecution.findMany({
    where: {
      status: { in: ["SEALED", "RETRYING"] },
      nextAttemptAt: { lte: input.now },
    },
    orderBy: [{ nextAttemptAt: "asc" }, { createdAt: "asc" }],
    take: input.limit,
  });
  return rows.map(buildExecutionEnvelope);
}

export async function publishAnalyticsIntegrationExecution(input: {
  readonly client: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly envelope: AnalyticsIntegrationExecutionEnvelope;
  readonly queueJobId: string;
  readonly publish: (
    envelope: AnalyticsIntegrationExecutionEnvelope,
  ) => Promise<void>;
  readonly now?: Date;
}): Promise<boolean> {
  if (!input.queueJobId) {
    throw new TypeError("Invalid analytics integration queue job identity");
  }
  const now = input.now ?? new Date();
  return input.client.$transaction(
    async (transaction) => {
      await transaction.$queryRaw(
        Prisma.sql`SELECT id FROM analytics_integration_executions WHERE id = ${input.envelope.executionId} FOR UPDATE`,
      );
      const execution =
        await transaction.analyticsIntegrationExecution.findUniqueOrThrow({
          where: { id: input.envelope.executionId },
        });
      assertEnvelopeMatchesExecution(input.envelope, execution);
      if (execution.status === "PUBLISHED" || execution.status === "RUNNING") {
        return false;
      }
      if (execution.status !== "SEALED" && execution.status !== "RETRYING") {
        return false;
      }
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "analyticsIntegrations",
        action: "recovery",
        expectedCapabilityActivationGeneration:
          execution.capabilityActivationGeneration,
        expectedCapabilityContractVersion: execution.capabilityContractVersion,
        now,
      });
      assertExecutionAdmission(admission, execution);
      await input.publish(buildExecutionEnvelope(execution));
      const published =
        await transaction.analyticsIntegrationExecution.updateMany({
          where: {
            id: execution.id,
            status: execution.status,
            manifestChecksum: execution.manifestChecksum,
          },
          data: {
            status: "PUBLISHED",
            queueJobId: input.queueJobId,
            publishedAt: now,
          },
        });
      if (published.count !== 1) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution publication was fenced",
        );
      }
      return true;
    },
    { timeout: 30_000 },
  );
}

export async function deferAnalyticsIntegrationExecutionPublication(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly failureCode: string;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  if (
    !input.executionId ||
    !/^[A-Z0-9_]{1,64}$/.test(input.failureCode) ||
    !Number.isFinite(now.getTime())
  ) {
    throw new TypeError("Invalid analytics integration publication deferral");
  }
  return (input.client ?? prisma).$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM analytics_integration_executions WHERE id = ${input.executionId} FOR UPDATE`,
    );
    const execution =
      await transaction.analyticsIntegrationExecution.findUnique({
        where: { id: input.executionId },
      });
    if (
      !execution ||
      (execution.status !== "SEALED" && execution.status !== "RETRYING")
    ) {
      return false;
    }
    const attempts = execution.attempts + 1;
    const deferred = await transaction.analyticsIntegrationExecution.updateMany(
      {
        where: { id: execution.id, status: execution.status },
        data: {
          status: "RETRYING",
          attempts,
          nextAttemptAt: new Date(
            now.getTime() +
              Math.min(60 * 60_000, 5_000 * 2 ** Math.min(attempts, 9)),
          ),
          lastErrorCode: input.failureCode,
        },
      },
    );
    return deferred.count === 1;
  });
}

export async function quarantineAnalyticsIntegrationExecution(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly failureCode: string;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  if (
    !input.executionId ||
    !/^[A-Z0-9_]{1,64}$/.test(input.failureCode) ||
    !Number.isFinite(now.getTime())
  ) {
    throw new TypeError("Invalid analytics integration quarantine");
  }
  return (input.client ?? prisma).$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM analytics_integration_executions WHERE id = ${input.executionId} FOR UPDATE`,
    );
    const execution =
      await transaction.analyticsIntegrationExecution.findUnique({
        where: { id: input.executionId },
      });
    if (
      !execution ||
      !["SEALED", "PUBLISHED", "RETRYING"].includes(execution.status)
    ) {
      return false;
    }
    const deliveries =
      await transaction.analyticsIntegrationPendingDelivery.findMany({
        where: { executionId: execution.id, status: "CLAIMED" },
      });
    if (deliveries.length > 0) {
      await transaction.analyticsIntegrationPendingDelivery.updateMany({
        where: {
          id: { in: deliveries.map(({ id }) => id) },
          status: "CLAIMED",
        },
        data: {
          status: "QUARANTINED",
          completedAt: now,
          failureCode: input.failureCode,
        },
      });
    }
    const estimatedBytes = deliveries.reduce(
      (sum, delivery) => sum + BigInt(delivery.estimatedBytes),
      0n,
    );
    const state = await transaction.analyticsIntegrationState.findUniqueOrThrow(
      {
        where: { id: execution.integrationStateId },
      },
    );
    if (
      state.pendingRows < BigInt(deliveries.length) ||
      state.pendingEstimatedBytes < estimatedBytes
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Analytics integration quarantine counters are inconsistent",
      );
    }
    await transaction.analyticsIntegrationState.update({
      where: { id: state.id },
      data: {
        pendingRows: { decrement: BigInt(deliveries.length) },
        pendingEstimatedBytes: { decrement: estimatedBytes },
        status: "PAUSED_BACKLOG",
        rescanRequired: true,
        lastErrorCode: input.failureCode,
      },
    });
    await transaction.analyticsIntegrationExecution.update({
      where: { id: execution.id },
      data: {
        status: "QUARANTINED",
        completedAt: now,
        lastErrorCode: input.failureCode,
      },
    });
    return true;
  });
}

export async function claimAnalyticsIntegrationExecution(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly envelope: AnalyticsIntegrationExecutionEnvelope;
  readonly workerId: string;
  readonly now?: Date;
  readonly leaseMs?: number;
}): Promise<ClaimedAnalyticsIntegrationExecution | null> {
  const now = input.now ?? new Date();
  const leaseMs = input.leaseMs ?? 5 * 60 * 1000;
  if (
    !input.workerId ||
    !Number.isFinite(now.getTime()) ||
    !Number.isSafeInteger(leaseMs) ||
    leaseMs < 1_000 ||
    leaseMs > MAX_EXECUTION_LEASE_MS
  ) {
    throw new TypeError("Invalid analytics integration execution claim");
  }
  return (input.client ?? prisma).$transaction(
    async (transaction) => {
      await transaction.$queryRaw(
        Prisma.sql`SELECT id FROM analytics_integration_executions WHERE id = ${input.envelope.executionId} FOR UPDATE`,
      );
      const execution =
        await transaction.analyticsIntegrationExecution.findUniqueOrThrow({
          where: { id: input.envelope.executionId },
        });
      assertEnvelopeMatchesExecution(input.envelope, execution);
      if (execution.status === "COMPLETED") return null;
      if (
        execution.status !== "PUBLISHED" &&
        execution.status !== "SEALED" &&
        execution.status !== "RETRYING"
      ) {
        return null;
      }
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "analyticsIntegrations",
        action: "claimExisting",
        expectedCapabilityActivationGeneration:
          execution.capabilityActivationGeneration,
        expectedCapabilityContractVersion: execution.capabilityContractVersion,
        now,
      });
      assertExecutionAdmission(admission, execution);
      const state =
        await transaction.analyticsIntegrationState.findUniqueOrThrow({
          where: { id: execution.integrationStateId },
        });
      if (
        state.projectId !== execution.projectId ||
        state.integrationType !== execution.integrationType ||
        state.generation !== execution.integrationGeneration ||
        (state.status !== "BOOTSTRAPPING_ACTIVE" &&
          state.status !== "ACTIVE" &&
          state.status !== "RESCANNING" &&
          state.status !== "DRAINING")
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration configuration generation changed",
        );
      }
      const manifest = parseManifest(execution.manifest);
      if (
        manifest.integrationStateId !== execution.integrationStateId ||
        manifest.integrationGeneration !==
          execution.integrationGeneration.toString() ||
        manifest.projectId !== execution.projectId ||
        sha256(manifestJson(manifest)) !== execution.manifestChecksum
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution manifest checksum failed",
        );
      }
      const deliveryIds = manifest.items.flatMap(
        ({ deliveryIds }) => deliveryIds,
      );
      if (
        deliveryIds.length !== execution.deliveryCount ||
        (execution.kind === "BOOTSTRAP"
          ? execution.deliveryCount !== 0 ||
            manifest.items.some(({ deliveryIds }) => deliveryIds.length !== 0)
          : manifest.items.length === 0 ||
            manifest.items.some(({ deliveryIds }) => deliveryIds.length === 0))
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution manifest count changed",
        );
      }
      const deliveries =
        await transaction.analyticsIntegrationPendingDelivery.findMany({
          where: {
            id: { in: deliveryIds },
            executionId: execution.id,
            status: "CLAIMED",
          },
          orderBy: { id: "asc" },
        });
      if (deliveries.length !== deliveryIds.length) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution membership changed",
        );
      }
      const claimed =
        await transaction.analyticsIntegrationExecution.updateMany({
          where: {
            id: execution.id,
            status: execution.status,
            manifestChecksum: execution.manifestChecksum,
          },
          data: {
            status: "RUNNING",
            claimOwner: input.workerId,
            claimExpiresAt: new Date(now.getTime() + leaseMs),
            attempts: { increment: 1 },
            lastErrorCode: null,
          },
        });
      if (claimed.count !== 1) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution claim was fenced",
        );
      }
      return {
        execution: {
          ...execution,
          status: "RUNNING",
          claimOwner: input.workerId,
          claimExpiresAt: new Date(now.getTime() + leaseMs),
          attempts: execution.attempts + 1,
          lastErrorCode: null,
        },
        manifest,
        deliveries,
      };
    },
    { timeout: 30_000 },
  );
}

export async function renewAnalyticsIntegrationExecutionClaim(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly executionId: string;
  readonly workerId: string;
  readonly now?: Date;
  readonly leaseMs?: number;
}): Promise<Date> {
  const now = input.now ?? new Date();
  const leaseMs = input.leaseMs ?? 5 * 60 * 1000;
  if (
    !/^aie_[a-f0-9]{28}$/.test(input.executionId) ||
    !input.workerId ||
    !Number.isFinite(now.getTime()) ||
    !Number.isSafeInteger(leaseMs) ||
    leaseMs < 1_000 ||
    leaseMs > MAX_EXECUTION_LEASE_MS
  ) {
    throw new TypeError(
      "Invalid analytics integration execution claim renewal",
    );
  }
  return (input.client ?? prisma).$transaction(
    async (transaction) => {
      await transaction.$queryRaw(
        Prisma.sql`SELECT id FROM analytics_integration_executions WHERE id = ${input.executionId} FOR UPDATE`,
      );
      const execution =
        await transaction.analyticsIntegrationExecution.findUnique({
          where: { id: input.executionId },
        });
      if (
        !execution ||
        execution.status !== "RUNNING" ||
        execution.claimOwner !== input.workerId ||
        !execution.claimExpiresAt ||
        execution.claimExpiresAt <= now
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution claim renewal was fenced",
        );
      }
      const admission = await lockAnalyticsAdmission({
        transaction,
        runtimeLeaseId: input.admissionContext.runtimeLeaseId,
        expectedBackend: input.admissionContext.backend,
        expectedDeploymentGeneration:
          input.admissionContext.deploymentGeneration,
        capability: "analyticsIntegrations",
        action: "claimExisting",
        expectedCapabilityActivationGeneration:
          execution.capabilityActivationGeneration,
        expectedCapabilityContractVersion: execution.capabilityContractVersion,
        now,
      });
      assertExecutionAdmission(admission, execution);
      const leaseExpiresAt = new Date(now.getTime() + leaseMs);
      const renewed =
        await transaction.analyticsIntegrationExecution.updateMany({
          where: {
            id: execution.id,
            status: "RUNNING",
            claimOwner: input.workerId,
            claimExpiresAt: { gt: now },
          },
          data: { claimExpiresAt: leaseExpiresAt },
        });
      if (renewed.count !== 1) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution claim renewal was fenced",
        );
      }
      return leaseExpiresAt;
    },
    { timeout: 30_000 },
  );
}

export async function deferAnalyticsIntegrationExecution(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly workerId: string;
  readonly failureCode: string;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  if (
    !input.executionId ||
    !input.workerId ||
    !/^[A-Z0-9_]{1,64}$/.test(input.failureCode) ||
    !Number.isFinite(now.getTime())
  ) {
    throw new TypeError("Invalid analytics integration execution deferral");
  }
  return (input.client ?? prisma).$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM analytics_integration_executions WHERE id = ${input.executionId} FOR UPDATE`,
    );
    const execution =
      await transaction.analyticsIntegrationExecution.findUnique({
        where: { id: input.executionId },
      });
    if (
      !execution ||
      execution.status !== "RUNNING" ||
      execution.claimOwner !== input.workerId
    ) {
      return false;
    }
    const delayMs = Math.min(
      60 * 60 * 1000,
      5_000 * 2 ** Math.min(execution.attempts, 9),
    );
    const deferred = await transaction.analyticsIntegrationExecution.updateMany(
      {
        where: {
          id: execution.id,
          status: "RUNNING",
          claimOwner: input.workerId,
        },
        data: {
          status: "RETRYING",
          nextAttemptAt: new Date(now.getTime() + delayMs),
          claimOwner: null,
          claimExpiresAt: null,
          lastErrorCode: input.failureCode,
        },
      },
    );
    return deferred.count === 1;
  });
}

export async function completeAnalyticsIntegrationExecution(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly workerId: string;
  readonly sourceDeletedDeliveryIds?: readonly string[];
  readonly lastSyncAt?: Date;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  if (
    !input.executionId ||
    !input.workerId ||
    !Number.isFinite(now.getTime()) ||
    (input.lastSyncAt !== undefined &&
      !Number.isFinite(input.lastSyncAt.getTime()))
  ) {
    throw new TypeError("Invalid analytics integration execution completion");
  }
  const sourceDeletedIds = [
    ...new Set(input.sourceDeletedDeliveryIds ?? []),
  ].sort();
  return (input.client ?? prisma).$transaction(
    async (transaction) => {
      await transaction.$queryRaw(
        Prisma.sql`SELECT id FROM analytics_integration_executions WHERE id = ${input.executionId} FOR UPDATE`,
      );
      const execution =
        await transaction.analyticsIntegrationExecution.findUnique({
          where: { id: input.executionId },
        });
      if (execution?.status === "COMPLETED") return false;
      if (
        !execution ||
        execution.status !== "RUNNING" ||
        execution.claimOwner !== input.workerId
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution completion was fenced",
        );
      }
      if (
        execution.scratchHostId ||
        execution.scratchRelativePath ||
        execution.scratchReservedBytes !== null ||
        execution.scratchLeaseExpiresAt
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration scratch lease is still active",
        );
      }
      const deliveries =
        await transaction.analyticsIntegrationPendingDelivery.findMany({
          where: { executionId: execution.id, status: "CLAIMED" },
        });
      if (deliveries.length !== execution.deliveryCount) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration completion membership changed",
        );
      }
      const deliveryIdSet = new Set(deliveries.map(({ id }) => id));
      if (sourceDeletedIds.some((id) => !deliveryIdSet.has(id))) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration source-deleted membership is invalid",
        );
      }
      if (sourceDeletedIds.length > 0) {
        const deleted =
          await transaction.analyticsIntegrationPendingDelivery.updateMany({
            where: {
              id: { in: sourceDeletedIds },
              executionId: execution.id,
              status: "CLAIMED",
            },
            data: {
              status: "SOURCE_DELETED",
              completedAt: now,
              claimOwner: null,
              claimExpiresAt: null,
            },
          });
        if (deleted.count !== sourceDeletedIds.length) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration source-deleted update was fenced",
          );
        }
      }
      const completedIds = deliveries
        .map(({ id }) => id)
        .filter((id) => !sourceDeletedIds.includes(id));
      if (completedIds.length > 0) {
        const completed =
          await transaction.analyticsIntegrationPendingDelivery.updateMany({
            where: {
              id: { in: completedIds },
              executionId: execution.id,
              status: "CLAIMED",
            },
            data: {
              status: "COMPLETED",
              completedAt: now,
              claimOwner: null,
              claimExpiresAt: null,
            },
          });
        if (completed.count !== completedIds.length) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration delivery completion was fenced",
          );
        }
      }
      const estimatedBytes = deliveries.reduce(
        (sum, delivery) => sum + BigInt(delivery.estimatedBytes),
        0n,
      );
      const state =
        await transaction.analyticsIntegrationState.findUniqueOrThrow({
          where: { id: execution.integrationStateId },
        });
      if (
        state.generation !== execution.integrationGeneration ||
        state.pendingRows < BigInt(deliveries.length) ||
        state.pendingEstimatedBytes < estimatedBytes
      ) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration delivery counters are inconsistent",
        );
      }
      const stateUpdated =
        await transaction.analyticsIntegrationState.updateMany({
          where: {
            id: state.id,
            generation: state.generation,
            pendingRows: state.pendingRows,
            pendingEstimatedBytes: state.pendingEstimatedBytes,
          },
          data: {
            pendingRows: { decrement: BigInt(deliveries.length) },
            pendingEstimatedBytes: { decrement: estimatedBytes },
            ...(execution.kind === "RESCAN" && {
              status: "ACTIVE" as const,
              rescanRequired: false,
              lastErrorCode: null,
            }),
          },
        });
      if (stateUpdated.count !== 1) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration delivery counter update was fenced",
        );
      }
      if (input.lastSyncAt) {
        const integrationUpdated =
          execution.integrationType === "POSTHOG"
            ? await transaction.posthogIntegration.updateMany({
                where: { projectId: execution.projectId, enabled: true },
                data: { lastSyncAt: input.lastSyncAt },
              })
            : execution.integrationType === "MIXPANEL"
              ? await transaction.mixpanelIntegration.updateMany({
                  where: { projectId: execution.projectId, enabled: true },
                  data: { lastSyncAt: input.lastSyncAt },
                })
              : await transaction.blobStorageIntegration.updateMany({
                  where: { projectId: execution.projectId, enabled: true },
                  data: { lastSyncAt: input.lastSyncAt },
                });
        if (integrationUpdated.count !== 1) {
          throw new AnalyticsIntegrationExecutionProvenanceError(
            "Analytics integration configuration changed during execution",
          );
        }
      }
      const terminal =
        await transaction.analyticsIntegrationExecution.updateMany({
          where: {
            id: execution.id,
            status: "RUNNING",
            claimOwner: input.workerId,
          },
          data: {
            status: "COMPLETED",
            completedAt: now,
            claimOwner: null,
            claimExpiresAt: null,
            lastErrorCode: null,
          },
        });
      if (terminal.count !== 1) {
        throw new AnalyticsIntegrationExecutionProvenanceError(
          "Analytics integration execution terminalization was fenced",
        );
      }
      if (execution.kind === "BOOTSTRAP") {
        const remainingBootstrapExecutions =
          await transaction.analyticsIntegrationExecution.count({
            where: {
              integrationStateId: execution.integrationStateId,
              integrationGeneration: execution.integrationGeneration,
              kind: "BOOTSTRAP",
              status: {
                in: ["SEALED", "PUBLISHED", "RUNNING", "RETRYING"],
              },
            },
          });
        if (remainingBootstrapExecutions === 0) {
          const currentState =
            await transaction.analyticsIntegrationState.findUniqueOrThrow({
              where: { id: execution.integrationStateId },
            });
          if (currentState.status === "BOOTSTRAPPING_ACTIVE") {
            await transaction.analyticsIntegrationPendingDelivery.updateMany({
              where: {
                integrationStateId: execution.integrationStateId,
                integrationGeneration: execution.integrationGeneration,
                status: "SUSPENDED",
                executionId: null,
              },
              data: {
                status: "PENDING",
                nextAttemptAt: now,
                failureCode: null,
              },
            });
            const activated =
              await transaction.analyticsIntegrationState.updateMany({
                where: {
                  id: execution.integrationStateId,
                  generation: execution.integrationGeneration,
                  status: "BOOTSTRAPPING_ACTIVE",
                  bootstrapManifestChecksum: { not: null },
                },
                data: {
                  status: "ACTIVE",
                  lastErrorCode: null,
                },
              });
            if (activated.count !== 1) {
              throw new AnalyticsIntegrationExecutionProvenanceError(
                "Analytics integration bootstrap activation was fenced",
              );
            }
          } else if (currentState.status !== "DRAINING") {
            throw new AnalyticsIntegrationExecutionProvenanceError(
              "Analytics integration bootstrap lifecycle changed",
            );
          }
        }
      }
      return true;
    },
    { timeout: 30_000 },
  );
}

export async function recoverExpiredAnalyticsIntegrationExecutions(input: {
  readonly client?: PrismaClient;
  readonly now: Date;
}): Promise<number> {
  if (!Number.isFinite(input.now.getTime())) {
    throw new TypeError("Invalid analytics integration recovery timestamp");
  }
  const recovered = await (
    input.client ?? prisma
  ).analyticsIntegrationExecution.updateMany({
    where: {
      status: "RUNNING",
      claimExpiresAt: { lte: input.now },
    },
    data: {
      status: "RETRYING",
      nextAttemptAt: input.now,
      claimOwner: null,
      claimExpiresAt: null,
      lastErrorCode: "INTEGRATION_EXECUTION_LEASE_EXPIRED",
    },
  });
  return recovered.count;
}

function validateScratchLease(input: {
  readonly executionId: string;
  readonly workerId: string;
  readonly hostId: string;
  readonly relativePath: string;
  readonly reservedBytes: bigint;
  readonly leaseExpiresAt: Date;
  readonly now: Date;
}): void {
  if (
    !/^aie_[a-f0-9]{28}$/.test(input.executionId) ||
    !input.workerId ||
    !input.hostId ||
    !new RegExp(`^parquet-${input.executionId}-[A-Za-z0-9_-]+$`).test(
      input.relativePath,
    ) ||
    input.reservedBytes < 1n ||
    !Number.isFinite(input.leaseExpiresAt.getTime()) ||
    input.leaseExpiresAt <= input.now
  ) {
    throw new TypeError("Invalid analytics integration scratch lease");
  }
}

export async function reserveAnalyticsIntegrationScratch(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly workerId: string;
  readonly hostId: string;
  readonly relativePath: string;
  readonly reservedBytes: bigint;
  readonly leaseExpiresAt: Date;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  validateScratchLease({ ...input, now });
  return (input.client ?? prisma).$transaction(async (transaction) => {
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM analytics_integration_executions WHERE id = ${input.executionId} FOR UPDATE`,
    );
    const execution =
      await transaction.analyticsIntegrationExecution.findUnique({
        where: { id: input.executionId },
      });
    if (
      !execution ||
      execution.integrationType !== "BLOB_STORAGE" ||
      execution.status !== "RUNNING" ||
      execution.claimOwner !== input.workerId
    ) {
      return false;
    }
    if (
      execution.scratchHostId &&
      (execution.scratchHostId !== input.hostId ||
        execution.scratchRelativePath !== input.relativePath ||
        execution.scratchReservedBytes !== input.reservedBytes)
    ) {
      throw new AnalyticsIntegrationExecutionProvenanceError(
        "Analytics integration scratch allocation changed",
      );
    }
    const reserved = await transaction.analyticsIntegrationExecution.updateMany(
      {
        where: {
          id: execution.id,
          status: "RUNNING",
          claimOwner: input.workerId,
        },
        data: {
          scratchHostId: input.hostId,
          scratchRelativePath: input.relativePath,
          scratchReservedBytes: input.reservedBytes,
          scratchLeaseExpiresAt: input.leaseExpiresAt,
        },
      },
    );
    return reserved.count === 1;
  });
}

export async function renewAnalyticsIntegrationScratch(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly workerId: string;
  readonly hostId: string;
  readonly relativePath: string;
  readonly reservedBytes: bigint;
  readonly leaseExpiresAt: Date;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  validateScratchLease({ ...input, now });
  const renewed = await (
    input.client ?? prisma
  ).analyticsIntegrationExecution.updateMany({
    where: {
      id: input.executionId,
      integrationType: "BLOB_STORAGE",
      status: "RUNNING",
      claimOwner: input.workerId,
      scratchHostId: input.hostId,
      scratchRelativePath: input.relativePath,
      scratchReservedBytes: input.reservedBytes,
      scratchLeaseExpiresAt: { gt: now },
    },
    data: { scratchLeaseExpiresAt: input.leaseExpiresAt },
  });
  return renewed.count === 1;
}

export async function releaseAnalyticsIntegrationScratch(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly workerId: string;
  readonly hostId: string;
  readonly relativePath: string;
}): Promise<boolean> {
  if (
    !/^aie_[a-f0-9]{28}$/.test(input.executionId) ||
    !input.workerId ||
    !input.hostId ||
    !new RegExp(`^parquet-${input.executionId}-[A-Za-z0-9_-]+$`).test(
      input.relativePath,
    )
  ) {
    throw new TypeError("Invalid analytics integration scratch release");
  }
  const released = await (
    input.client ?? prisma
  ).analyticsIntegrationExecution.updateMany({
    where: {
      id: input.executionId,
      claimOwner: input.workerId,
      scratchHostId: input.hostId,
      scratchRelativePath: input.relativePath,
    },
    data: {
      scratchHostId: null,
      scratchRelativePath: null,
      scratchReservedBytes: null,
      scratchLeaseExpiresAt: null,
    },
  });
  return released.count === 1;
}

export async function isAnalyticsIntegrationScratchLeaseLive(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly hostId: string;
  readonly relativePath: string;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  if (
    !/^aie_[a-f0-9]{28}$/.test(input.executionId) ||
    !input.hostId ||
    !input.relativePath ||
    !Number.isFinite(now.getTime())
  ) {
    return false;
  }
  return (
    (await (input.client ?? prisma).analyticsIntegrationExecution.count({
      where: {
        id: input.executionId,
        status: "RUNNING",
        scratchHostId: input.hostId,
        scratchRelativePath: input.relativePath,
        scratchLeaseExpiresAt: { gt: now },
      },
    })) === 1
  );
}

export async function clearExpiredAnalyticsIntegrationScratchLease(input: {
  readonly client?: PrismaClient;
  readonly executionId: string;
  readonly hostId: string;
  readonly relativePath: string;
  readonly now?: Date;
}): Promise<boolean> {
  const now = input.now ?? new Date();
  if (
    !/^aie_[a-f0-9]{28}$/.test(input.executionId) ||
    !input.hostId ||
    !input.relativePath ||
    !Number.isFinite(now.getTime())
  ) {
    throw new TypeError("Invalid expired analytics integration scratch lease");
  }
  const cleared = await (
    input.client ?? prisma
  ).analyticsIntegrationExecution.updateMany({
    where: {
      id: input.executionId,
      scratchHostId: input.hostId,
      scratchRelativePath: input.relativePath,
      OR: [
        { scratchLeaseExpiresAt: { lte: now } },
        { status: { not: "RUNNING" } },
      ],
    },
    data: {
      scratchHostId: null,
      scratchRelativePath: null,
      scratchReservedBytes: null,
      scratchLeaseExpiresAt: null,
    },
  });
  return cleared.count === 1;
}
