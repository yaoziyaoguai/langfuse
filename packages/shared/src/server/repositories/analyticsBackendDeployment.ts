import { createHash } from "node:crypto";

import { Prisma } from "@prisma/client";
import type {
  AnalyticsBackendDeploymentState,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import type { AnalyticsBackend } from "../analytics-persistence/analyticsBackend";
import { ANALYTICS_CAPABILITY_NAMES } from "../analytics-persistence/analyticsCapabilities";
import {
  fromPrismaAnalyticsServingRuntimeComponent,
  toPrismaAnalyticsBackend,
  type AnalyticsServingRuntimeComponentName,
} from "../analytics-persistence/analyticsBackendMapping";
import {
  assertAnalyticsQueueDrainEvidence as assertCompleteAnalyticsQueueDrainEvidence,
  type AnalyticsScoreDeletionQueueDrainEvidence,
  type AnalyticsScoreDeletionQueueDrainScope,
} from "../redis/analyticsScoreDeletionDrain";
import { configuredCommunityAnalyticsQueueInventory } from "../redis/analyticsQueueInventory";

const DEPLOYMENT_STATE_ID = "global";
const DEPLOYMENT_ADVISORY_LOCK = 7_681_221_833_480_517n;
const SHA256_HEX = /^[a-f0-9]{64}$/;
const ACTIVE_INGESTION_STATUSES = [
  "ACCEPTED",
  "QUEUED",
  "PERSISTED",
  "RETRYING",
] as const;
const ACTIVE_LOAD_STATUSES = ["PENDING", "LOADING", "UNKNOWN"] as const;
const ACTIVE_DELETION_STATUSES = [
  "SCHEDULED",
  "RETRYING",
  "NEEDS_ATTENTION",
] as const;
const RUNTIME_LEASE_GRACE_MS = 60_000;
const CLAIM_DRAIN_GRACE_MS = 60_000;

export type AnalyticsRuntimeInventoryEntry = {
  readonly instanceId: string;
  readonly component: AnalyticsServingRuntimeComponentName;
};

export type AnalyticsBackendEmptinessEvidence = {
  readonly source: {
    readonly backend: AnalyticsBackend;
    readonly empty: boolean;
    readonly evidenceDigest: string;
  };
  readonly target: {
    readonly backend: AnalyticsBackend;
    readonly empty: boolean;
    readonly evidenceDigest: string;
  };
};

function hash(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

function requireDigest(value: string, name: string): void {
  if (!SHA256_HEX.test(value)) {
    throw new TypeError(`${name} must be a SHA-256 digest`);
  }
}

export function fingerprintAnalyticsWorkloadEpoch(epoch: string): string {
  if (!epoch) throw new TypeError("Analytics workload epoch is required");
  return hash(epoch);
}

export function digestAnalyticsRuntimeInventory(
  inventory: readonly AnalyticsRuntimeInventoryEntry[],
): string {
  const normalized = inventory
    .map(({ component, instanceId }) => {
      if (!instanceId) throw new TypeError("Runtime instance ID is required");
      return `${component}:${instanceId}`;
    })
    .sort();
  if (new Set(normalized).size !== normalized.length) {
    throw new TypeError("Runtime inventory contains duplicate instances");
  }
  return hash(JSON.stringify(normalized));
}

function assertFoundationInventory(
  inventory: readonly AnalyticsRuntimeInventoryEntry[],
): void {
  const components = new Set(inventory.map(({ component }) => component));
  if (!components.has("web") || !components.has("worker")) {
    throw new TypeError(
      "Analytics runtime inventory must include both web and worker",
    );
  }
}

export const ANALYTICS_DEPLOYMENT_TRANSACTION_OPTIONS = {
  isolationLevel: "ReadCommitted",
  maxWait: 120_000,
  timeout: 180_000,
} as const;

async function withDeploymentTransaction<T>(
  client: PrismaClient,
  callback: (transaction: Prisma.TransactionClient) => Promise<T>,
): Promise<T> {
  // The deployment advisory lock is the serialization boundary. READ COMMITTED
  // lets a waiter observe the winner after the lock is released instead of
  // retrying a stale Serializable snapshot.
  return client.$transaction(
    callback,
    ANALYTICS_DEPLOYMENT_TRANSACTION_OPTIONS,
  );
}

async function databaseClock(
  transaction: Prisma.TransactionClient,
): Promise<Date> {
  const [row] = await transaction.$queryRaw<readonly { now: Date }[]>(
    Prisma.sql`SELECT clock_timestamp() AS now`,
  );
  if (!row || !Number.isFinite(row.now.getTime())) {
    throw new Error("Postgres did not return an analytics deployment clock");
  }
  return row.now;
}

export async function acquireAnalyticsDeploymentSharedLock(
  transaction: Prisma.TransactionClient,
): Promise<void> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock_shared(${DEPLOYMENT_ADVISORY_LOCK})::text AS locked`,
  );
}

export async function acquireAnalyticsDeploymentExclusiveLock(
  transaction: Prisma.TransactionClient,
): Promise<void> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock(${DEPLOYMENT_ADVISORY_LOCK})::text AS locked`,
  );
}

export async function lockAnalyticsBackendDeploymentState(
  transaction: Prisma.TransactionClient,
  mode: "SHARE" | "UPDATE",
): Promise<AnalyticsBackendDeploymentState | null> {
  const clause =
    mode === "SHARE" ? Prisma.sql`FOR SHARE` : Prisma.sql`FOR UPDATE`;
  await transaction.$queryRaw(
    Prisma.sql`SELECT id FROM analytics_backend_deployment_state WHERE id = ${DEPLOYMENT_STATE_ID} ${clause}`,
  );
  return transaction.analyticsBackendDeploymentState.findUnique({
    where: { id: DEPLOYMENT_STATE_ID },
  });
}

async function countExistingAnalyticsControlState(
  transaction: Prisma.TransactionClient,
): Promise<number> {
  const counts = await Promise.all([
    transaction.analyticsIngestionOperation.count(),
    transaction.analyticsDeletionOperation.count(),
    transaction.analyticsEntityHead.count(),
    transaction.analyticsCheckpointGeneration.count(),
    transaction.analyticsRetentionRun.count(),
    transaction.batchAction.count(),
    transaction.batchExport.count(),
    transaction.jobExecution.count(),
    transaction.jobConfiguration.count(),
    transaction.analyticsIntegrationState.count(),
    transaction.analyticsIntegrationPendingDelivery.count(),
    transaction.analyticsIntegrationExecution.count(),
  ]);
  return counts.reduce((sum, count) => sum + count, 0);
}

type DrainCounts = {
  readonly ingestionOperations: number;
  readonly ingestionOutbox: number;
  readonly loadBatches: number;
  readonly deletionOperations: number;
  readonly checkpoints: number;
  readonly retentionRuns: number;
  readonly batchActions: number;
  readonly batchExports: number;
  readonly jobExecutions: number;
  readonly integrationDeliveries: number;
  readonly integrationExecutions: number;
  readonly integrationBootstraps: number;
};

async function getUnstampedPendingWork(
  transaction: Prisma.TransactionClient,
): Promise<DrainCounts> {
  const [
    ingestionOperations,
    ingestionOutbox,
    loadBatches,
    deletionOperations,
    checkpoints,
    retentionRuns,
    batchActions,
    batchExports,
    jobExecutions,
    integrationDeliveries,
    integrationExecutions,
    integrationBootstraps,
  ] = await Promise.all([
    transaction.analyticsIngestionOperation.count({
      where: {
        analyticsBackend: null,
        status: { in: [...ACTIVE_INGESTION_STATUSES] },
      },
    }),
    transaction.analyticsIngestionOutboxV2.count({
      where: {
        operation: {
          analyticsBackend: null,
          status: { in: [...ACTIVE_INGESTION_STATUSES] },
        },
      },
    }),
    transaction.analyticsLoadBatch.count({
      where: {
        status: { in: [...ACTIVE_LOAD_STATUSES] },
        operation: { analyticsBackend: null },
      },
    }),
    transaction.analyticsDeletionOperation.count({
      where: {
        analyticsBackend: null,
        status: { in: [...ACTIVE_DELETION_STATUSES] },
      },
    }),
    transaction.analyticsCheckpointGeneration.count({
      where: { analyticsBackend: null, status: "PREPARING" },
    }),
    transaction.analyticsRetentionRun.count({
      where: { analyticsBackend: null, status: "RUNNING" },
    }),
    transaction.batchAction.count({
      where: { status: { in: ["QUEUED", "PROCESSING"] } },
    }),
    transaction.batchExport.count({
      where: { status: { notIn: ["COMPLETED", "FAILED", "CANCELLED"] } },
    }),
    transaction.jobExecution.count({
      where: { status: { in: ["PENDING", "DELAYED"] } },
    }),
    transaction.analyticsIntegrationPendingDelivery.count({
      where: {
        status: { in: ["SUSPENDED", "PENDING", "CLAIMED"] },
      },
    }),
    transaction.analyticsIntegrationExecution.count({
      where: {
        status: { in: ["SEALED", "PUBLISHED", "RUNNING", "RETRYING"] },
      },
    }),
    transaction.analyticsIntegrationState.count({
      where: {
        status: {
          in: [
            "BOOTSTRAPPING_DARK",
            "BOOTSTRAPPING_ACTIVE",
            "RESCANNING",
            "DRAINING",
          ],
        },
      },
    }),
  ]);
  return {
    ingestionOperations,
    ingestionOutbox,
    loadBatches,
    deletionOperations,
    checkpoints,
    retentionRuns,
    batchActions,
    batchExports,
    jobExecutions,
    integrationDeliveries,
    integrationExecutions,
    integrationBootstraps,
  };
}

async function getAllPendingWork(
  transaction: Prisma.TransactionClient,
): Promise<DrainCounts> {
  const [
    ingestionOperations,
    ingestionOutbox,
    loadBatches,
    deletionOperations,
    checkpoints,
    retentionRuns,
    batchActions,
    batchExports,
    jobExecutions,
    integrationDeliveries,
    integrationExecutions,
    integrationBootstraps,
  ] = await Promise.all([
    transaction.analyticsIngestionOperation.count({
      where: { status: { in: [...ACTIVE_INGESTION_STATUSES] } },
    }),
    transaction.analyticsIngestionOutboxV2.count({
      where: {
        operation: { status: { in: [...ACTIVE_INGESTION_STATUSES] } },
      },
    }),
    transaction.analyticsLoadBatch.count({
      where: { status: { in: [...ACTIVE_LOAD_STATUSES] } },
    }),
    transaction.analyticsDeletionOperation.count({
      where: { status: { in: [...ACTIVE_DELETION_STATUSES] } },
    }),
    transaction.analyticsCheckpointGeneration.count({
      where: { status: "PREPARING" },
    }),
    transaction.analyticsRetentionRun.count({ where: { status: "RUNNING" } }),
    transaction.batchAction.count({
      where: { status: { in: ["QUEUED", "PROCESSING"] } },
    }),
    transaction.batchExport.count({
      where: { status: { notIn: ["COMPLETED", "FAILED", "CANCELLED"] } },
    }),
    transaction.jobExecution.count({
      where: { status: { in: ["PENDING", "DELAYED"] } },
    }),
    transaction.analyticsIntegrationPendingDelivery.count({
      where: { status: { in: ["SUSPENDED", "PENDING", "CLAIMED"] } },
    }),
    transaction.analyticsIntegrationExecution.count({
      where: {
        status: { in: ["SEALED", "PUBLISHED", "RUNNING", "RETRYING"] },
      },
    }),
    transaction.analyticsIntegrationState.count({
      where: {
        status: {
          in: [
            "BOOTSTRAPPING_DARK",
            "BOOTSTRAPPING_ACTIVE",
            "RESCANNING",
            "DRAINING",
          ],
        },
      },
    }),
  ]);
  return {
    ingestionOperations,
    ingestionOutbox,
    loadBatches,
    deletionOperations,
    checkpoints,
    retentionRuns,
    batchActions,
    batchExports,
    jobExecutions,
    integrationDeliveries,
    integrationExecutions,
    integrationBootstraps,
  };
}

function pendingWorkTotal(counts: DrainCounts): number {
  return Object.values(counts).reduce((sum, count) => sum + count, 0);
}

function assertAnalyticsQueueDrainEvidence(input: {
  readonly evidence: AnalyticsScoreDeletionQueueDrainEvidence;
  readonly expectedScope: AnalyticsScoreDeletionQueueDrainScope;
  readonly expectedQueueNamespaceFingerprint: string;
  readonly transition: "adoption" | "switch";
}): void {
  requireDigest(
    input.expectedQueueNamespaceFingerprint,
    "Expected queue namespace fingerprint",
  );
  requireDigest(
    input.evidence.queueNamespaceFingerprint,
    "Queue drain namespace fingerprint",
  );
  requireDigest(input.evidence.evidenceDigest, "Queue drain evidence");
  if (
    input.evidence.backend !== input.expectedScope.backend ||
    input.evidence.deploymentGeneration !==
      input.expectedScope.deploymentGeneration.toString() ||
    input.evidence.workloadEpochFingerprint !==
      input.expectedScope.workloadEpochFingerprint ||
    input.evidence.queueNamespaceFingerprint !==
      input.expectedQueueNamespaceFingerprint
  ) {
    throw new Error(
      `Analytics backend ${input.transition} queue namespace does not match the runtime fleet`,
    );
  }
  assertCompleteAnalyticsQueueDrainEvidence({
    evidence: input.evidence,
    expectedInventory: configuredCommunityAnalyticsQueueInventory(),
  });
  if (
    !Number.isSafeInteger(input.evidence.pendingJobs) ||
    input.evidence.pendingJobs < 0
  ) {
    throw new TypeError("Invalid pending analytics queue job count");
  }
  if (!input.evidence.empty || input.evidence.pendingJobs !== 0) {
    throw new Error(
      `Analytics backend ${input.transition} has pending analytics queue work`,
    );
  }
}

async function bindDorisCapabilityRows(
  transaction: Prisma.TransactionClient,
  deploymentGeneration: bigint,
): Promise<void> {
  const updated = await transaction.analyticsCapabilityActivation.updateMany({
    data: {
      backend: "DORIS",
      deploymentGeneration,
      status: "DISABLED",
      captureEnabled: false,
      captureRequired: false,
      rescanRequired: false,
      cutoffState: Prisma.DbNull,
      cutoffActivationGeneration: null,
      cutoffDigest: null,
      bootstrapCompletedGeneration: null,
      bootstrapEvidenceDigest: null,
      bootstrapCompletedAt: null,
      activatedAt: null,
      drainingAt: null,
      disabledAt: null,
    },
  });
  if (updated.count !== ANALYTICS_CAPABILITY_NAMES.length) {
    throw new Error("Analytics capability catalog rows are incomplete");
  }
}

export type AnalyticsBackendStartupResolution =
  | {
      readonly mode: "READY";
      readonly marker: AnalyticsBackendDeploymentState;
      readonly initialized: boolean;
    }
  | { readonly mode: "ADOPTION_REQUIRED" }
  | {
      readonly mode: "MISMATCH";
      readonly reasonCode:
        | "BACKEND_MISMATCH"
        | "WORKLOAD_EPOCH_MISMATCH"
        | "QUEUE_NAMESPACE_MISMATCH"
        | "FOUNDATION_CONTRACT_MISMATCH";
      readonly marker: AnalyticsBackendDeploymentState;
    };

export async function getAnalyticsBackendDeploymentState(input?: {
  readonly client?: PrismaClient;
}): Promise<AnalyticsBackendDeploymentState | null> {
  return (input?.client ?? prisma).analyticsBackendDeploymentState.findUnique({
    where: { id: DEPLOYMENT_STATE_ID },
  });
}

export async function resolveAnalyticsBackendStartup(input: {
  readonly client?: PrismaClient;
  readonly backend: AnalyticsBackend;
  readonly workloadEpochFingerprint: string;
  readonly queueNamespaceFingerprint: string;
  readonly foundationContractVersion: number;
  readonly allowFreshInitialization: boolean;
  readonly freshDeploymentEvidence: {
    readonly selectedBackendEmpty: boolean;
    readonly evidenceDigest: string;
  };
  readonly now?: Date;
}): Promise<AnalyticsBackendStartupResolution> {
  requireDigest(input.workloadEpochFingerprint, "Workload epoch fingerprint");
  requireDigest(input.queueNamespaceFingerprint, "Queue namespace fingerprint");
  requireDigest(
    input.freshDeploymentEvidence.evidenceDigest,
    "Fresh deployment evidence",
  );
  if (input.foundationContractVersion < 1) {
    throw new TypeError("Foundation contract version must be positive");
  }
  const client = input.client ?? prisma;
  const backend = toPrismaAnalyticsBackend(input.backend);

  return withDeploymentTransaction(client, async (transaction) => {
    await acquireAnalyticsDeploymentExclusiveLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "UPDATE",
    );
    if (marker) {
      if (marker.backend !== backend) {
        return { mode: "MISMATCH", reasonCode: "BACKEND_MISMATCH", marker };
      }
      if (marker.workloadEpochFingerprint !== input.workloadEpochFingerprint) {
        return {
          mode: "MISMATCH",
          reasonCode: "WORKLOAD_EPOCH_MISMATCH",
          marker,
        };
      }
      if (
        marker.queueNamespaceFingerprint !== input.queueNamespaceFingerprint
      ) {
        return {
          mode: "MISMATCH",
          reasonCode: "QUEUE_NAMESPACE_MISMATCH",
          marker,
        };
      }
      if (
        marker.foundationContractVersion !== input.foundationContractVersion
      ) {
        return {
          mode: "MISMATCH",
          reasonCode: "FOUNDATION_CONTRACT_MISMATCH",
          marker,
        };
      }
      return { mode: "READY", marker, initialized: false };
    }

    if (
      !input.allowFreshInitialization ||
      !input.freshDeploymentEvidence.selectedBackendEmpty ||
      (await countExistingAnalyticsControlState(transaction)) > 0
    ) {
      return { mode: "ADOPTION_REQUIRED" };
    }
    const now = input.now ?? (await databaseClock(transaction));

    const created = await transaction.analyticsBackendDeploymentState.create({
      data: {
        id: DEPLOYMENT_STATE_ID,
        backend,
        generation: 1n,
        workloadEpochFingerprint: input.workloadEpochFingerprint,
        queueNamespaceFingerprint: input.queueNamespaceFingerprint,
        foundationContractVersion: input.foundationContractVersion,
        attestationDigest: input.freshDeploymentEvidence.evidenceDigest,
        createdAt: now,
      },
    });
    if (backend === "DORIS") {
      await bindDorisCapabilityRows(transaction, created.generation);
    }
    await transaction.analyticsBackendDeploymentTransition.create({
      data: {
        kind: "INITIALIZE",
        fromBackend: null,
        toBackend: backend,
        fromGeneration: null,
        toGeneration: created.generation,
        targetWorkloadEpochFingerprint: input.workloadEpochFingerprint,
        expectedInventoryDigest: hash("fresh:expected-empty-inventory"),
        observedInventoryDigest: hash("fresh:observed-empty-inventory"),
        drainEvidenceDigest: hash("fresh:no-durable-work"),
        denyProbeAttestationDigest:
          input.freshDeploymentEvidence.evidenceDigest,
        completedAt: now,
      },
    });
    return { mode: "READY", marker: created, initialized: true };
  });
}

export async function adoptExistingAnalyticsBackend(input: {
  readonly client?: PrismaClient;
  readonly expectedBackend: AnalyticsBackend;
  readonly workloadEpochFingerprint: string;
  readonly foundationContractVersion: number;
  readonly expectedInventory: readonly AnalyticsRuntimeInventoryEntry[];
  readonly expectedInventoryDigest: string;
  readonly verifyScoreDeletionQueuesEmpty: (
    scope: AnalyticsScoreDeletionQueueDrainScope,
  ) => Promise<AnalyticsScoreDeletionQueueDrainEvidence>;
  readonly drainAttestationDigest: string;
  readonly denyProbeAttestationDigest: string;
  readonly now?: Date;
}): Promise<AnalyticsBackendDeploymentState> {
  requireDigest(input.workloadEpochFingerprint, "Workload epoch fingerprint");
  requireDigest(input.expectedInventoryDigest, "Expected inventory");
  requireDigest(input.drainAttestationDigest, "Drain attestation");
  requireDigest(input.denyProbeAttestationDigest, "Deny-probe attestation");
  if (input.foundationContractVersion < 1) {
    throw new TypeError("Invalid analytics backend adoption request");
  }
  assertFoundationInventory(input.expectedInventory);
  const expectedInventoryDigest = digestAnalyticsRuntimeInventory(
    input.expectedInventory,
  );
  if (expectedInventoryDigest !== input.expectedInventoryDigest) {
    throw new Error(
      "Expected runtime inventory digest does not match inventory",
    );
  }
  const client = input.client ?? prisma;
  const backend = toPrismaAnalyticsBackend(input.expectedBackend);

  return withDeploymentTransaction(client, async (transaction) => {
    await acquireAnalyticsDeploymentExclusiveLock(transaction);
    if (await lockAnalyticsBackendDeploymentState(transaction, "UPDATE")) {
      throw new Error("Analytics backend deployment is already adopted");
    }
    const now = input.now ?? (await databaseClock(transaction));
    const leaseCutoff = new Date(now.getTime() - RUNTIME_LEASE_GRACE_MS);

    const observedLeases = await transaction.analyticsRuntimeLease.findMany({
      where: {
        component: { in: ["WEB", "WORKER"] },
        deploymentGeneration: 0n,
        supersededAt: null,
        leaseExpiresAt: { gt: leaseCutoff },
      },
      orderBy: { instanceId: "asc" },
    });
    const observedInventory = observedLeases.map((lease) => ({
      instanceId: lease.instanceId,
      component: fromPrismaAnalyticsServingRuntimeComponent(lease.component),
    }));
    const observedInventoryDigest =
      digestAnalyticsRuntimeInventory(observedInventory);
    if (
      observedInventoryDigest !== expectedInventoryDigest ||
      observedLeases.length !== input.expectedInventory.length
    ) {
      throw new Error("Runtime inventory census does not match adoption input");
    }
    if (
      observedLeases.some(
        (lease) =>
          lease.backend !== backend ||
          lease.workloadEpochFingerprint !== input.workloadEpochFingerprint ||
          !lease.queueNamespaceFingerprint ||
          lease.foundationContractVersion !== input.foundationContractVersion ||
          lease.state !== "QUIESCED",
      )
    ) {
      throw new Error(
        "All adoption runtime leases must be compatible and quiesced",
      );
    }
    const queueNamespaceFingerprint =
      observedLeases[0]?.queueNamespaceFingerprint;
    if (
      !queueNamespaceFingerprint ||
      observedLeases.some(
        (lease) =>
          lease.queueNamespaceFingerprint !== queueNamespaceFingerprint,
      )
    ) {
      throw new Error(
        "All adoption runtime leases must use one analytics queue namespace",
      );
    }

    const liveClaims = await transaction.analyticsBackendClaimLease.count({
      where: {
        releasedAt: null,
        leaseExpiresAt: { gt: leaseCutoff },
      },
    });
    if (liveClaims > 0) {
      throw new Error("Analytics backend adoption has live claim leases");
    }
    const pendingWork = await getUnstampedPendingWork(transaction);
    if (pendingWorkTotal(pendingWork) > 0) {
      throw new Error("Analytics backend adoption has unstamped pending work");
    }
    const queueDrainScope = {
      backend: input.expectedBackend,
      deploymentGeneration: 0n,
      workloadEpochFingerprint: input.workloadEpochFingerprint,
    } as const;
    const queueDrainEvidence =
      await input.verifyScoreDeletionQueuesEmpty(queueDrainScope);
    assertAnalyticsQueueDrainEvidence({
      evidence: queueDrainEvidence,
      expectedScope: queueDrainScope,
      expectedQueueNamespaceFingerprint: queueNamespaceFingerprint,
      transition: "adoption",
    });

    const drainEvidenceDigest = hash(
      JSON.stringify({
        attestation: input.drainAttestationDigest,
        counts: pendingWork,
        queueDrainEvidenceDigest: queueDrainEvidence.evidenceDigest,
        queueNamespaceFingerprint,
      }),
    );
    const marker = await transaction.analyticsBackendDeploymentState.create({
      data: {
        id: DEPLOYMENT_STATE_ID,
        backend,
        generation: 1n,
        workloadEpochFingerprint: input.workloadEpochFingerprint,
        queueNamespaceFingerprint,
        foundationContractVersion: input.foundationContractVersion,
        attestationDigest: input.denyProbeAttestationDigest,
        createdAt: now,
      },
    });
    if (backend === "DORIS") {
      await bindDorisCapabilityRows(transaction, marker.generation);
    }
    await transaction.analyticsBackendDeploymentTransition.create({
      data: {
        kind: "ADOPT_EXISTING",
        fromBackend: null,
        toBackend: backend,
        fromGeneration: 0n,
        toGeneration: marker.generation,
        targetWorkloadEpochFingerprint: input.workloadEpochFingerprint,
        expectedInventoryDigest,
        observedInventoryDigest,
        drainEvidenceDigest,
        denyProbeAttestationDigest: input.denyProbeAttestationDigest,
        completedAt: now,
      },
    });
    return marker;
  });
}

export async function switchAnalyticsBackend(input: {
  readonly client?: PrismaClient;
  readonly expectedBackend: AnalyticsBackend;
  readonly expectedGeneration: bigint;
  readonly expectedWorkloadEpochFingerprint: string;
  readonly targetBackend: AnalyticsBackend;
  readonly targetWorkloadEpochFingerprint: string;
  readonly targetFoundationContractVersion: number;
  readonly expectedQuiescedInventory: readonly AnalyticsRuntimeInventoryEntry[];
  readonly verifyBackendEmptiness: () => Promise<AnalyticsBackendEmptinessEvidence>;
  readonly verifyScoreDeletionQueuesEmpty: (
    scope: AnalyticsScoreDeletionQueueDrainScope,
  ) => Promise<AnalyticsScoreDeletionQueueDrainEvidence>;
  readonly externalDrainAttestationDigest: string;
  readonly denyProbeAttestationDigest: string;
  readonly now?: Date;
}): Promise<AnalyticsBackendDeploymentState> {
  requireDigest(
    input.expectedWorkloadEpochFingerprint,
    "Expected workload epoch fingerprint",
  );
  requireDigest(
    input.targetWorkloadEpochFingerprint,
    "Target workload epoch fingerprint",
  );
  requireDigest(input.externalDrainAttestationDigest, "Drain attestation");
  requireDigest(input.denyProbeAttestationDigest, "Deny-probe attestation");
  if (
    input.expectedWorkloadEpochFingerprint ===
    input.targetWorkloadEpochFingerprint
  ) {
    throw new TypeError(
      "Analytics backend switch requires a new workload epoch",
    );
  }
  if (
    input.expectedBackend === input.targetBackend ||
    input.expectedGeneration < 1n ||
    input.targetFoundationContractVersion < 1
  ) {
    throw new TypeError("Invalid analytics backend switch request");
  }
  assertFoundationInventory(input.expectedQuiescedInventory);
  const client = input.client ?? prisma;
  const expectedBackend = toPrismaAnalyticsBackend(input.expectedBackend);
  const targetBackend = toPrismaAnalyticsBackend(input.targetBackend);
  const expectedInventoryDigest = digestAnalyticsRuntimeInventory(
    input.expectedQuiescedInventory,
  );

  return withDeploymentTransaction(client, async (transaction) => {
    await acquireAnalyticsDeploymentExclusiveLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "UPDATE",
    );
    if (
      !marker ||
      marker.backend !== expectedBackend ||
      marker.generation !== input.expectedGeneration ||
      marker.workloadEpochFingerprint !==
        input.expectedWorkloadEpochFingerprint ||
      !marker.queueNamespaceFingerprint
    ) {
      throw new Error("Analytics backend switch marker CAS failed");
    }

    const emptinessEvidence = await input.verifyBackendEmptiness();
    requireDigest(
      emptinessEvidence.source.evidenceDigest,
      "Source backend empty evidence",
    );
    requireDigest(
      emptinessEvidence.target.evidenceDigest,
      "Target backend empty evidence",
    );
    if (
      emptinessEvidence.source.backend !== input.expectedBackend ||
      emptinessEvidence.target.backend !== input.targetBackend ||
      !emptinessEvidence.source.empty ||
      !emptinessEvidence.target.empty
    ) {
      throw new Error(
        "Analytics backend switch requires empty source and target backends",
      );
    }
    const now = input.now ?? (await databaseClock(transaction));
    const drainCutoff = new Date(now.getTime() - CLAIM_DRAIN_GRACE_MS);

    const runtimeBlockers = await transaction.analyticsRuntimeLease.count({
      where: {
        leaseExpiresAt: { gt: drainCutoff },
        OR: [
          { state: { not: "QUIESCED" } },
          { backend: { not: marker.backend } },
          { deploymentGeneration: { not: marker.generation } },
          {
            workloadEpochFingerprint: {
              not: marker.workloadEpochFingerprint,
            },
          },
          { queueNamespaceFingerprint: null },
          {
            queueNamespaceFingerprint: {
              not: marker.queueNamespaceFingerprint,
            },
          },
        ],
      },
    });
    if (runtimeBlockers > 0) {
      throw new Error("Analytics backend switch runtime fleet is not quiesced");
    }
    const currentLeases = await transaction.analyticsRuntimeLease.findMany({
      where: {
        component: { in: ["WEB", "WORKER"] },
        backend: marker.backend,
        deploymentGeneration: marker.generation,
        workloadEpochFingerprint: marker.workloadEpochFingerprint,
        leaseExpiresAt: { gt: drainCutoff },
      },
      orderBy: { instanceId: "asc" },
    });
    const observedInventory = currentLeases.map((lease) => ({
      instanceId: lease.instanceId,
      component: fromPrismaAnalyticsServingRuntimeComponent(lease.component),
    }));
    const observedInventoryDigest =
      digestAnalyticsRuntimeInventory(observedInventory);
    if (
      observedInventoryDigest !== expectedInventoryDigest ||
      currentLeases.some(
        (lease) =>
          lease.state !== "QUIESCED" ||
          lease.queueNamespaceFingerprint !== marker.queueNamespaceFingerprint,
      )
    ) {
      throw new Error("Analytics backend switch inventory does not match");
    }
    const liveClaims = await transaction.analyticsBackendClaimLease.count({
      where: { releasedAt: null, leaseExpiresAt: { gt: drainCutoff } },
    });
    if (liveClaims > 0) {
      throw new Error("Analytics backend switch has live claim leases");
    }
    const queueDrainScope = {
      backend: input.expectedBackend,
      deploymentGeneration: marker.generation,
      workloadEpochFingerprint: marker.workloadEpochFingerprint,
    } as const;
    const scoreDeletionQueueDrain =
      await input.verifyScoreDeletionQueuesEmpty(queueDrainScope);
    assertAnalyticsQueueDrainEvidence({
      evidence: scoreDeletionQueueDrain,
      expectedScope: queueDrainScope,
      expectedQueueNamespaceFingerprint: marker.queueNamespaceFingerprint,
      transition: "switch",
    });
    const pendingWork = await getAllPendingWork(transaction);
    if (pendingWorkTotal(pendingWork) > 0) {
      throw new Error("Analytics backend switch has pending durable work");
    }
    if ((await countExistingAnalyticsControlState(transaction)) > 0) {
      throw new Error(
        "Analytics backend switch cannot move historical analytics data",
      );
    }
    const capabilityBlockers =
      await transaction.analyticsCapabilityActivation.count({
        where: {
          OR: [
            { status: { not: "DISABLED" } },
            { captureEnabled: true },
            { captureRequired: true },
            { rescanRequired: true },
            { cutoffState: { not: Prisma.DbNull } },
          ],
        },
      });
    if (capabilityBlockers > 0) {
      throw new Error("Analytics capabilities must be disabled before switch");
    }

    const nextGeneration = marker.generation + 1n;
    const updated =
      await transaction.analyticsBackendDeploymentState.updateMany({
        where: {
          id: marker.id,
          backend: marker.backend,
          generation: marker.generation,
          workloadEpochFingerprint: marker.workloadEpochFingerprint,
          queueNamespaceFingerprint: marker.queueNamespaceFingerprint,
        },
        data: {
          backend: targetBackend,
          generation: nextGeneration,
          workloadEpochFingerprint: input.targetWorkloadEpochFingerprint,
          foundationContractVersion: input.targetFoundationContractVersion,
          attestationDigest: input.denyProbeAttestationDigest,
          updatedAt: now,
        },
      });
    if (updated.count !== 1) {
      throw new Error("Analytics backend switch marker CAS failed");
    }
    const resetCapabilities =
      await transaction.analyticsCapabilityActivation.updateMany({
        data: {
          deploymentGeneration: nextGeneration,
          generation: { increment: 1 },
          status: "DISABLED",
          captureEnabled: false,
          captureRequired: false,
          rescanRequired: false,
          cutoffState: Prisma.DbNull,
          cutoffActivationGeneration: null,
          cutoffDigest: null,
          bootstrapCompletedGeneration: null,
          bootstrapEvidenceDigest: null,
          bootstrapCompletedAt: null,
          activatedAt: null,
          drainingAt: null,
          disabledAt: now,
        },
      });
    if (resetCapabilities.count !== ANALYTICS_CAPABILITY_NAMES.length) {
      throw new Error("Analytics capability catalog rows are incomplete");
    }
    const drainEvidenceDigest = hash(
      JSON.stringify({
        attestation: input.externalDrainAttestationDigest,
        counts: pendingWork,
        sourceEmptyEvidenceDigest: emptinessEvidence.source.evidenceDigest,
        targetEmptyEvidenceDigest: emptinessEvidence.target.evidenceDigest,
        scoreDeletionQueueDrainEvidenceDigest:
          scoreDeletionQueueDrain.evidenceDigest,
        queueNamespaceFingerprint:
          scoreDeletionQueueDrain.queueNamespaceFingerprint,
        scoreDeletionQueuePendingJobs: scoreDeletionQueueDrain.pendingJobs,
      }),
    );
    await transaction.analyticsBackendDeploymentTransition.create({
      data: {
        kind: "SWITCH",
        fromBackend: marker.backend,
        toBackend: targetBackend,
        fromGeneration: marker.generation,
        toGeneration: nextGeneration,
        targetWorkloadEpochFingerprint: input.targetWorkloadEpochFingerprint,
        expectedInventoryDigest,
        observedInventoryDigest,
        drainEvidenceDigest,
        denyProbeAttestationDigest: input.denyProbeAttestationDigest,
        sourceEmptyEvidenceDigest: emptinessEvidence.source.evidenceDigest,
        targetEmptyEvidenceDigest: emptinessEvidence.target.evidenceDigest,
        completedAt: now,
      },
    });
    return transaction.analyticsBackendDeploymentState.findUniqueOrThrow({
      where: { id: marker.id },
    });
  });
}
