import { randomUUID } from "node:crypto";

import { Prisma } from "@prisma/client";
import type {
  AnalyticsBackendClaimLease,
  AnalyticsCapabilityActivation,
  AnalyticsRuntimeCapabilityContract,
  AnalyticsRuntimeLease,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import type { AnalyticsBackend } from "../analytics-persistence/analyticsBackend";
import {
  lockAnalyticsAdmission,
  type AnalyticsContractVersionRequirement,
} from "../analytics-persistence/analyticsBackendAdmission";
import {
  ANALYTICS_CAPABILITY_CATALOG,
  type AnalyticsCapabilityName,
  type AnalyticsCapabilityRuntimeRole,
} from "../analytics-persistence/analyticsCapabilities";
import {
  fromPrismaAnalyticsCapability,
  fromPrismaAnalyticsRuntimeComponent,
  toPrismaAnalyticsBackend,
  toPrismaAnalyticsCapability,
  toPrismaAnalyticsRuntimeComponent,
  toPrismaAnalyticsRuntimeRole,
  type AnalyticsRuntimeComponentName,
} from "../analytics-persistence/analyticsBackendMapping";
import {
  acquireAnalyticsDeploymentSharedLock,
  lockAnalyticsBackendDeploymentState,
} from "./analyticsBackendDeployment";

const SHA256_HEX = /^[a-f0-9]{64}$/;
const INSTANCE_LOCK_SEED = 8_216_401_973n;
const CLAIM_LOCK_SEED = 8_216_401_974n;

export type AnalyticsBackendClaimFence = {
  readonly runtimeLeaseId: string;
  readonly expectedBackend: AnalyticsBackend;
  readonly expectedDeploymentGeneration: bigint;
  readonly expectedWorkloadEpochFingerprint: string;
  readonly expectedRuntimeContractVersion: number;
  readonly capability?: AnalyticsCapabilityName;
  readonly expectedCapabilityActivationGeneration?: bigint;
  readonly expectedCapabilityContractVersion?: number;
  readonly requiredContract?: AnalyticsContractVersionRequirement;
  readonly action: "foundation" | "claimExisting" | "recovery";
};

export type AnalyticsRuntimeCapabilityContractInput = {
  readonly capability: AnalyticsCapabilityName;
  readonly supportedContractVersion: number;
  readonly installedRoles: readonly AnalyticsCapabilityRuntimeRole[];
};

type VersionRange = { readonly min: number; readonly max: number };

export type AnalyticsRuntimeCompatibilityEvidence = Pick<
  AnalyticsRuntimeLease,
  | "component"
  | "instanceId"
  | "buildId"
  | "acceptedSchemaVersionMin"
  | "acceptedSchemaVersionMax"
  | "acceptedCanonicalVersionMin"
  | "acceptedCanonicalVersionMax"
  | "state"
  | "leaseExpiresAt"
  | "supersededAt"
  | "quiescedAt"
>;

export type AnalyticsContractRolloutEvaluation =
  | { readonly ready: true; readonly reasonCode: null }
  | {
      readonly ready: false;
      readonly reasonCode:
        | "LIVE_RUNTIME_INVENTORY_MISMATCH"
        | "LIVE_RUNTIME_INCOMPATIBLE"
        | "ROLLBACK_NOT_ATTESTED";
    };

function rangeContainsAll(
  min: number,
  max: number,
  required: readonly number[],
): boolean {
  return required.every((version) => version >= min && version <= max);
}

export function evaluateAnalyticsContractRollout(input: {
  readonly leases: readonly AnalyticsRuntimeCompatibilityEvidence[];
  readonly now: Date;
  readonly expectedRuntimeInstanceIds: readonly string[];
  readonly requiredSchemaVersions: readonly number[];
  readonly requiredCanonicalVersions: readonly number[];
  readonly rollbackBuildId: string;
}): AnalyticsContractRolloutEvaluation {
  const versionSets = [
    input.requiredSchemaVersions,
    input.requiredCanonicalVersions,
  ];
  if (
    !Number.isFinite(input.now.getTime()) ||
    !input.rollbackBuildId ||
    input.expectedRuntimeInstanceIds.length === 0 ||
    new Set(input.expectedRuntimeInstanceIds).size !==
      input.expectedRuntimeInstanceIds.length ||
    input.expectedRuntimeInstanceIds.some((instanceId) => !instanceId) ||
    versionSets.some(
      (versions) =>
        versions.length === 0 ||
        new Set(versions).size !== versions.length ||
        versions.some(
          (version) => !Number.isSafeInteger(version) || version < 1,
        ),
    )
  ) {
    throw new TypeError("Invalid analytics contract rollout evidence");
  }

  const live = input.leases.filter(
    (lease) =>
      lease.state === "ACTIVE" &&
      lease.supersededAt === null &&
      lease.leaseExpiresAt > input.now &&
      (lease.component === "WEB" || lease.component === "WORKER"),
  );
  const actualInventory = [...new Set(live.map(({ instanceId }) => instanceId))]
    .sort()
    .join("\0");
  const expectedInventory = [...input.expectedRuntimeInstanceIds]
    .sort()
    .join("\0");
  const components = new Set(live.map(({ component }) => component));
  if (
    actualInventory !== expectedInventory ||
    !components.has("WEB") ||
    !components.has("WORKER")
  ) {
    return {
      ready: false,
      reasonCode: "LIVE_RUNTIME_INVENTORY_MISMATCH",
    };
  }
  if (
    live.some(
      (lease) =>
        !rangeContainsAll(
          lease.acceptedSchemaVersionMin,
          lease.acceptedSchemaVersionMax,
          input.requiredSchemaVersions,
        ) ||
        !rangeContainsAll(
          lease.acceptedCanonicalVersionMin,
          lease.acceptedCanonicalVersionMax,
          input.requiredCanonicalVersions,
        ),
    )
  ) {
    return { ready: false, reasonCode: "LIVE_RUNTIME_INCOMPATIBLE" };
  }

  const rollbackComponents = new Set(
    input.leases
      .filter(
        (lease) =>
          lease.buildId === input.rollbackBuildId &&
          lease.state === "QUIESCED" &&
          lease.quiescedAt !== null &&
          lease.quiescedAt <= input.now &&
          rangeContainsAll(
            lease.acceptedSchemaVersionMin,
            lease.acceptedSchemaVersionMax,
            input.requiredSchemaVersions,
          ) &&
          rangeContainsAll(
            lease.acceptedCanonicalVersionMin,
            lease.acceptedCanonicalVersionMax,
            input.requiredCanonicalVersions,
          ),
      )
      .map(({ component }) => component),
  );
  const rollbackAttested =
    rollbackComponents.has("WEB") && rollbackComponents.has("WORKER");
  return rollbackAttested
    ? { ready: true, reasonCode: null }
    : { ready: false, reasonCode: "ROLLBACK_NOT_ATTESTED" };
}

export async function assertAnalyticsContractRolloutReady(input: {
  readonly client?: PrismaClient;
  readonly expectedRuntimeInstanceIds: readonly string[];
  readonly requiredSchemaVersions: readonly number[];
  readonly requiredCanonicalVersions: readonly number[];
  readonly rollbackBuildId: string;
  readonly now?: Date;
}): Promise<void> {
  const client = input.client ?? prisma;
  await client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    if (!marker) throw new Error("Analytics backend adoption is required");
    await transaction.$queryRaw(
      Prisma.sql`
        SELECT id
        FROM analytics_runtime_leases
        WHERE backend::text = ${marker.backend}
          AND deployment_generation = ${marker.generation}
        ORDER BY id
        FOR SHARE
      `,
    );
    const leases = await transaction.analyticsRuntimeLease.findMany({
      where: {
        backend: marker.backend,
        deploymentGeneration: marker.generation,
      },
    });
    const evaluation = evaluateAnalyticsContractRollout({
      leases,
      now: input.now ?? (await databaseClock(transaction)),
      expectedRuntimeInstanceIds: input.expectedRuntimeInstanceIds,
      requiredSchemaVersions: input.requiredSchemaVersions,
      requiredCanonicalVersions: input.requiredCanonicalVersions,
      rollbackBuildId: input.rollbackBuildId,
    });
    if (!evaluation.ready) {
      throw new Error(
        `Analytics contract rollout is not ready: ${evaluation.reasonCode}`,
      );
    }
  });
}

function validateRange(range: VersionRange, name: string): void {
  if (
    !Number.isSafeInteger(range.min) ||
    !Number.isSafeInteger(range.max) ||
    range.min < 1 ||
    range.max < range.min
  ) {
    throw new TypeError(`Invalid ${name} version range`);
  }
}

function normalizeContracts(
  contracts: readonly AnalyticsRuntimeCapabilityContractInput[],
) {
  const capabilities = new Set<AnalyticsCapabilityName>();
  return contracts.map((contract) => {
    if (
      capabilities.has(contract.capability) ||
      !Number.isSafeInteger(contract.supportedContractVersion) ||
      contract.supportedContractVersion < 1 ||
      contract.installedRoles.length === 0
    ) {
      throw new TypeError("Invalid analytics runtime capability contract");
    }
    capabilities.add(contract.capability);
    const installedRoles = [...new Set(contract.installedRoles)]
      .sort()
      .map(toPrismaAnalyticsRuntimeRole);
    return {
      capability: toPrismaAnalyticsCapability(contract.capability),
      supportedContractVersion: contract.supportedContractVersion,
      installedRoles,
    };
  });
}

async function lockConstrainedActivations(
  transaction: Prisma.TransactionClient,
): Promise<readonly AnalyticsCapabilityActivation[]> {
  await transaction.$queryRaw(
    Prisma.sql`
      SELECT capability
      FROM analytics_capability_activations
      WHERE status IN ('DARK', 'ACTIVE', 'DRAINING')
         OR capture_enabled = true
         OR capture_required = true
      ORDER BY capability
      FOR SHARE
    `,
  );
  const locked = await transaction.analyticsCapabilityActivation.findMany({
    where: {
      OR: [
        { status: "DARK" },
        { status: { in: ["ACTIVE", "DRAINING"] } },
        { captureEnabled: true },
        { captureRequired: true },
      ],
    },
    orderBy: { capability: "asc" },
  });
  return locked.filter(
    (activation) =>
      activation.status === "ACTIVE" ||
      activation.status === "DRAINING" ||
      activation.captureEnabled ||
      activation.captureRequired,
  );
}

function assertCapabilityContracts(input: {
  readonly component: AnalyticsRuntimeComponentName;
  readonly contracts: readonly AnalyticsRuntimeCapabilityContract[];
  readonly activations: readonly AnalyticsCapabilityActivation[];
}): void {
  if (input.component === "checkpoint") {
    if (input.contracts.length > 0) {
      throw new Error("Checkpoint runtimes cannot advertise capability roles");
    }
    return;
  }
  const available = new Map(
    input.contracts.map((contract) => [contract.capability, contract]),
  );
  for (const activation of input.activations) {
    const catalogEntry =
      ANALYTICS_CAPABILITY_CATALOG[
        fromPrismaAnalyticsCapability(activation.capability)
      ];
    const contract = available.get(activation.capability);
    const requiredRoles = catalogEntry.components[input.component].map(
      toPrismaAnalyticsRuntimeRole,
    );
    if (
      !contract ||
      contract.supportedContractVersion < activation.minimumRuntimeContract ||
      requiredRoles.some((role) => !contract.installedRoles.includes(role))
    ) {
      throw new Error("Runtime capability contract is incompatible");
    }
  }
}

async function acquireRuntimeInstanceLock(
  transaction: Prisma.TransactionClient,
  instanceId: string,
): Promise<void> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock(hashtextextended(${instanceId}, ${INSTANCE_LOCK_SEED}))::text AS locked`,
  );
}

async function acquireClaimResourceLock(
  transaction: Prisma.TransactionClient,
  resourceKey: string,
): Promise<void> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock(hashtextextended(${resourceKey}, ${CLAIM_LOCK_SEED}))::text AS locked`,
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

function contractsMatch(
  existing: readonly AnalyticsRuntimeCapabilityContract[],
  requested: ReturnType<typeof normalizeContracts>,
): boolean {
  if (existing.length !== requested.length) return false;
  const byCapability = new Map(
    existing.map((contract) => [contract.capability, contract]),
  );
  return requested.every((contract) => {
    const current = byCapability.get(contract.capability);
    return (
      current?.supportedContractVersion === contract.supportedContractVersion &&
      current.installedRoles.length === contract.installedRoles.length &&
      current.installedRoles.every(
        (role, index) => role === contract.installedRoles[index],
      )
    );
  });
}

async function lockRuntimeLease(
  transaction: Prisma.TransactionClient,
  runtimeLeaseId: string,
): Promise<AnalyticsRuntimeLease | null> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT id FROM analytics_runtime_leases WHERE id = ${runtimeLeaseId} FOR UPDATE`,
  );
  return transaction.analyticsRuntimeLease.findUnique({
    where: { id: runtimeLeaseId },
  });
}

async function lockRuntimeLeaseForClaimCreation(
  transaction: Prisma.TransactionClient,
  runtimeLeaseId: string,
): Promise<AnalyticsRuntimeLease | null> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT id FROM analytics_runtime_leases WHERE id = ${runtimeLeaseId} FOR SHARE`,
  );
  return transaction.analyticsRuntimeLease.findUnique({
    where: { id: runtimeLeaseId },
  });
}

function markerMatchesLease(
  marker: Awaited<ReturnType<typeof lockAnalyticsBackendDeploymentState>>,
  lease: AnalyticsRuntimeLease,
): boolean {
  return marker
    ? lease.backend === marker.backend &&
        lease.deploymentGeneration === marker.generation &&
        lease.workloadEpochFingerprint === marker.workloadEpochFingerprint &&
        lease.queueNamespaceFingerprint === marker.queueNamespaceFingerprint &&
        lease.foundationContractVersion === marker.foundationContractVersion
    : lease.deploymentGeneration === 0n;
}

export async function registerAnalyticsRuntimeLease(input: {
  readonly client?: PrismaClient;
  readonly runtimeLeaseId?: string;
  readonly component: AnalyticsRuntimeComponentName;
  readonly instanceId: string;
  readonly backend: AnalyticsBackend;
  readonly deploymentGeneration: bigint;
  readonly workloadEpochFingerprint: string;
  readonly queueNamespaceFingerprint: string;
  readonly buildId: string;
  readonly foundationContractVersion: number;
  readonly acceptedSchemaVersion: VersionRange;
  readonly acceptedCanonicalVersion: VersionRange;
  readonly capabilityContracts: readonly AnalyticsRuntimeCapabilityContractInput[];
  readonly leaseMs: number;
  readonly now?: Date;
}): Promise<{
  readonly mode: "READY" | "ADOPTION_REQUIRED";
  readonly lease: AnalyticsRuntimeLease;
}> {
  if (
    !input.instanceId ||
    !input.buildId ||
    !SHA256_HEX.test(input.workloadEpochFingerprint) ||
    !SHA256_HEX.test(input.queueNamespaceFingerprint) ||
    !Number.isSafeInteger(input.foundationContractVersion) ||
    input.foundationContractVersion < 1 ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 1_000
  ) {
    throw new TypeError("Invalid analytics runtime lease");
  }
  validateRange(input.acceptedSchemaVersion, "schema");
  validateRange(input.acceptedCanonicalVersion, "canonical");
  const contracts = normalizeContracts(input.capabilityContracts);
  const client = input.client ?? prisma;
  const runtimeLeaseId = input.runtimeLeaseId ?? randomUUID();
  if (!runtimeLeaseId) throw new TypeError("Runtime lease ID is required");
  const backend = toPrismaAnalyticsBackend(input.backend);
  const component = toPrismaAnalyticsRuntimeComponent(input.component);

  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    if (marker) {
      if (
        backend !== marker.backend ||
        input.deploymentGeneration !== marker.generation ||
        input.workloadEpochFingerprint !== marker.workloadEpochFingerprint ||
        input.queueNamespaceFingerprint !== marker.queueNamespaceFingerprint ||
        input.foundationContractVersion !== marker.foundationContractVersion
      ) {
        throw new Error("Runtime lease does not match analytics deployment");
      }
    } else if (input.deploymentGeneration !== 0n) {
      throw new Error(
        "Marker-absent foundation leases require generation zero",
      );
    }
    const constrainedActivations =
      await lockConstrainedActivations(transaction);
    await acquireRuntimeInstanceLock(transaction, input.instanceId);
    await transaction.$queryRaw(
      Prisma.sql`SELECT id FROM analytics_runtime_leases WHERE instance_id = ${input.instanceId} AND superseded_at IS NULL FOR UPDATE`,
    );
    const existing = await transaction.analyticsRuntimeLease.findFirst({
      where: { instanceId: input.instanceId, supersededAt: null },
      include: { capabilityContracts: true },
    });
    const now = input.now ?? (await databaseClock(transaction));
    const candidateContracts = contracts.map((contract) => ({
      runtimeLeaseId,
      ...contract,
      createdAt: now,
      updatedAt: now,
    }));
    assertCapabilityContracts({
      component: input.component,
      contracts: candidateContracts,
      activations: constrainedActivations,
    });

    if (existing?.id === runtimeLeaseId) {
      const exactRetry =
        existing.component === component &&
        existing.backend === backend &&
        existing.deploymentGeneration === input.deploymentGeneration &&
        existing.workloadEpochFingerprint === input.workloadEpochFingerprint &&
        existing.queueNamespaceFingerprint ===
          input.queueNamespaceFingerprint &&
        existing.buildId === input.buildId &&
        existing.foundationContractVersion ===
          input.foundationContractVersion &&
        existing.acceptedSchemaVersionMin === input.acceptedSchemaVersion.min &&
        existing.acceptedSchemaVersionMax === input.acceptedSchemaVersion.max &&
        existing.acceptedCanonicalVersionMin ===
          input.acceptedCanonicalVersion.min &&
        existing.acceptedCanonicalVersionMax ===
          input.acceptedCanonicalVersion.max &&
        existing.state === "ACTIVE" &&
        existing.leaseExpiresAt > now &&
        contractsMatch(existing.capabilityContracts, contracts);
      if (!exactRetry) {
        throw new Error("Runtime lease incarnation cannot be reused");
      }
      return {
        mode: marker ? "READY" : "ADOPTION_REQUIRED",
        lease: existing,
      };
    }
    if (
      existing &&
      existing.state !== "QUIESCED" &&
      existing.leaseExpiresAt > now
    ) {
      throw new Error("A live runtime already owns this instance ID");
    }
    if (existing) {
      const superseded = await transaction.analyticsRuntimeLease.updateMany({
        where: { id: existing.id, supersededAt: null },
        data: { supersededAt: now },
      });
      if (superseded.count !== 1) {
        throw new Error("Runtime lease takeover lost its ownership race");
      }
    }

    const lease = await transaction.analyticsRuntimeLease.create({
      data: {
        id: runtimeLeaseId,
        component,
        instanceId: input.instanceId,
        backend,
        deploymentGeneration: input.deploymentGeneration,
        workloadEpochFingerprint: input.workloadEpochFingerprint,
        queueNamespaceFingerprint: input.queueNamespaceFingerprint,
        buildId: input.buildId,
        foundationContractVersion: input.foundationContractVersion,
        acceptedSchemaVersionMin: input.acceptedSchemaVersion.min,
        acceptedSchemaVersionMax: input.acceptedSchemaVersion.max,
        acceptedCanonicalVersionMin: input.acceptedCanonicalVersion.min,
        acceptedCanonicalVersionMax: input.acceptedCanonicalVersion.max,
        state: "ACTIVE",
        heartbeatAt: now,
        leaseExpiresAt: new Date(now.getTime() + input.leaseMs),
      },
    });
    if (contracts.length > 0) {
      await transaction.analyticsRuntimeCapabilityContract.createMany({
        data: contracts.map((contract) => ({
          runtimeLeaseId: lease.id,
          ...contract,
        })),
      });
    }
    return {
      mode: marker ? "READY" : "ADOPTION_REQUIRED",
      lease,
    };
  });
}

export async function renewAnalyticsRuntimeLease(input: {
  readonly client?: PrismaClient;
  readonly runtimeLeaseId: string;
  readonly leaseMs: number;
  readonly now?: Date;
}): Promise<boolean> {
  if (
    !input.runtimeLeaseId ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 1_000
  ) {
    throw new TypeError("Invalid analytics runtime lease renewal");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    const constrainedActivations =
      await lockConstrainedActivations(transaction);
    const lease = await lockRuntimeLease(transaction, input.runtimeLeaseId);
    const now = input.now ?? (await databaseClock(transaction));
    if (
      !lease ||
      lease.leaseExpiresAt <= now ||
      lease.supersededAt !== null ||
      lease.state === "QUIESCED" ||
      !markerMatchesLease(marker, lease)
    ) {
      return false;
    }
    const contracts =
      await transaction.analyticsRuntimeCapabilityContract.findMany({
        where: { runtimeLeaseId: lease.id },
      });
    try {
      assertCapabilityContracts({
        component: fromPrismaAnalyticsRuntimeComponent(lease.component),
        contracts,
        activations: constrainedActivations,
      });
    } catch {
      return false;
    }
    if (now <= lease.heartbeatAt) return true;
    const candidateExpiry = new Date(now.getTime() + input.leaseMs);
    await transaction.analyticsRuntimeLease.update({
      where: { id: lease.id },
      data: {
        heartbeatAt: now,
        leaseExpiresAt:
          candidateExpiry > lease.leaseExpiresAt
            ? candidateExpiry
            : lease.leaseExpiresAt,
      },
    });
    return true;
  });
}

export async function markAnalyticsRuntimeQuiesced(input: {
  readonly client?: PrismaClient;
  readonly runtimeLeaseId: string;
  readonly now?: Date;
}): Promise<boolean> {
  if (!input.runtimeLeaseId) {
    throw new TypeError("Runtime lease ID is required");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const lease = await lockRuntimeLease(transaction, input.runtimeLeaseId);
    const now = input.now ?? (await databaseClock(transaction));
    if (!lease || lease.supersededAt !== null || lease.state === "QUIESCED") {
      return false;
    }
    const activeClaims = await transaction.analyticsBackendClaimLease.count({
      where: {
        runtimeLeaseId: lease.id,
        releasedAt: null,
        leaseExpiresAt: { gt: now },
      },
    });
    if (activeClaims > 0) return false;
    const updated = await transaction.analyticsRuntimeLease.updateMany({
      where: { id: lease.id, state: { not: "QUIESCED" } },
      data: {
        state: "QUIESCED",
        quiescedAt: now,
      },
    });
    return updated.count === 1;
  });
}

function assertValidAnalyticsBackendClaimFence(
  input: AnalyticsBackendClaimFence,
): void {
  if (
    !input.runtimeLeaseId ||
    input.expectedDeploymentGeneration < 1n ||
    !SHA256_HEX.test(input.expectedWorkloadEpochFingerprint) ||
    !Number.isSafeInteger(input.expectedRuntimeContractVersion) ||
    input.expectedRuntimeContractVersion < 1 ||
    (input.action === "foundation") !== (input.capability === undefined) ||
    (input.expectedCapabilityActivationGeneration === undefined) !==
      (input.expectedCapabilityContractVersion === undefined) ||
    (input.expectedBackend === "doris" &&
      input.capability !== undefined &&
      input.expectedCapabilityActivationGeneration === undefined) ||
    (input.expectedBackend === "clickhouse" &&
      input.expectedCapabilityActivationGeneration !== undefined)
  ) {
    throw new TypeError("Invalid analytics backend claim fence");
  }
}

async function lockAndValidateAnalyticsBackendClaimLease(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly claimLeaseId: string;
  readonly fence: AnalyticsBackendClaimFence;
  readonly mode: "SHARE" | "UPDATE";
  readonly now?: Date;
}): Promise<{
  readonly claim: AnalyticsBackendClaimLease;
  readonly now: Date;
}> {
  if (!input.claimLeaseId) {
    throw new TypeError("Analytics backend claim lease ID is required");
  }
  assertValidAnalyticsBackendClaimFence(input.fence);
  const admission = await lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.fence.runtimeLeaseId,
    expectedBackend: input.fence.expectedBackend,
    expectedDeploymentGeneration: input.fence.expectedDeploymentGeneration,
    capability: input.fence.capability,
    action: input.fence.action,
    expectedCapabilityActivationGeneration:
      input.fence.expectedCapabilityActivationGeneration,
    expectedCapabilityContractVersion:
      input.fence.expectedCapabilityContractVersion,
    requiredContract: input.fence.requiredContract,
    now: input.now,
  });
  if (
    admission.workloadEpochFingerprint !==
      input.fence.expectedWorkloadEpochFingerprint ||
    admission.runtimeContractVersion !==
      input.fence.expectedRuntimeContractVersion
  ) {
    throw new Error("Analytics durable work provenance changed");
  }

  const lock =
    input.mode === "SHARE" ? Prisma.sql`FOR SHARE` : Prisma.sql`FOR UPDATE`;
  await input.transaction.$queryRaw(
    Prisma.sql`SELECT id FROM analytics_backend_claim_leases WHERE id = ${input.claimLeaseId} ${lock}`,
  );
  const claim = await input.transaction.analyticsBackendClaimLease.findUnique({
    where: { id: input.claimLeaseId },
  });
  const expectedCapability = input.fence.capability
    ? toPrismaAnalyticsCapability(input.fence.capability)
    : null;
  const expectedActivationGeneration =
    input.fence.expectedCapabilityActivationGeneration ?? null;
  const expectedCapabilityContractVersion =
    input.fence.expectedCapabilityContractVersion ?? null;
  if (
    !claim ||
    claim.runtimeLeaseId !== input.fence.runtimeLeaseId ||
    claim.backend !== admission.analyticsBackend ||
    claim.deploymentGeneration !== admission.deploymentGeneration ||
    claim.workloadEpochFingerprint !== admission.workloadEpochFingerprint ||
    claim.runtimeContractVersion !== admission.runtimeContractVersion ||
    claim.capability !== expectedCapability ||
    claim.capabilityActivationGeneration !== expectedActivationGeneration ||
    claim.capabilityContractVersion !== expectedCapabilityContractVersion ||
    claim.releasedAt !== null ||
    claim.leaseExpiresAt <= admission.admittedAt
  ) {
    throw new Error("Analytics backend claim lease is no longer admitted");
  }
  return { claim, now: admission.admittedAt };
}

/**
 * Revalidates one claim immediately before a page, batch, or analytics client
 * acquisition. Callers keep the surrounding transaction open for the IO so a
 * backend switch cannot pass the deployment lock mid-operation.
 */
export async function lockAnalyticsBackendClaimLeaseForIo(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly claimLeaseId: string;
  readonly fence: AnalyticsBackendClaimFence;
  readonly now?: Date;
}): Promise<AnalyticsBackendClaimLease> {
  const validated = await lockAndValidateAnalyticsBackendClaimLease({
    ...input,
    mode: "SHARE",
  });
  return validated.claim;
}

export async function renewAnalyticsBackendClaimLease(input: {
  readonly client?: PrismaClient;
  readonly claimLeaseId: string;
  readonly fence: AnalyticsBackendClaimFence;
  readonly leaseMs: number;
  readonly now?: Date;
}): Promise<AnalyticsBackendClaimLease> {
  if (!Number.isSafeInteger(input.leaseMs) || input.leaseMs < 1_000) {
    throw new TypeError("Invalid analytics backend claim lease renewal");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    const validated = await lockAndValidateAnalyticsBackendClaimLease({
      transaction,
      claimLeaseId: input.claimLeaseId,
      fence: input.fence,
      mode: "UPDATE",
      now: input.now,
    });
    const candidateExpiry = new Date(validated.now.getTime() + input.leaseMs);
    if (candidateExpiry <= validated.claim.leaseExpiresAt) {
      return validated.claim;
    }
    return transaction.analyticsBackendClaimLease.update({
      where: { id: validated.claim.id },
      data: { leaseExpiresAt: candidateExpiry },
    });
  });
}

export async function createAnalyticsBackendClaimLease(
  input: AnalyticsBackendClaimFence & {
    readonly client?: PrismaClient;
    readonly claimKind: string;
    readonly resourceIdentity: string;
    readonly leaseMs: number;
    readonly now?: Date;
  },
) {
  assertValidAnalyticsBackendClaimFence(input);
  if (
    !input.claimKind ||
    !input.resourceIdentity ||
    !Number.isSafeInteger(input.leaseMs) ||
    input.leaseMs < 1_000
  ) {
    throw new TypeError("Invalid analytics backend claim lease");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    const admission = await lockAnalyticsAdmission({
      transaction,
      runtimeLeaseId: input.runtimeLeaseId,
      expectedBackend: input.expectedBackend,
      expectedDeploymentGeneration: input.expectedDeploymentGeneration,
      capability: input.capability,
      action: input.action,
      expectedCapabilityActivationGeneration:
        input.expectedCapabilityActivationGeneration,
      expectedCapabilityContractVersion:
        input.expectedCapabilityContractVersion,
      requiredContract: input.requiredContract,
      now: input.now,
    });
    if (
      admission.workloadEpochFingerprint !==
        input.expectedWorkloadEpochFingerprint ||
      admission.runtimeContractVersion !== input.expectedRuntimeContractVersion
    ) {
      throw new Error("Analytics durable work provenance changed");
    }
    await acquireClaimResourceLock(
      transaction,
      [
        admission.analyticsBackend,
        admission.deploymentGeneration.toString(),
        input.claimKind,
        input.resourceIdentity,
      ].join(":"),
    );
    // 资源 advisory lock 可能等待较久，不能在等待期间锁住 runtime row，
    // 否则会阻塞 heartbeat。拿到资源锁后固定按 runtime row -> claim row
    // 加锁，与 release/quiesce 保持同序，并在最终写入前短暂重验 runtime。
    const runtimeLease = await lockRuntimeLeaseForClaimCreation(
      transaction,
      input.runtimeLeaseId,
    );
    const claimCreatedAt = input.now ?? (await databaseClock(transaction));
    if (
      !runtimeLease ||
      runtimeLease.state !== "ACTIVE" ||
      runtimeLease.supersededAt !== null ||
      runtimeLease.leaseExpiresAt <= claimCreatedAt ||
      runtimeLease.backend !== admission.analyticsBackend ||
      runtimeLease.deploymentGeneration !== admission.deploymentGeneration ||
      runtimeLease.workloadEpochFingerprint !==
        admission.workloadEpochFingerprint ||
      runtimeLease.foundationContractVersion !==
        admission.runtimeContractVersion
    ) {
      throw new Error("Analytics runtime lease is no longer active");
    }
    const existing = await transaction.analyticsBackendClaimLease.findFirst({
      where: {
        backend: admission.analyticsBackend,
        deploymentGeneration: admission.deploymentGeneration,
        claimKind: input.claimKind,
        resourceIdentity: input.resourceIdentity,
        releasedAt: null,
      },
    });
    if (existing?.leaseExpiresAt && existing.leaseExpiresAt > claimCreatedAt) {
      return null;
    }
    if (existing) {
      await transaction.analyticsBackendClaimLease.update({
        where: { id: existing.id },
        data: { releasedAt: claimCreatedAt },
      });
    }
    return transaction.analyticsBackendClaimLease.create({
      data: {
        runtimeLeaseId: input.runtimeLeaseId,
        backend: admission.analyticsBackend,
        deploymentGeneration: admission.deploymentGeneration,
        workloadEpochFingerprint: admission.workloadEpochFingerprint,
        runtimeContractVersion: admission.runtimeContractVersion,
        capability: input.capability
          ? toPrismaAnalyticsCapability(input.capability)
          : null,
        capabilityActivationGeneration:
          admission.capabilityActivationGeneration ?? null,
        capabilityContractVersion: admission.capabilityContractVersion ?? null,
        claimKind: input.claimKind,
        resourceIdentity: input.resourceIdentity,
        leaseExpiresAt: new Date(claimCreatedAt.getTime() + input.leaseMs),
      },
    });
  });
}

export async function releaseAnalyticsBackendClaimLease(input: {
  readonly client?: PrismaClient;
  readonly claimLeaseId: string;
  readonly runtimeLeaseId: string;
  readonly now?: Date;
}): Promise<boolean> {
  if (!input.claimLeaseId || !input.runtimeLeaseId) {
    throw new TypeError("Invalid analytics backend claim release");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    const lease = await lockRuntimeLease(transaction, input.runtimeLeaseId);
    const now = input.now ?? (await databaseClock(transaction));
    if (
      !lease ||
      lease.supersededAt !== null ||
      lease.state !== "ACTIVE" ||
      lease.leaseExpiresAt <= now
    ) {
      return false;
    }
    const released = await transaction.analyticsBackendClaimLease.updateMany({
      where: {
        id: input.claimLeaseId,
        runtimeLeaseId: input.runtimeLeaseId,
        releasedAt: null,
      },
      data: { releasedAt: now },
    });
    return released.count === 1;
  });
}
