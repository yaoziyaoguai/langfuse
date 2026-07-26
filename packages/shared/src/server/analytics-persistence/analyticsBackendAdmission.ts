import { Prisma } from "@prisma/client";
import type { Prisma as PrismaTypes } from "@prisma/client";

import type { AnalyticsBackend } from "./analyticsBackend";
import type {
  AnalyticsCapabilityName,
  AnalyticsCapabilityRuntimeRole,
} from "./analyticsCapabilities";
import {
  toPrismaAnalyticsBackend,
  toPrismaAnalyticsCapability,
  toPrismaAnalyticsRuntimeRole,
} from "./analyticsBackendMapping";
import {
  acquireAnalyticsDeploymentSharedLock,
  lockAnalyticsBackendDeploymentState,
} from "../repositories/analyticsBackendDeployment";

export * from "./analyticsDurableProvenance";

export type AnalyticsAdmissionAction =
  | "foundation"
  | "externalProducer"
  | "internalCapture"
  | "internalBootstrap"
  | "claimExisting"
  | "recovery";

export type AnalyticsRuntimeAdmissionContext = {
  readonly runtimeLeaseId: string;
  readonly backend: AnalyticsBackend;
  readonly deploymentGeneration: bigint;
};

export type AnalyticsContractVersionRequirement = {
  readonly schemaVersion: number;
  readonly canonicalizerVersion: string;
};

type AnalyticsRuntimeContractRange = {
  readonly acceptedSchemaVersionMin: number;
  readonly acceptedSchemaVersionMax: number;
  readonly acceptedCanonicalVersionMin: number;
  readonly acceptedCanonicalVersionMax: number;
};

function canonicalizerVersionNumber(value: string): number | null {
  if (!/^[1-9][0-9]*$/.test(value)) return null;
  const version = Number(value);
  return Number.isSafeInteger(version) ? version : null;
}

export function runtimeLeaseAcceptsAnalyticsContract(
  lease: AnalyticsRuntimeContractRange,
  required: AnalyticsContractVersionRequirement,
): boolean {
  const canonicalizerVersion = canonicalizerVersionNumber(
    required.canonicalizerVersion,
  );
  return (
    Number.isSafeInteger(required.schemaVersion) &&
    required.schemaVersion >= 1 &&
    canonicalizerVersion !== null &&
    required.schemaVersion >= lease.acceptedSchemaVersionMin &&
    required.schemaVersion <= lease.acceptedSchemaVersionMax &&
    canonicalizerVersion >= lease.acceptedCanonicalVersionMin &&
    canonicalizerVersion <= lease.acceptedCanonicalVersionMax
  );
}

export type AnalyticsAdmissionStamp = {
  readonly analyticsBackend: "CLICKHOUSE" | "DORIS";
  readonly deploymentGeneration: bigint;
  readonly workloadEpochFingerprint: string;
  readonly runtimeContractVersion: number;
  readonly admittingRuntimeLeaseId: string;
  readonly admittedAt: Date;
  readonly capabilityActivationGeneration?: bigint;
  readonly capabilityContractVersion?: number;
};

/**
 * Preserves pre-F0 unstamped work only while the deployment marker is absent.
 * The shared lock makes adoption serialize with the authoritative row create.
 */
export async function lockLegacyAnalyticsAdmission(
  transaction: PrismaTypes.TransactionClient,
): Promise<void> {
  await acquireAnalyticsDeploymentSharedLock(transaction);
  if (await lockAnalyticsBackendDeploymentState(transaction, "SHARE")) {
    throw new Error("Legacy unstamped analytics work is fenced by deployment");
  }
}

async function databaseClock(
  transaction: PrismaTypes.TransactionClient,
): Promise<Date> {
  const [row] = await transaction.$queryRaw<readonly { now: Date }[]>(
    Prisma.sql`SELECT clock_timestamp() AS now`,
  );
  if (!row) throw new Error("Postgres did not return its current timestamp");
  return row.now;
}

const ACTION_ROLE = {
  externalProducer: "producer",
  internalCapture: "capture",
  internalBootstrap: "recovery",
  claimExisting: "consumer",
  recovery: "recovery",
} as const satisfies Record<
  Exclude<AnalyticsAdmissionAction, "foundation">,
  AnalyticsCapabilityRuntimeRole
>;

function requiredRoleForAction(
  action: Exclude<AnalyticsAdmissionAction, "foundation">,
): AnalyticsCapabilityRuntimeRole {
  return ACTION_ROLE[action];
}

export async function lockAnalyticsAdmission(input: {
  readonly transaction: PrismaTypes.TransactionClient;
  readonly runtimeLeaseId: string;
  readonly expectedBackend: AnalyticsBackend;
  readonly expectedDeploymentGeneration: bigint;
  readonly capability?: AnalyticsCapabilityName;
  readonly action: AnalyticsAdmissionAction;
  readonly expectedCapabilityActivationGeneration?: bigint;
  readonly expectedCapabilityContractVersion?: number;
  readonly requiredContract?: AnalyticsContractVersionRequirement;
  readonly now?: Date;
}): Promise<AnalyticsAdmissionStamp> {
  const hasExpectedActivationGeneration =
    input.expectedCapabilityActivationGeneration !== undefined;
  const hasExpectedContractVersion =
    input.expectedCapabilityContractVersion !== undefined;
  if (
    !input.runtimeLeaseId ||
    input.expectedDeploymentGeneration < 1n ||
    (input.action === "foundation") !== (input.capability === undefined) ||
    hasExpectedActivationGeneration !== hasExpectedContractVersion ||
    ((input.action === "claimExisting" || input.action === "recovery") &&
      input.expectedBackend === "doris" &&
      !hasExpectedActivationGeneration) ||
    (input.expectedBackend === "clickhouse" &&
      hasExpectedActivationGeneration) ||
    ((input.action === "externalProducer" ||
      input.action === "internalCapture" ||
      input.action === "internalBootstrap") &&
      hasExpectedActivationGeneration) ||
    (input.requiredContract !== undefined &&
      (input.requiredContract.schemaVersion < 1 ||
        !Number.isSafeInteger(input.requiredContract.schemaVersion) ||
        canonicalizerVersionNumber(
          input.requiredContract.canonicalizerVersion,
        ) === null))
  ) {
    throw new TypeError("Invalid analytics admission request");
  }
  const expectedBackend = toPrismaAnalyticsBackend(input.expectedBackend);

  await acquireAnalyticsDeploymentSharedLock(input.transaction);
  const marker = await lockAnalyticsBackendDeploymentState(
    input.transaction,
    "SHARE",
  );
  if (!marker) throw new Error("Analytics backend adoption is required");
  if (
    marker.backend !== expectedBackend ||
    marker.generation !== input.expectedDeploymentGeneration
  ) {
    throw new Error("Analytics backend deployment generation changed");
  }

  let activation:
    | Awaited<
        ReturnType<
          typeof input.transaction.analyticsCapabilityActivation.findUnique
        >
      >
    | undefined;
  if (input.capability && marker.backend === "DORIS") {
    const capability = toPrismaAnalyticsCapability(input.capability);
    await input.transaction.$queryRaw(
      Prisma.sql`SELECT capability FROM analytics_capability_activations WHERE capability::text = ${input.capability} FOR SHARE`,
    );
    activation =
      await input.transaction.analyticsCapabilityActivation.findUnique({
        where: { capability },
      });
    if (
      !activation ||
      activation.backend !== marker.backend ||
      activation.deploymentGeneration !== marker.generation
    ) {
      throw new Error(
        "Analytics capability activation does not match deployment",
      );
    }
    const actionAllowed =
      (input.action === "externalProducer" && activation.status === "ACTIVE") ||
      (input.action === "internalCapture" &&
        ((activation.status === "DARK" && activation.captureEnabled) ||
          activation.status === "ACTIVE" ||
          (activation.status === "DRAINING" &&
            (activation.captureEnabled || activation.captureRequired)))) ||
      (input.action === "internalBootstrap" &&
        (activation.status === "DARK" || activation.status === "ACTIVE")) ||
      ((input.action === "claimExisting" || input.action === "recovery") &&
        (activation.status === "ACTIVE" || activation.status === "DRAINING"));
    if (!actionAllowed) {
      throw new Error("Analytics capability is not active for this action");
    }
    if (
      hasExpectedActivationGeneration &&
      (activation.generation !== input.expectedCapabilityActivationGeneration ||
        activation.contractVersion !== input.expectedCapabilityContractVersion)
    ) {
      throw new Error("Analytics capability provenance generation changed");
    }
  }

  // deployment transaction lock 已阻止 backend/generation 在本事务中切换。
  // runtime row 不能再加 FOR SHARE：长 IO 会持续持有本事务，而 heartbeat
  // 必须并发更新同一 row 才能维持 lease；claim/capability 各自由自己的 row fence 保护。
  const lease = await input.transaction.analyticsRuntimeLease.findUnique({
    where: { id: input.runtimeLeaseId },
    include: { capabilityContracts: true },
  });
  const now = input.now ?? (await databaseClock(input.transaction));
  if (
    !lease ||
    lease.state !== "ACTIVE" ||
    lease.leaseExpiresAt <= now ||
    lease.supersededAt !== null ||
    lease.backend !== marker.backend ||
    lease.deploymentGeneration !== marker.generation ||
    lease.workloadEpochFingerprint !== marker.workloadEpochFingerprint ||
    !marker.queueNamespaceFingerprint ||
    lease.queueNamespaceFingerprint !== marker.queueNamespaceFingerprint ||
    lease.foundationContractVersion !== marker.foundationContractVersion
  ) {
    throw new Error("Analytics runtime lease is not admitted");
  }
  if (
    input.requiredContract &&
    !runtimeLeaseAcceptsAnalyticsContract(lease, input.requiredContract)
  ) {
    throw new Error("Analytics runtime lease does not accept work contract");
  }

  if (input.capability && activation) {
    const capability = toPrismaAnalyticsCapability(input.capability);
    const contract = lease.capabilityContracts.find(
      (item) => item.capability === capability,
    );
    if (input.action === "foundation") {
      throw new Error("Foundation admission cannot target a capability");
    }
    const requiredRole = toPrismaAnalyticsRuntimeRole(
      requiredRoleForAction(input.action),
    );
    if (
      !contract ||
      contract.supportedContractVersion < activation.minimumRuntimeContract ||
      !contract.installedRoles.includes(requiredRole)
    ) {
      throw new Error(
        "Analytics runtime lease lacks the required capability role",
      );
    }
  }

  return {
    analyticsBackend: marker.backend,
    deploymentGeneration: marker.generation,
    workloadEpochFingerprint: marker.workloadEpochFingerprint,
    runtimeContractVersion: marker.foundationContractVersion,
    admittingRuntimeLeaseId: lease.id,
    admittedAt: now,
    ...(activation
      ? {
          capabilityActivationGeneration: activation.generation,
          capabilityContractVersion: activation.contractVersion,
        }
      : {}),
  };
}

/**
 * Serializes a VISIBLE-side-effect hook with capability activation without
 * making disabled/DARK-before-census capabilities block core ingestion.
 */
export async function lockAnalyticsCapabilityCaptureIfEnabled(input: {
  readonly transaction: PrismaTypes.TransactionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext;
  readonly capability: AnalyticsCapabilityName;
  readonly now?: Date;
}): Promise<{
  readonly admission: AnalyticsAdmissionStamp & {
    readonly capabilityActivationGeneration: bigint;
    readonly capabilityContractVersion: number;
  };
  readonly activationStatus: "DARK" | "ACTIVE" | "DRAINING";
  readonly captureExpiresAt: Date | null;
  readonly captureRowBudget: number | null;
  readonly captureRows: bigint;
} | null> {
  if (
    !input.admissionContext.runtimeLeaseId ||
    input.admissionContext.backend !== "doris" ||
    input.admissionContext.deploymentGeneration < 1n
  ) {
    throw new TypeError("Invalid analytics capability capture context");
  }

  await acquireAnalyticsDeploymentSharedLock(input.transaction);
  const marker = await lockAnalyticsBackendDeploymentState(
    input.transaction,
    "SHARE",
  );
  if (
    !marker ||
    marker.backend !== "DORIS" ||
    marker.generation !== input.admissionContext.deploymentGeneration
  ) {
    throw new Error("Analytics capability capture deployment changed");
  }

  const capability = toPrismaAnalyticsCapability(input.capability);
  await input.transaction.$queryRaw(
    Prisma.sql`SELECT capability FROM analytics_capability_activations WHERE capability::text = ${input.capability} FOR UPDATE`,
  );
  const activation =
    await input.transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability },
    });
  const captureEnabled =
    (activation.status === "DARK" && activation.captureEnabled) ||
    activation.status === "ACTIVE" ||
    (activation.status === "DRAINING" &&
      (activation.captureEnabled || activation.captureRequired));
  if (!captureEnabled) return null;
  if (activation.status === "DISABLED") {
    throw new Error("Disabled analytics capability cannot capture work");
  }

  const admission = await lockAnalyticsAdmission({
    transaction: input.transaction,
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend: input.admissionContext.backend,
    expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
    capability: input.capability,
    action: "internalCapture",
    now: input.now,
  });
  if (
    admission.capabilityActivationGeneration === undefined ||
    admission.capabilityContractVersion === undefined
  ) {
    throw new Error("Analytics capability capture provenance is incomplete");
  }
  return {
    admission: {
      ...admission,
      capabilityActivationGeneration: admission.capabilityActivationGeneration,
      capabilityContractVersion: admission.capabilityContractVersion,
    },
    activationStatus: activation.status,
    captureExpiresAt: activation.captureExpiresAt,
    captureRowBudget: activation.captureRowBudget,
    captureRows: activation.captureRows,
  };
}
