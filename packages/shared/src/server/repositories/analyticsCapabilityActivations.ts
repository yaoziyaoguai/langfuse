import { Prisma } from "@prisma/client";
import type {
  AnalyticsCapabilityActivation,
  PrismaClient,
} from "@prisma/client";

import { prisma } from "../../db";
import {
  ANALYTICS_CAPABILITY_CATALOG,
  ANALYTICS_CAPABILITY_NAMES,
  type AnalyticsCapabilityName,
} from "../analytics-persistence/analyticsCapabilities";
import {
  fromPrismaAnalyticsRuntimeComponent,
  toPrismaAnalyticsCapability,
  toPrismaAnalyticsRuntimeRole,
} from "../analytics-persistence/analyticsBackendMapping";
import {
  acquireAnalyticsDeploymentSharedLock,
  lockAnalyticsBackendDeploymentState,
} from "./analyticsBackendDeployment";
import { prepareAnalyticsEvaluationReplayHandoff } from "./analyticsEvaluationCapability";
import { prepareDorisAnalyticsIntegrationDarkCapture } from "./analyticsIntegrationDeliveries";

const SHA256_HEX = /^[a-f0-9]{64}$/;
const RUNTIME_LEASE_GRACE_MS = 60_000;
const EVALUATION_DARK_CAPTURE_WINDOW_MS = 24 * 60 * 60_000;
const EVALUATION_DARK_CAPTURE_ROW_BUDGET = 250_000;
const INTEGRATION_DARK_CAPTURE_WINDOW_MS = 24 * 60 * 60_000;
const INTEGRATION_DARK_CAPTURE_ROW_BUDGET = 1_000_000;
const CAPABILITY_TRANSITION_LOCK_KEY = 181_865_275_000_008n;

async function acquireCapabilityTransitionLock(
  transaction: Prisma.TransactionClient,
): Promise<void> {
  await transaction.$queryRaw(
    Prisma.sql`SELECT pg_advisory_xact_lock(${CAPABILITY_TRANSITION_LOCK_KEY})::text AS locked`,
  );
}

async function assertCapabilityDependenciesActive(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly capability: AnalyticsCapabilityName;
  readonly backend: "DORIS";
  readonly deploymentGeneration: bigint;
}): Promise<void> {
  for (const dependency of ANALYTICS_CAPABILITY_CATALOG[input.capability]
    .dependencies) {
    const activation =
      await input.transaction.analyticsCapabilityActivation.findUnique({
        where: { capability: toPrismaAnalyticsCapability(dependency) },
      });
    if (
      !activation ||
      activation.backend !== input.backend ||
      activation.deploymentGeneration !== input.deploymentGeneration ||
      activation.status !== "ACTIVE"
    ) {
      throw new Error("Analytics capability dependency is not active");
    }
  }
}

async function assertCapabilityHasNoEnabledDependents(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly capability: AnalyticsCapabilityName;
}): Promise<void> {
  const dependents = ANALYTICS_CAPABILITY_NAMES.filter((candidate) =>
    ANALYTICS_CAPABILITY_CATALOG[candidate].dependencies.some(
      (dependency) => dependency === input.capability,
    ),
  );
  if (dependents.length === 0) return;
  const activeDependent =
    await input.transaction.analyticsCapabilityActivation.findFirst({
      where: {
        capability: {
          in: dependents.map(toPrismaAnalyticsCapability),
        },
        status: { not: "DISABLED" },
      },
      select: { capability: true },
    });
  if (activeDependent) {
    throw new Error("Analytics capability still has an enabled dependent");
  }
}

async function lockCapabilityActivation(
  transaction: Prisma.TransactionClient,
  capability: AnalyticsCapabilityName,
): Promise<AnalyticsCapabilityActivation> {
  const prismaCapability = toPrismaAnalyticsCapability(capability);
  await transaction.$queryRaw(
    Prisma.sql`SELECT capability FROM analytics_capability_activations WHERE capability::text = ${capability} FOR UPDATE`,
  );
  return transaction.analyticsCapabilityActivation.findUniqueOrThrow({
    where: { capability: prismaCapability },
  });
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

function capabilitySupportsCapture(
  capability: AnalyticsCapabilityName,
): boolean {
  return Object.values(
    ANALYTICS_CAPABILITY_CATALOG[capability].components,
  ).some((roles) => roles.includes("capture"));
}

function validateRuntimeInventory(instanceIds: readonly string[]): void {
  if (
    instanceIds.length === 0 ||
    new Set(instanceIds).size !== instanceIds.length ||
    instanceIds.some((instanceId) => !instanceId)
  ) {
    throw new TypeError("Invalid analytics capability activation census");
  }
}

async function assertRuntimeCapabilityCensus(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly marker: NonNullable<
    Awaited<ReturnType<typeof lockAnalyticsBackendDeploymentState>>
  >;
  readonly activation: AnalyticsCapabilityActivation;
  readonly capability: AnalyticsCapabilityName;
  readonly expectedRuntimeInstanceIds: readonly string[];
  readonly now: Date;
}): Promise<void> {
  const leaseCutoff = new Date(input.now.getTime() - RUNTIME_LEASE_GRACE_MS);
  await input.transaction.$queryRaw(
    Prisma.sql`
      SELECT id
      FROM analytics_runtime_leases
      WHERE component IN ('WEB', 'WORKER')
        AND state <> 'QUIESCED'
        AND lease_expires_at > ${leaseCutoff}
      ORDER BY instance_id, id
      FOR SHARE
    `,
  );
  const recentLeases = await input.transaction.analyticsRuntimeLease.findMany({
    where: {
      component: { in: ["WEB", "WORKER"] },
      state: { not: "QUIESCED" },
      leaseExpiresAt: { gt: leaseCutoff },
    },
    include: { capabilityContracts: true },
    orderBy: [{ instanceId: "asc" }, { id: "asc" }],
  });
  if (
    recentLeases.some(
      (lease) =>
        lease.leaseExpiresAt <= input.now || lease.supersededAt !== null,
    )
  ) {
    throw new Error(
      "Runtime capability census is blocked by a recently expired lease grace window",
    );
  }
  const liveLeases = recentLeases.filter(
    (lease) => lease.leaseExpiresAt > input.now && lease.supersededAt === null,
  );
  const expectedIds = [...input.expectedRuntimeInstanceIds].sort();
  const observedIds = liveLeases.map(({ instanceId }) => instanceId).sort();
  if (JSON.stringify(expectedIds) !== JSON.stringify(observedIds)) {
    throw new Error("Runtime capability census does not match expected fleet");
  }
  if (
    liveLeases.some(
      (lease) =>
        lease.state !== "ACTIVE" ||
        lease.backend !== input.marker.backend ||
        lease.deploymentGeneration !== input.marker.generation ||
        lease.workloadEpochFingerprint !==
          input.marker.workloadEpochFingerprint ||
        lease.foundationContractVersion !==
          input.marker.foundationContractVersion,
    )
  ) {
    throw new Error("Runtime capability census contains incompatible leases");
  }

  const catalogEntry = ANALYTICS_CAPABILITY_CATALOG[input.capability];
  const observedComponents = new Set(
    liveLeases.map(({ component }) =>
      fromPrismaAnalyticsRuntimeComponent(component),
    ),
  );
  for (const component of ["web", "worker"] as const) {
    if (
      catalogEntry.components[component].length > 0 &&
      !observedComponents.has(component)
    ) {
      throw new Error("Runtime capability census is missing a component");
    }
  }
  for (const lease of liveLeases) {
    const component = fromPrismaAnalyticsRuntimeComponent(lease.component);
    if (component !== "web" && component !== "worker") {
      throw new Error(
        "Runtime capability census contains an unsupported component",
      );
    }
    const contract = lease.capabilityContracts.find(
      ({ capability }) => capability === input.activation.capability,
    );
    const requiredRoles = catalogEntry.components[component].map(
      toPrismaAnalyticsRuntimeRole,
    );
    if (
      !contract ||
      contract.supportedContractVersion <
        input.activation.minimumRuntimeContract ||
      requiredRoles.some((role) => !contract.installedRoles.includes(role))
    ) {
      throw new Error("Runtime capability census contract is incompatible");
    }
  }
}

export async function beginAnalyticsCapabilityDark(input: {
  readonly client?: PrismaClient;
  readonly capability: AnalyticsCapabilityName;
  readonly expectedGeneration: bigint;
  readonly contractVersion: number;
  readonly minimumRuntimeContract: number;
  readonly now?: Date;
}): Promise<AnalyticsCapabilityActivation> {
  if (
    input.expectedGeneration < 1n ||
    !Number.isSafeInteger(input.contractVersion) ||
    input.contractVersion < 1 ||
    !Number.isSafeInteger(input.minimumRuntimeContract) ||
    input.minimumRuntimeContract < 1
  ) {
    throw new TypeError("Invalid analytics capability dark activation");
  }
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireCapabilityTransitionLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    if (!marker || marker.backend !== "DORIS") {
      throw new Error(
        "Doris capability activation requires a Doris deployment",
      );
    }
    const current = await lockCapabilityActivation(
      transaction,
      input.capability,
    );
    if (
      current.status !== "DISABLED" ||
      current.generation !== input.expectedGeneration
    ) {
      throw new Error("Analytics capability dark activation CAS failed");
    }
    return transaction.analyticsCapabilityActivation.update({
      where: { capability: current.capability },
      data: {
        backend: "DORIS",
        deploymentGeneration: marker.generation,
        generation: { increment: 1 },
        contractVersion: input.contractVersion,
        minimumRuntimeContract: input.minimumRuntimeContract,
        status: "DARK",
        captureEnabled: false,
        captureStartedAt: null,
        captureExpiresAt: null,
        captureRowBudget: null,
        captureRows: 0n,
        bootstrapCompletedGeneration: null,
        bootstrapEvidenceDigest: null,
        bootstrapCompletedAt: null,
        activatedAt: null,
        drainingAt: null,
        disabledAt: null,
        updatedAt: now,
      },
    });
  });
}

export async function enableAnalyticsCapabilityDarkCapture(input: {
  readonly client?: PrismaClient;
  readonly capability: AnalyticsCapabilityName;
  readonly expectedDeploymentGeneration: bigint;
  readonly expectedActivationGeneration: bigint;
  readonly expectedRuntimeInstanceIds: readonly string[];
  readonly now?: Date;
}): Promise<AnalyticsCapabilityActivation> {
  validateRuntimeInventory(input.expectedRuntimeInstanceIds);
  if (!capabilitySupportsCapture(input.capability)) {
    throw new Error("Analytics capability does not define a capture role");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireCapabilityTransitionLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    if (
      !marker ||
      marker.backend !== "DORIS" ||
      marker.generation !== input.expectedDeploymentGeneration
    ) {
      throw new Error("Analytics capability deployment generation changed");
    }
    const activation = await lockCapabilityActivation(
      transaction,
      input.capability,
    );
    if (
      activation.status !== "DARK" ||
      activation.deploymentGeneration !== marker.generation ||
      activation.generation !== input.expectedActivationGeneration ||
      activation.captureEnabled
    ) {
      throw new Error("Analytics capability dark capture CAS failed");
    }
    const now = input.now ?? (await databaseClock(transaction));
    const captureWindow =
      input.capability === "evaluations" ||
      input.capability === "analyticsIntegrations"
        ? {
            captureStartedAt: now,
            captureExpiresAt: new Date(
              now.getTime() +
                (input.capability === "evaluations"
                  ? EVALUATION_DARK_CAPTURE_WINDOW_MS
                  : INTEGRATION_DARK_CAPTURE_WINDOW_MS),
            ),
            captureRowBudget:
              input.capability === "evaluations"
                ? EVALUATION_DARK_CAPTURE_ROW_BUDGET
                : INTEGRATION_DARK_CAPTURE_ROW_BUDGET,
            captureRows: 0n,
          }
        : {};
    await assertRuntimeCapabilityCensus({
      transaction,
      marker,
      activation,
      capability: input.capability,
      expectedRuntimeInstanceIds: input.expectedRuntimeInstanceIds,
      now,
    });
    const cutoffState = activation.cutoffState;
    const isEvaluationReplayCutoff =
      input.capability === "evaluations" &&
      activation.rescanRequired &&
      typeof cutoffState === "object" &&
      cutoffState !== null &&
      !Array.isArray(cutoffState) &&
      "kind" in cutoffState &&
      cutoffState.kind === "evaluation_operation_cutoff";
    const replayHandoff = isEvaluationReplayCutoff
      ? await prepareAnalyticsEvaluationReplayHandoff({
          transaction,
          activation,
          now,
        })
      : null;
    if (input.capability === "analyticsIntegrations") {
      await prepareDorisAnalyticsIntegrationDarkCapture({
        transaction,
        now,
      });
    }
    return transaction.analyticsCapabilityActivation.update({
      where: { capability: activation.capability },
      data: {
        captureEnabled: true,
        ...captureWindow,
        ...(replayHandoff
          ? {
              cutoffState: replayHandoff.cutoffState,
              cutoffDigest: replayHandoff.cutoffDigest,
            }
          : {}),
        updatedAt: now,
      },
    });
  });
}

export async function completeAnalyticsCapabilityBootstrap(input: {
  readonly client?: PrismaClient;
  readonly capability: AnalyticsCapabilityName;
  readonly expectedDeploymentGeneration: bigint;
  readonly expectedActivationGeneration: bigint;
  readonly expectedCutoffDigest?: string;
  readonly verifyDurableBootstrap: (
    transaction: Prisma.TransactionClient,
  ) => Promise<{ readonly bootstrapEvidenceDigest: string }>;
  readonly now?: Date;
}): Promise<AnalyticsCapabilityActivation> {
  if (
    input.expectedDeploymentGeneration < 1n ||
    input.expectedActivationGeneration < 1n ||
    (input.expectedCutoffDigest !== undefined &&
      !SHA256_HEX.test(input.expectedCutoffDigest))
  ) {
    throw new TypeError("Invalid analytics capability bootstrap completion");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireCapabilityTransitionLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    if (
      !marker ||
      marker.backend !== "DORIS" ||
      marker.generation !== input.expectedDeploymentGeneration
    ) {
      throw new Error("Analytics capability deployment generation changed");
    }
    const activation = await lockCapabilityActivation(
      transaction,
      input.capability,
    );
    if (
      activation.status !== "DARK" ||
      activation.deploymentGeneration !== marker.generation ||
      activation.generation !== input.expectedActivationGeneration
    ) {
      throw new Error("Analytics capability bootstrap CAS failed");
    }
    if (
      activation.bootstrapCompletedGeneration === activation.generation &&
      activation.bootstrapEvidenceDigest
    ) {
      const retryEvidence = await input.verifyDurableBootstrap(transaction);
      if (
        retryEvidence.bootstrapEvidenceDigest !==
        activation.bootstrapEvidenceDigest
      ) {
        throw new Error("Analytics capability bootstrap evidence changed");
      }
      return activation;
    }
    if (
      activation.rescanRequired !==
        (input.expectedCutoffDigest !== undefined) ||
      (activation.rescanRequired &&
        activation.cutoffDigest !== input.expectedCutoffDigest)
    ) {
      throw new Error("Analytics capability replay cutoff changed");
    }
    if (activation.captureRequired && !activation.captureEnabled) {
      throw new Error("Analytics capability capture handoff is incomplete");
    }
    const evidence = await input.verifyDurableBootstrap(transaction);
    if (!SHA256_HEX.test(evidence.bootstrapEvidenceDigest)) {
      throw new TypeError("Bootstrap evidence must be a SHA-256 digest");
    }
    const now = input.now ?? (await databaseClock(transaction));
    return transaction.analyticsCapabilityActivation.update({
      where: { capability: activation.capability },
      data: {
        captureRequired: false,
        rescanRequired: false,
        cutoffState: Prisma.DbNull,
        cutoffActivationGeneration: null,
        cutoffDigest: null,
        bootstrapCompletedGeneration: activation.generation,
        bootstrapEvidenceDigest: evidence.bootstrapEvidenceDigest,
        bootstrapCompletedAt: now,
        updatedAt: now,
      },
    });
  });
}

export async function activateAnalyticsCapability(input: {
  readonly client?: PrismaClient;
  readonly capability: AnalyticsCapabilityName;
  readonly expectedDeploymentGeneration: bigint;
  readonly expectedActivationGeneration: bigint;
  readonly expectedRuntimeInstanceIds: readonly string[];
  readonly expectedBootstrapEvidenceDigest: string;
  readonly now?: Date;
}): Promise<AnalyticsCapabilityActivation> {
  if (
    input.expectedDeploymentGeneration < 1n ||
    input.expectedActivationGeneration < 1n ||
    !SHA256_HEX.test(input.expectedBootstrapEvidenceDigest)
  ) {
    throw new TypeError("Invalid analytics capability activation census");
  }
  validateRuntimeInventory(input.expectedRuntimeInstanceIds);
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireCapabilityTransitionLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    if (
      !marker ||
      marker.backend !== "DORIS" ||
      marker.generation !== input.expectedDeploymentGeneration
    ) {
      throw new Error("Analytics capability deployment generation changed");
    }
    const activation = await lockCapabilityActivation(
      transaction,
      input.capability,
    );
    if (
      activation.status !== "DARK" ||
      activation.backend !== marker.backend ||
      activation.deploymentGeneration !== marker.generation ||
      activation.generation !== input.expectedActivationGeneration
    ) {
      throw new Error("Analytics capability activation CAS failed");
    }
    if (
      activation.bootstrapCompletedGeneration !== activation.generation ||
      activation.bootstrapEvidenceDigest !==
        input.expectedBootstrapEvidenceDigest ||
      activation.rescanRequired ||
      activation.captureRequired ||
      activation.cutoffState !== null ||
      activation.cutoffActivationGeneration !== null ||
      activation.cutoffDigest !== null
    ) {
      throw new Error("Analytics capability bootstrap is not durably complete");
    }
    if (
      capabilitySupportsCapture(input.capability) &&
      !activation.captureEnabled
    ) {
      throw new Error("Analytics capability capture is not enabled");
    }
    const now = input.now ?? (await databaseClock(transaction));
    await assertRuntimeCapabilityCensus({
      transaction,
      marker,
      activation,
      capability: input.capability,
      expectedRuntimeInstanceIds: input.expectedRuntimeInstanceIds,
      now,
    });
    await assertCapabilityDependenciesActive({
      transaction,
      capability: input.capability,
      backend: marker.backend,
      deploymentGeneration: marker.generation,
    });

    if (input.capability === "evaluations") {
      await transaction.analyticsEvaluationDispatch.updateMany({
        where: {
          capabilityActivationGeneration: activation.generation,
          deploymentGeneration: marker.generation,
          status: "SUSPENDED",
        },
        data: {
          status: "PENDING",
          nextAttemptAt: now,
          failureCode: null,
        },
      });
    }
    if (input.capability === "analyticsIntegrations") {
      const promoted = await transaction.analyticsIntegrationState.updateMany({
        where: { status: "BOOTSTRAPPING_DARK" },
        data: { status: "BOOTSTRAPPING_ACTIVE" },
      });
      const enabledCount =
        (await transaction.posthogIntegration.count({
          where: { enabled: true },
        })) +
        (await transaction.mixpanelIntegration.count({
          where: { enabled: true },
        })) +
        (await transaction.blobStorageIntegration.count({
          where: { enabled: true },
        }));
      if (promoted.count !== enabledCount) {
        throw new Error(
          "Analytics integration activation bootstrap inventory changed",
        );
      }
    }
    const updated = await transaction.analyticsCapabilityActivation.updateMany({
      where: {
        capability: activation.capability,
        backend: marker.backend,
        deploymentGeneration: marker.generation,
        generation: activation.generation,
        status: "DARK",
      },
      data: {
        status: "ACTIVE",
        captureStartedAt: null,
        captureExpiresAt: null,
        captureRowBudget: null,
        activatedAt: now,
        updatedAt: now,
      },
    });
    if (updated.count !== 1) {
      throw new Error("Analytics capability activation CAS failed");
    }
    return transaction.analyticsCapabilityActivation.findUniqueOrThrow({
      where: { capability: activation.capability },
    });
  });
}

export async function beginAnalyticsCapabilityDrain(input: {
  readonly client?: PrismaClient;
  readonly capability: AnalyticsCapabilityName;
  readonly expectedDeploymentGeneration: bigint;
  readonly expectedActivationGeneration: bigint;
  readonly now?: Date;
}): Promise<AnalyticsCapabilityActivation> {
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireCapabilityTransitionLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    if (
      !marker ||
      marker.backend !== "DORIS" ||
      marker.generation !== input.expectedDeploymentGeneration
    ) {
      throw new Error("Analytics capability deployment generation changed");
    }
    const activation = await lockCapabilityActivation(
      transaction,
      input.capability,
    );
    if (
      activation.status !== "ACTIVE" ||
      activation.deploymentGeneration !== marker.generation ||
      activation.generation !== input.expectedActivationGeneration
    ) {
      throw new Error("Analytics capability drain CAS failed");
    }
    await assertCapabilityHasNoEnabledDependents({
      transaction,
      capability: input.capability,
    });
    const now = input.now ?? (await databaseClock(transaction));
    if (input.capability === "analyticsIntegrations") {
      await transaction.analyticsIntegrationState.updateMany({
        where: {
          status: {
            in: [
              "BOOTSTRAPPING_ACTIVE",
              "ACTIVE",
              "PAUSED_BACKLOG",
              "RESCANNING",
            ],
          },
        },
        data: { status: "DRAINING" },
      });
    }
    return transaction.analyticsCapabilityActivation.update({
      where: { capability: activation.capability },
      data: {
        status: "DRAINING",
        drainingAt: now,
        updatedAt: now,
      },
    });
  });
}

export async function disableAnalyticsCapability(input: {
  readonly client?: PrismaClient;
  readonly capability: AnalyticsCapabilityName;
  readonly expectedDeploymentGeneration: bigint;
  readonly expectedActivationGeneration: bigint;
  readonly captureRequired: boolean;
  readonly rescanRequired: boolean;
  readonly verifyDurableDrain: (
    transaction: Prisma.TransactionClient,
    provenance: {
      readonly capability: AnalyticsCapabilityName;
      readonly deploymentGeneration: bigint;
      readonly activationGeneration: bigint;
      readonly capabilityContractVersion: number;
    },
  ) => Promise<void>;
  readonly sealReplayCutoff?: (
    transaction: Prisma.TransactionClient,
  ) => Promise<{
    readonly cutoffState: Prisma.InputJsonValue;
    readonly cutoffDigest: string;
  }>;
  readonly now?: Date;
}): Promise<AnalyticsCapabilityActivation> {
  if (input.captureRequired && !input.rescanRequired) {
    throw new Error("Disabled capture requires a replay cutoff");
  }
  if (
    input.rescanRequired !== (input.sealReplayCutoff !== undefined) ||
    (input.captureRequired && !capabilitySupportsCapture(input.capability))
  ) {
    throw new TypeError("Invalid analytics capability replay cutoff request");
  }
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    await acquireAnalyticsDeploymentSharedLock(transaction);
    await acquireCapabilityTransitionLock(transaction);
    const marker = await lockAnalyticsBackendDeploymentState(
      transaction,
      "SHARE",
    );
    if (
      !marker ||
      marker.backend !== "DORIS" ||
      marker.generation !== input.expectedDeploymentGeneration
    ) {
      throw new Error("Analytics capability deployment generation changed");
    }
    const activation = await lockCapabilityActivation(
      transaction,
      input.capability,
    );
    if (
      activation.status !== "DRAINING" ||
      activation.deploymentGeneration !== marker.generation ||
      activation.generation !== input.expectedActivationGeneration
    ) {
      throw new Error("Analytics capability disable CAS failed");
    }
    if (input.captureRequired && !activation.captureEnabled) {
      throw new Error("Analytics capability capture handoff is incomplete");
    }
    await input.verifyDurableDrain(transaction, {
      capability: input.capability,
      deploymentGeneration: activation.deploymentGeneration,
      activationGeneration: activation.generation,
      capabilityContractVersion: activation.contractVersion,
    });
    const cutoff = input.sealReplayCutoff
      ? await input.sealReplayCutoff(transaction)
      : null;
    if (cutoff && !SHA256_HEX.test(cutoff.cutoffDigest)) {
      throw new TypeError("Replay cutoff must include a SHA-256 digest");
    }
    const now = input.now ?? (await databaseClock(transaction));
    if (input.capability === "analyticsIntegrations") {
      await transaction.analyticsIntegrationState.updateMany({
        where: { status: "DRAINING" },
        data: {
          rescanRequired: input.rescanRequired,
          lastErrorCode: input.rescanRequired
            ? "INTEGRATION_REPLAY_REQUIRED"
            : null,
        },
      });
    }
    return transaction.analyticsCapabilityActivation.update({
      where: { capability: activation.capability },
      data: {
        status: "DISABLED",
        captureEnabled: false,
        captureStartedAt: null,
        captureExpiresAt: null,
        captureRowBudget: null,
        captureRequired: input.captureRequired,
        rescanRequired: input.rescanRequired,
        cutoffState: cutoff?.cutoffState ?? Prisma.DbNull,
        cutoffActivationGeneration: cutoff ? activation.generation : null,
        cutoffDigest: cutoff?.cutoffDigest ?? null,
        bootstrapCompletedGeneration: null,
        bootstrapEvidenceDigest: null,
        bootstrapCompletedAt: null,
        disabledAt: now,
        updatedAt: now,
      },
    });
  });
}
