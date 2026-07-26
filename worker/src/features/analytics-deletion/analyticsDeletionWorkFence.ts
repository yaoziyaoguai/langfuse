import type { AnalyticsDeletionOperation, PrismaClient } from "@prisma/client";
import { UnrecoverableError } from "bullmq";
import {
  analyticsDurableProvenanceFromRecord,
  analyticsDurableProvenanceMatches,
  createAnalyticsBackendClaimLease,
  deserializeAnalyticsDurableProvenance,
  lockAnalyticsAdmission,
  lockAnalyticsBackendClaimLeaseForIo,
  lockLegacyAnalyticsAdmission,
  releaseAnalyticsBackendClaimLease,
  type AnalyticsBackend,
  type AnalyticsDurableProvenance,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";

type DeletionProvenanceRecord = Pick<
  AnalyticsDeletionOperation,
  | "id"
  | "analyticsBackend"
  | "deploymentGeneration"
  | "workloadEpochFingerprint"
  | "runtimeContractVersion"
  | "producerRuntimeLeaseId"
>;

type FenceDependencies = {
  readonly createClaim: typeof createAnalyticsBackendClaimLease;
  readonly lockClaimForIo: typeof lockAnalyticsBackendClaimLeaseForIo;
  readonly lockAdmission: typeof lockAnalyticsAdmission;
  readonly lockLegacyAdmission: typeof lockLegacyAnalyticsAdmission;
  readonly releaseClaim: typeof releaseAnalyticsBackendClaimLease;
};

const defaultDependencies: FenceDependencies = {
  createClaim: createAnalyticsBackendClaimLease,
  lockClaimForIo: lockAnalyticsBackendClaimLeaseForIo,
  lockAdmission: lockAnalyticsAdmission,
  lockLegacyAdmission: lockLegacyAnalyticsAdmission,
  releaseClaim: releaseAnalyticsBackendClaimLease,
};

const ANALYTICS_DELETION_CLAIM_MS = 30 * 60_000;
const ANALYTICS_DELETION_FENCE_TIMEOUT_MS = 35 * 60_000;

function unrecoverableProvenanceError(): UnrecoverableError {
  return new UnrecoverableError(
    "Analytics durable provenance does not match the queue delivery",
  );
}

type DurableWorkFenceInput = {
  readonly client: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly selectedBackend: AnalyticsBackend;
  readonly claimKind: string;
  readonly resourceIdentity: string;
  readonly run: () => Promise<void>;
  readonly dependencies?: FenceDependencies;
};

async function withValidatedAnalyticsDurableWorkFence(
  input: DurableWorkFenceInput & {
    readonly provenance: AnalyticsDurableProvenance | null;
  },
): Promise<void> {
  const dependencies = input.dependencies ?? defaultDependencies;
  if (!input.provenance) {
    if (input.selectedBackend !== "clickhouse") {
      throw unrecoverableProvenanceError();
    }
    if (input.admissionContext) {
      if (input.admissionContext.backend !== "clickhouse") {
        throw unrecoverableProvenanceError();
      }
      await input.client.$transaction(
        async (transaction) => {
          try {
            await dependencies.lockAdmission({
              transaction,
              runtimeLeaseId: input.admissionContext!.runtimeLeaseId,
              expectedBackend: "clickhouse",
              expectedDeploymentGeneration:
                input.admissionContext!.deploymentGeneration,
              action: "foundation",
            });
          } catch {
            throw unrecoverableProvenanceError();
          }
          await input.run();
        },
        { timeout: ANALYTICS_DELETION_FENCE_TIMEOUT_MS },
      );
      return;
    }
    await input.client.$transaction(
      async (transaction) => {
        try {
          await dependencies.lockLegacyAdmission(transaction);
        } catch {
          throw unrecoverableProvenanceError();
        }
        await input.run();
      },
      { timeout: ANALYTICS_DELETION_FENCE_TIMEOUT_MS },
    );
    return;
  }

  const expectedBackend: AnalyticsBackend =
    input.provenance.analyticsBackend === "DORIS" ? "doris" : "clickhouse";
  if (
    input.selectedBackend !== expectedBackend ||
    !input.admissionContext ||
    input.admissionContext.backend !== expectedBackend ||
    input.admissionContext.deploymentGeneration !==
      input.provenance.deploymentGeneration
  ) {
    throw unrecoverableProvenanceError();
  }
  const fence = {
    runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    expectedBackend,
    expectedDeploymentGeneration: input.provenance.deploymentGeneration,
    expectedWorkloadEpochFingerprint: input.provenance.workloadEpochFingerprint,
    expectedRuntimeContractVersion: input.provenance.runtimeContractVersion,
    action: "foundation" as const,
  };
  const claim = await dependencies.createClaim({
    client: input.client,
    ...fence,
    claimKind: input.claimKind,
    resourceIdentity: input.resourceIdentity,
    leaseMs: ANALYTICS_DELETION_CLAIM_MS,
  });
  if (!claim) {
    throw new Error("Analytics durable work is already claimed");
  }
  try {
    await input.client.$transaction(
      async (transaction) => {
        await dependencies.lockClaimForIo({
          transaction,
          claimLeaseId: claim.id,
          fence,
        });
        await input.run();
      },
      { timeout: ANALYTICS_DELETION_FENCE_TIMEOUT_MS },
    );
  } finally {
    await dependencies.releaseClaim({
      client: input.client,
      claimLeaseId: claim.id,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
    });
  }
}

export async function withAnalyticsDurableWorkFence(
  input: DurableWorkFenceInput & {
    readonly serializedProvenance: unknown;
  },
): Promise<void> {
  let provenance: AnalyticsDurableProvenance | null;
  try {
    provenance =
      input.serializedProvenance === undefined
        ? null
        : deserializeAnalyticsDurableProvenance(input.serializedProvenance);
  } catch {
    throw unrecoverableProvenanceError();
  }
  await withValidatedAnalyticsDurableWorkFence({ ...input, provenance });
}

export async function withAnalyticsDeletionWorkFence(input: {
  readonly client: PrismaClient;
  readonly operation: DeletionProvenanceRecord | null;
  readonly serializedProvenance: unknown;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly selectedBackend: AnalyticsBackend;
  readonly claimKind: string;
  readonly run: () => Promise<void>;
  readonly dependencies?: FenceDependencies;
}): Promise<void> {
  let authoritative = null;
  let delivered = null;
  try {
    authoritative = input.operation
      ? analyticsDurableProvenanceFromRecord(input.operation)
      : null;
    delivered =
      input.serializedProvenance === undefined
        ? null
        : deserializeAnalyticsDurableProvenance(input.serializedProvenance);
  } catch {
    throw unrecoverableProvenanceError();
  }
  if (
    (authoritative === null) !== (delivered === null) ||
    (authoritative !== null &&
      delivered !== null &&
      !analyticsDurableProvenanceMatches(authoritative, delivered))
  ) {
    throw unrecoverableProvenanceError();
  }
  await withValidatedAnalyticsDurableWorkFence({
    client: input.client,
    provenance: authoritative,
    admissionContext: input.admissionContext,
    selectedBackend: input.selectedBackend,
    claimKind: input.claimKind,
    resourceIdentity: input.operation?.id ?? "",
    run: input.run,
    dependencies: input.dependencies,
  });
}
