import { prisma } from "@langfuse/shared/src/db";
import type { Prisma } from "@langfuse/shared/src/db";
import { InternalServerError, NotImplementedError } from "@langfuse/shared";
import {
  createAnalyticsBackendClaimLease,
  lockAnalyticsAdmission,
  lockAnalyticsBackendClaimLeaseForIo,
  lockLegacyAnalyticsAdmission,
  logger,
  renewAnalyticsBackendClaimLease,
  releaseAnalyticsBackendClaimLease,
  serializeAnalyticsDurableProvenance,
  type SerializedAnalyticsDurableProvenance,
} from "@langfuse/shared/src/server";
import type { BatchActionProcessingEventType } from "@langfuse/shared/src/server";

import { getWebAnalyticsDurableWorkState } from "@/src/server/analyticsRuntime";

const ANALYTICS_QUEUE_PUBLISH_CLAIM_MS = 30 * 60_000;
const ANALYTICS_QUEUE_PUBLISH_HEARTBEAT_MS = 60_000;
const ANALYTICS_QUEUE_PUBLISH_FENCE_TIMEOUT_MS = 35 * 60_000;
type AnalyticsBatchActionId = `${BatchActionProcessingEventType["actionId"]}`;

const DORIS_SUPPORTED_BATCH_ACTIONS = new Set<AnalyticsBatchActionId>([
  "trace-add-to-annotation-queue",
  "session-add-to-annotation-queue",
  "observation-add-to-annotation-queue",
  "observation-add-to-dataset",
  "dataset-delete",
]);
const DORIS_EVALUATION_BATCH_ACTIONS = new Set<AnalyticsBatchActionId>([
  "eval-create",
  "observation-run-batched-evaluation",
]);

type AnalyticsQueuePublicationAdmissionInput<T> = {
  readonly claimKind: string;
  readonly resourceIdentity: string;
  readonly supportedBackends: readonly ("clickhouse" | "doris")[];
  readonly unsupportedMessage: string;
  readonly capabilities?: readonly ("evaluations" | "experiments")[];
  readonly requiresManagedDoris?: boolean;
  readonly publish: (guard: AnalyticsQueuePublicationGuard) => Promise<T>;
};

export type AnalyticsQueuePublicationGuard = {
  /** Immutable producer provenance included in managed queue deliveries. */
  readonly durableProvenance?: SerializedAnalyticsDurableProvenance;
  /** Revalidates and extends the managed publication claim before final I/O. */
  readonly assertActive: () => Promise<void>;
  /** Holds the deployment switch fence until the enclosed I/O has settled. */
  readonly withIoFence: <T>(
    execute: (transaction: Prisma.TransactionClient) => Promise<T>,
  ) => Promise<T>;
};

function selectedBackend(
  durableWorkState: ReturnType<typeof getWebAnalyticsDurableWorkState>,
): "clickhouse" | "doris" {
  if (durableWorkState.mode === "UNAVAILABLE") {
    throw new InternalServerError(
      "Analytics queue publication requires runtime admission",
    );
  }
  if (durableWorkState.mode === "LEGACY_COMPATIBILITY") {
    return durableWorkState.backend;
  }
  return durableWorkState.provenance.analyticsBackend === "DORIS"
    ? "doris"
    : "clickhouse";
}

export function assertAnalyticsQueuePublicationSupported(input: {
  readonly supportedBackends: readonly ("clickhouse" | "doris")[];
  readonly unsupportedMessage: string;
}): void {
  const backend = selectedBackend(getWebAnalyticsDurableWorkState());
  if (!input.supportedBackends.includes(backend)) {
    throw new NotImplementedError(input.unsupportedMessage);
  }
}

export async function withAnalyticsQueuePublicationAdmission<T>(
  input: AnalyticsQueuePublicationAdmissionInput<T>,
): Promise<T> {
  const durableWorkState = getWebAnalyticsDurableWorkState();
  const backend = selectedBackend(durableWorkState);
  if (!input.supportedBackends.includes(backend)) {
    throw new NotImplementedError(input.unsupportedMessage);
  }
  if (
    input.requiresManagedDoris &&
    backend === "doris" &&
    durableWorkState.mode !== "MANAGED"
  ) {
    throw new InternalServerError(
      "Doris queue publication requires managed runtime provenance",
    );
  }

  if (durableWorkState.mode === "LEGACY_COMPATIBILITY") {
    const legacyGuard: AnalyticsQueuePublicationGuard = {
      assertActive: () =>
        prisma.$transaction(async (transaction) => {
          await lockLegacyAnalyticsAdmission(transaction);
        }),
      withIoFence: (execute) =>
        prisma.$transaction(
          async (transaction) => {
            await lockLegacyAnalyticsAdmission(transaction);
            return execute(transaction);
          },
          {
            maxWait: 120_000,
            timeout: ANALYTICS_QUEUE_PUBLISH_FENCE_TIMEOUT_MS,
          },
        ),
    };
    return input.publish(legacyGuard);
  }
  if (durableWorkState.mode !== "MANAGED") {
    throw new InternalServerError(
      "Analytics queue publication requires runtime admission",
    );
  }

  const provenance = durableWorkState.provenance;
  const fence = {
    runtimeLeaseId: provenance.producerRuntimeLeaseId,
    expectedBackend: backend,
    expectedDeploymentGeneration: provenance.deploymentGeneration,
    expectedWorkloadEpochFingerprint: provenance.workloadEpochFingerprint,
    expectedRuntimeContractVersion: provenance.runtimeContractVersion,
    action: "foundation" as const,
  };
  const claim = await createAnalyticsBackendClaimLease({
    ...fence,
    claimKind: input.claimKind,
    resourceIdentity: input.resourceIdentity,
    leaseMs: ANALYTICS_QUEUE_PUBLISH_CLAIM_MS,
  });
  if (!claim) {
    throw new InternalServerError(
      "Analytics queue publication is already in progress",
    );
  }

  let stopped = false;
  let renewalFailure: unknown;
  let renewalChain = Promise.resolve();
  const enqueueRenewal = () => {
    renewalChain = renewalChain.then(async () => {
      if (stopped || renewalFailure !== undefined) return;
      try {
        await renewAnalyticsBackendClaimLease({
          claimLeaseId: claim.id,
          fence,
          leaseMs: ANALYTICS_QUEUE_PUBLISH_CLAIM_MS,
        });
      } catch (error) {
        renewalFailure = error;
      }
    });
    return renewalChain;
  };
  const assertRenewed = async () => {
    await enqueueRenewal();
    if (renewalFailure !== undefined) throw renewalFailure;
  };
  const heartbeat = setInterval(() => {
    enqueueRenewal();
  }, ANALYTICS_QUEUE_PUBLISH_HEARTBEAT_MS);
  heartbeat.unref();
  const stopHeartbeat = async () => {
    stopped = true;
    clearInterval(heartbeat);
    await renewalChain;
  };

  const guard: AnalyticsQueuePublicationGuard = {
    durableProvenance: serializeAnalyticsDurableProvenance(provenance),
    assertActive: assertRenewed,
    withIoFence: async (execute) => {
      await assertRenewed();
      return prisma.$transaction(
        async (transaction) => {
          for (const capability of input.capabilities ?? []) {
            await lockAnalyticsAdmission({
              transaction,
              runtimeLeaseId: provenance.producerRuntimeLeaseId,
              expectedBackend: backend,
              expectedDeploymentGeneration: provenance.deploymentGeneration,
              capability,
              action: "externalProducer",
            });
          }
          await lockAnalyticsBackendClaimLeaseForIo({
            transaction,
            claimLeaseId: claim.id,
            fence,
          });
          return execute(transaction);
        },
        {
          maxWait: 120_000,
          timeout: ANALYTICS_QUEUE_PUBLISH_FENCE_TIMEOUT_MS,
        },
      );
    },
  };

  try {
    // publish 失败或响应不确定时保留 claim；有界 Redis producer 会停止重发，
    // 而 switch 仍会等 claim expiry 并现场检查队列。
    const result = await input.publish(guard);
    await stopHeartbeat();
    try {
      const released = await releaseAnalyticsBackendClaimLease({
        claimLeaseId: claim.id,
        runtimeLeaseId: provenance.producerRuntimeLeaseId,
      });
      if (!released) {
        logger.error("Analytics queue publication claim release was rejected", {
          claimKind: input.claimKind,
          resourceIdentity: input.resourceIdentity,
        });
      }
    } catch (error) {
      // 发布已经成功，清理失败不能反转用户可见结果；未释放 claim 会让切换
      // fail closed，直到 expiry 后再由现场 queue drain 给出最终证据。
      logger.error("Analytics queue publication claim release failed", error);
    }
    return result;
  } finally {
    await stopHeartbeat();
  }
}

function batchActionAdmission(actionId: AnalyticsBatchActionId): {
  readonly supportedBackends: readonly ("clickhouse" | "doris")[];
  readonly unsupportedMessage: string;
  readonly capabilities?: readonly ("evaluations" | "experiments")[];
  readonly requiresManagedDoris?: boolean;
} {
  if (actionId === "score-delete") {
    return {
      supportedBackends: ["clickhouse", "doris"],
      unsupportedMessage: "",
      requiresManagedDoris: true,
    };
  }
  if (DORIS_EVALUATION_BATCH_ACTIONS.has(actionId)) {
    return {
      supportedBackends: ["clickhouse", "doris"],
      unsupportedMessage: "",
      capabilities: ["evaluations"],
    };
  }
  if (DORIS_SUPPORTED_BATCH_ACTIONS.has(actionId)) {
    return {
      supportedBackends: ["clickhouse", "doris"],
      unsupportedMessage: "",
    };
  }
  return {
    supportedBackends: ["clickhouse"],
    unsupportedMessage: `Doris batch action ${actionId} is not implemented`,
  };
}

export function assertAnalyticsBatchActionPublicationSupported(
  actionId: AnalyticsBatchActionId,
): void {
  assertAnalyticsQueuePublicationSupported(batchActionAdmission(actionId));
}

export function withAnalyticsBatchActionPublicationAdmission<T>(input: {
  readonly actionId: AnalyticsBatchActionId;
  readonly resourceIdentity: string;
  readonly additionalCapabilities?: readonly "experiments"[];
  readonly publish: (guard: AnalyticsQueuePublicationGuard) => Promise<T>;
}): Promise<T> {
  const admission = batchActionAdmission(input.actionId);
  return withAnalyticsQueuePublicationAdmission({
    claimKind: "batch-action-publish",
    resourceIdentity: `${input.actionId}:${input.resourceIdentity}`,
    ...admission,
    capabilities: [
      ...(admission.capabilities ?? []),
      ...(input.additionalCapabilities ?? []),
    ],
    publish: input.publish,
  });
}
