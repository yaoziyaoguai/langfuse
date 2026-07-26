import { hostname } from "node:os";

import {
  AnalyticsRuntimeController,
  ANALYTICS_CONTRACT_COMPATIBILITY,
  ClickHouseClientManager,
  DorisClientManager,
  fenceAnalyticsRuntimeIo,
  fingerprintConfiguredAnalyticsQueueNamespace,
  isBlobStorageEndpointConnectionValidationEnabled,
  logger,
  probeSelectedAnalyticsBackendEmptiness,
  resolveAnalyticsRuntimeWorkloadEpoch,
} from "@langfuse/shared/src/server";

import { VERSION } from "./constants/VERSION";
import { env } from "./env";
import { WorkerManager } from "./queues/workerManager";

const ANALYTICS_RUNTIME_LEASE_MS = 120_000;
const ANALYTICS_RUNTIME_HEARTBEAT_MS = 30_000;

const batchExportCapabilityContracts = () =>
  env.LANGFUSE_ANALYTICS_BACKEND === "doris" &&
  env.QUEUE_CONSUMER_BATCH_EXPORT_QUEUE_IS_ENABLED === "true" &&
  env.LANGFUSE_S3_BATCH_EXPORT_ENABLED === "true" &&
  Boolean(env.LANGFUSE_S3_BATCH_EXPORT_BUCKET)
    ? [
        {
          capability: "coreBatchExports" as const,
          supportedContractVersion: 1,
          installedRoles: ["consumer", "recovery"] as const,
        },
        {
          capability: "datasetRunExports" as const,
          supportedContractVersion: 1,
          installedRoles: ["consumer", "recovery"] as const,
        },
      ]
    : [];

const evaluationCapabilityContracts = () =>
  env.LANGFUSE_ANALYTICS_BACKEND === "doris" &&
  env.QUEUE_CONSUMER_EVAL_EXECUTION_QUEUE_IS_ENABLED === "true" &&
  env.QUEUE_CONSUMER_CODE_EVAL_EXECUTION_QUEUE_IS_ENABLED === "true"
    ? [
        {
          capability: "evaluations" as const,
          supportedContractVersion: 1,
          installedRoles: ["capture", "consumer", "recovery"] as const,
        },
      ]
    : [];

const experimentCapabilityContracts = () =>
  env.LANGFUSE_ANALYTICS_BACKEND === "doris" &&
  env.QUEUE_CONSUMER_EXPERIMENT_CREATE_QUEUE_IS_ENABLED === "true"
    ? [
        {
          capability: "experiments" as const,
          supportedContractVersion: 1,
          installedRoles: ["consumer", "recovery"] as const,
        },
        {
          capability: "datasetRunIngestion" as const,
          supportedContractVersion: 1,
          installedRoles: ["producer", "consumer", "recovery"] as const,
        },
      ]
    : [];

const analyticsIntegrationCapabilityContracts = () =>
  env.LANGFUSE_ANALYTICS_BACKEND === "doris" &&
  env.QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED === "true" &&
  env.QUEUE_CONSUMER_MIXPANEL_INTEGRATION_QUEUE_IS_ENABLED === "true" &&
  env.QUEUE_CONSUMER_BLOB_STORAGE_INTEGRATION_QUEUE_IS_ENABLED === "true" &&
  isBlobStorageEndpointConnectionValidationEnabled()
    ? [
        {
          capability: "analyticsIntegrations" as const,
          supportedContractVersion: 1,
          installedRoles: ["capture", "consumer", "recovery"] as const,
        },
      ]
    : [];

type StopHandler = () => void | Promise<void>;
type FencedAnalyticsStopOperation = {
  failureMessage: string;
  stop: () => void | Promise<unknown>;
};

let controller: AnalyticsRuntimeController | null = null;
let initialization: Promise<void> | null = null;
let fenceOperation: Promise<void> | null = null;
let fenced = false;
const stopHandlers = new Set<StopHandler>();

const stopHandler = async (handler: StopHandler): Promise<void> => {
  try {
    await handler();
  } catch (error) {
    logger.error("Failed to stop a fenced worker analytics workload", error);
  }
};

const createFencedAnalyticsStopOperations =
  (): FencedAnalyticsStopOperation[] => [
    {
      failureMessage: "Failed to close workers after analytics runtime fence",
      stop: () => WorkerManager.fenceRegistrations(),
    },
    ...[...stopHandlers].map((handler) => ({
      failureMessage: "Failed to stop a fenced worker analytics workload",
      stop: handler,
    })),
    {
      failureMessage:
        "Failed to close ClickHouse connections after analytics runtime fence",
      stop: () => ClickHouseClientManager.getInstance().closeAllConnections(),
    },
    {
      failureMessage:
        "Failed to close Doris connections after analytics runtime fence",
      stop: () => DorisClientManager.getInstance().closeAllConnections(),
    },
  ];

const stopFencedAnalyticsWorkloads = async (
  operations: FencedAnalyticsStopOperation[],
): Promise<void> => {
  const results = await Promise.allSettled(
    operations.map(async ({ stop }) => {
      await stop();
    }),
  );

  results.forEach((result, index) => {
    const operation = operations[index];
    if (result.status === "rejected" && operation) {
      logger.error(operation.failureMessage, result.reason);
    }
  });
};

const fenceWorkerAnalyticsRuntime = (): Promise<void> => {
  if (fenceOperation) return fenceOperation;

  fenceAnalyticsRuntimeIo();
  fenced = true;
  logger.error("Analytics runtime lease fenced; stopping worker workloads");
  const stopOperations = createFencedAnalyticsStopOperations();
  let resolveFence!: () => void;
  let rejectFence!: (reason: unknown) => void;
  fenceOperation = new Promise<void>((resolve, reject) => {
    resolveFence = resolve;
    rejectFence = reject;
  });
  stopFencedAnalyticsWorkloads(stopOperations).then(resolveFence, rejectFence);
  return fenceOperation;
};

export const isWorkerAnalyticsRuntimeFenced = (): boolean => fenced;

export const assertWorkerAnalyticsRuntimeNotFenced = (): void => {
  if (fenced) {
    throw new Error("Analytics runtime lease fenced before worker bootstrap");
  }
};

export const registerAnalyticsRuntimeStopHandler = (
  handler: StopHandler,
): (() => void) => {
  if (fenced) {
    stopHandler(handler).catch(() => undefined);
    return () => undefined;
  }

  stopHandlers.add(handler);
  return () => stopHandlers.delete(handler);
};

export const initializeWorkerAnalyticsRuntime = (): Promise<void> => {
  if (initialization) return initialization;

  initialization = (async () => {
    const workloadEpoch = await resolveAnalyticsRuntimeWorkloadEpoch({
      value: env.LANGFUSE_ANALYTICS_WORKLOAD_EPOCH,
      file: env.LANGFUSE_ANALYTICS_WORKLOAD_EPOCH_FILE,
    });
    controller = new AnalyticsRuntimeController({
      component: "worker",
      instanceId:
        env.LANGFUSE_ANALYTICS_RUNTIME_INSTANCE_ID ?? `worker:${hostname()}`,
      backend: env.LANGFUSE_ANALYTICS_BACKEND,
      workloadEpoch,
      queueNamespaceFingerprint: fingerprintConfiguredAnalyticsQueueNamespace(),
      allowFreshInitialization:
        env.LANGFUSE_ANALYTICS_ALLOW_FRESH_INITIALIZATION === "true",
      buildId: env.BUILD_ID ?? VERSION,
      foundationContractVersion: 1,
      acceptedSchemaVersion: {
        min: Math.min(
          ...ANALYTICS_CONTRACT_COMPATIBILITY.readableSchemaVersions,
        ),
        max: Math.max(
          ...ANALYTICS_CONTRACT_COMPATIBILITY.readableSchemaVersions,
        ),
      },
      acceptedCanonicalVersion: {
        min: Math.min(
          ...ANALYTICS_CONTRACT_COMPATIBILITY.readableCanonicalizerVersions.map(
            Number,
          ),
        ),
        max: Math.max(
          ...ANALYTICS_CONTRACT_COMPATIBILITY.readableCanonicalizerVersions.map(
            Number,
          ),
        ),
      },
      capabilityContracts: [
        ...batchExportCapabilityContracts(),
        ...evaluationCapabilityContracts(),
        ...experimentCapabilityContracts(),
        ...analyticsIntegrationCapabilityContracts(),
      ],
      leaseMs: ANALYTICS_RUNTIME_LEASE_MS,
      onFenced: fenceWorkerAnalyticsRuntime,
    });

    const emptiness = workloadEpoch
      ? await probeSelectedAnalyticsBackendEmptiness({
          backend: env.LANGFUSE_ANALYTICS_BACKEND,
        })
      : {
          selectedBackendEmpty: false,
          evidenceDigest: "0".repeat(64),
        };

    await controller.initialize(emptiness);
    controller.startHeartbeat(ANALYTICS_RUNTIME_HEARTBEAT_MS);
  })();

  return initialization;
};

export const checkWorkerAnalyticsRuntimeReadiness =
  async (): Promise<boolean> =>
    controller ? controller.checkReadiness() : false;

export const getWorkerAnalyticsAdmissionContext = (): ReturnType<
  AnalyticsRuntimeController["getAdmissionContext"]
> | null => {
  if (!controller) return null;
  try {
    return controller.getAdmissionContext();
  } catch {
    return null;
  }
};

export const quiesceWorkerAnalyticsRuntime = async (): Promise<boolean> =>
  controller ? controller.quiesce() : false;
