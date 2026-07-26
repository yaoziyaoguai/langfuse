import { hostname } from "node:os";

import { VERSION } from "@/src/constants";
import { env } from "@/src/env.mjs";
import { prisma } from "@langfuse/shared/src/db";
import {
  AnalyticsRuntimeController,
  ANALYTICS_CONTRACT_COMPATIBILITY,
  checkAnalyticsReadiness,
  ClickHouseClientManager,
  DorisClientManager,
  fenceAnalyticsRuntimeIo,
  fingerprintConfiguredAnalyticsQueueNamespace,
  initializeClickhouseCompatibility,
  isBlobStorageEndpointConnectionValidationEnabled,
  logger,
  parseDorisQueryConfig,
  PrismaAnalyticsCompatibilityControlState,
  probeSelectedAnalyticsBackendEmptiness,
  resolveAnalyticsRuntimeWorkloadEpoch,
  resolveDorisNodeEnv,
  SUPPORTED_DORIS_CANONICALIZER_VERSIONS,
  SUPPORTED_DORIS_SCHEMA_VERSIONS,
} from "@langfuse/shared/src/server";

const ANALYTICS_RUNTIME_LEASE_MS = 120_000;
const ANALYTICS_RUNTIME_HEARTBEAT_MS = 30_000;

declare global {
  var webAnalyticsRuntimeController: AnalyticsRuntimeController | undefined;
  var webAnalyticsRuntimeInitialization: Promise<void> | undefined;
  var webAnalyticsRuntimeFenced: boolean | undefined;
}

globalThis.webAnalyticsRuntimeFenced =
  globalThis.webAnalyticsRuntimeFenced ?? false;

const assertSelectedAnalyticsBackendReady = async (): Promise<void> => {
  if (env.LANGFUSE_ANALYTICS_BACKEND === "clickhouse") {
    await initializeClickhouseCompatibility();
    return;
  }

  const client = DorisClientManager.getInstance().getClient(
    parseDorisQueryConfig(
      {
        DORIS_QUERY_URL: env.DORIS_QUERY_URL,
        DORIS_QUERY_USER: env.DORIS_QUERY_USER,
        DORIS_QUERY_PASSWORD: env.DORIS_QUERY_PASSWORD,
        DORIS_QUERY_TLS_ENABLED: env.DORIS_QUERY_TLS_ENABLED,
        DORIS_QUERY_TLS_CA_PATH: env.DORIS_QUERY_TLS_CA_PATH,
        DORIS_QUERY_MAX_CONNECTIONS: String(env.DORIS_QUERY_MAX_CONNECTIONS),
        DORIS_QUERY_CONNECT_TIMEOUT_MS: String(
          env.DORIS_QUERY_CONNECT_TIMEOUT_MS,
        ),
        DORIS_QUERY_TIMEOUT_MS: String(env.DORIS_QUERY_TIMEOUT_MS),
      },
      resolveDorisNodeEnv(env.NODE_ENV, env.DORIS_LOCAL_DEV_MODE),
    ),
  );
  const readiness = await checkAnalyticsReadiness({
    executor: client,
    controlState: new PrismaAnalyticsCompatibilityControlState(prisma),
    supportedCanonicalizerVersions: SUPPORTED_DORIS_CANONICALIZER_VERSIONS,
    supportedSchemaVersions: SUPPORTED_DORIS_SCHEMA_VERSIONS,
  });
  if (!readiness.ready) {
    throw new Error(
      `Doris analytics readiness check failed: ${readiness.code}`,
    );
  }
};

export const initializeWebAnalyticsRuntime = (): Promise<void> => {
  if (globalThis.webAnalyticsRuntimeInitialization) {
    return globalThis.webAnalyticsRuntimeInitialization;
  }

  globalThis.webAnalyticsRuntimeInitialization = (async () => {
    await assertSelectedAnalyticsBackendReady();

    const workloadEpoch = await resolveAnalyticsRuntimeWorkloadEpoch({
      value: env.LANGFUSE_ANALYTICS_WORKLOAD_EPOCH,
      file: env.LANGFUSE_ANALYTICS_WORKLOAD_EPOCH_FILE,
    });

    const controller = new AnalyticsRuntimeController({
      component: "web",
      instanceId:
        env.LANGFUSE_ANALYTICS_RUNTIME_INSTANCE_ID ?? `web:${hostname()}`,
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
        {
          capability: "coreBatchExports",
          supportedContractVersion: 1,
          installedRoles: ["producer"],
        },
        {
          capability: "evaluations",
          supportedContractVersion: 1,
          installedRoles: ["producer"],
        },
        {
          capability: "experiments",
          supportedContractVersion: 1,
          installedRoles: ["producer"],
        },
        {
          capability: "datasetRunIngestion",
          supportedContractVersion: 1,
          installedRoles: ["producer"],
        },
        {
          capability: "datasetRunExports",
          supportedContractVersion: 1,
          installedRoles: ["producer"],
        },
        ...(env.LANGFUSE_ANALYTICS_BACKEND === "doris" &&
        isBlobStorageEndpointConnectionValidationEnabled()
          ? [
              {
                capability: "analyticsIntegrations" as const,
                supportedContractVersion: 1,
                installedRoles: ["producer"] as const,
              },
            ]
          : []),
      ],
      leaseMs: ANALYTICS_RUNTIME_LEASE_MS,
      onFenced: async () => {
        globalThis.webAnalyticsRuntimeFenced = true;
        fenceAnalyticsRuntimeIo();
        logger.error("Analytics runtime lease fenced; web restart required");
        await Promise.allSettled([
          ClickHouseClientManager.getInstance().closeAllConnections(),
          DorisClientManager.getInstance().closeAllConnections(),
        ]);
      },
    });
    globalThis.webAnalyticsRuntimeController = controller;

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

  return globalThis.webAnalyticsRuntimeInitialization;
};

export const checkWebAnalyticsRuntimeReadiness = async (): Promise<boolean> =>
  globalThis.webAnalyticsRuntimeController
    ? globalThis.webAnalyticsRuntimeController.checkReadiness()
    : false;

export const isWebAnalyticsRuntimeFenced = (): boolean =>
  globalThis.webAnalyticsRuntimeFenced === true;

export const getWebAnalyticsAdmissionContext = (): ReturnType<
  AnalyticsRuntimeController["getAdmissionContext"]
> | null => {
  if (!globalThis.webAnalyticsRuntimeController) return null;
  try {
    return globalThis.webAnalyticsRuntimeController.getAdmissionContext();
  } catch {
    return null;
  }
};

export const getWebAnalyticsDurableWorkState = (): ReturnType<
  AnalyticsRuntimeController["getDurableWorkState"]
> => {
  if (!globalThis.webAnalyticsRuntimeController) {
    return { mode: "UNAVAILABLE" };
  }
  try {
    return globalThis.webAnalyticsRuntimeController.getDurableWorkState();
  } catch {
    return { mode: "UNAVAILABLE" };
  }
};

export const quiesceWebAnalyticsRuntime = async (): Promise<boolean> =>
  globalThis.webAnalyticsRuntimeController
    ? globalThis.webAnalyticsRuntimeController.quiesce()
    : false;
