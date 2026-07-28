import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  backend: "clickhouse" as "clickhouse" | "doris",
  workloadEpoch: undefined as string | undefined,
  workloadEpochFile: undefined as string | undefined,
  allowFreshInitialization: "false" as "true" | "false",
  runtimeInstanceId: undefined as string | undefined,
  buildId: undefined as string | undefined,
  blobConnectionValidationEnabled: true,
}));

const mocks = vi.hoisted(() => ({
  controllerConfigs: [] as Array<Record<string, unknown>>,
  controllers: [] as Array<{
    initialize: ReturnType<typeof vi.fn>;
    startHeartbeat: ReturnType<typeof vi.fn>;
    checkReadiness: ReturnType<typeof vi.fn>;
    getAdmissionContext: ReturnType<typeof vi.fn>;
    getDurableProvenance: ReturnType<typeof vi.fn>;
    getDurableWorkState: ReturnType<typeof vi.fn>;
    quiesce: ReturnType<typeof vi.fn>;
  }>,
  checkAnalyticsReadiness: vi.fn(),
  closeClickhouseConnections: vi.fn(async () => []),
  closeDorisConnections: vi.fn(async () => undefined),
  fenceAnalyticsRuntimeIo: vi.fn(),
  fingerprintConfiguredAnalyticsQueueNamespace: vi.fn(() => "q".repeat(64)),
  initializeClickhouseCompatibility: vi.fn(),
  isBlobStorageEndpointConnectionValidationEnabled: vi.fn(
    () => state.blobConnectionValidationEnabled,
  ),
  probeSelectedAnalyticsBackendEmptiness: vi.fn(),
  resolveAnalyticsRuntimeWorkloadEpoch: vi.fn(),
}));

vi.mock("node:os", () => ({
  hostname: () => "test-host",
}));

vi.mock("@/src/constants", () => ({
  VERSION: "v-test-version",
}));

vi.mock("@/src/env.mjs", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return state.backend;
    },
    get LANGFUSE_ANALYTICS_WORKLOAD_EPOCH() {
      return state.workloadEpoch;
    },
    get LANGFUSE_ANALYTICS_WORKLOAD_EPOCH_FILE() {
      return state.workloadEpochFile;
    },
    get LANGFUSE_ANALYTICS_ALLOW_FRESH_INITIALIZATION() {
      return state.allowFreshInitialization;
    },
    get LANGFUSE_ANALYTICS_RUNTIME_INSTANCE_ID() {
      return state.runtimeInstanceId;
    },
    get BUILD_ID() {
      return state.buildId;
    },
    NODE_ENV: "test",
    DORIS_LOCAL_DEV_MODE: "false",
  },
}));

vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));

vi.mock("@langfuse/shared/src/server", () => ({
  AnalyticsRuntimeController: class {
    initialize = vi.fn().mockResolvedValue({ mode: "READY" });
    startHeartbeat = vi.fn();
    checkReadiness = vi.fn().mockResolvedValue(true);
    getAdmissionContext = vi.fn().mockReturnValue({
      runtimeLeaseId: "lease-1",
      backend: "clickhouse",
      deploymentGeneration: 1n,
    });
    getDurableProvenance = vi.fn().mockReturnValue({
      analyticsBackend: "CLICKHOUSE",
      deploymentGeneration: 1n,
      workloadEpochFingerprint: "a".repeat(64),
      runtimeContractVersion: 1,
      producerRuntimeLeaseId: "lease-1",
    });
    getDurableWorkState = vi.fn(() => ({
      mode: "MANAGED",
      provenance: this.getDurableProvenance(),
    }));
    quiesce = vi.fn().mockResolvedValue(true);

    constructor(config: Record<string, unknown>) {
      mocks.controllerConfigs.push(config);
      mocks.controllers.push(this);
    }
  },
  ANALYTICS_CONTRACT_COMPATIBILITY: {
    writerSchemaVersion: 1,
    writerCanonicalizerVersion: "2",
    readableSchemaVersions: [1],
    readableCanonicalizerVersions: ["1", "2"],
  },
  ClickHouseClientManager: {
    getInstance: () => ({
      closeAllConnections: mocks.closeClickhouseConnections,
    }),
  },
  checkAnalyticsReadiness: mocks.checkAnalyticsReadiness,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION: "2",
  CURRENT_ANALYTICS_SCHEMA_VERSION: 1,
  DorisClientManager: {
    getInstance: () => ({
      getClient: () => ({ query: vi.fn() }),
      closeAllConnections: mocks.closeDorisConnections,
    }),
  },
  fenceAnalyticsRuntimeIo: mocks.fenceAnalyticsRuntimeIo,
  fingerprintConfiguredAnalyticsQueueNamespace:
    mocks.fingerprintConfiguredAnalyticsQueueNamespace,
  initializeClickhouseCompatibility: mocks.initializeClickhouseCompatibility,
  isBlobStorageEndpointConnectionValidationEnabled:
    mocks.isBlobStorageEndpointConnectionValidationEnabled,
  logger: { debug: vi.fn(), error: vi.fn() },
  parseDorisQueryConfig: vi.fn(() => ({})),
  PrismaAnalyticsCompatibilityControlState: class {},
  probeSelectedAnalyticsBackendEmptiness:
    mocks.probeSelectedAnalyticsBackendEmptiness,
  resolveDorisNodeEnv: vi.fn((nodeEnv) => nodeEnv),
  resolveAnalyticsRuntimeWorkloadEpoch:
    mocks.resolveAnalyticsRuntimeWorkloadEpoch,
  redis: null,
  SUPPORTED_DORIS_CANONICALIZER_VERSIONS: ["1", "2"],
  SUPPORTED_DORIS_SCHEMA_VERSIONS: [1],
}));

const resetRuntimeGlobals = () => {
  globalThis.webAnalyticsRuntimeController = undefined;
  globalThis.webAnalyticsRuntimeInitialization = undefined;
  globalThis.webAnalyticsRuntimeFenced = false;
};

const importRuntime = async () => await import("@/src/server/analyticsRuntime");

describe("web analytics runtime", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    resetRuntimeGlobals();
    mocks.controllerConfigs.length = 0;
    mocks.controllers.length = 0;
    state.backend = "clickhouse";
    state.workloadEpoch = undefined;
    state.workloadEpochFile = undefined;
    state.allowFreshInitialization = "false";
    state.runtimeInstanceId = undefined;
    state.buildId = undefined;
    state.blobConnectionValidationEnabled = true;
    mocks.checkAnalyticsReadiness.mockResolvedValue({ ready: true });
    mocks.probeSelectedAnalyticsBackendEmptiness.mockResolvedValue({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });
    mocks.resolveAnalyticsRuntimeWorkloadEpoch.mockImplementation(
      ({ value, file }: { value?: string; file?: string }) =>
        Promise.resolve(file ? "mounted-epoch" : value),
    );
  });

  afterEach(() => {
    resetRuntimeGlobals();
  });

  it("keeps legacy compatibility without probing backend emptiness", async () => {
    const runtime = await importRuntime();

    await runtime.initializeWebAnalyticsRuntime();

    expect(mocks.initializeClickhouseCompatibility).toHaveBeenCalledOnce();
    expect(mocks.checkAnalyticsReadiness).not.toHaveBeenCalled();
    expect(mocks.probeSelectedAnalyticsBackendEmptiness).not.toHaveBeenCalled();
    expect(mocks.controllerConfigs[0]).toMatchObject({
      component: "web",
      instanceId: "web:test-host",
      backend: "clickhouse",
      workloadEpoch: undefined,
      queueNamespaceFingerprint: "q".repeat(64),
      buildId: "v-test-version",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 2 },
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
      ],
      leaseMs: 120_000,
    });
    expect(mocks.controllers[0]?.initialize).toHaveBeenCalledWith({
      selectedBackendEmpty: false,
      evidenceDigest: "0".repeat(64),
    });
    expect(mocks.controllers[0]?.startHeartbeat).toHaveBeenCalledWith(30_000);
  });

  it("checks Doris schema before probing and registering a lease", async () => {
    state.backend = "doris";
    state.workloadEpoch = "rollout-1";
    state.allowFreshInitialization = "true";
    state.runtimeInstanceId = "web-pod-7";
    state.buildId = "build-123";
    const runtime = await importRuntime();

    await runtime.initializeWebAnalyticsRuntime();

    expect(mocks.initializeClickhouseCompatibility).not.toHaveBeenCalled();
    expect(mocks.checkAnalyticsReadiness).toHaveBeenCalledOnce();
    expect(mocks.probeSelectedAnalyticsBackendEmptiness).toHaveBeenCalledWith({
      backend: "doris",
    });
    expect(mocks.controllerConfigs[0]).toMatchObject({
      instanceId: "web-pod-7",
      backend: "doris",
      workloadEpoch: "rollout-1",
      allowFreshInitialization: true,
      buildId: "build-123",
      capabilityContracts: expect.arrayContaining([
        {
          capability: "analyticsIntegrations",
          supportedContractVersion: 1,
          installedRoles: ["producer"],
        },
      ]),
    });
    expect(
      mocks.checkAnalyticsReadiness.mock.invocationCallOrder[0],
    ).toBeLessThan(
      mocks.probeSelectedAnalyticsBackendEmptiness.mock
        .invocationCallOrder[0] ?? 0,
    );
    expect(
      mocks.probeSelectedAnalyticsBackendEmptiness.mock.invocationCallOrder[0],
    ).toBeLessThan(
      mocks.controllers[0]?.initialize.mock.invocationCallOrder[0] ?? 0,
    );
    expect(mocks.controllers[0]?.initialize).toHaveBeenCalledWith({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });
  });

  it("does not advertise analytics integrations without connection-time blob validation", async () => {
    state.backend = "doris";
    state.workloadEpoch = "rollout-1";
    state.blobConnectionValidationEnabled = false;
    const runtime = await importRuntime();

    await runtime.initializeWebAnalyticsRuntime();

    expect(mocks.controllerConfigs[0]).toMatchObject({
      capabilityContracts: expect.not.arrayContaining([
        expect.objectContaining({ capability: "analyticsIntegrations" }),
      ]),
    });
  });

  it("fails closed before probing or leasing when static readiness fails", async () => {
    state.backend = "doris";
    state.workloadEpoch = "rollout-1";
    mocks.checkAnalyticsReadiness.mockResolvedValue({
      ready: false,
      code: "SCHEMA_MISMATCH",
      schemaVersion: 0,
    });
    const runtime = await importRuntime();

    await expect(runtime.initializeWebAnalyticsRuntime()).rejects.toThrow(
      "Doris analytics readiness check failed: SCHEMA_MISMATCH",
    );
    expect(mocks.probeSelectedAnalyticsBackendEmptiness).not.toHaveBeenCalled();
    expect(mocks.controllers).toHaveLength(0);
  });

  it("uses the workload epoch resolved from a mounted file", async () => {
    state.workloadEpochFile = "/run/secrets/analytics-workload-epoch";
    const runtime = await importRuntime();

    await runtime.initializeWebAnalyticsRuntime();

    expect(mocks.resolveAnalyticsRuntimeWorkloadEpoch).toHaveBeenCalledWith({
      value: undefined,
      file: "/run/secrets/analytics-workload-epoch",
    });
    expect(mocks.controllerConfigs[0]).toMatchObject({
      workloadEpoch: "mounted-epoch",
    });
    expect(mocks.probeSelectedAnalyticsBackendEmptiness).toHaveBeenCalledOnce();
  });

  it("reuses the initialized controller across module reloads", async () => {
    const firstRuntime = await importRuntime();
    await firstRuntime.initializeWebAnalyticsRuntime();

    vi.resetModules();
    const reloadedRuntime = await importRuntime();
    await reloadedRuntime.initializeWebAnalyticsRuntime();

    expect(mocks.controllerConfigs).toHaveLength(1);
    expect(mocks.initializeClickhouseCompatibility).toHaveBeenCalledOnce();
  });

  it("delegates readiness and quiescence to the initialized controller", async () => {
    const runtime = await importRuntime();
    await runtime.initializeWebAnalyticsRuntime();

    await expect(runtime.checkWebAnalyticsRuntimeReadiness()).resolves.toBe(
      true,
    );
    await expect(runtime.quiesceWebAnalyticsRuntime()).resolves.toBe(true);
  });

  it("exposes admission context only while the controller is ready", async () => {
    const runtime = await importRuntime();

    expect(runtime.getWebAnalyticsAdmissionContext()).toBeNull();

    await runtime.initializeWebAnalyticsRuntime();
    const controller = mocks.controllers[0];
    for (const mode of [
      "LEGACY_COMPATIBILITY",
      "ADOPTION_REQUIRED",
      "FENCED",
    ]) {
      controller?.getAdmissionContext.mockImplementationOnce(() => {
        throw new Error(`${mode} has no admission context`);
      });
      expect(runtime.getWebAnalyticsAdmissionContext()).toBeNull();
    }
    expect(runtime.getWebAnalyticsAdmissionContext()).toEqual({
      runtimeLeaseId: "lease-1",
      backend: "clickhouse",
      deploymentGeneration: 1n,
    });
  });

  it("exposes durable provenance only while the controller is ready", async () => {
    const runtime = await importRuntime();

    expect(runtime.getWebAnalyticsDurableWorkState()).toEqual({
      mode: "UNAVAILABLE",
    });
    await runtime.initializeWebAnalyticsRuntime();
    expect(runtime.getWebAnalyticsDurableWorkState()).toEqual({
      mode: "MANAGED",
      provenance: {
        analyticsBackend: "CLICKHOUSE",
        deploymentGeneration: 1n,
        workloadEpochFingerprint: "a".repeat(64),
        runtimeContractVersion: 1,
        producerRuntimeLeaseId: "lease-1",
      },
    });
    mocks.controllers[0]?.getDurableWorkState.mockReturnValueOnce({
      mode: "LEGACY_COMPATIBILITY",
      backend: "clickhouse",
    });
    expect(runtime.getWebAnalyticsDurableWorkState()).toEqual({
      mode: "LEGACY_COMPATIBILITY",
      backend: "clickhouse",
    });
    mocks.controllers[0]?.getDurableWorkState.mockReturnValueOnce({
      mode: "UNAVAILABLE",
    });
    expect(runtime.getWebAnalyticsDurableWorkState()).toEqual({
      mode: "UNAVAILABLE",
    });
  });

  it("fences analytics IO and closes both backend clients when the lease is lost", async () => {
    const runtime = await importRuntime();
    await runtime.initializeWebAnalyticsRuntime();

    expect(runtime.isWebAnalyticsRuntimeFenced()).toBe(false);
    const onFenced = mocks.controllerConfigs[0]
      ?.onFenced as () => Promise<void>;
    await onFenced();

    expect(runtime.isWebAnalyticsRuntimeFenced()).toBe(true);
    expect(mocks.fenceAnalyticsRuntimeIo).toHaveBeenCalledOnce();
    expect(mocks.closeClickhouseConnections).toHaveBeenCalledOnce();
    expect(mocks.closeDorisConnections).toHaveBeenCalledOnce();
  });
});
