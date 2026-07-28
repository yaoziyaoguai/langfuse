import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  backend: "clickhouse" as "clickhouse" | "doris",
  workloadEpoch: undefined as string | undefined,
  workloadEpochFile: undefined as string | undefined,
  allowFreshInitialization: "false" as "true" | "false",
  runtimeInstanceId: undefined as string | undefined,
  buildId: undefined as string | undefined,
  batchExportQueueEnabled: "true" as "true" | "false",
  batchExportStorageEnabled: "true" as "true" | "false",
  batchExportBucket: "test-batch-exports" as string | undefined,
  evalExecutionQueueEnabled: "true" as "true" | "false",
  codeEvalExecutionQueueEnabled: "true" as "true" | "false",
  experimentQueueEnabled: "true" as "true" | "false",
  posthogQueueEnabled: "true" as "true" | "false",
  mixpanelQueueEnabled: "true" as "true" | "false",
  blobQueueEnabled: "true" as "true" | "false",
  blobConnectionValidationEnabled: true,
}));

const mocks = vi.hoisted(() => ({
  closeClickhouse: vi.fn(),
  closeDoris: vi.fn(),
  controllerConfigs: [] as Array<Record<string, unknown>>,
  controllers: [] as Array<{
    initialize: ReturnType<typeof vi.fn>;
    startHeartbeat: ReturnType<typeof vi.fn>;
    checkReadiness: ReturnType<typeof vi.fn>;
    getAdmissionContext: ReturnType<typeof vi.fn>;
    quiesce: ReturnType<typeof vi.fn>;
  }>,
  fenceAnalyticsRuntimeIo: vi.fn(),
  fingerprintConfiguredAnalyticsQueueNamespace: vi.fn(() => "q".repeat(64)),
  isBlobStorageEndpointConnectionValidationEnabled: vi.fn(
    () => state.blobConnectionValidationEnabled,
  ),
  probeSelectedAnalyticsBackendEmptiness: vi.fn(),
  fenceRegistrations: vi.fn(),
  loggerError: vi.fn(),
  order: [] as string[],
  resolveAnalyticsRuntimeWorkloadEpoch: vi.fn(),
}));

vi.mock("node:os", () => ({
  hostname: () => "test-host",
}));

vi.mock("./env", () => ({
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
    get QUEUE_CONSUMER_BATCH_EXPORT_QUEUE_IS_ENABLED() {
      return state.batchExportQueueEnabled;
    },
    get LANGFUSE_S3_BATCH_EXPORT_ENABLED() {
      return state.batchExportStorageEnabled;
    },
    get LANGFUSE_S3_BATCH_EXPORT_BUCKET() {
      return state.batchExportBucket;
    },
    get QUEUE_CONSUMER_EVAL_EXECUTION_QUEUE_IS_ENABLED() {
      return state.evalExecutionQueueEnabled;
    },
    get QUEUE_CONSUMER_CODE_EVAL_EXECUTION_QUEUE_IS_ENABLED() {
      return state.codeEvalExecutionQueueEnabled;
    },
    get QUEUE_CONSUMER_EXPERIMENT_CREATE_QUEUE_IS_ENABLED() {
      return state.experimentQueueEnabled;
    },
    get QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED() {
      return state.posthogQueueEnabled;
    },
    get QUEUE_CONSUMER_MIXPANEL_INTEGRATION_QUEUE_IS_ENABLED() {
      return state.mixpanelQueueEnabled;
    },
    get QUEUE_CONSUMER_BLOB_STORAGE_INTEGRATION_QUEUE_IS_ENABLED() {
      return state.blobQueueEnabled;
    },
  },
}));

vi.mock("@langfuse/shared/src/server", () => ({
  ANALYTICS_CONTRACT_COMPATIBILITY: {
    readableCanonicalizerVersions: ["1", "2"],
    readableSchemaVersions: [1],
  },
  AnalyticsRuntimeController: class {
    initialize = vi.fn().mockResolvedValue({ mode: "READY" });
    startHeartbeat = vi.fn();
    checkReadiness = vi.fn().mockResolvedValue(true);
    getAdmissionContext = vi.fn(() => ({
      runtimeLeaseId: "runtime-lease",
      backend: state.backend,
      deploymentGeneration: 1n,
    }));
    quiesce = vi.fn().mockResolvedValue(true);

    constructor(config: Record<string, unknown>) {
      mocks.controllerConfigs.push(config);
      mocks.controllers.push(this);
    }
  },
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: mocks.closeClickhouse }),
  },
  CURRENT_ANALYTICS_CANONICALIZER_VERSION: "2",
  CURRENT_ANALYTICS_SCHEMA_VERSION: 1,
  DorisClientManager: {
    getInstance: () => ({ closeAllConnections: mocks.closeDoris }),
  },
  fenceAnalyticsRuntimeIo: mocks.fenceAnalyticsRuntimeIo,
  fingerprintConfiguredAnalyticsQueueNamespace:
    mocks.fingerprintConfiguredAnalyticsQueueNamespace,
  isBlobStorageEndpointConnectionValidationEnabled:
    mocks.isBlobStorageEndpointConnectionValidationEnabled,
  logger: { error: mocks.loggerError },
  probeSelectedAnalyticsBackendEmptiness:
    mocks.probeSelectedAnalyticsBackendEmptiness,
  resolveAnalyticsRuntimeWorkloadEpoch:
    mocks.resolveAnalyticsRuntimeWorkloadEpoch,
}));

vi.mock("./queues/workerManager", () => ({
  WorkerManager: {
    fenceRegistrations: mocks.fenceRegistrations,
  },
}));

const importRuntime = async () => await import("./analyticsRuntime");

describe("worker analytics runtime", () => {
  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    mocks.order.length = 0;
    mocks.controllerConfigs.length = 0;
    mocks.controllers.length = 0;
    state.backend = "clickhouse";
    state.workloadEpoch = undefined;
    state.workloadEpochFile = undefined;
    state.allowFreshInitialization = "false";
    state.runtimeInstanceId = undefined;
    state.buildId = undefined;
    state.batchExportQueueEnabled = "true";
    state.batchExportStorageEnabled = "true";
    state.batchExportBucket = "test-batch-exports";
    state.evalExecutionQueueEnabled = "true";
    state.codeEvalExecutionQueueEnabled = "true";
    state.experimentQueueEnabled = "true";
    state.posthogQueueEnabled = "true";
    state.mixpanelQueueEnabled = "true";
    state.blobQueueEnabled = "true";
    state.blobConnectionValidationEnabled = true;
    mocks.probeSelectedAnalyticsBackendEmptiness.mockResolvedValue({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });
    mocks.closeClickhouse.mockResolvedValue(undefined);
    mocks.closeDoris.mockResolvedValue(undefined);
    mocks.fenceRegistrations.mockResolvedValue(undefined);
    mocks.resolveAnalyticsRuntimeWorkloadEpoch.mockImplementation(
      ({ value, file }: { value?: string; file?: string }) =>
        Promise.resolve(file ? "mounted-epoch" : value),
    );
  });

  it("keeps legacy compatibility without probing backend emptiness", async () => {
    const runtime = await importRuntime();

    await runtime.initializeWorkerAnalyticsRuntime();

    expect(mocks.probeSelectedAnalyticsBackendEmptiness).not.toHaveBeenCalled();
    expect(mocks.controllers[0]?.initialize).toHaveBeenCalledWith({
      selectedBackendEmpty: false,
      evidenceDigest: "0".repeat(64),
    });
    expect(mocks.controllerConfigs[0]).toMatchObject({
      instanceId: "worker:test-host",
      buildId: "v3.224.2",
      queueNamespaceFingerprint: "q".repeat(64),
    });
    expect(mocks.controllers[0]?.startHeartbeat).toHaveBeenCalledWith(30_000);
  });

  it("probes the selected backend and builds the fixed runtime contract", async () => {
    state.backend = "doris";
    state.workloadEpoch = "rollout-1";
    state.allowFreshInitialization = "true";
    state.buildId = "build-123";
    const runtime = await importRuntime();

    await runtime.initializeWorkerAnalyticsRuntime();

    expect(mocks.probeSelectedAnalyticsBackendEmptiness).toHaveBeenCalledWith({
      backend: "doris",
    });
    expect(mocks.controllerConfigs[0]).toMatchObject({
      component: "worker",
      instanceId: "worker:test-host",
      backend: "doris",
      workloadEpoch: "rollout-1",
      allowFreshInitialization: true,
      buildId: "build-123",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 2 },
      capabilityContracts: [
        {
          capability: "coreBatchExports",
          supportedContractVersion: 1,
          installedRoles: ["consumer", "recovery"],
        },
        {
          capability: "datasetRunExports",
          supportedContractVersion: 1,
          installedRoles: ["consumer", "recovery"],
        },
        {
          capability: "evaluations",
          supportedContractVersion: 1,
          installedRoles: ["capture", "consumer", "recovery"],
        },
        {
          capability: "experiments",
          supportedContractVersion: 1,
          installedRoles: ["consumer", "recovery"],
        },
        {
          capability: "datasetRunIngestion",
          supportedContractVersion: 1,
          installedRoles: ["producer", "consumer", "recovery"],
        },
        {
          capability: "analyticsIntegrations",
          supportedContractVersion: 1,
          installedRoles: ["capture", "consumer", "recovery"],
        },
      ],
      leaseMs: 120_000,
    });
    expect(mocks.controllers[0]?.initialize).toHaveBeenCalledWith({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });
  });

  it.each([
    ["disabled queue consumer", "false", "true", "test-batch-exports"],
    ["disabled export storage", "true", "false", "test-batch-exports"],
    ["missing export bucket", "true", "true", undefined],
  ] as const)(
    "does not advertise batch export roles with %s",
    async (_name, queueEnabled, storageEnabled, bucket) => {
      state.backend = "doris";
      state.workloadEpoch = "rollout-1";
      state.batchExportQueueEnabled = queueEnabled;
      state.batchExportStorageEnabled = storageEnabled;
      state.batchExportBucket = bucket;
      const runtime = await importRuntime();

      await runtime.initializeWorkerAnalyticsRuntime();

      expect(mocks.controllerConfigs[0]).toMatchObject({
        capabilityContracts: [
          {
            capability: "evaluations",
            supportedContractVersion: 1,
            installedRoles: ["capture", "consumer", "recovery"],
          },
          {
            capability: "experiments",
            supportedContractVersion: 1,
            installedRoles: ["consumer", "recovery"],
          },
          {
            capability: "datasetRunIngestion",
            supportedContractVersion: 1,
            installedRoles: ["producer", "consumer", "recovery"],
          },
          {
            capability: "analyticsIntegrations",
            supportedContractVersion: 1,
            installedRoles: ["capture", "consumer", "recovery"],
          },
        ],
      });
    },
  );

  it.each([
    ["disabled execution consumer", "false", "true"],
    ["disabled code execution consumer", "true", "false"],
  ] as const)(
    "does not advertise evaluation roles with %s",
    async (_name, executionEnabled, codeEnabled) => {
      state.backend = "doris";
      state.workloadEpoch = "rollout-1";
      state.batchExportQueueEnabled = "false";
      state.evalExecutionQueueEnabled = executionEnabled;
      state.codeEvalExecutionQueueEnabled = codeEnabled;
      const runtime = await importRuntime();

      await runtime.initializeWorkerAnalyticsRuntime();

      expect(mocks.controllerConfigs[0]).toMatchObject({
        capabilityContracts: [
          {
            capability: "experiments",
            supportedContractVersion: 1,
            installedRoles: ["consumer", "recovery"],
          },
          {
            capability: "datasetRunIngestion",
            supportedContractVersion: 1,
            installedRoles: ["producer", "consumer", "recovery"],
          },
          {
            capability: "analyticsIntegrations",
            supportedContractVersion: 1,
            installedRoles: ["capture", "consumer", "recovery"],
          },
        ],
      });
    },
  );

  it("does not advertise analytics integrations without connection-time blob validation", async () => {
    state.backend = "doris";
    state.workloadEpoch = "rollout-1";
    state.blobConnectionValidationEnabled = false;
    const runtime = await importRuntime();

    await runtime.initializeWorkerAnalyticsRuntime();

    expect(mocks.controllerConfigs[0]).toMatchObject({
      capabilityContracts: expect.not.arrayContaining([
        expect.objectContaining({ capability: "analyticsIntegrations" }),
      ]),
    });
  });

  it("uses an explicit runtime instance id", async () => {
    state.runtimeInstanceId = "worker-pod-7";
    const runtime = await importRuntime();

    await runtime.initializeWorkerAnalyticsRuntime();

    expect(mocks.controllerConfigs[0]).toMatchObject({
      instanceId: "worker-pod-7",
    });
  });

  it("uses the workload epoch resolved from a mounted file", async () => {
    state.workloadEpochFile = "/run/secrets/analytics-workload-epoch";
    const runtime = await importRuntime();

    await runtime.initializeWorkerAnalyticsRuntime();

    expect(mocks.resolveAnalyticsRuntimeWorkloadEpoch).toHaveBeenCalledWith({
      value: undefined,
      file: "/run/secrets/analytics-workload-epoch",
    });
    expect(mocks.controllerConfigs[0]).toMatchObject({
      workloadEpoch: "mounted-epoch",
    });
    expect(mocks.probeSelectedAnalyticsBackendEmptiness).toHaveBeenCalledOnce();
  });

  it("synchronously fences analytics I/O and stops every held workload once", async () => {
    let releaseClickhouse: () => void = () => undefined;
    const clickhouseClosed = new Promise<void>((resolve) => {
      releaseClickhouse = resolve;
    });
    let releaseWorkers: () => void = () => undefined;
    const workersStopped = new Promise<void>((resolve) => {
      releaseWorkers = resolve;
    });
    mocks.fenceAnalyticsRuntimeIo.mockImplementation(() => {
      mocks.order.push("io-fence");
    });
    mocks.fenceRegistrations.mockImplementation(() => {
      mocks.order.push("workers");
      return workersStopped;
    });
    mocks.closeClickhouse.mockImplementation(() => {
      mocks.order.push("clickhouse");
      return clickhouseClosed;
    });
    mocks.closeDoris.mockImplementation(async () => {
      mocks.order.push("doris");
    });
    const runtime = await importRuntime();
    const firstStop = vi.fn(async () => {
      mocks.order.push("first-stop");
    });
    const secondStop = vi.fn(async () => {
      mocks.order.push("second-stop");
    });
    const unregisterFirstStop =
      runtime.registerAnalyticsRuntimeStopHandler(firstStop);
    runtime.registerAnalyticsRuntimeStopHandler(secondStop);
    await runtime.initializeWorkerAnalyticsRuntime();

    const onFenced = mocks.controllerConfigs[0]
      ?.onFenced as () => Promise<void>;
    const firstFence = onFenced();

    expect(mocks.fenceAnalyticsRuntimeIo).toHaveBeenCalledOnce();
    expect(mocks.order[0]).toBe("io-fence");
    expect(mocks.fenceRegistrations).toHaveBeenCalledOnce();
    expect(firstStop).toHaveBeenCalledOnce();
    expect(secondStop).toHaveBeenCalledOnce();
    expect(mocks.closeClickhouse).toHaveBeenCalledOnce();
    expect(mocks.closeDoris).toHaveBeenCalledOnce();
    unregisterFirstStop();

    const secondFence = onFenced();
    expect(secondFence).toBe(firstFence);

    await vi.waitFor(() => {
      expect(firstStop).toHaveBeenCalledOnce();
      expect(secondStop).toHaveBeenCalledOnce();
      expect(mocks.closeClickhouse).toHaveBeenCalledOnce();
      expect(mocks.closeDoris).toHaveBeenCalledOnce();
    });
    releaseWorkers();
    const fenceSettled = vi.fn();
    void firstFence.then(fenceSettled);
    await new Promise((resolve) => setTimeout(resolve, 0));
    expect(fenceSettled).not.toHaveBeenCalled();
    releaseClickhouse();
    await firstFence;
    expect(fenceSettled).toHaveBeenCalledOnce();

    expect(mocks.fenceRegistrations).toHaveBeenCalledOnce();
    expect(firstStop).toHaveBeenCalledOnce();
    expect(secondStop).toHaveBeenCalledOnce();
    expect(mocks.closeClickhouse).toHaveBeenCalledOnce();
    expect(mocks.closeDoris).toHaveBeenCalledOnce();
    expect(() => runtime.assertWorkerAnalyticsRuntimeNotFenced()).toThrow(
      "Analytics runtime lease fenced before worker bootstrap",
    );
  });

  it("reuses the fence operation when a stop handler re-enters", async () => {
    const runtime = await importRuntime();
    let onFenced: () => Promise<void>;
    let reentrantFence: Promise<void> | undefined;
    runtime.registerAnalyticsRuntimeStopHandler(() => {
      reentrantFence = onFenced();
    });
    await runtime.initializeWorkerAnalyticsRuntime();
    onFenced = mocks.controllerConfigs[0]?.onFenced as () => Promise<void>;

    const firstFence = onFenced();
    await firstFence;

    expect(reentrantFence).toBe(firstFence);
    expect(mocks.fenceAnalyticsRuntimeIo).toHaveBeenCalledOnce();
    expect(mocks.fenceRegistrations).toHaveBeenCalledOnce();
    expect(mocks.closeClickhouse).toHaveBeenCalledOnce();
    expect(mocks.closeDoris).toHaveBeenCalledOnce();
  });

  it("reports a worker close failure after fencing", async () => {
    mocks.fenceRegistrations.mockRejectedValue(new Error("close failed"));
    const runtime = await importRuntime();
    await runtime.initializeWorkerAnalyticsRuntime();

    const onFenced = mocks.controllerConfigs[0]
      ?.onFenced as () => Promise<void>;
    await onFenced();

    expect(mocks.closeClickhouse).toHaveBeenCalledOnce();
    expect(mocks.closeDoris).toHaveBeenCalledOnce();
    expect(mocks.loggerError).toHaveBeenCalledWith(
      "Failed to close workers after analytics runtime fence",
      expect.any(Error),
    );
  });

  it("delegates readiness and quiescence to the initialized controller", async () => {
    const runtime = await importRuntime();
    expect(runtime.getWorkerAnalyticsAdmissionContext()).toBeNull();
    await runtime.initializeWorkerAnalyticsRuntime();

    await expect(runtime.checkWorkerAnalyticsRuntimeReadiness()).resolves.toBe(
      true,
    );
    expect(runtime.getWorkerAnalyticsAdmissionContext()).toEqual({
      runtimeLeaseId: "runtime-lease",
      backend: "clickhouse",
      deploymentGeneration: 1n,
    });
    mocks.controllers[0]?.getAdmissionContext.mockImplementationOnce(() => {
      throw new Error("not ready");
    });
    expect(runtime.getWorkerAnalyticsAdmissionContext()).toBeNull();
    await expect(runtime.quiesceWorkerAnalyticsRuntime()).resolves.toBe(true);
  });
});
