import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  getAnalyticsBackendDeploymentState: vi.fn(),
  markAnalyticsRuntimeQuiesced: vi.fn(),
  registerAnalyticsRuntimeLease: vi.fn(),
  renewAnalyticsRuntimeLease: vi.fn(),
  resolveAnalyticsBackendStartup: vi.fn(),
}));

vi.mock("../../db", () => ({ prisma: {} }));

vi.mock("../repositories/analyticsBackendDeployment", () => ({
  fingerprintAnalyticsWorkloadEpoch: vi.fn(() => "epoch-fingerprint"),
  getAnalyticsBackendDeploymentState: mocks.getAnalyticsBackendDeploymentState,
  resolveAnalyticsBackendStartup: mocks.resolveAnalyticsBackendStartup,
}));

vi.mock("../repositories/analyticsRuntimeLeases", () => ({
  markAnalyticsRuntimeQuiesced: mocks.markAnalyticsRuntimeQuiesced,
  registerAnalyticsRuntimeLease: mocks.registerAnalyticsRuntimeLease,
  renewAnalyticsRuntimeLease: mocks.renewAnalyticsRuntimeLease,
}));

import { AnalyticsRuntimeController } from "./AnalyticsRuntimeController";
import {
  assertAnalyticsRuntimeIoAllowed,
  fenceAnalyticsRuntimeIo,
  resetAnalyticsRuntimeIoFenceForTests,
} from "./analyticsRuntimeIoFence";

describe("AnalyticsRuntimeController fencing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    resetAnalyticsRuntimeIoFenceForTests();
    mocks.resolveAnalyticsBackendStartup.mockResolvedValue({
      mode: "READY",
      initialized: false,
      marker: { generation: 1n },
    });
    mocks.registerAnalyticsRuntimeLease.mockResolvedValue({
      mode: "READY",
      lease: { id: "runtime-lease" },
    });
    mocks.renewAnalyticsRuntimeLease.mockResolvedValue(false);
    mocks.markAnalyticsRuntimeQuiesced.mockResolvedValue(true);
    mocks.getAnalyticsBackendDeploymentState.mockResolvedValue(null);
  });

  it("notifies its fence handler exactly once after renewal is rejected", async () => {
    const onFenced = vi.fn();
    const controller = new AnalyticsRuntimeController({
      client: {} as never,
      component: "worker",
      instanceId: "worker:test-host",
      backend: "clickhouse",
      workloadEpoch: "rollout-1",
      queueNamespaceFingerprint: "b".repeat(64),
      buildId: "v-test",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 120_000,
      onFenced,
    });

    await controller.initialize({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });

    expect(
      globalThis.analyticsRuntimeIoLeaseDeadlineMonotonicMs,
    ).toBeGreaterThan(performance.now());

    await expect(controller.renew()).resolves.toBe(false);
    await expect(controller.renew()).resolves.toBe(false);

    expect(onFenced).toHaveBeenCalledOnce();
    expect(mocks.renewAnalyticsRuntimeLease).toHaveBeenCalledOnce();
    expect(() => controller.getAdmissionContext()).toThrow(
      expect.objectContaining({ code: "ANALYTICS_UNAVAILABLE" }),
    );
    expect(controller.getDurableWorkState()).toEqual({ mode: "UNAVAILABLE" });
    await expect(controller.checkReadiness()).resolves.toBe(false);
  });

  it("notifies its fence handler when the local I/O lease fences first", async () => {
    const onFenced = vi.fn();
    const controller = new AnalyticsRuntimeController({
      client: {} as never,
      component: "worker",
      instanceId: "worker:test-host",
      backend: "clickhouse",
      workloadEpoch: "rollout-1",
      queueNamespaceFingerprint: "b".repeat(64),
      buildId: "v-test",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 120_000,
      onFenced,
    });
    await controller.initialize({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });

    fenceAnalyticsRuntimeIo();

    expect(onFenced).toHaveBeenCalledOnce();
    await expect(controller.checkReadiness()).resolves.toBe(false);
  });

  it("preserves repository database clocks when initialization omits now", async () => {
    const controller = new AnalyticsRuntimeController({
      client: {} as never,
      component: "worker",
      instanceId: "worker:test-host",
      backend: "clickhouse",
      workloadEpoch: "rollout-1",
      queueNamespaceFingerprint: "b".repeat(64),
      buildId: "v-test",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 120_000,
    });

    await controller.initialize({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });

    expect(mocks.resolveAnalyticsBackendStartup).toHaveBeenCalledWith(
      expect.objectContaining({ now: undefined }),
    );
    expect(mocks.registerAnalyticsRuntimeLease).toHaveBeenCalledWith(
      expect.objectContaining({ now: undefined }),
    );
  });

  it("exposes the durable provenance of a ready managed runtime", async () => {
    const controller = new AnalyticsRuntimeController({
      client: {} as never,
      component: "web",
      instanceId: "web:test-host",
      backend: "doris",
      workloadEpoch: "rollout-1",
      queueNamespaceFingerprint: "b".repeat(64),
      buildId: "v-test",
      foundationContractVersion: 3,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 120_000,
    });

    expect(() => controller.getDurableProvenance()).toThrow(
      "Analytics runtime is not managed by a deployment marker",
    );
    expect(controller.getDurableWorkState()).toEqual({ mode: "UNAVAILABLE" });
    await controller.initialize({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });

    expect(controller.getDurableProvenance()).toEqual({
      analyticsBackend: "DORIS",
      deploymentGeneration: 1n,
      workloadEpochFingerprint: "epoch-fingerprint",
      runtimeContractVersion: 3,
      producerRuntimeLeaseId: "runtime-lease",
    });
    expect(controller.getDurableWorkState()).toEqual({
      mode: "MANAGED",
      provenance: {
        analyticsBackend: "DORIS",
        deploymentGeneration: 1n,
        workloadEpochFingerprint: "epoch-fingerprint",
        runtimeContractVersion: 3,
        producerRuntimeLeaseId: "runtime-lease",
      },
    });
  });

  it("uses the database clock to reject an expired readiness lease", async () => {
    const databaseNow = new Date("2200-01-01T00:00:00.000Z");
    const client = {
      $transaction: vi.fn(
        async (callback: (transaction: unknown) => Promise<unknown>) =>
          callback(client),
      ),
      $queryRaw: vi.fn().mockResolvedValue([{ now: databaseNow }]),
      analyticsBackendDeploymentState: {
        findUnique: vi.fn().mockResolvedValue({
          backend: "CLICKHOUSE",
          generation: 1n,
          workloadEpochFingerprint: "epoch-fingerprint",
          queueNamespaceFingerprint: "b".repeat(64),
          foundationContractVersion: 1,
        }),
      },
      analyticsRuntimeLease: {
        findUnique: vi.fn().mockResolvedValue({
          id: "runtime-lease",
          backend: "CLICKHOUSE",
          deploymentGeneration: 1n,
          workloadEpochFingerprint: "epoch-fingerprint",
          queueNamespaceFingerprint: "b".repeat(64),
          foundationContractVersion: 1,
          state: "ACTIVE",
          supersededAt: null,
          leaseExpiresAt: new Date("2100-01-01T00:00:00.000Z"),
        }),
      },
    };
    const controller = new AnalyticsRuntimeController({
      client: client as never,
      component: "worker",
      instanceId: "worker:test-host",
      backend: "clickhouse",
      workloadEpoch: "rollout-1",
      queueNamespaceFingerprint: "b".repeat(64),
      buildId: "v-test",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 120_000,
    });
    await controller.initialize({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });

    await expect(controller.checkReadiness()).resolves.toBe(false);
    expect(client.$queryRaw).toHaveBeenCalledOnce();
  });

  it("treats a successfully quiesced lease as idempotently quiesced", async () => {
    const controller = new AnalyticsRuntimeController({
      client: {} as never,
      component: "worker",
      instanceId: "worker:test-host",
      backend: "clickhouse",
      workloadEpoch: "rollout-1",
      queueNamespaceFingerprint: "b".repeat(64),
      buildId: "v-test",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 120_000,
    });
    await controller.initialize({
      selectedBackendEmpty: true,
      evidenceDigest: "a".repeat(64),
    });

    await expect(controller.quiesce()).resolves.toBe(true);
    await expect(controller.quiesce()).resolves.toBe(true);

    expect(mocks.markAnalyticsRuntimeQuiesced).toHaveBeenCalledOnce();
    expect(() => assertAnalyticsRuntimeIoAllowed()).toThrow(
      expect.objectContaining({ code: "ANALYTICS_UNAVAILABLE" }),
    );
  });

  it("treats legacy compatibility as already quiesced", async () => {
    const controller = new AnalyticsRuntimeController({
      client: {} as never,
      component: "web",
      instanceId: "web:test-host",
      backend: "clickhouse",
      workloadEpoch: undefined,
      queueNamespaceFingerprint: "b".repeat(64),
      buildId: "v-test",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 1 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 120_000,
    });
    await controller.initialize({
      selectedBackendEmpty: false,
      evidenceDigest: "0".repeat(64),
    });

    await expect(controller.quiesce()).resolves.toBe(true);
    expect(mocks.markAnalyticsRuntimeQuiesced).not.toHaveBeenCalled();
    expect(controller.getDurableWorkState()).toEqual({
      mode: "LEGACY_COMPATIBILITY",
      backend: "clickhouse",
    });
  });
});
