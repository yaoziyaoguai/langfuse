import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  ensureState: vi.fn(),
  publish: vi.fn(),
  sealBootstrap: vi.fn(),
  sealIncremental: vi.fn(),
  replay: vi.fn(),
  scan: vi.fn(),
  findState: vi.fn(),
  admission: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    analyticsIntegrationState: {
      findUniqueOrThrow: mocks.findState,
    },
  },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  DorisAnalyticsIntegrationExportSource: class {
    scanBootstrapIdentities = mocks.scan;
  },
  ensureDorisAnalyticsIntegrationState: mocks.ensureState,
  publishAnalyticsIntegrationExecution: mocks.publish,
  replayDorisAnalyticsIntegrationDrainCapture: mocks.replay,
  sealDorisAnalyticsIntegrationBootstrapManifest: mocks.sealBootstrap,
  sealAnalyticsIntegrationExecution: mocks.sealIncremental,
}));
vi.mock("../../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: mocks.admission,
}));

import { scheduleDorisAnalyticsIntegrations } from "./scheduleDorisAnalyticsIntegrations";

const envelope = {
  executionId: "aie_1234567890abcdef1234567890ab",
  projectId: "project-1",
  integrationType: "POSTHOG" as const,
  integrationGeneration: "1",
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "1",
  workloadEpochFingerprint: "e".repeat(64),
  runtimeContractVersion: 1,
  capabilityActivationGeneration: "2",
  capabilityContractVersion: 1,
  manifestChecksum: "c".repeat(64),
};

describe("scheduleDorisAnalyticsIntegrations", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.admission.mockReturnValue({
      runtimeLeaseId: "worker-1",
      backend: "doris",
      deploymentGeneration: 1n,
    });
    mocks.scan.mockResolvedValue([
      { deliveryKind: "TRACE", entityKey: "trace-1" },
    ]);
    mocks.sealBootstrap.mockResolvedValue([envelope]);
    mocks.sealIncremental.mockResolvedValue(envelope);
    mocks.replay.mockResolvedValue(0);
  });

  it("seals DARK bootstrap work without queue or third-party effects", async () => {
    const darkState = {
      projectId: "project-1",
      integrationType: "POSTHOG",
      status: "BOOTSTRAPPING_DARK",
      bootstrapManifestChecksum: null,
    };
    mocks.ensureState.mockResolvedValue(darkState);
    mocks.findState.mockResolvedValue({
      ...darkState,
      bootstrapManifestChecksum: "c".repeat(64),
    });
    const add = vi.fn();

    await expect(
      scheduleDorisAnalyticsIntegrations({
        integrationType: "POSTHOG",
        projectIds: ["project-1"],
        queue: { add } as never,
        jobName: "posthog",
      }),
    ).resolves.toBe(0);
    expect(mocks.scan).toHaveBeenCalledOnce();
    expect(mocks.sealBootstrap).toHaveBeenCalledOnce();
    expect(mocks.publish).not.toHaveBeenCalled();
    expect(add).not.toHaveBeenCalled();
  });

  it("publishes sealed bootstrap work only after the config becomes active", async () => {
    mocks.ensureState.mockResolvedValue({
      projectId: "project-1",
      integrationType: "POSTHOG",
      status: "BOOTSTRAPPING_ACTIVE",
      bootstrapManifestChecksum: "c".repeat(64),
      bootstrapManifest: {
        items: [{ deliveryKind: "TRACE", entityKey: "trace-1" }],
      },
    });
    const add = vi.fn().mockResolvedValue({
      getState: vi.fn().mockResolvedValue("waiting"),
    });
    mocks.publish.mockImplementation(async ({ publish }) => {
      await publish(envelope);
      return true;
    });

    await expect(
      scheduleDorisAnalyticsIntegrations({
        integrationType: "POSTHOG",
        projectIds: ["project-1"],
        queue: { add } as never,
        jobName: "posthog",
      }),
    ).resolves.toBe(1);
    expect(mocks.scan).not.toHaveBeenCalled();
    expect(add).toHaveBeenCalledWith(
      "posthog",
      expect.objectContaining({ payload: envelope }),
      expect.objectContaining({ jobId: envelope.executionId }),
    );
  });

  it("seals immutable pending deliveries only for ACTIVE incremental work", async () => {
    mocks.ensureState.mockResolvedValue({
      projectId: "project-1",
      integrationType: "POSTHOG",
      status: "ACTIVE",
      bootstrapManifestChecksum: "c".repeat(64),
    });
    const add = vi.fn().mockResolvedValue({
      getState: vi.fn().mockResolvedValue("waiting"),
    });
    mocks.publish.mockImplementation(async ({ publish }) => {
      await publish(envelope);
      return true;
    });

    await expect(
      scheduleDorisAnalyticsIntegrations({
        integrationType: "POSTHOG",
        projectIds: ["project-1"],
        queue: { add } as never,
        jobName: "posthog",
      }),
    ).resolves.toBe(1);
    expect(mocks.sealIncremental).toHaveBeenCalledOnce();
    expect(mocks.sealBootstrap).not.toHaveBeenCalled();
  });

  it("transfers DRAINING capture before sealing the replacement bootstrap", async () => {
    mocks.ensureState.mockResolvedValue({
      projectId: "project-1",
      integrationType: "POSTHOG",
      status: "BOOTSTRAPPING_DARK",
      bootstrapManifestChecksum: null,
      rescanRequired: true,
    });
    mocks.findState
      .mockResolvedValueOnce({
        projectId: "project-1",
        integrationType: "POSTHOG",
        status: "BOOTSTRAPPING_DARK",
        bootstrapManifestChecksum: null,
        rescanRequired: false,
      })
      .mockResolvedValueOnce({
        projectId: "project-1",
        integrationType: "POSTHOG",
        status: "BOOTSTRAPPING_DARK",
        bootstrapManifestChecksum: "c".repeat(64),
        rescanRequired: false,
      });

    await expect(
      scheduleDorisAnalyticsIntegrations({
        integrationType: "POSTHOG",
        projectIds: ["project-1"],
        queue: { add: vi.fn() } as never,
        jobName: "posthog",
      }),
    ).resolves.toBe(0);
    expect(mocks.replay).toHaveBeenCalledOnce();
    expect(mocks.scan).toHaveBeenCalledOnce();
    expect(mocks.sealBootstrap).toHaveBeenCalledOnce();
  });
});
