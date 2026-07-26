import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  durableWorkState: {
    mode: "MANAGED" as const,
    provenance: {
      analyticsBackend: "DORIS" as const,
      deploymentGeneration: 7n,
      workloadEpochFingerprint: "a".repeat(64),
      runtimeContractVersion: 1,
      producerRuntimeLeaseId: "web-runtime",
    },
  } as
    | {
        mode: "MANAGED";
        provenance: {
          analyticsBackend: "CLICKHOUSE" | "DORIS";
          deploymentGeneration: bigint;
          workloadEpochFingerprint: string;
          runtimeContractVersion: number;
          producerRuntimeLeaseId: string;
        };
      }
    | { mode: "LEGACY_COMPATIBILITY"; backend: "clickhouse" | "doris" }
    | { mode: "UNAVAILABLE" },
}));

const mocks = vi.hoisted(() => ({
  createClaim: vi.fn(),
  lockClaimForIo: vi.fn(),
  lockAnalyticsAdmission: vi.fn(),
  lockLegacyAdmission: vi.fn(),
  renewClaim: vi.fn(),
  releaseClaim: vi.fn(),
  transaction: vi.fn(),
}));

vi.mock("@/src/server/analyticsRuntime", () => ({
  getWebAnalyticsDurableWorkState: () => state.durableWorkState,
}));

vi.mock("@langfuse/shared", () => ({
  BatchEvalSourceTable: {
    EVENTS: "events",
    EXPERIMENTS: "experiments",
  },
  InternalServerError: class InternalServerError extends Error {
    readonly httpCode = 500;
  },
  NotImplementedError: class NotImplementedError extends Error {
    readonly httpCode = 501;
  },
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: { $transaction: mocks.transaction },
}));

vi.mock("@langfuse/shared/src/server", () => ({
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
  createAnalyticsBackendClaimLease: mocks.createClaim,
  lockAnalyticsAdmission: mocks.lockAnalyticsAdmission,
  lockAnalyticsBackendClaimLeaseForIo: mocks.lockClaimForIo,
  lockLegacyAnalyticsAdmission: mocks.lockLegacyAdmission,
  logger: { debug: vi.fn(), error: vi.fn() },
  redis: null,
  renewAnalyticsBackendClaimLease: mocks.renewClaim,
  releaseAnalyticsBackendClaimLease: mocks.releaseClaim,
  serializeAnalyticsDurableProvenance: (provenance: {
    deploymentGeneration: bigint;
  }) => ({
    ...provenance,
    deploymentGeneration: provenance.deploymentGeneration.toString(),
  }),
}));

import {
  assertAnalyticsBatchActionPublicationSupported,
  withAnalyticsBatchActionPublicationAdmission,
} from "@/src/server/analyticsQueuePublicationAdmission";

describe("analytics batch-action publication admission", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    state.durableWorkState = {
      mode: "MANAGED",
      provenance: {
        analyticsBackend: "DORIS",
        deploymentGeneration: 7n,
        workloadEpochFingerprint: "a".repeat(64),
        runtimeContractVersion: 1,
        producerRuntimeLeaseId: "web-runtime",
      },
    };
    mocks.createClaim.mockResolvedValue({ id: "claim-1" });
    mocks.lockAnalyticsAdmission.mockResolvedValue({
      analyticsBackend: "DORIS",
    });
    mocks.renewClaim.mockResolvedValue({ id: "claim-1" });
    mocks.releaseClaim.mockResolvedValue(true);
    mocks.transaction.mockImplementation(
      async (run: (transaction: object) => Promise<unknown>) => run({}),
    );
  });

  it("renews a managed claim while publication remains in flight", async () => {
    vi.useFakeTimers();
    let finishPublish!: () => void;
    const publish = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finishPublish = resolve;
        }),
    );

    try {
      const publication = withAnalyticsBatchActionPublicationAdmission({
        actionId: "observation-add-to-dataset",
        resourceIdentity: "batch-heartbeat",
        publish,
      });
      await vi.advanceTimersByTimeAsync(0);
      expect(publish).toHaveBeenCalledOnce();

      await vi.advanceTimersByTimeAsync(60_000);

      expect(mocks.renewClaim).toHaveBeenCalledWith(
        expect.objectContaining({
          claimLeaseId: "claim-1",
          fence: expect.objectContaining({
            expectedBackend: "doris",
            expectedDeploymentGeneration: 7n,
          }),
        }),
      );
      finishPublish();
      await publication;
    } finally {
      vi.useRealTimers();
    }
  });

  it("holds the deployment fence around final producer IO", async () => {
    const io = vi.fn(async () => "queued");
    const publish = vi.fn((guard) => guard.withIoFence(io));

    await expect(
      withAnalyticsBatchActionPublicationAdmission({
        actionId: "observation-add-to-dataset",
        resourceIdentity: "batch-fenced",
        publish,
      }),
    ).resolves.toBe("queued");

    expect(mocks.lockClaimForIo).toHaveBeenCalledWith({
      transaction: expect.any(Object),
      claimLeaseId: "claim-1",
      fence: expect.objectContaining({
        runtimeLeaseId: "web-runtime",
        expectedBackend: "doris",
        expectedDeploymentGeneration: 7n,
      }),
    });
    expect(mocks.lockClaimForIo.mock.invocationCallOrder[0]).toBeLessThan(
      io.mock.invocationCallOrder[0]!,
    );
  });

  it("stops before producer IO when claim renewal is rejected", async () => {
    const renewalError = new Error("claim generation changed");
    mocks.renewClaim.mockRejectedValueOnce(renewalError);
    const io = vi.fn(async () => "unreachable");

    await expect(
      withAnalyticsBatchActionPublicationAdmission({
        actionId: "observation-add-to-dataset",
        resourceIdentity: "batch-stale",
        publish: (guard) => guard.withIoFence(io),
      }),
    ).rejects.toBe(renewalError);

    expect(mocks.lockClaimForIo).not.toHaveBeenCalled();
    expect(io).not.toHaveBeenCalled();
    expect(mocks.releaseClaim).not.toHaveBeenCalled();
  });

  it("allows Doris-supported batch actions under a Doris claim", async () => {
    const publish = vi.fn(async () => "queued");

    await expect(
      withAnalyticsBatchActionPublicationAdmission({
        actionId: "observation-add-to-dataset",
        resourceIdentity: "batch-1",
        publish,
      }),
    ).resolves.toBe("queued");

    expect(mocks.createClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedBackend: "doris",
        claimKind: "batch-action-publish",
        resourceIdentity: "observation-add-to-dataset:batch-1",
      }),
    );
    expect(mocks.releaseClaim).toHaveBeenCalledOnce();
  });

  it.each(["eval-create", "observation-run-batched-evaluation"] as const)(
    "locks the active Doris evaluation capability before %s producer IO",
    async (actionId) => {
      const io = vi.fn(async () => "queued");

      await expect(
        withAnalyticsBatchActionPublicationAdmission({
          actionId,
          resourceIdentity: "evaluation-batch",
          publish: (guard) => guard.withIoFence(io),
        }),
      ).resolves.toBe("queued");

      expect(mocks.lockAnalyticsAdmission).toHaveBeenCalledWith({
        transaction: expect.any(Object),
        runtimeLeaseId: "web-runtime",
        expectedBackend: "doris",
        expectedDeploymentGeneration: 7n,
        capability: "evaluations",
        action: "externalProducer",
      });
      expect(
        mocks.lockAnalyticsAdmission.mock.invocationCallOrder[0],
      ).toBeLessThan(mocks.lockClaimForIo.mock.invocationCallOrder[0]!);
      expect(mocks.lockClaimForIo.mock.invocationCallOrder[0]).toBeLessThan(
        io.mock.invocationCallOrder[0]!,
      );
    },
  );

  it("locks experiments as a dependency for experiment evaluation publication", async () => {
    const io = vi.fn(async () => "queued");

    await expect(
      withAnalyticsBatchActionPublicationAdmission({
        actionId: "observation-run-batched-evaluation",
        resourceIdentity: "experiment-evaluation",
        additionalCapabilities: ["experiments"],
        publish: (guard) => guard.withIoFence(io),
      }),
    ).resolves.toBe("queued");

    expect(mocks.lockAnalyticsAdmission).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({ capability: "evaluations" }),
    );
    expect(mocks.lockAnalyticsAdmission).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ capability: "experiments" }),
    );
    expect(
      mocks.lockAnalyticsAdmission.mock.invocationCallOrder[1],
    ).toBeLessThan(mocks.lockClaimForIo.mock.invocationCallOrder[0]!);
  });

  it("does not publish historical evaluation work when the capability is inactive", async () => {
    const inactive = new Error("evaluation capability is inactive");
    mocks.lockAnalyticsAdmission.mockRejectedValueOnce(inactive);
    const io = vi.fn();

    await expect(
      withAnalyticsBatchActionPublicationAdmission({
        actionId: "eval-create",
        resourceIdentity: "inactive-evaluation",
        publish: (guard) => guard.withIoFence(io),
      }),
    ).rejects.toBe(inactive);

    expect(mocks.lockClaimForIo).not.toHaveBeenCalled();
    expect(io).not.toHaveBeenCalled();
  });

  it("rejects unsupported Doris work before producer side effects", async () => {
    const publish = vi.fn();

    expect(() =>
      assertAnalyticsBatchActionPublicationSupported("trace-delete"),
    ).toThrow("not implemented");
    await expect(
      withAnalyticsBatchActionPublicationAdmission({
        actionId: "trace-delete",
        resourceIdentity: "batch-2",
        publish,
      }),
    ).rejects.toMatchObject({ httpCode: 501 });

    expect(mocks.createClaim).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
  });

  it("allows implemented Doris dataset deletion", () => {
    expect(() =>
      assertAnalyticsBatchActionPublicationSupported("dataset-delete"),
    ).not.toThrow();
  });

  it("holds legacy adoption admission while publishing", async () => {
    state.durableWorkState = {
      mode: "LEGACY_COMPATIBILITY",
      backend: "clickhouse",
    };
    const io = vi.fn(async () => undefined);
    const publish = vi.fn((guard) => guard.withIoFence(io));

    await withAnalyticsBatchActionPublicationAdmission({
      actionId: "eval-create",
      resourceIdentity: "batch-legacy",
      publish,
    });

    expect(mocks.lockLegacyAdmission).toHaveBeenCalledOnce();
    expect(mocks.lockLegacyAdmission.mock.invocationCallOrder[0]).toBeLessThan(
      io.mock.invocationCallOrder[0]!,
    );
    expect(mocks.transaction).toHaveBeenCalledWith(
      expect.any(Function),
      expect.objectContaining({ timeout: 35 * 60_000 }),
    );
  });
});
