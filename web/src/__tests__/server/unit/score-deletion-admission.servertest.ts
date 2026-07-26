import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  durableWorkState: {
    mode: "MANAGED" as const,
    provenance: {
      analyticsBackend: "CLICKHOUSE" as const,
      deploymentGeneration: 7n,
      workloadEpochFingerprint: "a".repeat(64),
      runtimeContractVersion: 3,
      producerRuntimeLeaseId: "runtime-producer",
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
  createAnalyticsBackendClaimLease: vi.fn(),
  lockLegacyAnalyticsAdmission: vi.fn(),
  releaseAnalyticsBackendClaimLease: vi.fn(),
  loggerError: vi.fn(),
  transaction: vi.fn(),
}));

vi.mock("@/src/server/analyticsRuntime", () => ({
  getWebAnalyticsDurableWorkState: () => state.durableWorkState,
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: { $transaction: mocks.transaction },
}));

vi.mock("@langfuse/shared/src/server", () => ({
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
  createAnalyticsBackendClaimLease: mocks.createAnalyticsBackendClaimLease,
  logger: { debug: vi.fn(), error: mocks.loggerError },
  lockLegacyAnalyticsAdmission: mocks.lockLegacyAnalyticsAdmission,
  redis: null,
  releaseAnalyticsBackendClaimLease: mocks.releaseAnalyticsBackendClaimLease,
  serializeAnalyticsDurableProvenance: (provenance: {
    deploymentGeneration: bigint;
  }) => ({
    ...provenance,
    deploymentGeneration: provenance.deploymentGeneration.toString(),
  }),
}));

import { withScoreDeletionAdmission } from "@/src/features/scores/server/scoreDeletionAdmission";

function deferred<T = void>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  const promise = new Promise<T>((next) => {
    resolve = next;
  });
  return { promise, resolve };
}

describe("score deletion producer admission", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    state.durableWorkState = {
      mode: "MANAGED",
      provenance: {
        analyticsBackend: "CLICKHOUSE",
        deploymentGeneration: 7n,
        workloadEpochFingerprint: "a".repeat(64),
        runtimeContractVersion: 3,
        producerRuntimeLeaseId: "runtime-producer",
      },
    };
    mocks.transaction.mockImplementation(
      async (run: (transaction: object) => Promise<unknown>) => run({}),
    );
    mocks.createAnalyticsBackendClaimLease.mockResolvedValue({
      id: "score-delete-claim",
    });
    mocks.releaseAnalyticsBackendClaimLease.mockResolvedValue(true);
  });

  it("keeps switch blocked by a live managed ClickHouse claim until queue.add settles", async () => {
    const add = deferred();
    const publish = vi.fn(() => add.promise);

    const producer = withScoreDeletionAdmission({
      resourceIdentity: "score-delete-job-1",
      publish,
    });

    await vi.waitFor(() => expect(publish).toHaveBeenCalledOnce());
    expect(mocks.createAnalyticsBackendClaimLease).toHaveBeenCalledWith(
      expect.objectContaining({
        runtimeLeaseId: "runtime-producer",
        expectedBackend: "clickhouse",
        expectedDeploymentGeneration: 7n,
        expectedWorkloadEpochFingerprint: "a".repeat(64),
        expectedRuntimeContractVersion: 3,
        action: "foundation",
        resourceIdentity: "score-delete-job-1",
      }),
    );
    expect(mocks.releaseAnalyticsBackendClaimLease).not.toHaveBeenCalled();

    add.resolve();
    await producer;
    expect(mocks.releaseAnalyticsBackendClaimLease).toHaveBeenCalledWith({
      claimLeaseId: "score-delete-claim",
      runtimeLeaseId: "runtime-producer",
    });
  });

  it("retains the managed ClickHouse claim when queue.add is uncertain", async () => {
    const publishError = new Error("redis connection lost");

    await expect(
      withScoreDeletionAdmission({
        resourceIdentity: "score-delete-job-2",
        publish: vi.fn().mockRejectedValue(publishError),
      }),
    ).rejects.toBe(publishError);

    expect(mocks.releaseAnalyticsBackendClaimLease).not.toHaveBeenCalled();
  });

  it.each([
    {
      name: "returns false",
      release: () =>
        mocks.releaseAnalyticsBackendClaimLease.mockResolvedValue(false),
    },
    {
      name: "throws",
      release: () =>
        mocks.releaseAnalyticsBackendClaimLease.mockRejectedValue(
          new Error("postgres unavailable"),
        ),
    },
  ])(
    "keeps the successful publisher result when claim release $name",
    async ({ release }) => {
      release();

      await expect(
        withScoreDeletionAdmission({
          resourceIdentity: "score-delete-job-release-failure",
          publish: vi.fn(async () => "published"),
        }),
      ).resolves.toBe("published");

      expect(mocks.loggerError).toHaveBeenCalledOnce();
    },
  );

  it("checks legacy ClickHouse admission before queue lookup or add", async () => {
    state.durableWorkState = {
      mode: "LEGACY_COMPATIBILITY",
      backend: "clickhouse",
    };
    const io = vi.fn(async () => undefined);
    const publish = vi.fn((guard) => guard.withIoFence(io));

    await withScoreDeletionAdmission({
      resourceIdentity: "legacy-score-delete-job",
      publish,
    });

    expect(mocks.lockLegacyAnalyticsAdmission).toHaveBeenCalledOnce();
    expect(
      mocks.lockLegacyAnalyticsAdmission.mock.invocationCallOrder[0],
    ).toBeLessThan(io.mock.invocationCallOrder[0]!);
    expect(mocks.createAnalyticsBackendClaimLease).not.toHaveBeenCalled();
  });

  it("exposes immutable managed Doris provenance to the queue publisher", async () => {
    state.durableWorkState = {
      mode: "MANAGED",
      provenance: {
        analyticsBackend: "DORIS",
        deploymentGeneration: 7n,
        workloadEpochFingerprint: "a".repeat(64),
        runtimeContractVersion: 3,
        producerRuntimeLeaseId: "runtime-producer",
      },
    };
    const publish = vi.fn(async (guard) => guard.durableProvenance);

    await expect(
      withScoreDeletionAdmission({
        resourceIdentity: "managed-doris-score-delete",
        publish,
      }),
    ).resolves.toEqual({
      analyticsBackend: "DORIS",
      deploymentGeneration: "7",
      workloadEpochFingerprint: "a".repeat(64),
      runtimeContractVersion: 3,
      producerRuntimeLeaseId: "runtime-producer",
    });
  });

  it.each([
    {
      state: {
        mode: "LEGACY_COMPATIBILITY" as const,
        backend: "doris" as const,
      },
    },
    { state: { mode: "UNAVAILABLE" as const } },
  ])("fails $state.mode closed before producer side effects", async (input) => {
    state.durableWorkState = input.state;
    const publish = vi.fn();

    await expect(
      withScoreDeletionAdmission({ resourceIdentity: "job-1", publish }),
    ).rejects.toThrow();

    expect(mocks.transaction).not.toHaveBeenCalled();
    expect(mocks.createAnalyticsBackendClaimLease).not.toHaveBeenCalled();
    expect(mocks.lockLegacyAnalyticsAdmission).not.toHaveBeenCalled();
    expect(mocks.releaseAnalyticsBackendClaimLease).not.toHaveBeenCalled();
    expect(publish).not.toHaveBeenCalled();
  });
});
