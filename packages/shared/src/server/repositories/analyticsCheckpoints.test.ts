import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  acquireDeploymentSharedLock: vi.fn(),
}));

vi.mock("../../db", () => ({ prisma: {} }));
vi.mock("../analytics-persistence/analyticsBackendAdmission", () => ({
  lockAnalyticsAdmission: vi.fn(),
  lockLegacyAnalyticsAdmission: vi.fn(),
}));
vi.mock("./analyticsBackendDeployment", () => ({
  acquireAnalyticsDeploymentSharedLock: mocks.acquireDeploymentSharedLock,
}));

import {
  acquireAnalyticsMutationPermit,
  acquireAnalyticsRetentionMutationPermit,
  renewAnalyticsCheckpointLease,
  withAnalyticsCheckpointIoFence,
} from "./analyticsCheckpoints";

const activeCheckpoint = {
  generation: 7n,
  operationHighWatermarkAcceptedAtNanos: 10n,
  deletionHighWatermarkCreatedAt: new Date("2026-07-22T00:00:00.000Z"),
};

describe("analytics checkpoint mutation admission", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("uses the database clock when deciding whether a checkpoint is active", async () => {
    const databaseNow = new Date("2026-07-22T00:00:30.000Z");
    const skewedProcessNow = new Date("2026-07-22T01:00:00.000Z");
    const findFirst = vi.fn(
      (query: { where: { leaseExpiresAt: { gt: Date } } }) =>
        query.where.leaseExpiresAt.gt.getTime() === databaseNow.getTime()
          ? activeCheckpoint
          : null,
    );
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ locked: "true" }])
        .mockResolvedValueOnce([{ now: databaseNow }]),
      analyticsCheckpointGeneration: { findFirst },
    };
    const client = {
      $transaction: vi.fn(
        (execute: (value: typeof transaction) => Promise<unknown>) =>
          execute(transaction),
      ),
    };

    await expect(
      acquireAnalyticsMutationPermit({
        client: client as never,
        mutation: {
          kind: "ingestion",
          checkpointGeneration: 7n,
          operationAcceptedAtNanos: 11n,
        },
        now: skewedProcessNow,
      }),
    ).resolves.toMatchObject({
      outcome: "held",
      checkpointGeneration: 7n,
    });
    expect(findFirst).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({
          leaseExpiresAt: { gt: databaseNow },
        }),
      }),
    );
  });

  it("holds the transaction-scoped lock when retention reaches an active checkpoint", async () => {
    const databaseNow = new Date("2026-07-22T00:00:30.000Z");
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ locked: "true" }])
        .mockResolvedValueOnce([{ now: databaseNow }]),
      analyticsCheckpointGeneration: {
        findFirst: vi.fn().mockResolvedValue(activeCheckpoint),
      },
    };

    await expect(
      acquireAnalyticsRetentionMutationPermit({
        transaction: transaction as never,
      }),
    ).resolves.toEqual({
      outcome: "held",
      checkpointGeneration: 7n,
      reasonCode: "CHECKPOINT_FENCE",
    });
    expect(transaction.$queryRaw).toHaveBeenCalledTimes(2);
  });

  it("uses the database clock when renewing checkpoint ownership", async () => {
    const databaseNow = new Date("2026-07-22T00:00:30.000Z");
    const updateMany = vi.fn().mockResolvedValue({ count: 1 });
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ locked: "true" }])
        .mockResolvedValueOnce([{ now: databaseNow }]),
      analyticsCheckpointGeneration: {
        findUnique: vi.fn().mockResolvedValue({
          ...activeCheckpoint,
          status: "PREPARING",
          leaseOwner: "checkpoint-worker",
          leaseExpiresAt: new Date("2026-07-22T00:01:00.000Z"),
          analyticsBackend: null,
          deploymentGeneration: null,
          workloadEpochFingerprint: null,
          runtimeContractVersion: null,
          producerRuntimeLeaseId: null,
        }),
        updateMany,
      },
    };
    const client = {
      $transaction: vi.fn(
        (execute: (value: typeof transaction) => Promise<unknown>) =>
          execute(transaction),
      ),
    };

    await expect(
      renewAnalyticsCheckpointLease({
        client: client as never,
        generation: 7n,
        leaseOwner: "checkpoint-worker",
        leaseMs: 60_000,
      }),
    ).resolves.toBe(true);
    expect(updateMany).toHaveBeenCalledWith(
      expect.objectContaining({
        data: {
          leaseExpiresAt: new Date("2026-07-22T00:01:30.000Z"),
        },
      }),
    );
  });

  it("holds the shared checkpoint advisory lock through external capture", async () => {
    const databaseNow = new Date("2026-07-22T00:00:30.000Z");
    const execute = vi.fn().mockResolvedValue("captured");
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ locked: "true" }])
        .mockResolvedValueOnce([{ locked: "true" }])
        .mockResolvedValueOnce([{ now: databaseNow }]),
      analyticsCheckpointGeneration: {
        findUnique: vi.fn().mockResolvedValue({
          ...activeCheckpoint,
          status: "PREPARING",
          leaseOwner: "checkpoint-worker",
          leaseExpiresAt: new Date("2026-07-22T00:01:00.000Z"),
          analyticsBackend: null,
          deploymentGeneration: null,
          workloadEpochFingerprint: null,
          runtimeContractVersion: null,
          producerRuntimeLeaseId: null,
        }),
      },
    };
    const client = {
      $transaction: vi.fn(
        (run: (value: typeof transaction) => Promise<unknown>) =>
          run(transaction),
      ),
    };

    await expect(
      withAnalyticsCheckpointIoFence({
        client: client as never,
        generation: 7n,
        leaseOwner: "checkpoint-worker",
        transactionTimeoutMs: 10_000,
        execute,
      }),
    ).resolves.toBe("captured");
    expect(mocks.acquireDeploymentSharedLock).toHaveBeenCalledWith(transaction);
    expect(
      mocks.acquireDeploymentSharedLock.mock.invocationCallOrder[0],
    ).toBeLessThan(transaction.$queryRaw.mock.invocationCallOrder[0]!);
    expect(transaction.$queryRaw).toHaveBeenCalledTimes(3);
    expect(execute).toHaveBeenCalledOnce();
  });
});
