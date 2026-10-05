import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  acquireMutationPermit: vi.fn(),
}));

vi.mock("../../db", () => ({ prisma: {} }));
vi.mock("../analytics-persistence/analyticsBackendAdmission", () => ({
  lockAnalyticsAdmission: vi.fn(),
  lockLegacyAnalyticsAdmission: vi.fn(),
}));
vi.mock("./analyticsCheckpoints", () => ({
  acquireAnalyticsRetentionMutationPermit: mocks.acquireMutationPermit,
}));

import {
  analyticsProjectRetentionStateId,
  completeAnalyticsRetentionRun,
  findAnalyticsEntityHeadsForRetention,
  getAnalyticsRetentionBarrier,
  startOrResumeAnalyticsRetention,
} from "./analyticsRetention";

describe("analytics retention checkpoint fence", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("does not select a Doris deletion batch while a checkpoint is active", async () => {
    mocks.acquireMutationPermit.mockResolvedValue({
      outcome: "held",
      checkpointGeneration: 9n,
      reasonCode: "CHECKPOINT_FENCE",
    });
    const findMany = vi.fn();

    await expect(
      findAnalyticsEntityHeadsForRetention({
        client: { analyticsEntityHead: { findMany } } as never,
        cutoffDate: new Date("2026-06-01T00:00:00.000Z"),
        entityType: "EVENT",
        limit: 100,
      }),
    ).rejects.toThrow("held by the analytics checkpoint");
    expect(findMany).not.toHaveBeenCalled();
  });

  it("selects the batch only after acquiring the transaction-scoped permit", async () => {
    const heads = [{ id: "event-1" }];
    const findMany = vi.fn().mockResolvedValue(heads);
    const client = {
      analyticsEntityHead: { findMany },
      analyticsCapabilityActivation: {
        findUnique: vi.fn().mockResolvedValue(null),
      },
    } as never;
    mocks.acquireMutationPermit.mockResolvedValue({
      outcome: "allowed",
      checkpointGeneration: null,
    });

    await expect(
      findAnalyticsEntityHeadsForRetention({
        client,
        cutoffDate: new Date("2026-06-01T00:00:00.000Z"),
        entityType: "EVENT",
        limit: 100,
        projectId: "project-1",
      }),
    ).resolves.toEqual(heads);
    expect(mocks.acquireMutationPermit).toHaveBeenCalledWith({
      transaction: client,
    });
    expect(
      mocks.acquireMutationPermit.mock.invocationCallOrder[0],
    ).toBeLessThan(findMany.mock.invocationCallOrder[0]!);
    expect(findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({ projectId: "project-1" }),
      }),
    );
  });

  it("does not start or complete a purge watermark transition during a checkpoint", async () => {
    mocks.acquireMutationPermit.mockResolvedValue({
      outcome: "held",
      checkpointGeneration: 9n,
      reasonCode: "CHECKPOINT_FENCE",
    });
    const transaction = {
      analyticsRetentionState: {
        upsert: vi.fn(),
        findUnique: vi.fn(),
      },
    };
    const client = {
      $transaction: vi.fn(
        (execute: (value: typeof transaction) => Promise<unknown>) =>
          execute(transaction),
      ),
    };

    await expect(
      startOrResumeAnalyticsRetention({
        client: client as never,
        retentionDays: 30,
        stateId: analyticsProjectRetentionStateId("project-1"),
      }),
    ).rejects.toThrow("held by the analytics checkpoint");
    await expect(
      completeAnalyticsRetentionRun({
        client: transaction as never,
        runId: "retention-run",
      }),
    ).rejects.toThrow("held by the analytics checkpoint");
    expect(transaction.analyticsRetentionState.upsert).not.toHaveBeenCalled();
    expect(
      transaction.analyticsRetentionState.findUnique,
    ).not.toHaveBeenCalled();
  });

  it("derives a new purge cutoff and start time from the database clock", async () => {
    const databaseNow = new Date("2026-07-20T23:59:59.000Z");
    const create = vi.fn(async ({ data }) => ({
      ...data,
      completedAt: null,
      createdAt: databaseNow,
      updatedAt: databaseNow,
    }));
    const transaction = {
      $queryRaw: vi.fn().mockResolvedValue([{ now: databaseNow }]),
      analyticsRetentionState: {
        upsert: vi.fn().mockResolvedValue({
          activeRunId: null,
          purgedBefore: null,
        }),
        updateMany: vi.fn().mockResolvedValue({ count: 1 }),
      },
      analyticsRetentionRun: { create },
      analyticsCapabilityActivation: {
        findUnique: vi.fn().mockResolvedValue(null),
      },
    };
    const client = {
      $transaction: vi.fn(
        (execute: (value: typeof transaction) => Promise<unknown>) =>
          execute(transaction),
      ),
    };
    mocks.acquireMutationPermit.mockResolvedValue({
      outcome: "allowed",
      checkpointGeneration: null,
    });

    await expect(
      startOrResumeAnalyticsRetention({
        client: client as never,
        retentionDays: 30,
        stateId: analyticsProjectRetentionStateId("project-1"),
      }),
    ).resolves.toMatchObject({
      cutoffDate: new Date("2026-06-20T00:00:00.000Z"),
      startedAt: databaseNow,
    });
    expect(create).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          cutoffDate: new Date("2026-06-20T00:00:00.000Z"),
          startedAt: databaseNow,
        }),
      }),
    );
    expect(transaction.analyticsRetentionState.upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: "project:project-1" },
      }),
    );
  });

  it("returns the strongest durable or active retention barrier", async () => {
    const client = {
      analyticsRetentionState: {
        findUnique: vi.fn().mockResolvedValue({
          purgedBefore: new Date("2026-05-01T00:00:00.000Z"),
          activeCutoff: new Date("2026-06-01T00:00:00.000Z"),
        }),
      },
    };

    await expect(
      getAnalyticsRetentionBarrier({ client: client as never }),
    ).resolves.toEqual(new Date("2026-06-01T00:00:00.000Z"));
  });

  it("combines the global and project retention barriers", async () => {
    const findUnique = vi.fn(async ({ where: { id } }) =>
      id === "global"
        ? {
            purgedBefore: new Date("2026-05-01T00:00:00.000Z"),
            activeCutoff: null,
          }
        : {
            purgedBefore: new Date("2026-06-01T00:00:00.000Z"),
            activeCutoff: new Date("2026-07-01T00:00:00.000Z"),
          },
    );
    const client = {
      analyticsRetentionState: { findUnique },
    };

    await expect(
      getAnalyticsRetentionBarrier({
        client: client as never,
        projectId: "project-1",
      }),
    ).resolves.toEqual(new Date("2026-07-01T00:00:00.000Z"));
    expect(findUnique).toHaveBeenCalledWith(
      expect.objectContaining({ where: { id: "project:project-1" } }),
    );
  });

  it("completes the retention state that owns the run", async () => {
    const cutoffDate = new Date("2026-07-01T00:00:00.000Z");
    const updateState = vi.fn(async () => undefined);
    const transaction = {
      $queryRaw: vi.fn(async () => [
        { now: new Date("2026-07-28T00:00:00.000Z") },
      ]),
      analyticsCapabilityActivation: {
        findUnique: vi.fn(async () => null),
      },
      analyticsRetentionState: {
        findUnique: vi.fn(async () => ({
          id: "project:project-1",
          activeRunId: "run-1",
          purgedBefore: null,
        })),
        update: updateState,
      },
      analyticsRetentionRun: {
        findUniqueOrThrow: vi.fn(async () => ({
          id: "run-1",
          status: "RUNNING",
          phase: "COMPLETE",
          cutoffDate,
        })),
        update: vi.fn(async () => undefined),
      },
    };
    mocks.acquireMutationPermit.mockResolvedValue({
      outcome: "allowed",
      checkpointGeneration: null,
    });

    await expect(
      completeAnalyticsRetentionRun({
        client: transaction as never,
        runId: "run-1",
      }),
    ).resolves.toBe(true);
    expect(updateState).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { id: "project:project-1" },
        data: expect.objectContaining({ purgedBefore: cutoffDate }),
      }),
    );
  });
});
