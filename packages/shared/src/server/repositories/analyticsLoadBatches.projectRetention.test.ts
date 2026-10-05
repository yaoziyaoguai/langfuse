import { describe, expect, it, vi } from "vitest";

vi.mock("../../db", () => ({ prisma: {} }));

import { cancelAnalyticsLoadBatchIfRetained } from "./analyticsLoadBatches";

describe("analytics load project retention revalidation", () => {
  it("cancels an old pending Doris load against the project cutoff", async () => {
    const findRetentionState = vi.fn(async ({ where: { id } }) =>
      id === "project:project-1"
        ? {
            purgedBefore: new Date("2026-07-01T00:00:00.000Z"),
            activeCutoff: null,
          }
        : null,
    );
    const transaction = {
      analyticsLoadBatch: {
        findFirstOrThrow: vi.fn(async () => ({
          status: "PENDING",
          partitionDate: new Date("2026-06-01T00:00:00.000Z"),
          fenceGeneration: 1n,
          leaseOwner: null,
        })),
        updateMany: vi.fn(async () => ({ count: 1 })),
      },
      analyticsRetentionState: {
        findUnique: findRetentionState,
      },
      analyticsIngestionCandidate: {
        findMany: vi.fn(async () => [{ id: "candidate-1" }]),
        updateMany: vi.fn(async () => ({ count: 1 })),
      },
    };
    const client = {
      $transaction: vi.fn(async (execute) => execute(transaction)),
    };

    await expect(
      cancelAnalyticsLoadBatchIfRetained({
        client: client as never,
        loadBatchId: "load-1",
        projectId: "project-1",
      }),
    ).resolves.toEqual({ outcome: "cancelled" });
    expect(findRetentionState).toHaveBeenCalledWith(
      expect.objectContaining({ where: { id: "project:project-1" } }),
    );
    expect(transaction.analyticsLoadBatch.updateMany).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          status: "CANCELLED_BY_DELETION",
          lastErrorCode: "RETENTION_BARRIER",
        }),
      }),
    );
  });
});
