import type { PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  AnalyticsProjectDeletionInProgressError,
  findRecoverableDeletionOperations,
  scheduleTraceDeletionOperations,
} from "./analyticsDeletionOperations";

describe("findRecoverableDeletionOperations", () => {
  it("selects only stale, unleased retryable operations in stable order", async () => {
    const findMany = vi.fn().mockResolvedValue([]);
    const client = {
      analyticsDeletionOperation: { findMany },
    } as unknown as PrismaClient;
    const updatedBefore = new Date("2026-07-18T11:59:30.000Z");
    const leaseExpiredBefore = new Date("2026-07-18T12:00:00.000Z");

    await findRecoverableDeletionOperations({
      client,
      scopes: ["TRACE"],
      updatedBefore,
      leaseExpiredBefore,
      limit: 100,
    });

    expect(findMany).toHaveBeenCalledWith({
      where: {
        scope: { in: ["TRACE"] },
        status: { in: ["RETRYING", "SCHEDULED"] },
        completedAt: null,
        updatedAt: { lte: updatedBefore },
        OR: [
          { leaseOwner: null },
          { leaseExpiresAt: null },
          { leaseExpiresAt: { lte: leaseExpiredBefore } },
        ],
      },
      orderBy: [{ updatedAt: "asc" }, { id: "asc" }],
      take: 100,
    });
  });

  it("rejects an empty recovery scope", () => {
    expect(() =>
      findRecoverableDeletionOperations({
        client: {} as PrismaClient,
        scopes: [],
        updatedBefore: new Date(),
        leaseExpiredBefore: new Date(),
        limit: 100,
      }),
    ).toThrow("Invalid deletion recovery query");
  });

  it("does not create trace deletion state after a project deletion fence exists", async () => {
    const projectFind = vi.fn();
    const transaction = {
      $queryRaw: vi.fn().mockResolvedValue([]),
      analyticsCheckpointGeneration: {
        findFirst: vi.fn().mockResolvedValue(null),
      },
      analyticsProjectDeletionGeneration: {
        findUnique: vi.fn().mockResolvedValue({ generation: 1n }),
      },
      project: { findFirstOrThrow: projectFind },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      scheduleTraceDeletionOperations({
        client,
        projectId: "project-1",
        organizationId: "org-1",
        traceIds: ["trace-1"],
        requester: { principalType: "system", principalId: "test" },
      }),
    ).rejects.toBeInstanceOf(AnalyticsProjectDeletionInProgressError);
    expect(projectFind).not.toHaveBeenCalled();
  });
});
