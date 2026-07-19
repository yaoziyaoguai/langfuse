import type { PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  calculateAnalyticsIngestionRetryDelayMs,
  handoffLegacyAnalyticsIngestionOutbox,
} from "./analyticsIngestionOperations";

describe("calculateAnalyticsIngestionRetryDelayMs", () => {
  it("applies deterministic jitter to capped exponential backoff", () => {
    const first = calculateAnalyticsIngestionRetryDelayMs({
      operationId: "operation-1",
      generation: 1,
    });
    const second = calculateAnalyticsIngestionRetryDelayMs({
      operationId: "operation-1",
      generation: 2,
    });
    const capped = calculateAnalyticsIngestionRetryDelayMs({
      operationId: "operation-1",
      generation: 50,
    });

    expect(first).toBeGreaterThanOrEqual(3_750);
    expect(first).toBeLessThanOrEqual(6_250);
    expect(second).toBeGreaterThan(first);
    expect(capped).toBeGreaterThanOrEqual(22.5 * 60_000);
    expect(capped).toBeLessThanOrEqual(30 * 60_000);
    expect(
      calculateAnalyticsIngestionRetryDelayMs({
        operationId: "operation-1",
        generation: 50,
      }),
    ).toBe(capped);
  });

  it("rejects invalid generations", () => {
    expect(() =>
      calculateAnalyticsIngestionRetryDelayMs({
        operationId: "operation-1",
        generation: 0,
      }),
    ).toThrow("Invalid analytics ingestion retry generation");
  });
});

describe("handoffLegacyAnalyticsIngestionOutbox", () => {
  it("moves a legacy row to V2 under the operation lock", async () => {
    const now = new Date("2026-07-18T12:05:00.000Z");
    const createV2 = vi.fn().mockResolvedValue({ id: "v2-1" });
    const deleteLegacy = vi.fn().mockResolvedValue({ count: 1 });
    const findMany = vi.fn().mockResolvedValue([
      {
        id: "legacy-1",
        operationId: "operation-1",
        operation: { projectId: "project-1" },
      },
    ]);
    let claimId = "";
    const claimRows = vi.fn(
      async ({ data }: { data: { lockedBy: string } }) => {
        claimId = data.lockedBy;
        return { count: 1 };
      },
    );
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ acquired: true }])
        .mockResolvedValueOnce([{ id: "operation-1" }]),
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue({
          id: "operation-1",
          projectId: "project-1",
          terminalAt: null,
        }),
      },
      analyticsIngestionOutbox: {
        findMany,
        updateMany: claimRows,
        findUnique: vi.fn(async () => ({
          id: "legacy-1",
          attempts: 3,
          lockedBy: claimId,
        })),
        deleteMany: deleteLegacy,
      },
      analyticsIngestionOutboxV2: {
        findUnique: vi.fn().mockResolvedValue(null),
        create: createV2,
      },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      handoffLegacyAnalyticsIngestionOutbox({ client, now, limit: 10 }),
    ).resolves.toBe(1);
    expect(findMany).toHaveBeenCalledWith({
      where: {
        operation: { terminalAt: null },
        OR: [
          { lockedBy: null },
          { lockedBy: { not: { startsWith: "doris-handoff:" } } },
          { lockedUntil: null },
          { lockedUntil: { lte: now } },
        ],
      },
      orderBy: [{ updatedAt: "asc" }, { id: "asc" }],
      take: 10,
      select: {
        id: true,
        operationId: true,
        operation: { select: { projectId: true } },
      },
    });
    expect(claimRows).toHaveBeenCalledWith({
      where: { id: { in: ["legacy-1"] } },
      data: {
        lockedBy: expect.stringMatching(/^doris-handoff:/),
        lockedUntil: new Date("2026-07-18T12:10:00.000Z"),
      },
    });
    expect(createV2).toHaveBeenCalledWith({
      data: {
        operationId: "operation-1",
        status: "PENDING",
        generation: 1,
        attempts: 3,
        nextAttemptAt: now,
      },
    });
    expect(deleteLegacy).toHaveBeenCalledWith({
      where: { id: "legacy-1", operationId: "operation-1" },
    });
  });

  it("leaves a terminal legacy operation untouched", async () => {
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([{ acquired: true }])
        .mockResolvedValueOnce([{ id: "operation-1" }]),
      analyticsIngestionOperation: {
        findFirst: vi.fn().mockResolvedValue({
          id: "operation-1",
          projectId: "project-1",
          terminalAt: new Date("2026-07-18T12:00:00.000Z"),
        }),
      },
      analyticsIngestionOutbox: {
        findMany: vi.fn().mockResolvedValue([
          {
            id: "legacy-1",
            operationId: "operation-1",
            operation: { projectId: "project-1" },
          },
        ]),
        updateMany: vi.fn().mockResolvedValue({ count: 1 }),
        findUnique: vi.fn(),
        deleteMany: vi.fn(),
      },
      analyticsIngestionOutboxV2: {
        findUnique: vi.fn(),
        create: vi.fn(),
      },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      handoffLegacyAnalyticsIngestionOutbox({
        client,
        now: new Date("2026-07-18T12:05:00.000Z"),
        limit: 10,
      }),
    ).resolves.toBe(0);
    expect(
      transaction.analyticsIngestionOutbox.findUnique,
    ).not.toHaveBeenCalled();
    expect(
      transaction.analyticsIngestionOutboxV2.create,
    ).not.toHaveBeenCalled();
    expect(
      transaction.analyticsIngestionOutbox.deleteMany,
    ).not.toHaveBeenCalled();
  });

  it("does not scan when another replica owns the handoff lock", async () => {
    const findMany = vi.fn();
    const transaction = {
      $queryRaw: vi.fn().mockResolvedValue([{ acquired: false }]),
      analyticsIngestionOutbox: { findMany },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      handoffLegacyAnalyticsIngestionOutbox({
        client,
        now: new Date("2026-07-18T12:05:00.000Z"),
        limit: 10,
      }),
    ).resolves.toBe(0);
    expect(findMany).not.toHaveBeenCalled();
  });
});
