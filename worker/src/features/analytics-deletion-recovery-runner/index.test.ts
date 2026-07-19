import type { AnalyticsDeletionOperation } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import { AnalyticsDeletionRecoveryRunner } from ".";

function operation(
  id: string,
  scope: "TRACE" | "PROJECT" = "TRACE",
): AnalyticsDeletionOperation {
  const now = new Date("2026-07-18T00:00:00.000Z");
  return {
    id,
    scope,
    organizationId: "org-1",
    projectId: "project-1",
    traceId: scope === "TRACE" ? `trace-${id}` : null,
    generation: 1n,
    checkpointGeneration: 0n,
    workerFence: 0n,
    leaseOwner: null,
    leaseExpiresAt: null,
    requesterPrincipalType: "system",
    requesterPrincipalId: "test",
    status: "RETRYING",
    phase: "visibility_barrier",
    logicallyInvisible: false,
    cancellationReasonCode: null,
    statusExpiresAt: new Date("2026-08-18T00:00:00.000Z"),
    completedAt: null,
    createdAt: now,
    updatedAt: now,
  };
}

describe("AnalyticsDeletionRecoveryRunner", () => {
  it("reprocesses stale unleased operations and immediately drains a full successful batch", async () => {
    const now = new Date("2026-07-18T12:00:00.000Z");
    const operations = [operation("one"), operation("two", "PROJECT")];
    const findRecoverableOperations = vi.fn().mockResolvedValue(operations);
    const processOperation = vi.fn().mockResolvedValue(undefined);
    const runner = new AnalyticsDeletionRecoveryRunner({
      intervalMs: 30_000,
      batchSize: 2,
      findRecoverableOperations,
      processOperation,
      now: () => now,
    });

    await expect(runner.processBatch()).resolves.toBe(0);
    expect(findRecoverableOperations).toHaveBeenCalledWith({
      updatedBefore: new Date("2026-07-18T11:58:00.000Z"),
      leaseExpiredBefore: now,
      limit: 2,
    });
    expect(processOperation.mock.calls.map(([item]) => item.id)).toEqual([
      "one",
      "two",
    ]);
  });

  it("isolates operation failures and waits before retrying them", async () => {
    const operations = [operation("one"), operation("two")];
    const processOperation = vi
      .fn()
      .mockRejectedValueOnce(new Error("Doris unavailable"))
      .mockResolvedValueOnce(undefined);
    const runner = new AnalyticsDeletionRecoveryRunner({
      intervalMs: 30_000,
      batchSize: 2,
      findRecoverableOperations: vi.fn().mockResolvedValue(operations),
      processOperation,
    });

    await expect(runner.processBatch()).resolves.toBeUndefined();
    expect(processOperation).toHaveBeenCalledTimes(2);
  });

  it("does not query operations while Doris readiness is closed", async () => {
    const findRecoverableOperations = vi.fn();
    const runner = new AnalyticsDeletionRecoveryRunner({
      intervalMs: 30_000,
      batchSize: 100,
      findRecoverableOperations,
      processOperation: vi.fn(),
      assertReady: vi.fn().mockRejectedValue(new Error("not ready")),
    });

    await expect(runner.processBatch()).rejects.toThrow("not ready");
    expect(findRecoverableOperations).not.toHaveBeenCalled();
  });
});
