import { describe, expect, it, vi } from "vitest";

import { AnalyticsIngestionOutboxRunner } from ".";

describe("AnalyticsIngestionOutboxRunner", () => {
  it("drains full batches immediately with the durable worker identity", async () => {
    const now = new Date("2026-07-18T13:00:00.000Z");
    const recoverStale = vi.fn().mockResolvedValue(0);
    const publishBatch = vi.fn().mockResolvedValue(25);
    const runner = new AnalyticsIngestionOutboxRunner({
      workerId: "worker-1",
      intervalMs: 500,
      batchSize: 25,
      publishBatch,
      recoverStale,
      now: () => now,
    });

    await expect(runner.processBatch()).resolves.toBe(0);
    expect(recoverStale).toHaveBeenCalledWith({
      now,
      updatedBefore: new Date("2026-07-18T12:58:00.000Z"),
      limit: 25,
    });
    expect(publishBatch).toHaveBeenCalledWith({
      workerId: "worker-1",
      limit: 25,
    });
  });

  it("does not publish while Doris readiness is closed", async () => {
    const publishBatch = vi.fn();
    const runner = new AnalyticsIngestionOutboxRunner({
      workerId: "worker-1",
      intervalMs: 500,
      batchSize: 25,
      publishBatch,
      recoverStale: vi.fn(),
      assertReady: vi.fn().mockRejectedValue(new Error("not ready")),
    });

    await expect(runner.processBatch()).rejects.toThrow("not ready");
    expect(publishBatch).not.toHaveBeenCalled();
  });

  it("immediately drains a full stale-recovery batch", async () => {
    const publishBatch = vi.fn().mockResolvedValue(0);
    const runner = new AnalyticsIngestionOutboxRunner({
      workerId: "worker-1",
      intervalMs: 500,
      batchSize: 25,
      publishBatch,
      recoverStale: vi.fn().mockResolvedValue(25),
    });

    await expect(runner.processBatch()).resolves.toBe(0);
    expect(publishBatch).toHaveBeenCalledOnce();
  });

  it("hands legacy rows off before recovering and publishing V2 rows", async () => {
    const now = new Date("2026-07-18T13:00:00.000Z");
    const handoffLegacy = vi.fn().mockResolvedValue(25);
    const recoverStale = vi.fn().mockResolvedValue(0);
    const publishBatch = vi.fn().mockResolvedValue(0);
    const runner = new AnalyticsIngestionOutboxRunner({
      workerId: "worker-1",
      intervalMs: 500,
      batchSize: 25,
      handoffLegacy,
      recoverStale,
      publishBatch,
      now: () => now,
    });

    await expect(runner.processBatch()).resolves.toBe(0);
    expect(handoffLegacy).toHaveBeenCalledWith({ now, limit: 25 });
    expect(handoffLegacy.mock.invocationCallOrder[0]).toBeLessThan(
      recoverStale.mock.invocationCallOrder[0]!,
    );
    expect(recoverStale.mock.invocationCallOrder[0]).toBeLessThan(
      publishBatch.mock.invocationCallOrder[0]!,
    );
  });
});
