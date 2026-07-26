import { describe, expect, it, vi } from "vitest";

import { AnalyticsIngestionRawReconciler } from ".";

describe("AnalyticsIngestionRawReconciler", () => {
  it("runs recovery only after readiness and immediately repeats a full batch", async () => {
    const order: string[] = [];
    const reconcile = vi.fn(async () => {
      order.push("reconcile");
      return {
        scanned: 25,
        recovered: 25,
        existing: 0,
        invalid: 0,
        nextCursor: "page-2",
      };
    });
    const runner = new AnalyticsIngestionRawReconciler({
      intervalMs: 60_000,
      batchSize: 25,
      assertReady: vi.fn(async () => {
        order.push("ready");
      }),
      reconcile,
    });

    await expect(runner.processBatch()).resolves.toBe(0);
    expect(order).toEqual(["ready", "reconcile"]);
    expect(reconcile).toHaveBeenCalledWith(25, undefined);
  });

  it("does not inspect object storage while readiness is closed", async () => {
    const reconcile = vi.fn();
    const runner = new AnalyticsIngestionRawReconciler({
      intervalMs: 60_000,
      batchSize: 25,
      assertReady: vi.fn(async () => {
        throw new Error("not ready");
      }),
      reconcile,
    });

    await expect(runner.processBatch()).rejects.toThrow("not ready");
    expect(reconcile).not.toHaveBeenCalled();
  });

  it("retains the continuation cursor while invalid work waits for the regular interval", async () => {
    const reconcile = vi
      .fn()
      .mockResolvedValueOnce({
        scanned: 25,
        recovered: 0,
        existing: 0,
        invalid: 25,
        nextCursor: "page-2",
      })
      .mockResolvedValueOnce({
        scanned: 1,
        recovered: 1,
        existing: 0,
        invalid: 0,
      });
    const runner = new AnalyticsIngestionRawReconciler({
      intervalMs: 60_000,
      batchSize: 25,
      assertReady: vi.fn(async () => undefined),
      reconcile,
    });

    await expect(runner.processBatch()).resolves.toBeUndefined();
    await expect(runner.processBatch()).resolves.toBeUndefined();

    expect(reconcile).toHaveBeenNthCalledWith(1, 25, undefined);
    expect(reconcile).toHaveBeenNthCalledWith(2, 25, "page-2");
  });

  it("immediately advances across an existing-only page", async () => {
    const reconcile = vi.fn(async () => ({
      scanned: 0,
      recovered: 0,
      existing: 0,
      invalid: 0,
      nextCursor: "page-2",
    }));
    const runner = new AnalyticsIngestionRawReconciler({
      intervalMs: 60_000,
      batchSize: 25,
      assertReady: vi.fn(async () => undefined),
      reconcile,
    });

    await expect(runner.processBatch()).resolves.toBe(0);
    expect(reconcile).toHaveBeenCalledWith(25, undefined);
  });
});
