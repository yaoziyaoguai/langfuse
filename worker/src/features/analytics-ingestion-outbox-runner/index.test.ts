import { describe, expect, it, vi } from "vitest";

import { AnalyticsIngestionOutboxRunner } from ".";

describe("AnalyticsIngestionOutboxRunner", () => {
  it("drains full batches immediately with the durable worker identity", async () => {
    const publishBatch = vi.fn().mockResolvedValue(25);
    const runner = new AnalyticsIngestionOutboxRunner({
      workerId: "worker-1",
      intervalMs: 500,
      batchSize: 25,
      publishBatch,
    });

    await expect(runner.processBatch()).resolves.toBe(0);
    expect(publishBatch).toHaveBeenCalledWith({
      workerId: "worker-1",
      limit: 25,
    });
  });
});
