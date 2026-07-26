import { describe, expect, it, vi } from "vitest";

import { withBatchExportLeaseHeartbeat } from "./BatchExportLeaseHeartbeat";

describe("withBatchExportLeaseHeartbeat", () => {
  it("aborts in-flight work when a lease renewal is fenced", async () => {
    vi.useFakeTimers();
    const renewalFailure = new Error("lease fenced");
    const operation = withBatchExportLeaseHeartbeat({
      intervalMs: 1_000,
      renew: vi.fn().mockRejectedValue(renewalFailure),
      run: (signal) =>
        new Promise<void>((_resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
        }),
    });
    const expectation = expect(operation).rejects.toBe(renewalFailure);

    await vi.advanceTimersByTimeAsync(1_000);
    await expectation;
    vi.useRealTimers();
  });

  it("stops renewals after successful work", async () => {
    vi.useFakeTimers();
    const renew = vi.fn().mockResolvedValue(undefined);
    await expect(
      withBatchExportLeaseHeartbeat({
        intervalMs: 1_000,
        renew,
        run: async () => "done",
      }),
    ).resolves.toBe("done");
    await vi.advanceTimersByTimeAsync(2_000);
    expect(renew).not.toHaveBeenCalled();
    vi.useRealTimers();
  });
});
