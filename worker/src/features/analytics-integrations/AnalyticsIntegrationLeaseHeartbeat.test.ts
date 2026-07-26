import { describe, expect, it, vi } from "vitest";

import { withAnalyticsIntegrationLeaseHeartbeat } from "./AnalyticsIntegrationLeaseHeartbeat";

describe("withAnalyticsIntegrationLeaseHeartbeat", () => {
  it("fails the operation after a fenced renewal instead of reporting success", async () => {
    vi.useFakeTimers();
    const renewalFailure = new Error("lease fenced");
    let finish!: () => void;
    const operation = withAnalyticsIntegrationLeaseHeartbeat({
      intervalMs: 1_000,
      renew: vi.fn().mockRejectedValue(renewalFailure),
      run: () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    });
    const expectation = expect(operation).rejects.toBe(renewalFailure);

    await vi.advanceTimersByTimeAsync(1_000);
    finish();
    await expectation;
    vi.useRealTimers();
  });

  it("stops renewals after successful work", async () => {
    vi.useFakeTimers();
    const renew = vi.fn().mockResolvedValue(undefined);
    await expect(
      withAnalyticsIntegrationLeaseHeartbeat({
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
