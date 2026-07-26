import { describe, expect, it, vi } from "vitest";

import { resolveAnalyticsRuntimeWorkloadEpoch } from "./analyticsWorkloadEpoch";

describe("resolveAnalyticsRuntimeWorkloadEpoch", () => {
  it("keeps legacy startup when no epoch source is configured", async () => {
    await expect(resolveAnalyticsRuntimeWorkloadEpoch({})).resolves.toBe(
      undefined,
    );
  });

  it("trims a direct epoch without exposing it to another reader", async () => {
    const readTextFile = vi.fn();

    await expect(
      resolveAnalyticsRuntimeWorkloadEpoch({
        value: "  direct-epoch\n",
        readTextFile,
      }),
    ).resolves.toBe("direct-epoch");
    expect(readTextFile).not.toHaveBeenCalled();
  });

  it("reads and trims an epoch from an absolute mounted file", async () => {
    const readTextFile = vi.fn().mockResolvedValue("mounted-epoch\n");

    await expect(
      resolveAnalyticsRuntimeWorkloadEpoch({
        file: "/run/secrets/analytics-workload-epoch",
        readTextFile,
      }),
    ).resolves.toBe("mounted-epoch");
    expect(readTextFile).toHaveBeenCalledWith(
      "/run/secrets/analytics-workload-epoch",
    );
  });

  it("rejects ambiguous, relative, empty, and oversized sources", async () => {
    await expect(
      resolveAnalyticsRuntimeWorkloadEpoch({
        value: "direct",
        file: "/run/secrets/epoch",
      }),
    ).rejects.toThrow(/only one/i);
    await expect(
      resolveAnalyticsRuntimeWorkloadEpoch({ file: "relative/epoch" }),
    ).rejects.toThrow(/absolute path/i);
    await expect(
      resolveAnalyticsRuntimeWorkloadEpoch({ value: "   " }),
    ).rejects.toThrow(/must not be empty/i);
    await expect(
      resolveAnalyticsRuntimeWorkloadEpoch({ value: "x".repeat(4_097) }),
    ).rejects.toThrow(/maximum supported size/i);
  });
});
