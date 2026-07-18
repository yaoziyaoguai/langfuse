import { describe, expect, it, vi } from "vitest";

import { PrismaAnalyticsCompatibilityControlState } from "../compatibility";

describe("PrismaAnalyticsCompatibilityControlState", () => {
  it("counts every non-expired receipt outside the supported contract set", async () => {
    const count = vi.fn().mockResolvedValue(2);
    const state = new PrismaAnalyticsCompatibilityControlState({
      analyticsIngestionOperation: { count },
    } as never);
    const now = new Date("2026-07-18T12:00:00.000Z");

    await expect(
      state.countIncompatibleRecoverableOperations({
        supportedCanonicalizerVersions: ["1", "2"],
        supportedSchemaVersions: [2],
        now,
      }),
    ).resolves.toBe(2);
    expect(count).toHaveBeenCalledWith({
      where: {
        recoverableUntil: { gt: now },
        OR: [
          { canonicalizerVersion: { notIn: ["1", "2"] } },
          { schemaVersion: { notIn: [2] } },
        ],
      },
    });
  });
});
