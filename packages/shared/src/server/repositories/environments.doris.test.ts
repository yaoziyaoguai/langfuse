import { describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  filterOptionValues: vi.fn().mockResolvedValue([
    { column: "environment", value: "production", count: 2 },
    { column: "environment", value: "staging", count: 1 },
  ]),
}));

vi.mock("./telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => ({
    observations: { filterOptionValues: mocks.filterOptionValues },
  }),
}));

import { getEnvironmentsForProject } from "./environments";

describe("environment repository Doris routing", () => {
  it("reads bounded visible event environments and includes the default", async () => {
    const fromTimestamp = new Date("2026-07-17T00:00:00.000Z");

    await expect(
      getEnvironmentsForProject({ projectId: "project-1", fromTimestamp }),
    ).resolves.toEqual([
      { environment: "production" },
      { environment: "staging" },
      { environment: "default" },
    ]);
    expect(mocks.filterOptionValues).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        range: { from: fromTimestamp, to: expect.any(Date) },
        column: "environment",
        limit: 1_000,
      }),
    );
  });
});
