import { describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  sessionCount: vi.fn(),
  traceSessionFindFirst: vi.fn(),
}));

vi.mock("../../db", () => ({
  prisma: {
    traceSession: { findFirst: mocks.traceSessionFindFirst },
  },
}));

vi.mock("./telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => ({
    sessions: { count: mocks.sessionCount },
  }),
}));

import { hasAnySession } from "./trace-sessions";

describe("trace session repository Doris routing", () => {
  it("derives session existence from Doris events", async () => {
    mocks.sessionCount.mockResolvedValue(1);

    await expect(hasAnySession("project-1")).resolves.toBe(true);

    expect(mocks.sessionCount).toHaveBeenCalledWith({
      projectId: "project-1",
      range: { from: new Date(0), to: expect.any(Date) },
      filters: [],
    });
    expect(mocks.traceSessionFindFirst).not.toHaveBeenCalled();
  });
});
