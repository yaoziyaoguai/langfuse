import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ fenced: false }));
const mocks = vi.hoisted(() => ({
  queryRaw: vi.fn(),
}));

vi.mock("@/src/constants", () => ({ VERSION: "vtest" }));
vi.mock("@/src/env.mjs", () => ({
  env: { LANGFUSE_MIGRATION_V4_WRITE_MODE: "events" },
}));
vi.mock("@/src/server/analyticsRuntime", () => ({
  isWebAnalyticsRuntimeFenced: () => state.fenced,
}));
vi.mock("@langfuse/shared/src/db", () => ({
  prisma: { $queryRaw: mocks.queryRaw },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  convertDateToClickhouseDateTime: vi.fn(),
  logger: { debug: vi.fn(), error: vi.fn() },
  measureAndReturn: vi.fn(),
  queryClickhouse: vi.fn(),
  redis: null,
  traceException: vi.fn(),
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
}));

import { runHealthCheck } from "@/src/features/public-api/server/health-service";

describe("web health service analytics runtime fencing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    state.fenced = false;
  });

  it("fails liveness after the analytics runtime lease is fenced", async () => {
    state.fenced = true;

    await expect(
      runHealthCheck({
        failIfDatabaseUnavailable: false,
        failIfNoRecentEvents: false,
      }),
    ).resolves.toEqual({
      isHealthy: false,
      status: "Analytics runtime lease fenced",
      version: "test",
    });
    expect(mocks.queryRaw).not.toHaveBeenCalled();
  });
});
