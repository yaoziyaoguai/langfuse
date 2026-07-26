import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  closeClickhouse: vi.fn(),
  closeDoris: vi.fn(),
  disconnectPrisma: vi.fn(),
  disconnectRedis: vi.fn(),
  loggerError: vi.fn(),
  quiesceWebAnalyticsRuntime: vi.fn(),
  shutdownRateLimit: vi.fn(),
}));

const state = vi.hoisted(() => ({
  analyticsBackend: "doris",
  redisPresent: true,
  redisStatus: "ready",
}));

vi.mock("@langfuse/shared/src/server", () => ({
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: mocks.closeClickhouse }),
  },
  DorisClientManager: {
    getInstance: () => ({ closeAllConnections: mocks.closeDoris }),
  },
  logger: { debug: vi.fn(), error: mocks.loggerError, info: vi.fn() },
  get redis() {
    return state.redisPresent
      ? { status: state.redisStatus, disconnect: mocks.disconnectRedis }
      : null;
  },
}));
vi.mock("@langfuse/shared/src/db", () => ({
  prisma: { $disconnect: mocks.disconnectPrisma },
}));
vi.mock("@/src/features/public-api/server/RateLimitService", () => ({
  RateLimitService: { shutdown: mocks.shutdownRateLimit },
}));
vi.mock("@/src/server/analyticsRuntime", () => ({
  quiesceWebAnalyticsRuntime: mocks.quiesceWebAnalyticsRuntime,
}));
vi.mock("@/src/env.mjs", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return state.analyticsBackend;
    },
  },
}));

import { shutdown } from "@/src/utils/shutdown";

describe("web shutdown", () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.clearAllMocks();
    state.analyticsBackend = "doris";
    state.redisPresent = true;
    state.redisStatus = "ready";
    mocks.closeClickhouse.mockResolvedValue(undefined);
    mocks.closeDoris.mockResolvedValue(undefined);
    mocks.disconnectPrisma.mockResolvedValue(undefined);
    mocks.quiesceWebAnalyticsRuntime.mockResolvedValue(true);
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it("quiesces the runtime after stopping work and before Prisma disconnects", async () => {
    const shutdownPromise = shutdown("SIGTERM");

    await vi.advanceTimersByTimeAsync(110_000);
    await shutdownPromise;

    expect(mocks.shutdownRateLimit).toHaveBeenCalledOnce();
    expect(mocks.quiesceWebAnalyticsRuntime).toHaveBeenCalledOnce();
    expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
    expect(mocks.shutdownRateLimit.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.quiesceWebAnalyticsRuntime.mock.invocationCallOrder[0] ?? 0,
    );
    expect(
      mocks.quiesceWebAnalyticsRuntime.mock.invocationCallOrder[0],
    ).toBeLessThan(mocks.disconnectPrisma.mock.invocationCallOrder[0] ?? 0);
  });

  it.each([
    { expectedState: "absent", redisPresent: false, redisStatus: "ready" },
    { expectedState: "end", redisPresent: true, redisStatus: "end" },
  ])(
    "still disconnects Prisma and resolves with Redis $expectedState",
    async ({ redisPresent, redisStatus }) => {
      state.redisPresent = redisPresent;
      state.redisStatus = redisStatus;
      const shutdownPromise = shutdown("SIGTERM");

      await vi.advanceTimersByTimeAsync(110_000);

      expect(mocks.disconnectRedis).not.toHaveBeenCalled();
      expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
      await shutdownPromise;
    },
  );

  it.each([
    {
      analyticsBackend: "clickhouse",
      selectedClose: mocks.closeClickhouse,
      unselectedClose: mocks.closeDoris,
    },
    {
      analyticsBackend: "doris",
      selectedClose: mocks.closeDoris,
      unselectedClose: mocks.closeClickhouse,
    },
  ])(
    "only closes $analyticsBackend analytics connections",
    async ({ analyticsBackend, selectedClose, unselectedClose }) => {
      state.analyticsBackend = analyticsBackend;
      const shutdownPromise = shutdown("SIGTERM");

      await vi.advanceTimersByTimeAsync(110_000);
      await shutdownPromise;

      expect(selectedClose).toHaveBeenCalledOnce();
      expect(unselectedClose).not.toHaveBeenCalled();
    },
  );

  it("disconnects Prisma even when runtime quiescence fails", async () => {
    mocks.quiesceWebAnalyticsRuntime.mockRejectedValue(
      new Error("lease database unavailable"),
    );
    const shutdownPromise = shutdown("SIGTERM");

    await vi.advanceTimersByTimeAsync(110_000);
    await shutdownPromise;

    expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
    expect(mocks.loggerError).toHaveBeenCalledWith(
      "Failed to quiesce analytics runtime lease",
      expect.any(Error),
    );
  });

  it("reports a refused runtime quiesce while continuing cleanup", async () => {
    mocks.quiesceWebAnalyticsRuntime.mockResolvedValue(false);
    const shutdownPromise = shutdown("SIGTERM");

    await vi.advanceTimersByTimeAsync(110_000);
    await shutdownPromise;

    expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
    expect(mocks.loggerError).toHaveBeenCalledWith(
      "Failed to quiesce analytics runtime lease",
      expect.any(Error),
    );
  });

  it("continues cleanup without quiescing when the selected backend does not close", async () => {
    mocks.closeDoris.mockRejectedValue(new Error("pool close failed"));
    const shutdownPromise = shutdown("SIGTERM");

    await vi.advanceTimersByTimeAsync(110_000);
    await shutdownPromise;

    expect(mocks.quiesceWebAnalyticsRuntime).not.toHaveBeenCalled();
    expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
    expect(mocks.loggerError).toHaveBeenCalledWith(
      "Failed to close Doris analytics connections",
      expect.any(Error),
    );
  }, 1_000);

  it("resolves shutdown when Prisma disconnect fails", async () => {
    mocks.disconnectPrisma.mockRejectedValue(
      new Error("Prisma disconnect failed"),
    );
    const shutdownPromise = shutdown("SIGTERM");

    await vi.advanceTimersByTimeAsync(110_000);
    await shutdownPromise;

    expect(mocks.loggerError).toHaveBeenCalledWith(
      "Failed to disconnect Prisma",
      expect.any(Error),
    );
  }, 1_000);
});
