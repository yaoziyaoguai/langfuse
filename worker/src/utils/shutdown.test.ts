import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  closeClickhouse: vi.fn(),
  closeDoris: vi.fn(),
  closeWorkers: vi.fn(),
  disconnectPrisma: vi.fn(),
  order: [] as string[],
  loggerError: vi.fn(),
  quiesceRuntime: vi.fn(),
  redisDisconnect: vi.fn(),
  stopWorkloads: vi.fn(),
  shutdownWriter: vi.fn(),
}));

const state = vi.hoisted(() => ({
  analyticsBackend: "doris",
}));

vi.mock("@langfuse/shared/src/server", () => ({
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: mocks.closeClickhouse }),
  },
  DorisClientManager: {
    getInstance: () => ({ closeAllConnections: mocks.closeDoris }),
  },
  logger: {
    error: mocks.loggerError,
    info: vi.fn(),
  },
  redis: { disconnect: mocks.redisDisconnect },
}));
vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    $disconnect: mocks.disconnectPrisma,
  },
}));
vi.mock("@langfuse/shared/analytics-backend", () => ({
  isAnalyticsBackend: vi.fn((backend, expected) => backend === expected),
}));
vi.mock("../services/ClickhouseWriter", () => ({
  ClickhouseWriter: {
    getInstance: () => ({ shutdown: mocks.shutdownWriter }),
  },
}));
vi.mock("../features/health", () => ({ setSigtermReceived: vi.fn() }));
vi.mock("../index", () => ({ server: { close: vi.fn() } }));
vi.mock("../features/tokenisation/usage", () => ({
  freeAllTokenizers: vi.fn(),
}));
vi.mock("../features/tokenisation/async-usage", () => ({
  getTokenCountWorkerManager: () => ({ terminate: vi.fn() }),
}));
vi.mock("../queues/workerManager", () => ({
  WorkerManager: {
    closeWorkers: mocks.closeWorkers,
  },
}));
vi.mock("../features/blobstorage/inFlightExports", () => ({
  logInFlightBlobExportsOnShutdown: vi.fn(),
}));
vi.mock("../backgroundMigrations/backgroundMigrationManager", () => ({
  BackgroundMigrationManager: { close: vi.fn() },
}));
vi.mock("../app", () => ({
  stopWorkerWorkloads: mocks.stopWorkloads,
}));
vi.mock("../env", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return state.analyticsBackend;
    },
  },
}));
vi.mock("../analyticsRuntime", () => ({
  quiesceWorkerAnalyticsRuntime: mocks.quiesceRuntime,
}));

import { onShutdown } from "./shutdown";

describe("worker analytics runtime shutdown", () => {
  beforeEach(() => {
    mocks.order.length = 0;
    vi.clearAllMocks();
    state.analyticsBackend = "doris";
    mocks.closeWorkers.mockImplementation(async () => {
      mocks.order.push("workers");
    });
    mocks.disconnectPrisma.mockImplementation(async () => {
      mocks.order.push("prisma");
    });
    mocks.quiesceRuntime.mockImplementation(async () => {
      mocks.order.push("quiesce");
      return true;
    });
    mocks.stopWorkloads.mockImplementation(async () => {
      mocks.order.push("workloads");
    });
    mocks.shutdownWriter.mockResolvedValue(undefined);
  });

  it("stops work before quiescing and disconnects Prisma last", async () => {
    await onShutdown("SIGTERM");

    expect(mocks.order).toEqual(
      expect.arrayContaining(["workloads", "workers", "quiesce", "prisma"]),
    );
    expect(mocks.order.indexOf("workloads")).toBeLessThan(
      mocks.order.indexOf("quiesce"),
    );
    expect(mocks.order.indexOf("workers")).toBeLessThan(
      mocks.order.indexOf("quiesce"),
    );
    expect(mocks.order.indexOf("quiesce")).toBeLessThan(
      mocks.order.indexOf("prisma"),
    );
  });

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

      await onShutdown("SIGTERM");

      expect(selectedClose).toHaveBeenCalledOnce();
      expect(unselectedClose).not.toHaveBeenCalled();
    },
  );

  it("continues cleanup without quiescing when worker drain fails", async () => {
    mocks.closeWorkers.mockRejectedValue(new Error("worker close failed"));

    await onShutdown("SIGTERM");

    expect(mocks.quiesceRuntime).not.toHaveBeenCalled();
    expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
    expect(mocks.loggerError).toHaveBeenCalledWith(
      "Failed to drain worker workloads",
      expect.any(Error),
    );
  });

  it("does not quiesce when the ClickHouse writer cannot drain", async () => {
    state.analyticsBackend = "clickhouse";
    mocks.shutdownWriter.mockRejectedValue(new Error("writer did not drain"));

    await onShutdown("SIGTERM");

    expect(mocks.quiesceRuntime).not.toHaveBeenCalled();
    expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
    expect(mocks.loggerError).toHaveBeenCalledWith(
      "Failed to shut down Clickhouse writer",
      expect.any(Error),
    );
  });

  it("reports a refused runtime quiesce while continuing cleanup", async () => {
    mocks.quiesceRuntime.mockResolvedValue(false);

    await onShutdown("SIGTERM");

    expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
    expect(mocks.loggerError).toHaveBeenCalledWith(
      "Failed to quiesce analytics runtime lease",
      expect.any(Error),
    );
  });

  it("shares one shutdown operation across concurrent signals", async () => {
    let releaseWorkers!: () => void;
    mocks.closeWorkers.mockImplementation(
      () =>
        new Promise<void>((resolve) => {
          releaseWorkers = resolve;
        }),
    );

    const first = onShutdown("SIGTERM");
    const second = onShutdown("SIGINT");
    await new Promise((resolve) => setImmediate(resolve));

    expect(mocks.closeWorkers).toHaveBeenCalledOnce();
    expect(mocks.stopWorkloads).toHaveBeenCalledOnce();

    releaseWorkers();
    await Promise.all([first, second]);
    expect(mocks.disconnectPrisma).toHaveBeenCalledOnce();
  });
});
