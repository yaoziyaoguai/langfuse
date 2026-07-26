import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  initializeWebAnalyticsRuntime: vi.fn(),
  initializeModuleLoaded: vi.fn(),
  observabilityModuleLoaded: vi.fn(),
}));

vi.mock("@/src/server/analyticsRuntime", () => ({
  initializeWebAnalyticsRuntime: mocks.initializeWebAnalyticsRuntime,
}));
vi.mock("@/src/initialize", () => {
  mocks.initializeModuleLoaded();
  return {};
});
vi.mock("@/src/observability.config", () => {
  mocks.observabilityModuleLoaded();
  return {};
});
vi.mock("@langfuse/shared/src/server", () => ({
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
  logger: { debug: vi.fn() },
  redis: null,
}));

describe("node instrumentation", () => {
  const originalNextRuntime = process.env.NEXT_RUNTIME;
  const originalRunNextInit = process.env.NEXT_PUBLIC_LANGFUSE_RUN_NEXT_INIT;

  beforeEach(() => {
    vi.resetModules();
    vi.clearAllMocks();
    process.env.NEXT_RUNTIME = "nodejs";
  });

  afterEach(() => {
    if (originalNextRuntime === undefined) delete process.env.NEXT_RUNTIME;
    else process.env.NEXT_RUNTIME = originalNextRuntime;
    if (originalRunNextInit === undefined) {
      delete process.env.NEXT_PUBLIC_LANGFUSE_RUN_NEXT_INIT;
    } else {
      process.env.NEXT_PUBLIC_LANGFUSE_RUN_NEXT_INIT = originalRunNextInit;
    }
  });

  it("initializes the analytics runtime when optional init scripts are disabled", async () => {
    process.env.NEXT_PUBLIC_LANGFUSE_RUN_NEXT_INIT = "false";
    const { register } = await import("@/src/instrumentation");

    await register();

    expect(mocks.initializeWebAnalyticsRuntime).toHaveBeenCalledOnce();
    expect(mocks.initializeModuleLoaded).not.toHaveBeenCalled();
    expect(mocks.observabilityModuleLoaded).not.toHaveBeenCalled();
  });
});
