import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ backend: "clickhouse" }));
const mocks = vi.hoisted(() => ({
  assertDorisAnalyticsReady: vi.fn(),
  initializeWorkerAnalyticsRuntime: vi.fn(),
  initializeClickhouseCompatibility: vi.fn(),
  upsertDefaultModelPrices: vi.fn(),
  upsertManagedEvaluators: vi.fn(),
  upsertLangfuseDashboards: vi.fn(),
}));

vi.mock("./env", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return state.backend;
    },
  },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  initializeClickhouseCompatibility: mocks.initializeClickhouseCompatibility,
}));
vi.mock("./services/dorisAnalyticsReadiness", () => ({
  assertDorisAnalyticsReady: mocks.assertDorisAnalyticsReady,
}));
vi.mock("./analyticsRuntime", () => ({
  initializeWorkerAnalyticsRuntime: mocks.initializeWorkerAnalyticsRuntime,
}));
vi.mock("./scripts/upsertDefaultModelPrices", () => ({
  upsertDefaultModelPrices: mocks.upsertDefaultModelPrices,
}));
vi.mock("./scripts/upsertManagedEvaluators", () => ({
  upsertManagedEvaluators: mocks.upsertManagedEvaluators,
}));
vi.mock("./scripts/upsertLangfuseDashboards", () => ({
  upsertLangfuseDashboards: mocks.upsertLangfuseDashboards,
}));

import { initializeWorker } from "./initialize";

describe("initializeWorker analytics backend", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.initializeClickhouseCompatibility.mockResolvedValue(undefined);
    mocks.assertDorisAnalyticsReady.mockResolvedValue(undefined);
    mocks.initializeWorkerAnalyticsRuntime.mockResolvedValue(undefined);
  });

  it("initializes only ClickHouse when selected", async () => {
    state.backend = "clickhouse";

    await initializeWorker();

    expect(mocks.initializeClickhouseCompatibility).toHaveBeenCalledOnce();
    expect(mocks.assertDorisAnalyticsReady).not.toHaveBeenCalled();
    expect(mocks.initializeWorkerAnalyticsRuntime).toHaveBeenCalledOnce();
    expect(
      mocks.initializeClickhouseCompatibility.mock.invocationCallOrder[0],
    ).toBeLessThan(
      mocks.initializeWorkerAnalyticsRuntime.mock.invocationCallOrder[0] ?? 0,
    );
  });

  it("checks only Doris when selected", async () => {
    state.backend = "doris";

    await initializeWorker();

    expect(mocks.assertDorisAnalyticsReady).toHaveBeenCalledWith({
      force: true,
    });
    expect(mocks.initializeClickhouseCompatibility).not.toHaveBeenCalled();
    expect(mocks.initializeWorkerAnalyticsRuntime).toHaveBeenCalledOnce();
    expect(
      mocks.assertDorisAnalyticsReady.mock.invocationCallOrder[0],
    ).toBeLessThan(
      mocks.initializeWorkerAnalyticsRuntime.mock.invocationCallOrder[0] ?? 0,
    );
  });

  it("does not lease when the selected backend static check fails", async () => {
    state.backend = "clickhouse";
    mocks.initializeClickhouseCompatibility.mockRejectedValue(
      new Error("schema mismatch"),
    );

    await expect(initializeWorker()).rejects.toThrow("schema mismatch");

    expect(mocks.initializeWorkerAnalyticsRuntime).not.toHaveBeenCalled();
  });
});
