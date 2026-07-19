import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ backend: "clickhouse" }));
const mocks = vi.hoisted(() => ({
  assertDorisAnalyticsReady: vi.fn(),
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
  });

  it("initializes only ClickHouse when selected", async () => {
    state.backend = "clickhouse";

    await initializeWorker();

    expect(mocks.initializeClickhouseCompatibility).toHaveBeenCalledOnce();
    expect(mocks.assertDorisAnalyticsReady).not.toHaveBeenCalled();
  });

  it("checks only Doris when selected", async () => {
    state.backend = "doris";

    await initializeWorker();

    expect(mocks.assertDorisAnalyticsReady).toHaveBeenCalledWith({
      force: true,
    });
    expect(mocks.initializeClickhouseCompatibility).not.toHaveBeenCalled();
  });
});
