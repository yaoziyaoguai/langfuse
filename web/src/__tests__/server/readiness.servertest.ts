import type { NextApiRequest, NextApiResponse } from "next";
import { createMocks } from "node-mocks-http";

const mocks = vi.hoisted(() => ({
  analyticsBackend: "doris" as "clickhouse" | "doris",
  checkAnalyticsReadiness: vi.fn(),
  runtimeReady: vi.fn(),
}));

vi.mock("@/src/env.mjs", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return mocks.analyticsBackend;
    },
    NODE_ENV: "test",
  },
}));
vi.mock("@/src/features/public-api/server/cors", () => ({
  cors: vi.fn(),
  runMiddleware: vi.fn().mockResolvedValue(undefined),
}));
vi.mock("@/src/features/telemetry", () => ({
  telemetry: vi.fn().mockResolvedValue(undefined),
}));
vi.mock("@/src/utils/shutdown", () => ({ isSigtermReceived: () => false }));
vi.mock("@/src/server/analyticsRuntime", () => ({
  checkWebAnalyticsRuntimeReadiness: mocks.runtimeReady,
}));
vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));
vi.mock("@langfuse/shared/src/server", () => ({
  checkAnalyticsReadiness: mocks.checkAnalyticsReadiness,
  DorisClientManager: {
    getInstance: () => ({ getClient: () => ({ query: vi.fn() }) }),
  },
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
  redis: null,
  logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn() },
  parseDorisQueryConfig: vi.fn(() => ({})),
  resolveDorisNodeEnv: vi.fn((nodeEnv) => nodeEnv),
  PrismaAnalyticsCompatibilityControlState: class {},
  SUPPORTED_DORIS_CANONICALIZER_VERSIONS: ["1"],
  SUPPORTED_DORIS_SCHEMA_VERSIONS: [2],
  traceException: vi.fn(),
}));

import handler from "@/src/pages/api/public/ready";

async function callHandler(query?: NextApiRequest["query"]) {
  const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
    method: "GET",
    query,
  });
  await handler(req, res);
  return res;
}

describe("public readiness", () => {
  beforeEach(() => {
    mocks.analyticsBackend = "doris";
    mocks.checkAnalyticsReadiness.mockReset();
    mocks.runtimeReady.mockReset().mockResolvedValue(true);
  });

  it("returns a sanitized 503 when Doris schema readiness fails", async () => {
    mocks.checkAnalyticsReadiness.mockResolvedValue({
      ready: false,
      code: "SCHEMA_MISMATCH",
      schemaVersion: 1,
    });

    const res = await callHandler();

    expect(res._getStatusCode()).toBe(503);
    expect(res._getJSONData()).toMatchObject({
      status: "Analytics readiness check failed",
      analytics: "SCHEMA_MISMATCH",
      schemaVersion: 1,
    });
  });

  it("checks the runtime without probing Doris when ClickHouse is selected", async () => {
    mocks.analyticsBackend = "clickhouse";

    const res = await callHandler();

    expect(res._getStatusCode()).toBe(200);
    expect(mocks.checkAnalyticsReadiness).not.toHaveBeenCalled();
    expect(mocks.runtimeReady).toHaveBeenCalledOnce();
  });

  it("fails closed when the caller expects the other analytics backend", async () => {
    mocks.analyticsBackend = "clickhouse";

    const res = await callHandler({ analyticsBackend: "doris" });

    expect(res._getStatusCode()).toBe(503);
    expect(res._getJSONData()).toMatchObject({
      status: "Expected analytics backend is not selected",
    });
    expect(mocks.checkAnalyticsReadiness).not.toHaveBeenCalled();
    expect(mocks.runtimeReady).not.toHaveBeenCalled();
  });

  it("fails readiness for either backend when the runtime lease is not ready", async () => {
    mocks.analyticsBackend = "clickhouse";
    mocks.runtimeReady.mockResolvedValue(false);

    const res = await callHandler();

    expect(res._getStatusCode()).toBe(503);
    expect(res._getJSONData()).toMatchObject({
      status: "Analytics runtime readiness check failed",
    });
    expect(mocks.checkAnalyticsReadiness).not.toHaveBeenCalled();
  });
});
