import type { NextApiRequest, NextApiResponse } from "next";
import { createMocks } from "node-mocks-http";

const mocks = vi.hoisted(() => ({
  checkAnalyticsReadiness: vi.fn(),
}));

vi.mock("@/src/env.mjs", () => ({
  env: {
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
vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));
vi.mock("@langfuse/shared/src/server", () => ({
  checkAnalyticsReadiness: mocks.checkAnalyticsReadiness,
  DorisClientManager: {
    getInstance: () => ({
      getClient: () => ({ query: vi.fn() }),
      closeAllConnections: vi.fn(),
    }),
  },
  redis: null,
  logger: { debug: vi.fn(), info: vi.fn(), warn: vi.fn() },
  parseDorisQueryConfig: vi.fn(() => ({})),
  resolveDorisNodeEnv: vi.fn(() => "test"),
  PrismaAnalyticsCompatibilityControlState: class {},
  SUPPORTED_DORIS_CANONICALIZER_VERSIONS: ["1"],
  SUPPORTED_DORIS_SCHEMA_VERSIONS: [2],
  traceException: vi.fn(),
}));

import handler from "@/src/pages/api/public/ready";

async function callHandler() {
  const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
    method: "GET",
  });
  await handler(req, res);
  return res;
}

describe("public readiness", () => {
  beforeEach(() => {
    mocks.checkAnalyticsReadiness.mockReset();
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
    });
    expect(res._getJSONData()).not.toHaveProperty("analytics");
    expect(res._getJSONData()).not.toHaveProperty("schemaVersion");
  });
});
