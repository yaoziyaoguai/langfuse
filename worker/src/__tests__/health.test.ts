import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  analyticsBackend: "doris" as "clickhouse" | "doris",
  checkAnalyticsReadiness: vi.fn(),
  ping: vi.fn().mockResolvedValue("PONG"),
  queryRaw: vi.fn().mockResolvedValue([{ one: 1 }]),
}));

vi.mock("../env", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return mocks.analyticsBackend;
    },
    NODE_ENV: "test",
    QUEUE_CONSUMER_EVENT_PROPAGATION_QUEUE_IS_ENABLED: "false",
    LANGFUSE_MIGRATION_V4_WRITE_MODE: "events",
    LANGFUSE_EVENT_PROPAGATION_STUCK_THRESHOLD_MINUTES: 15,
  },
}));
vi.mock("@langfuse/shared/src/db", () => ({
  prisma: { $queryRaw: mocks.queryRaw },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  checkAnalyticsReadiness: mocks.checkAnalyticsReadiness,
  DorisClientManager: {
    getInstance: () => ({ getClient: () => ({ query: vi.fn() }) }),
  },
  logger: { info: vi.fn(), warn: vi.fn() },
  parseDorisQueryConfig: vi.fn(() => ({})),
  resolveDorisNodeEnv: vi.fn((nodeEnv) => nodeEnv),
  PrismaAnalyticsCompatibilityControlState: class {},
  redis: { ping: mocks.ping },
  SUPPORTED_DORIS_CANONICALIZER_VERSIONS: ["1"],
  SUPPORTED_DORIS_SCHEMA_VERSIONS: [2],
}));
vi.mock("../features/eventPropagation/handleEventPropagationJob", () => ({
  getLastProcessedPartition: vi.fn().mockResolvedValue(null),
  getLastRunStartedAt: vi.fn().mockResolvedValue(null),
}));

import { checkContainerHealth } from "../features/health";

function response() {
  const res = {
    status: vi.fn(),
    json: vi.fn(),
  };
  res.status.mockReturnValue(res);
  res.json.mockReturnValue(res);
  return res;
}

describe("worker Doris readiness", () => {
  beforeEach(() => {
    mocks.analyticsBackend = "doris";
    mocks.checkAnalyticsReadiness.mockReset();
  });

  it("keeps liveness independent from Doris", async () => {
    const res = response();

    await checkContainerHealth(res as never, { failOnSigterm: false });

    expect(res.json).toHaveBeenCalledWith({ status: "ok" });
    expect(mocks.checkAnalyticsReadiness).not.toHaveBeenCalled();
  });

  it("fails readiness with a sanitized schema status", async () => {
    mocks.checkAnalyticsReadiness.mockResolvedValue({
      ready: false,
      code: "SCHEMA_MISMATCH",
      schemaVersion: 1,
    });
    const res = response();

    await checkContainerHealth(res as never, { failOnSigterm: true });

    expect(res.status).toHaveBeenCalledWith(503);
    expect(res.json).toHaveBeenCalledWith({
      status: "Analytics readiness check failed",
      analytics: "SCHEMA_MISMATCH",
      schemaVersion: 1,
    });
  });
});
