import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  createClient: vi.fn(),
  close: vi.fn(async () => undefined),
  query: vi.fn(),
  env: {
    CLICKHOUSE_URL: "https://primary.internal:8123",
    CLICKHOUSE_READ_ONLY_URL: "https://readonly.internal:8123",
    CLICKHOUSE_USER: "probe-user",
    CLICKHOUSE_PASSWORD: "secret-a",
    CLICKHOUSE_DB: "langfuse",
  },
}));

vi.mock("../../env", () => ({ env: mocks.env }));
vi.mock("@clickhouse/client", () => ({ createClient: mocks.createClient }));

import {
  CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES,
  probeClickHouseAnalyticsBackendEmptiness,
} from "./emptiness";
import {
  fenceAnalyticsRuntimeIo,
  resetAnalyticsRuntimeIoFenceForTests,
} from "../analytics-persistence/analyticsRuntimeIoFence";

describe("ClickHouse production emptiness executor", () => {
  beforeEach(() => {
    resetAnalyticsRuntimeIoFenceForTests();
    mocks.createClient.mockReset();
    mocks.close.mockClear();
    mocks.query.mockReset();
    mocks.env.CLICKHOUSE_READ_ONLY_URL = "https://readonly.internal:8123";
    mocks.env.CLICKHOUSE_PASSWORD = "secret-a";

    mocks.query.mockImplementation(async ({ query }: { query: string }) => ({
      json: async () => {
        if (query.includes("FROM schema_migrations")) {
          return [{ version: 36, dirty: 0 }];
        }
        if (query.includes("FROM system.tables")) {
          return [
            { name: "schema_migrations", engine: "MergeTree" },
            ...CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES.map((name) => ({
              name,
              engine: "ReplacingMergeTree",
            })),
          ];
        }
        if (query.includes("FROM system.parts")) return [];
        throw new Error("unexpected query");
      },
    }));
    mocks.createClient.mockReturnValue({
      query: mocks.query,
      close: mocks.close,
    });
  });

  it("constructs and closes an independent client pinned to read-only mode", async () => {
    await expect(
      probeClickHouseAnalyticsBackendEmptiness(),
    ).resolves.toMatchObject({ empty: true });

    expect(mocks.createClient).toHaveBeenCalledWith(
      expect.objectContaining({
        url: "https://readonly.internal:8123",
        username: "probe-user",
        password: "secret-a",
        database: "langfuse",
        clickhouse_settings: { readonly: "1" },
      }),
    );
    expect(mocks.close).toHaveBeenCalledOnce();
  });

  it("does not include endpoint or credentials in otherwise identical evidence", async () => {
    const first = await probeClickHouseAnalyticsBackendEmptiness();
    mocks.env.CLICKHOUSE_READ_ONLY_URL = "https://other.internal:8123";
    mocks.env.CLICKHOUSE_PASSWORD = "secret-b";
    const second = await probeClickHouseAnalyticsBackendEmptiness();

    expect(first.evidenceDigest).toBe(second.evidenceDigest);
  });

  it("rejects a direct emptiness client request after the runtime is fenced", async () => {
    fenceAnalyticsRuntimeIo();

    await expect(
      probeClickHouseAnalyticsBackendEmptiness(),
    ).resolves.toMatchObject({ empty: false });
    expect(mocks.query).not.toHaveBeenCalled();
    expect(mocks.close).toHaveBeenCalledOnce();
  });
});
