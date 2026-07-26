import { describe, expect, it } from "vitest";

import {
  getSeederRequiredEnvVars,
  resolveSeederAnalyticsBackend,
} from "./backend";

describe("seeder analytics backend", () => {
  it("defaults to ClickHouse and accepts both supported values", () => {
    expect(resolveSeederAnalyticsBackend(undefined)).toBe("clickhouse");
    expect(resolveSeederAnalyticsBackend("clickhouse")).toBe("clickhouse");
    expect(resolveSeederAnalyticsBackend("doris")).toBe("doris");
  });

  it("rejects an unknown backend with a stable configuration hint", () => {
    expect(() => resolveSeederAnalyticsBackend("postgres")).toThrow(
      'unsupported LANGFUSE_ANALYTICS_BACKEND="postgres"',
    );
  });

  it("never requires ClickHouse variables in Doris mode", () => {
    expect(getSeederRequiredEnvVars("doris")).toEqual(["DATABASE_URL"]);
    expect(getSeederRequiredEnvVars("clickhouse")).toEqual([
      "DATABASE_URL",
      "CLICKHOUSE_URL",
      "CLICKHOUSE_USER",
      "CLICKHOUSE_PASSWORD",
    ]);
  });
});
