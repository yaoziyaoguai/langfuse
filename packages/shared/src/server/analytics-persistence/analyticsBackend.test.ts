import { describe, expect, it } from "vitest";

import {
  resolveAnalyticsBackend,
  type AnalyticsBackend,
} from "./analyticsBackend";

describe("resolveAnalyticsBackend", () => {
  it.each<readonly [string | undefined, AnalyticsBackend]>([
    [undefined, "clickhouse"],
    ["clickhouse", "clickhouse"],
    ["doris", "doris"],
  ])("resolves %s to %s", (configured, expected) => {
    expect(resolveAnalyticsBackend(configured)).toBe(expected);
  });

  it("rejects unsupported or ambiguous backend values", () => {
    expect(() => resolveAnalyticsBackend("CLICKHOUSE")).toThrow(
      "LANGFUSE_ANALYTICS_BACKEND must be one of: clickhouse, doris",
    );
    expect(() => resolveAnalyticsBackend("clickhouse,doris")).toThrow(
      "LANGFUSE_ANALYTICS_BACKEND must be one of: clickhouse, doris",
    );
  });
});
