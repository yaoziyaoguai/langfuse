import { describe, expect, it } from "vitest";

import { resolveAnalyticsWorkerTopology } from "./analyticsBackendTopology";

describe("resolveAnalyticsWorkerTopology", () => {
  it.each([
    [undefined, "clickhouse", true, false],
    ["clickhouse", "clickhouse", true, false],
    ["doris", "doris", false, true],
  ] as const)(
    "resolves %s to one exclusive worker topology",
    (configured, backend, clickhouseEnabled, dorisEnabled) => {
      expect(resolveAnalyticsWorkerTopology(configured)).toEqual({
        backend,
        clickhouseAnalyticsEnabled: clickhouseEnabled,
        dorisAnalyticsEnabled: dorisEnabled,
      });
    },
  );

  it("rejects invalid and dual-backend values", () => {
    expect(() => resolveAnalyticsWorkerTopology("clickhouse,doris")).toThrow(
      "LANGFUSE_ANALYTICS_BACKEND must be one of: clickhouse, doris",
    );
  });
});
