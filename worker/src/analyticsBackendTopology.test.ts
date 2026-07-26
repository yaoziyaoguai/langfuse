import { describe, expect, it } from "vitest";

import { resolveAnalyticsWorkerTopology } from "./analyticsBackendTopology";

describe("resolveAnalyticsWorkerTopology", () => {
  it.each([
    [undefined, "clickhouse", true, false, true, true],
    ["clickhouse", "clickhouse", true, false, true, true],
    ["doris", "doris", false, true, true, false],
  ] as const)(
    "resolves %s to one exclusive worker topology",
    (
      configured,
      backend,
      clickhouseEnabled,
      dorisEnabled,
      coreBatchExportsEnabled,
      legacyTraceDeletionCleanerEnabled,
    ) => {
      expect(resolveAnalyticsWorkerTopology(configured)).toEqual({
        backend,
        clickhouseAnalyticsEnabled: clickhouseEnabled,
        dorisAnalyticsEnabled: dorisEnabled,
        coreBatchExportsEnabled,
        legacyTraceDeletionCleanerEnabled,
      });
    },
  );

  it("rejects invalid and dual-backend values", () => {
    expect(() => resolveAnalyticsWorkerTopology("clickhouse,doris")).toThrow(
      "LANGFUSE_ANALYTICS_BACKEND must be one of: clickhouse, doris",
    );
  });
});
