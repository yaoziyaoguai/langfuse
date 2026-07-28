import { describe, expect, it } from "vitest";

import { isMonitorRuntimeAvailable } from "./monitorRuntime";

describe("monitor runtime availability", () => {
  it("allows Doris because its native analytics schema includes events", () => {
    expect(
      isMonitorRuntimeAvailable({
        analyticsBackend: "doris",
        v4WriteMode: "legacy",
      }),
    ).toBe(true);
  });

  it.each(["dual", "events_only"] as const)(
    "preserves ClickHouse support in %s mode",
    (v4WriteMode) => {
      expect(
        isMonitorRuntimeAvailable({
          analyticsBackend: "clickhouse",
          v4WriteMode,
        }),
      ).toBe(true);
    },
  );

  it("keeps ClickHouse legacy deployments fenced", () => {
    expect(
      isMonitorRuntimeAvailable({
        analyticsBackend: "clickhouse",
        v4WriteMode: "legacy",
      }),
    ).toBe(false);
  });

  it("fails closed while the client session is loading", () => {
    expect(
      isMonitorRuntimeAvailable({
        analyticsBackend: undefined,
        v4WriteMode: undefined,
      }),
    ).toBe(false);
  });

  it("fails closed when only the write mode has loaded", () => {
    expect(
      isMonitorRuntimeAvailable({
        analyticsBackend: undefined,
        v4WriteMode: "dual",
      }),
    ).toBe(false);
  });
});
