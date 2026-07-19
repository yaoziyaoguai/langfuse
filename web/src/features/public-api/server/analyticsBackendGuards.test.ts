import { describe, expect, it } from "vitest";

import { legacyIngestionRejection } from "./analyticsBackendGuards";

describe("legacyIngestionRejection", () => {
  it.each([
    [false, "doris", "legacy", null],
    [true, "doris", "legacy", null],
    [true, "clickhouse", "events_only", "events_only"],
    [true, "clickhouse", "legacy", null],
  ] as const)(
    "resolves reject=%s backend=%s writeMode=%s",
    (rejectLegacyRoute, backend, clickhouseWriteMode, expected) => {
      expect(
        legacyIngestionRejection({
          rejectLegacyRoute,
          backend,
          clickhouseWriteMode,
        }),
      ).toBe(expected);
    },
  );
});
