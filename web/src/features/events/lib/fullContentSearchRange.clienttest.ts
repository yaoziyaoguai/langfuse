import { describe, expect, it } from "vitest";

import {
  MAX_FULL_CONTENT_SEARCH_RANGE_MS,
  validateFullContentSearchRange,
} from "@/src/features/events/lib/fullContentSearchRange";

const TO = new Date("2026-07-18T00:00:00.000Z");

describe("validateFullContentSearchRange", () => {
  it("requires a complete range for content search", () => {
    expect(
      validateFullContentSearchRange({
        filters: [],
        searchQuery: "refund policy",
        searchType: ["id", "content"],
        range: undefined,
      }),
    ).toBe("missing");
  });

  it("rejects input, output, and metadata filters beyond 30 days", () => {
    for (const column of ["input", "output", "metadata"]) {
      expect(
        validateFullContentSearchRange({
          filters: [
            {
              type: "string",
              column,
              operator: "contains",
              value: "needle",
            },
          ],
          searchQuery: null,
          searchType: ["id"],
          range: {
            from: new Date(TO.getTime() - MAX_FULL_CONTENT_SEARCH_RANGE_MS - 1),
            to: TO,
          },
        }),
      ).toBe("too_wide");
    }
  });

  it("accepts the exact 30-day boundary and ignores id-only search", () => {
    const thirtyDays = {
      from: new Date(TO.getTime() - MAX_FULL_CONTENT_SEARCH_RANGE_MS),
      to: TO,
    };
    expect(
      validateFullContentSearchRange({
        filters: [],
        searchQuery: "needle",
        searchType: ["content"],
        range: thirtyDays,
      }),
    ).toBe("valid");
    expect(
      validateFullContentSearchRange({
        filters: [],
        searchQuery: "needle",
        searchType: ["id"],
        range: undefined,
      }),
    ).toBe("valid");
  });
});
