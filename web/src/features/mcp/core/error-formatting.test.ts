import { describe, expect, it } from "vitest";

import { InvalidRequestError } from "@langfuse/shared";
import { logger } from "@langfuse/shared/src/server";
import { formatErrorForUser } from "./error-formatting";

vi.mock("@langfuse/shared/src/server", () => ({
  logger: {
    error: vi.fn(),
    warn: vi.fn(),
  },
}));

describe("MCP error formatting", () => {
  it("preserves structured Doris time-range validation metadata", () => {
    const validationError = Object.assign(
      new InvalidRequestError(
        "Analytics query requires a valid bounded UTC time range",
      ),
      {
        code: "InvalidTimeRange",
        maxDays: 30,
        acceptedRange: {
          from: new Date("2026-01-01T00:00:00.000Z"),
          to: new Date("2026-02-01T00:00:00.001Z"),
        },
      },
    );
    const error = formatErrorForUser(validationError);

    expect(JSON.parse(error.message.slice(error.message.indexOf("{")))).toEqual(
      {
        message: "Analytics query requires a valid bounded UTC time range",
        error: "InvalidTimeRange",
        code: "InvalidTimeRange",
        maxDays: 30,
        acceptedRange: {
          from: "2026-01-01T00:00:00.000Z",
          to: "2026-02-01T00:00:00.001Z",
        },
      },
    );
  });

  it("does not expose or log an unexpected Doris error message", () => {
    const sensitiveMessage =
      "Doris timeout SELECT input FROM events password=do-not-log";
    const error = formatErrorForUser(new Error(sensitiveMessage));

    expect(error.message).toContain(
      "An unexpected error occurred. Please try again later.",
    );
    expect(JSON.stringify(vi.mocked(logger.error).mock.calls)).not.toContain(
      sensitiveMessage,
    );
  });
});
