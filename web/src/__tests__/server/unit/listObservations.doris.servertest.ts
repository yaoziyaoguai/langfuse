import { describe, expect, it, vi } from "vitest";

import { AnalyticsQueryValidationError } from "@langfuse/shared/src/server";
import type { ServerContext } from "../../../features/mcp/types";

const mocks = vi.hoisted(() => ({
  getObservations: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", async () => ({
  ...(await vi.importActual("@langfuse/shared/src/server")),
  getObservationsV2FromEventsTableForPublicApi: mocks.getObservations,
}));

vi.mock("../../../features/mcp/core/run-mcp-tool", () => ({
  runMcpTool: vi.fn(async ({ fn }) =>
    fn({
      setAttribute: vi.fn(),
      setAttributes: vi.fn(),
    }),
  ),
}));

import { handleListObservations } from "../../../features/mcp/features/observations/tools/listObservations";

const context: ServerContext = {
  projectId: "project-1",
  orgId: "org-1",
  apiKeyId: "api-key-1",
  accessLevel: "project",
  publicKey: "pk-lf-test",
};

describe("listObservations Doris errors", () => {
  it("returns structured time-range metadata through the MCP handler", async () => {
    const acceptedRange = {
      from: new Date("2026-01-01T00:00:00.000Z"),
      to: new Date("2026-02-01T00:00:00.001Z"),
    };
    mocks.getObservations.mockRejectedValueOnce(
      new AnalyticsQueryValidationError(acceptedRange),
    );

    const error = await handleListObservations(
      {
        fields: ["id", "input"],
        limit: 10,
        fromStartTime: acceptedRange.from.toISOString(),
        toStartTime: acceptedRange.to.toISOString(),
      },
      context,
    ).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(Error);
    const message = (error as Error).message;
    expect(JSON.parse(message.slice(message.indexOf("{")))).toEqual({
      message: "Analytics query requires a valid bounded UTC time range",
      error: "InvalidTimeRange",
      code: "InvalidTimeRange",
      maxDays: 30,
      acceptedRange: {
        from: "2026-01-01T00:00:00.000Z",
        to: "2026-02-01T00:00:00.001Z",
      },
    });
  });

  it("does not expose an unexpected Doris error through the MCP handler", async () => {
    const sensitiveMessage =
      "Doris timeout SELECT input FROM events password=do-not-expose";
    mocks.getObservations.mockRejectedValueOnce(new Error(sensitiveMessage));

    const error = await handleListObservations(
      {
        fields: ["id"],
        limit: 10,
        fromStartTime: "2026-01-01T00:00:00.000Z",
        toStartTime: "2026-01-02T00:00:00.000Z",
      },
      context,
    ).catch((caught: unknown) => caught);

    expect(error).toBeInstanceOf(Error);
    expect((error as Error).message).toContain(
      "An unexpected error occurred. Please try again later.",
    );
    expect((error as Error).message).not.toContain(sensitiveMessage);
  });
});
