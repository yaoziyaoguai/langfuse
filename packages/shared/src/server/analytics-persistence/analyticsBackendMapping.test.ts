import { describe, expect, it } from "vitest";

import {
  fromPrismaAnalyticsRuntimeComponent,
  toPrismaAnalyticsRuntimeComponent,
} from "./analyticsBackendMapping";

describe("analytics backend mapping", () => {
  it("maps checkpoint runtimes without assigning them to the worker fleet", () => {
    expect(toPrismaAnalyticsRuntimeComponent("checkpoint" as never)).toBe(
      "CHECKPOINT",
    );
    expect(fromPrismaAnalyticsRuntimeComponent("CHECKPOINT" as never)).toBe(
      "checkpoint",
    );
  });
});
