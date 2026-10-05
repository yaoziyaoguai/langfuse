import { describe, expect, it } from "vitest";

import { getVisibleProductModules, PRODUCT_MODULES } from "./productModules";

describe("getVisibleProductModules", () => {
  it("shows every module when neither list is configured", () => {
    expect(getVisibleProductModules()).toEqual(PRODUCT_MODULES);
  });

  it("uses the visible allowlist when both modes are configured", () => {
    expect(
      getVisibleProductModules(
        "tracing, datasets, tracing, unknown",
        "tracing",
      ),
    ).toEqual(["tracing", "datasets"]);
  });

  it("removes only valid hidden modules", () => {
    expect(getVisibleProductModules(undefined, "playground, unknown")).toEqual([
      "dashboards",
      "tracing",
      "evaluation",
      "prompt-management",
      "datasets",
    ]);
  });
});
