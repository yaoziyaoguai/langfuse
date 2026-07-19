import { describe, expect, it, vi } from "vitest";

import {
  COMMUNITY_CAPABILITIES,
  capabilityForMcpFeature,
  capabilityForMcpTool,
  capabilityForPagePath,
  capabilityForPublicApiPath,
  capabilityForTrpcPath,
  createUnsupportedFeatureApiHandler,
  isCommunityCapabilityAvailable,
} from "./communityAvailability";

describe("Doris R1A community capability contract", () => {
  it.each([
    ["evaluations", "R1B_EVALUATIONS_UNAVAILABLE"],
    ["experiments", "R1B_EXPERIMENTS_UNAVAILABLE"],
    ["batchExports", "R2_BATCH_EXPORTS_UNAVAILABLE"],
    ["customDashboards", "R2_CUSTOM_DASHBOARDS_UNAVAILABLE"],
  ] as const)("keeps %s inactive with stable code", (capability, code) => {
    expect(isCommunityCapabilityAvailable(capability)).toBe(false);
    expect(isCommunityCapabilityAvailable(capability, "clickhouse")).toBe(true);
    expect(COMMUNITY_CAPABILITIES[capability]).toMatchObject({
      error: "UnsupportedFeature",
      code,
    });
  });

  it("makes monitors available on both analytics backends", () => {
    expect(isCommunityCapabilityAvailable("monitors", "doris")).toBe(true);
    expect(isCommunityCapabilityAvailable("monitors", "clickhouse")).toBe(true);
  });

  it("maps every inactive server channel to the same capability", () => {
    expect(capabilityForTrpcPath("evals.create")).toBe("evaluations");
    expect(capabilityForTrpcPath("batchAction.runEvaluation.create")).toBe(
      "evaluations",
    );
    expect(capabilityForTrpcPath("experiments.create")).toBe("experiments");
    expect(capabilityForTrpcPath("datasets.runItemsByRunId")).toBe(
      "experiments",
    );
    expect(capabilityForTrpcPath("datasets.itemsByDatasetId")).toBeNull();
    expect(capabilityForTrpcPath("monitors.all")).toBe("monitors");
    expect(capabilityForTrpcPath("batchExport.create")).toBe("batchExports");
    expect(capabilityForTrpcPath("dashboard.createDashboard")).toBe(
      "customDashboards",
    );
    expect(capabilityForTrpcPath("dashboard.getHomeDashboard")).toBeNull();
    expect(capabilityForTrpcPath("scores.create")).toBeNull();
    expect(capabilityForMcpFeature("evals")).toBe("evaluations");
    expect(capabilityForMcpFeature("experiments")).toBe("experiments");
    expect(capabilityForMcpFeature("monitors")).toBe("monitors");
    expect(capabilityForMcpFeature("dashboardWidgets")).toBe(
      "customDashboards",
    );
    expect(capabilityForMcpTool("createDatasetRunItem")).toBe("experiments");
    expect(capabilityForMcpTool("listDatasetRuns")).toBe("experiments");
    expect(capabilityForMcpTool("listDatasetItems")).toBeNull();
    expect(capabilityForPublicApiPath("/api/public/experiments")).toBe(
      "experiments",
    );
    expect(
      capabilityForPublicApiPath(
        "/api/public/unstable/evaluation-rules/rule-1",
      ),
    ).toBe("evaluations");
    expect(capabilityForPublicApiPath("/api/public/traces")).toBeNull();
    expect(capabilityForPagePath("/project/[projectId]/evals")).toBe(
      "evaluations",
    );
    expect(capabilityForPagePath("/project/[projectId]/traces")).toBeNull();
  });

  it("returns structured HTTP 501 without running a route implementation", () => {
    const json = vi.fn();
    const status = vi.fn(() => ({ json }));
    createUnsupportedFeatureApiHandler("experiments")(
      {} as never,
      { status } as never,
    );

    expect(status).toHaveBeenCalledWith(501);
    expect(json).toHaveBeenCalledWith(COMMUNITY_CAPABILITIES.experiments);
  });

  it.each([
    "/api/public/integrations/blob-storage",
    "/api/public/integrations/blob-storage/config-1",
  ])("gates blob-storage route %s", (path) => {
    expect(capabilityForPublicApiPath(path)).toBe("batchExports");
  });
});
