import { describe, expect, it, vi } from "vitest";

import {
  COMMUNITY_CAPABILITIES,
  capabilityForMcpFeature,
  capabilityForMcpTool,
  capabilityForTrpcPath,
  createUnsupportedFeatureApiHandler,
  isCommunityCapabilityAvailable,
} from "./communityAvailability";
import blobStorageCollectionHandler from "../../pages/api/public/integrations/blob-storage";
import blobStorageItemHandler from "../../pages/api/public/integrations/blob-storage/[id]";

describe("Doris R1A community capability contract", () => {
  it.each([
    ["evaluations", "R1B_EVALUATIONS_UNAVAILABLE"],
    ["experiments", "R1B_EXPERIMENTS_UNAVAILABLE"],
    ["monitors", "R2_MONITORS_UNAVAILABLE"],
    ["batchExports", "R2_BATCH_EXPORTS_UNAVAILABLE"],
    ["customDashboards", "R2_CUSTOM_DASHBOARDS_UNAVAILABLE"],
  ] as const)("keeps %s inactive with stable code", (capability, code) => {
    expect(isCommunityCapabilityAvailable(capability)).toBe(false);
    expect(COMMUNITY_CAPABILITIES[capability]).toMatchObject({
      error: "UnsupportedFeature",
      code,
    });
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
    ["collection", blobStorageCollectionHandler],
    ["item", blobStorageItemHandler],
  ])("gates every blob-storage %s route", (_route, handler) => {
    const json = vi.fn();
    const status = vi.fn(() => ({ json }));

    handler({} as never, { status } as never);

    expect(status).toHaveBeenCalledWith(501);
    expect(json).toHaveBeenCalledWith(COMMUNITY_CAPABILITIES.batchExports);
  });
});
