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
  isCommunityBatchExportTableAvailable,
  isCommunityPageAvailable,
} from "./communityAvailability";
import { COMMUNITY_PARITY_MANIFEST } from "./communityCapabilityManifest.test-fixture";
import {
  collectCommunitySourceInventory,
  isExecutableEvidence,
  missingExecutableEvidence,
} from "./communityCapabilitySourceInventory.test-helper";
import { ANALYTICS_CAPABILITY_NAMES } from "@langfuse/shared/src/server";

type GatedEvidenceChannel = "public" | "page" | "trpc" | "mcp";
type ManifestEntry = (typeof COMMUNITY_PARITY_MANIFEST)[number];
type GatedManifestEntry = Extract<
  ManifestEntry,
  { readonly gateCapability: unknown }
>;

const isGatedManifestEntry = (
  entry: ManifestEntry,
): entry is GatedManifestEntry => "gateCapability" in entry;

const resolveEvidenceCapability = (evidence: string) => {
  const separator = evidence.indexOf(":");
  const channel = evidence.slice(0, separator) as GatedEvidenceChannel;
  const target = evidence.slice(separator + 1).split(":", 1)[0] ?? "";
  switch (channel) {
    case "public":
      return capabilityForPublicApiPath(target);
    case "page":
      return capabilityForPagePath(target);
    case "trpc":
      return capabilityForTrpcPath(target);
    case "mcp":
      return capabilityForMcpTool(target) ?? capabilityForMcpFeature(target);
    default:
      return null;
  }
};

describe("Doris R1A community capability contract", () => {
  it("freezes every product row with an owner, rationale, and activation class", () => {
    const ids = COMMUNITY_PARITY_MANIFEST.map(({ id }) => id);
    expect(new Set(ids).size).toBe(ids.length);
    expect(
      COMMUNITY_PARITY_MANIFEST.every(
        ({ evidence, rationale }) =>
          evidence.length > 0 && rationale.length > 0,
      ),
    ).toBe(true);
    expect(
      COMMUNITY_PARITY_MANIFEST.filter(
        ({ activationClass }) => activationClass === "durable-activation",
      ).every(
        (entry) =>
          "durableCapability" in entry && entry.durableCapability !== undefined,
      ),
    ).toBe(true);
    expect(
      COMMUNITY_PARITY_MANIFEST.filter(
        ({ activationClass }) => activationClass !== "durable-activation",
      ).every(
        (entry) =>
          !("durableCapability" in entry) ||
          entry.durableCapability === undefined,
      ),
    ).toBe(true);
  });

  it("binds the manifest to exactly the six durable activation rows", () => {
    const manifestCapabilities = [
      ...new Set(
        COMMUNITY_PARITY_MANIFEST.flatMap((entry) =>
          "durableCapability" in entry && entry.durableCapability
            ? [entry.durableCapability]
            : [],
        ),
      ),
    ].sort();
    expect(manifestCapabilities).toEqual(
      [...ANALYTICS_CAPABILITY_NAMES].sort(),
    );
  });

  it("records explicit rationale for all exclusions", () => {
    const excluded = COMMUNITY_PARITY_MANIFEST.filter(
      ({ scope }) => scope === "excluded",
    );
    expect(excluded.length).toBeGreaterThan(0);
    expect(
      excluded.every(
        ({ currentDorisStatus, targetDorisStatus }) =>
          currentDorisStatus === "cloud-ee-excluded" &&
          targetDorisStatus === "excluded",
      ),
    ).toBe(true);
  });

  it("binds every executable manifest evidence item to a registered source surface", () => {
    const inventory = collectCommunitySourceInventory();
    const evidence = COMMUNITY_PARITY_MANIFEST.flatMap((entry) =>
      entry.evidence.map((item) => `${entry.id} -> ${item}`),
    );
    const evidenceItems = COMMUNITY_PARITY_MANIFEST.flatMap(
      (entry) => entry.evidence,
    );
    const missing = new Set(
      missingExecutableEvidence(evidenceItems, inventory),
    );

    expect(
      evidence.filter((item) =>
        [...missing].some((missingItem) => item.endsWith(` -> ${missingItem}`)),
      ),
    ).toEqual([]);
  });

  it("keeps source-discovered restricted surfaces aligned with the corpus and gate", () => {
    const inventory = collectCommunitySourceInventory();
    const gatedEntries = COMMUNITY_PARITY_MANIFEST.filter(isGatedManifestEntry);

    for (const entry of gatedEntries) {
      const capabilities = entry.evidence
        .filter(isExecutableEvidence)
        .map(resolveEvidenceCapability)
        .filter((capability) => capability !== null);
      expect(capabilities, entry.id).not.toHaveLength(0);
      expect(new Set(capabilities), entry.id).toEqual(
        new Set([entry.gateCapability]),
      );
    }

    const manifestChannelCoverage = new Set(
      gatedEntries.flatMap((entry) =>
        entry.evidence.flatMap((evidence) => {
          const separator = evidence.indexOf(":");
          const channel = evidence.slice(0, separator) as GatedEvidenceChannel;
          if (!(["public", "page", "trpc", "mcp"] as const).includes(channel)) {
            return [];
          }
          return resolveEvidenceCapability(evidence) === entry.gateCapability
            ? [`${channel}:${entry.gateCapability}`]
            : [];
        }),
      ),
    );

    const registeredRestrictedSurfaces = [
      ...[...inventory.publicRoutes].map(
        (surface) =>
          ["public", surface, capabilityForPublicApiPath(surface)] as const,
      ),
      ...[...inventory.pageRoutes].map(
        (surface) => ["page", surface, capabilityForPagePath(surface)] as const,
      ),
      ...[...inventory.trpcPaths].map(
        (surface) => ["trpc", surface, capabilityForTrpcPath(surface)] as const,
      ),
      ...[...inventory.mcpToolsByFeature].flatMap(([feature, tools]) =>
        [...tools].map(
          (surface) =>
            [
              "mcp",
              `${feature}.${surface}`,
              capabilityForMcpTool(surface) ?? capabilityForMcpFeature(feature),
            ] as const,
        ),
      ),
    ].filter((surface) => surface[2] !== null);

    expect(
      registeredRestrictedSurfaces.filter(
        ([channel, , capability]) =>
          !manifestChannelCoverage.has(`${channel}:${capability}`),
      ),
    ).toEqual([]);

    for (const entry of gatedEntries) {
      for (const evidence of entry.evidence) {
        const separator = evidence.indexOf(":");
        const channel = evidence.slice(0, separator);
        const target = evidence.slice(separator + 1);
        const routes =
          channel === "public"
            ? inventory.publicRoutes
            : channel === "page"
              ? inventory.pageRoutes
              : null;
        if (!routes) continue;
        const descendants = [...routes].filter(
          (route) => route === target || route.startsWith(`${target}/`),
        );
        expect(descendants, evidence).not.toHaveLength(0);
        expect(
          new Set(
            descendants.map((route) =>
              channel === "public"
                ? capabilityForPublicApiPath(route)
                : capabilityForPagePath(route),
            ),
          ),
          evidence,
        ).toEqual(new Set([entry.gateCapability]));
      }
    }
  });

  it("keeps Doris analytics integrations eligible but closed until durable activation", () => {
    expect(
      isCommunityCapabilityAvailable("analyticsIntegrations", "doris"),
    ).toBe(true);
    expect(
      isCommunityCapabilityAvailable("analyticsIntegrations", "clickhouse"),
    ).toBe(true);
    const path = "/project/[projectId]/settings/integrations/posthog";
    expect(isCommunityPageAvailable(path, "doris")).toBe(false);
    expect(
      isCommunityPageAvailable(path, "doris", ["analyticsIntegrations"]),
    ).toBe(true);
    expect(COMMUNITY_CAPABILITIES.analyticsIntegrations).toMatchObject({
      error: "UnsupportedFeature",
      code: "R2_ANALYTICS_INTEGRATIONS_UNAVAILABLE",
    });
  });

  it("keeps Doris experiments eligible but closed until durable activation", () => {
    expect(isCommunityCapabilityAvailable("experiments", "doris")).toBe(true);
    expect(
      isCommunityPageAvailable("/project/[projectId]/experiments", "doris"),
    ).toBe(false);
    expect(
      isCommunityPageAvailable("/project/[projectId]/experiments", "doris", [
        "experiments",
      ]),
    ).toBe(true);
  });

  it("keeps Doris evaluations eligible but closed until durable activation", () => {
    expect(isCommunityCapabilityAvailable("evaluations", "doris")).toBe(true);
    expect(
      isCommunityPageAvailable("/project/[projectId]/evals", "doris"),
    ).toBe(false);
    expect(
      isCommunityPageAvailable("/project/[projectId]/evals", "doris", [
        "evaluations",
      ]),
    ).toBe(true);
  });

  it("lets the batch export router apply its durable Doris activation gate", () => {
    expect(isCommunityCapabilityAvailable("monitors", "doris")).toBe(true);
    expect(isCommunityCapabilityAvailable("monitors", "clickhouse")).toBe(true);
    expect(isCommunityCapabilityAvailable("batchExports", "doris")).toBe(true);
    expect(isCommunityCapabilityAvailable("batchExports", "clickhouse")).toBe(
      true,
    );
    expect(isCommunityCapabilityAvailable("customDashboards", "doris")).toBe(
      true,
    );
    expect(
      isCommunityCapabilityAvailable("customDashboards", "clickhouse"),
    ).toBe(true);
  });

  it("applies capability availability consistently to page routes", () => {
    expect(
      isCommunityPageAvailable("/project/[projectId]/dashboards", "doris"),
    ).toBe(true);
    expect(
      isCommunityPageAvailable("/project/[projectId]/monitors", "doris"),
    ).toBe(true);
    expect(
      isCommunityPageAvailable("/project/[projectId]/evals", "doris"),
    ).toBe(false);
    expect(
      isCommunityPageAvailable("/project/[projectId]/evals", "clickhouse"),
    ).toBe(true);
  });

  it("maps every inactive server channel to the same capability", () => {
    expect(capabilityForTrpcPath("evals.createJob")).toBe("evaluations");
    expect(capabilityForTrpcPath("batchAction.runEvaluation.create")).toBe(
      "evaluations",
    );
    expect(capabilityForTrpcPath("experiments.createExperiment")).toBe(
      "experiments",
    );
    expect(capabilityForTrpcPath("datasets.runItemsByRunId")).toBe(
      "experiments",
    );
    expect(capabilityForTrpcPath("datasets.countAllDatasetItems")).toBe(
      "experiments",
    );
    expect(capabilityForTrpcPath("datasets.itemsByDatasetId")).toBeNull();
    expect(capabilityForTrpcPath("monitors.all")).toBe("monitors");
    expect(capabilityForTrpcPath("batchExport.create")).toBe("batchExports");
    expect(capabilityForTrpcPath("posthogIntegration.create")).toBe(
      "analyticsIntegrations",
    );
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
    expect(capabilityForPublicApiPath(path)).toBe("analyticsIntegrations");
  });

  it("opens dataset-run exports only after their durable activation", () => {
    expect(
      isCommunityBatchExportTableAvailable("dataset_run_items", "doris"),
    ).toBe(false);
    expect(
      isCommunityBatchExportTableAvailable("dataset_run_items", "clickhouse"),
    ).toBe(true);
    expect(
      isCommunityBatchExportTableAvailable("dataset_run_items", "doris", true),
    ).toBe(true);
    expect(isCommunityBatchExportTableAvailable("events", "doris")).toBe(true);
    expect(isCommunityBatchExportTableAvailable("traces", "doris")).toBe(true);
  });
});
