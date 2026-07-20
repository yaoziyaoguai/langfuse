import type { NextApiRequest, NextApiResponse } from "next";
import type { AnalyticsBackend } from "@langfuse/shared/analytics-backend";

export type CommunityCapability =
  | "evaluations"
  | "experiments"
  | "monitors"
  | "batchExports"
  | "analyticsIntegrations"
  | "customDashboards";

export type UnsupportedFeatureBody = {
  readonly error: "UnsupportedFeature";
  readonly code:
    | "R1B_EVALUATIONS_UNAVAILABLE"
    | "R1B_EXPERIMENTS_UNAVAILABLE"
    | "R2_MONITORS_UNAVAILABLE"
    | "R2_BATCH_EXPORTS_UNAVAILABLE"
    | "R2_ANALYTICS_INTEGRATIONS_UNAVAILABLE"
    | "R2_CUSTOM_DASHBOARDS_UNAVAILABLE";
  readonly message: string;
  readonly recovery: string;
};

export const COMMUNITY_CAPABILITIES: Readonly<
  Record<CommunityCapability, UnsupportedFeatureBody>
> = {
  evaluations: {
    error: "UnsupportedFeature",
    code: "R1B_EVALUATIONS_UNAVAILABLE",
    message: "Evaluator execution is not available in the Doris R1A release.",
    recovery:
      "Adopt the R1B evaluation capability only after its owner, usage evidence, Doris implementation, and backlog policy are approved.",
  },
  experiments: {
    error: "UnsupportedFeature",
    code: "R1B_EXPERIMENTS_UNAVAILABLE",
    message:
      "Experiment execution and analytics are not available in the Doris R1A release.",
    recovery:
      "Adopt the R1B experiment capability only after its owner, usage evidence, Doris implementation, and backlog policy are approved.",
  },
  monitors: {
    error: "UnsupportedFeature",
    code: "R2_MONITORS_UNAVAILABLE",
    message: "Product monitors are not available in the Doris R1A release.",
    recovery:
      "Create a separately reviewed Doris monitor implementation before enabling this capability.",
  },
  batchExports: {
    error: "UnsupportedFeature",
    code: "R2_BATCH_EXPORTS_UNAVAILABLE",
    message:
      "Analytics batch exports are not available in the Doris R1A release.",
    recovery:
      "Create a separately reviewed Doris export implementation before enabling this capability.",
  },
  analyticsIntegrations: {
    error: "UnsupportedFeature",
    code: "R2_ANALYTICS_INTEGRATIONS_UNAVAILABLE",
    message:
      "Third-party analytics integrations are not available with the Doris backend.",
    recovery:
      "Use the ClickHouse backend until the integration event source is migrated to the analytics storage interface.",
  },
  customDashboards: {
    error: "UnsupportedFeature",
    code: "R2_CUSTOM_DASHBOARDS_UNAVAILABLE",
    message:
      "Custom dashboard and widget authoring is not available in the Doris R1A release.",
    recovery:
      "Use the curated Home dashboard presets, or create a separately reviewed Doris custom-dashboard implementation before enabling authoring.",
  },
};

export const isCommunityCapabilityAvailable = (
  capability: CommunityCapability,
  backend: AnalyticsBackend = "doris",
): boolean =>
  backend === "clickhouse" ||
  capability === "monitors" ||
  capability === "batchExports" ||
  capability === "customDashboards";

export class CommunityCapabilityUnavailableError extends Error {
  readonly body: UnsupportedFeatureBody;

  constructor(capability: CommunityCapability) {
    const body = COMMUNITY_CAPABILITIES[capability];
    super(body.message);
    this.name = "CommunityCapabilityUnavailableError";
    this.body = body;
  }
}

export function assertCommunityCapability(
  capability: CommunityCapability,
  backend: AnalyticsBackend = "doris",
): void {
  if (!isCommunityCapabilityAvailable(capability, backend)) {
    throw new CommunityCapabilityUnavailableError(capability);
  }
}

export function capabilityForTrpcPath(
  path: string,
): CommunityCapability | null {
  if (
    path.startsWith("evals.") ||
    path.startsWith("defaultLlmModel.") ||
    path.startsWith("batchAction.runEvaluation.")
  ) {
    return "evaluations";
  }
  if (path.startsWith("experiments.")) return "experiments";
  if (
    /^datasets\.(runById|baseRunDataByDatasetId|runsByDatasetId|runsByDatasetIdMetrics|runFilterOptions|runItemFilterOptions|runItemsByItemId|runItemsByRunId|datasetItemsWithRunData|runItemCompareCount|deleteDatasetRuns|upsertRemoteExperiment|getRemoteExperiment|triggerRemoteExperiment|deleteRemoteExperiment)$/.test(
      path,
    )
  ) {
    return "experiments";
  }
  if (path.startsWith("monitors.")) return "monitors";
  if (path.startsWith("batchExport.")) return "batchExports";
  if (
    path.startsWith("posthogIntegration.") ||
    path.startsWith("mixpanelIntegration.") ||
    path.startsWith("blobStorageIntegration.")
  ) {
    return "analyticsIntegrations";
  }
  if (
    /^(dashboard\.(allDashboards|getDashboard|createDashboard|updateDashboardDefinition|updateDashboardMetadata|cloneDashboard|setHomeDashboard|updateDashboardFilters|delete)|dashboardWidgets\.)/.test(
      path,
    )
  ) {
    return "customDashboards";
  }
  return null;
}

export function capabilityForPublicApiPath(
  path: string,
): CommunityCapability | null {
  const pathname = path.split("?", 1)[0] ?? path;
  if (
    /^\/api\/public\/(dataset-run-items|datasets\/[^/]+\/runs(?:\/|$)|experiment-items(?:\/|$)|experiments(?:\/|$))/.test(
      pathname,
    )
  ) {
    return "experiments";
  }
  if (/^\/api\/public\/integrations\/blob-storage(?:\/|$)/.test(pathname)) {
    return "analyticsIntegrations";
  }
  if (
    /^\/api\/public\/unstable\/(evaluation-rules|evaluators)(?:\/|$)/.test(
      pathname,
    )
  ) {
    return "evaluations";
  }
  if (
    /^\/api\/public\/unstable\/(dashboard-widgets|dashboards)(?:\/|$)/.test(
      pathname,
    )
  ) {
    return "customDashboards";
  }
  return null;
}

export function capabilityForPagePath(
  path: string,
): CommunityCapability | null {
  if (/^\/project\/\[projectId\]\/dashboards(?:\/|$)/.test(path)) {
    return "customDashboards";
  }
  if (
    /^\/project\/\[projectId\]\/(evals|experiments|monitors)(?:\/|$)/.test(path)
  ) {
    return path.includes("/evals")
      ? "evaluations"
      : path.includes("/monitors")
        ? "monitors"
        : "experiments";
  }
  if (
    /^\/project\/\[projectId\]\/datasets\/\[datasetId\]\/(compare|experiments|items\/\[itemId\]\/runs|runs\/\[runId\])(?:\/|$)/.test(
      path,
    )
  ) {
    return "experiments";
  }
  if (
    /^\/project\/\[projectId\]\/settings\/integrations\/(blobstorage|mixpanel|posthog)$/.test(
      path,
    )
  ) {
    return "analyticsIntegrations";
  }
  return null;
}

export function isCommunityBatchExportTableAvailable(
  tableName: string,
  backend: AnalyticsBackend,
): boolean {
  return backend === "clickhouse" || tableName !== "dataset_run_items";
}

export function isCommunityPageAvailable(
  path: string,
  backend: AnalyticsBackend,
): boolean {
  const capability = capabilityForPagePath(path);
  return (
    capability === null || isCommunityCapabilityAvailable(capability, backend)
  );
}

export function capabilityForMcpFeature(
  featureName: string,
): CommunityCapability | null {
  if (featureName === "evals") return "evaluations";
  if (featureName === "experiments") return "experiments";
  if (featureName === "monitors") return "monitors";
  if (featureName === "dashboardWidgets") return "customDashboards";
  return null;
}

export function capabilityForMcpTool(
  toolName: string,
): CommunityCapability | null {
  if (
    /^(createDatasetRunItem|listDatasetRunItems|listDatasetRuns|getDatasetRun|deleteDatasetRun)$/.test(
      toolName,
    )
  ) {
    return "experiments";
  }
  return null;
}

export function createUnsupportedFeatureApiHandler(
  capability: CommunityCapability,
): (req: NextApiRequest, res: NextApiResponse) => void {
  return (_req, res) => {
    res.status(501).json(COMMUNITY_CAPABILITIES[capability]);
  };
}
