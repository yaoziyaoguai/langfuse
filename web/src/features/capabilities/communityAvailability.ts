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
    message: "Evaluator execution is not active for this Doris deployment.",
    recovery:
      "Complete the evaluator bootstrap and runtime census, then activate the durable evaluations capability.",
  },
  experiments: {
    error: "UnsupportedFeature",
    code: "R1B_EXPERIMENTS_UNAVAILABLE",
    message:
      "Experiment execution and analytics are not active for this Doris deployment.",
    recovery:
      "Complete the experiment and dataset-run ingestion bootstrap, then activate both durable capabilities.",
  },
  monitors: {
    error: "UnsupportedFeature",
    code: "R2_MONITORS_UNAVAILABLE",
    message: "Product monitors are not active for this Doris deployment.",
    recovery:
      "Verify the shared analytics query runtime and enable the monitor worker and routes for this deployment.",
  },
  batchExports: {
    error: "UnsupportedFeature",
    code: "R2_BATCH_EXPORTS_UNAVAILABLE",
    message:
      "Analytics batch exports are not active for this Doris deployment.",
    recovery:
      "Complete the core or dataset-run export bootstrap and runtime census, then activate the matching durable export capability.",
  },
  analyticsIntegrations: {
    error: "UnsupportedFeature",
    code: "R2_ANALYTICS_INTEGRATIONS_UNAVAILABLE",
    message:
      "Third-party analytics integrations are not active for this Doris deployment.",
    recovery:
      "Complete the sealed bootstrap and runtime census, then activate the durable analytics integrations capability.",
  },
  customDashboards: {
    error: "UnsupportedFeature",
    code: "R2_CUSTOM_DASHBOARDS_UNAVAILABLE",
    message:
      "Custom dashboard and widget authoring is not active for this Doris deployment.",
    recovery:
      "Verify the shared analytics query runtime and enable the dashboard and widget routes for this deployment.",
  },
};

export const isCommunityCapabilityAvailable = (
  capability: CommunityCapability,
  backend: AnalyticsBackend = "doris",
): boolean =>
  backend === "clickhouse" ||
  capability === "evaluations" ||
  capability === "experiments" ||
  capability === "monitors" ||
  capability === "batchExports" ||
  capability === "analyticsIntegrations" ||
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
    /^datasets\.(runById|baseRunDataByDatasetId|runsByDatasetId|runsByDatasetIdMetrics|runFilterOptions|runItemFilterOptions|runItemsByItemId|runItemsByRunId|datasetItemsWithRunData|runItemCompareCount|countAllDatasetItems|deleteDatasetRuns|upsertRemoteExperiment|getRemoteExperiment|triggerRemoteExperiment|deleteRemoteExperiment)$/.test(
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

type PublicApiCapabilityResolution =
  | {
      readonly isValidRequestUrl: false;
      readonly capability: null;
    }
  | {
      readonly isValidRequestUrl: true;
      readonly capability: CommunityCapability | null;
    };

const CAPABILITY_PATH_ORIGIN = "http://langfuse.local";
const ENCODED_PATH_SEPARATOR = /%(?:25)*(?:2f|5c)/i;

function canonicalizePublicApiPathname(path: string): string | null {
  // 安全边界：这里只接受 Node/Next 传给 route handler 的 origin-form URL。
  // WHATWG URL 负责拆分 query/hash 和处理 dot segments；随后按 Next 的路径边界
  // decode 一层并规范化分隔符。多层编码分隔符不递归解释，而是直接判为无效。
  if (!path.startsWith("/") || /^[\\/]{2}/.test(path)) return null;

  try {
    const parsedUrl = new URL(path, CAPABILITY_PATH_ORIGIN);
    if (parsedUrl.origin !== CAPABILITY_PATH_ORIGIN) return null;

    const decodedPathname = decodeURIComponent(parsedUrl.pathname);
    if (
      /^[\\/]{2}/.test(decodedPathname) ||
      ENCODED_PATH_SEPARATOR.test(decodedPathname)
    ) {
      return null;
    }

    const normalizedSeparators = decodedPathname
      .replace(/\\/g, "/")
      .replace(/\/{2,}/g, "/");
    const canonicalUrl = new URL(normalizedSeparators, CAPABILITY_PATH_ORIGIN);

    return canonicalUrl.origin === CAPABILITY_PATH_ORIGIN
      ? canonicalUrl.pathname
      : null;
  } catch {
    return null;
  }
}

export function resolvePublicApiCapability(
  path: string,
): PublicApiCapabilityResolution {
  const pathname = canonicalizePublicApiPathname(path);
  if (pathname === null) {
    return { isValidRequestUrl: false, capability: null };
  }

  let capability: CommunityCapability | null = null;
  if (
    /^\/api\/public\/(dataset-run-items|datasets\/[^/]+\/runs(?:\/|$)|experiment-items(?:\/|$)|experiments(?:\/|$))/.test(
      pathname,
    )
  ) {
    capability = "experiments";
  } else if (
    /^\/api\/public\/integrations\/blob-storage(?:\/|$)/.test(pathname)
  ) {
    capability = "analyticsIntegrations";
  } else if (
    /^\/api\/public\/unstable\/(evaluation-rules|evaluators)(?:\/|$)/.test(
      pathname,
    )
  ) {
    capability = "evaluations";
  } else if (
    /^\/api\/public\/unstable\/(dashboard-widgets|dashboards)(?:\/|$)/.test(
      pathname,
    )
  ) {
    capability = "customDashboards";
  }

  return { isValidRequestUrl: true, capability };
}

export function capabilityForPublicApiPath(
  path: string,
): CommunityCapability | null {
  return resolvePublicApiCapability(path).capability;
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
  datasetRunExportsActive = false,
): boolean {
  return (
    backend === "clickhouse" ||
    tableName !== "dataset_run_items" ||
    datasetRunExportsActive
  );
}

export function isCommunityPageAvailable(
  path: string,
  backend: AnalyticsBackend,
  activeDorisCapabilities: readonly CommunityCapability[] = [],
): boolean {
  const capability = capabilityForPagePath(path);
  if (capability === null) return true;
  if (!isCommunityCapabilityAvailable(capability, backend)) return false;
  if (
    backend === "doris" &&
    (capability === "evaluations" ||
      capability === "experiments" ||
      capability === "analyticsIntegrations")
  ) {
    return activeDorisCapabilities.includes(capability);
  }
  return true;
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
