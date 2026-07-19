import type { McpFeatureModule } from "../../server/registry";
import {
  getMetricsSchemaTool,
  handleGetMetricsSchema,
} from "./tools/getMetricsSchema";
import { queryMetricsTool, handleQueryMetrics } from "./tools/queryMetrics";

export const metricsFeature = {
  name: "metrics",
  description:
    "Analyze project usage, quality, cost, and performance metrics from Langfuse data",
  tools: [
    {
      definition: queryMetricsTool,
      handler: handleQueryMetrics,
    },
    {
      definition: getMetricsSchemaTool,
      handler: handleGetMetricsSchema,
    },
  ],
} as const satisfies McpFeatureModule;
