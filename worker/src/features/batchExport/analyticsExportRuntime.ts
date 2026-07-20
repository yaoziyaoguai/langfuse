import { resolveAnalyticsBackend } from "@langfuse/shared/analytics-backend";

import { env } from "../../env";
import type { AnalyticsExportSource } from "./AnalyticsExportSource";
import {
  createClickHouseAnalyticsExportSource,
  createDorisAnalyticsExportSource,
} from "./analyticsExportSourceFactories";

type AnalyticsExportSourceFactories = {
  readonly backend: "clickhouse" | "doris";
  readonly createClickHouse: () => AnalyticsExportSource;
  readonly createDoris: () => AnalyticsExportSource;
};

export function createAnalyticsExportSource(
  factories: AnalyticsExportSourceFactories,
): AnalyticsExportSource {
  return factories.backend === "doris"
    ? factories.createDoris()
    : factories.createClickHouse();
}

let analyticsExportSource: AnalyticsExportSource | undefined;

export function getAnalyticsExportSource(): AnalyticsExportSource {
  analyticsExportSource ??= createAnalyticsExportSource({
    backend: resolveAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND),
    createClickHouse: createClickHouseAnalyticsExportSource,
    createDoris: createDorisAnalyticsExportSource,
  });
  return analyticsExportSource;
}
