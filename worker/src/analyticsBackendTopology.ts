import {
  resolveAnalyticsBackend,
  type AnalyticsBackend,
} from "@langfuse/shared/analytics-backend";

export type AnalyticsWorkerTopology = {
  readonly backend: AnalyticsBackend;
  readonly clickhouseAnalyticsEnabled: boolean;
  readonly dorisAnalyticsEnabled: boolean;
};

/** Resolves the mutually exclusive analytics worker topology at process start. */
export function resolveAnalyticsWorkerTopology(
  configured: string | undefined,
): AnalyticsWorkerTopology {
  const backend = resolveAnalyticsBackend(configured);
  return {
    backend,
    clickhouseAnalyticsEnabled: backend === "clickhouse",
    dorisAnalyticsEnabled: backend === "doris",
  };
}
