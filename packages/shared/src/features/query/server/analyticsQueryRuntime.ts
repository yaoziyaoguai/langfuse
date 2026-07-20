import { env } from "../../../env";
import {
  resolveAnalyticsBackend,
  type AnalyticsBackend,
} from "../../../server/analytics-persistence";
import { getDorisQueryExecutor } from "../../../server/repositories/telemetry/doris/runtime";
import type { AnalyticsQueryEngine } from "./AnalyticsQueryEngine";
import { createClickHouseAnalyticsQueryEngine } from "./adapters/clickhouse/ClickHouseAnalyticsQueryEngine";
import { createDorisAnalyticsQueryEngine } from "./adapters/doris/DorisAnalyticsQueryAdapter";

type AnalyticsQueryEngineFactories = {
  readonly backend: AnalyticsBackend;
  readonly createClickHouse: () => AnalyticsQueryEngine;
  readonly createDoris: () => AnalyticsQueryEngine;
};

export function createAnalyticsQueryEngine(
  factories: AnalyticsQueryEngineFactories,
): AnalyticsQueryEngine {
  return factories.backend === "doris"
    ? factories.createDoris()
    : factories.createClickHouse();
}

let analyticsQueryEngine: AnalyticsQueryEngine | undefined;

export function getAnalyticsQueryEngine(): AnalyticsQueryEngine {
  analyticsQueryEngine ??= createAnalyticsQueryEngine({
    backend: resolveAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND),
    createClickHouse: createClickHouseAnalyticsQueryEngine,
    createDoris: () => createDorisAnalyticsQueryEngine(getDorisQueryExecutor()),
  });
  return analyticsQueryEngine;
}
