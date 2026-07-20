import { resolveAnalyticsBackend } from "@langfuse/shared/analytics-backend";

import { env } from "../../env";
import type { AnalyticsEvaluationTargetSource } from "./AnalyticsEvaluationTargetSource";
import {
  createClickHouseEvaluationTargetSource,
  createDorisEvaluationTargetSource,
} from "./evaluationTargetSourceFactories";

type EvaluationTargetSourceFactories = {
  readonly backend: "clickhouse" | "doris";
  readonly createClickHouse: () => AnalyticsEvaluationTargetSource;
  readonly createDoris: () => AnalyticsEvaluationTargetSource;
};

export function createAnalyticsEvaluationTargetSource(
  factories: EvaluationTargetSourceFactories,
): AnalyticsEvaluationTargetSource {
  return factories.backend === "doris"
    ? factories.createDoris()
    : factories.createClickHouse();
}

let evaluationTargetSource: AnalyticsEvaluationTargetSource | undefined;

export function getAnalyticsEvaluationTargetSource() {
  evaluationTargetSource ??= createAnalyticsEvaluationTargetSource({
    backend: resolveAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND),
    createClickHouse: createClickHouseEvaluationTargetSource,
    createDoris: createDorisEvaluationTargetSource,
  });
  return evaluationTargetSource;
}
