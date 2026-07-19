import { upsertDefaultModelPrices } from "./scripts/upsertDefaultModelPrices";
import { upsertManagedEvaluators } from "./scripts/upsertManagedEvaluators";
import { upsertLangfuseDashboards } from "./scripts/upsertLangfuseDashboards";
import { initializeClickhouseCompatibility } from "@langfuse/shared/src/server";
import { isAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { env } from "./env";
import { assertDorisAnalyticsReady } from "./services/dorisAnalyticsReadiness";

export const initializeWorker = async (): Promise<void> => {
  if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "clickhouse")) {
    await initializeClickhouseCompatibility();
  } else {
    // Fail closed before registering consumers against an incompatible schema.
    await assertDorisAnalyticsReady({ force: true });
  }

  await Promise.all([
    upsertDefaultModelPrices(),
    upsertManagedEvaluators(),
    upsertLangfuseDashboards(),
  ]);
};
