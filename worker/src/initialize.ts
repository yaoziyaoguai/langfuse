import { upsertDefaultModelPrices } from "./scripts/upsertDefaultModelPrices";
import { upsertLangfuseDashboards } from "./scripts/upsertLangfuseDashboards";
import { assertDorisAnalyticsReady } from "./services/dorisAnalyticsReadiness";

export const initializeWorker = async (): Promise<void> => {
  // Fail closed before app.ts can register any BullMQ consumer or outbox
  // publisher against an incompatible Doris schema.
  await assertDorisAnalyticsReady({ force: true });
  await Promise.all([upsertDefaultModelPrices(), upsertLangfuseDashboards()]);
};
