import type { AnalyticsIntegrationType, Prisma } from "@prisma/client";

import { env } from "@/src/env.mjs";
import {
  syncDorisAnalyticsIntegrationConfigState,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";
import {
  getWebAnalyticsAdmissionContext,
  isWebAnalyticsRuntimeFenced,
} from "@/src/server/analyticsRuntime";

export function getDorisIntegrationMutationAdmission(): AnalyticsRuntimeAdmissionContext | null {
  if (env.LANGFUSE_ANALYTICS_BACKEND !== "doris") return null;
  const admission = getWebAnalyticsAdmissionContext();
  if (
    !admission ||
    admission.backend !== "doris" ||
    isWebAnalyticsRuntimeFenced()
  ) {
    throw new Error(
      "Doris analytics integration capability is not admitted for configuration changes",
    );
  }
  return admission;
}

export async function syncDorisIntegrationMutation(input: {
  readonly transaction: Prisma.TransactionClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly projectId: string;
  readonly integrationType: AnalyticsIntegrationType;
  readonly enabled: boolean;
}): Promise<void> {
  if (!input.admissionContext) return;
  await syncDorisAnalyticsIntegrationConfigState({
    transaction: input.transaction,
    admissionContext: input.admissionContext,
    projectId: input.projectId,
    integrationType: input.integrationType,
    enabled: input.enabled,
  });
}
