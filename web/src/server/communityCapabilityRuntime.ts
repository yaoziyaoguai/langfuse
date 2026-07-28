import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import type { AnalyticsBackend } from "@langfuse/shared/analytics-backend";

import {
  isCommunityCapabilityAvailable,
  type CommunityCapability,
} from "@/src/features/capabilities/communityAvailability";

export type ActiveDorisCommunityCapability =
  | "evaluations"
  | "experiments"
  | "analyticsIntegrations";

const DURABLE_DORIS_CAPABILITIES = {
  evaluations: "EVALUATIONS",
  experiments: "EXPERIMENTS",
  analyticsIntegrations: "ANALYTICS_INTEGRATIONS",
} as const satisfies Partial<
  Record<
    CommunityCapability,
    "EVALUATIONS" | "EXPERIMENTS" | "ANALYTICS_INTEGRATIONS"
  >
>;

const INTERNAL_DORIS_CAPABILITIES = {
  datasetRunIngestion: "DATASET_RUN_INGESTION",
  datasetRunExports: "DATASET_RUN_EXPORTS",
} as const;

export type InternalDorisCapability = keyof typeof INTERNAL_DORIS_CAPABILITIES;

export async function getActiveDorisCommunityCapabilities(
  client: Pick<
    PrismaClient,
    "analyticsBackendDeploymentState" | "analyticsCapabilityActivation"
  > = prisma,
): Promise<readonly ActiveDorisCommunityCapability[]> {
  const deployment = await client.analyticsBackendDeploymentState.findUnique({
    where: { id: "global" },
    select: { backend: true, generation: true },
  });
  if (!deployment || deployment.backend !== "DORIS") return [];

  const active = await client.analyticsCapabilityActivation.findMany({
    where: {
      capability: { in: Object.values(DURABLE_DORIS_CAPABILITIES) },
      backend: "DORIS",
      deploymentGeneration: deployment.generation,
      status: "ACTIVE",
    },
    select: { capability: true },
  });
  const activeRows = new Set(active.map(({ capability }) => capability));
  return (
    Object.entries(DURABLE_DORIS_CAPABILITIES) as readonly [
      ActiveDorisCommunityCapability,
      "EVALUATIONS" | "EXPERIMENTS" | "ANALYTICS_INTEGRATIONS",
    ][]
  )
    .filter(([, capability]) => activeRows.has(capability))
    .map(([capability]) => capability);
}

export async function isCommunityCapabilityRuntimeAvailable(
  capability: CommunityCapability,
  backend: AnalyticsBackend,
  client?: Pick<
    PrismaClient,
    "analyticsBackendDeploymentState" | "analyticsCapabilityActivation"
  >,
): Promise<boolean> {
  if (!isCommunityCapabilityAvailable(capability, backend)) return false;
  if (backend === "clickhouse" || !(capability in DURABLE_DORIS_CAPABILITIES)) {
    return true;
  }
  return (await getActiveDorisCommunityCapabilities(client)).includes(
    capability as ActiveDorisCommunityCapability,
  );
}

export async function isInternalDorisCapabilityActive(
  capability: InternalDorisCapability,
  client: Pick<
    PrismaClient,
    "analyticsBackendDeploymentState" | "analyticsCapabilityActivation"
  > = prisma,
): Promise<boolean> {
  const deployment = await client.analyticsBackendDeploymentState.findUnique({
    where: { id: "global" },
    select: { backend: true, generation: true },
  });
  if (!deployment || deployment.backend !== "DORIS") return false;
  const activation = await client.analyticsCapabilityActivation.findUnique({
    where: { capability: INTERNAL_DORIS_CAPABILITIES[capability] },
    select: {
      backend: true,
      deploymentGeneration: true,
      status: true,
    },
  });
  return (
    activation?.backend === "DORIS" &&
    activation.deploymentGeneration === deployment.generation &&
    activation.status === "ACTIVE"
  );
}
