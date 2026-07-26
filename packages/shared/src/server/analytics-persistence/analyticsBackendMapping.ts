import type {
  AnalyticsBackendType,
  AnalyticsCapability,
  AnalyticsRuntimeCapabilityRole,
  AnalyticsRuntimeComponent,
} from "@prisma/client";

import type { AnalyticsBackend } from "./analyticsBackend";
import type {
  AnalyticsCapabilityName,
  AnalyticsCapabilityRuntimeRole,
} from "./analyticsCapabilities";

export type AnalyticsServingRuntimeComponentName = "web" | "worker";
export type AnalyticsRuntimeComponentName =
  | AnalyticsServingRuntimeComponentName
  | "checkpoint";

const BACKEND_TO_PRISMA = {
  clickhouse: "CLICKHOUSE",
  doris: "DORIS",
} as const satisfies Record<AnalyticsBackend, AnalyticsBackendType>;

const COMPONENT_TO_PRISMA = {
  web: "WEB",
  worker: "WORKER",
  checkpoint: "CHECKPOINT",
} as const satisfies Record<
  AnalyticsRuntimeComponentName,
  AnalyticsRuntimeComponent
>;

const CAPABILITY_TO_PRISMA = {
  coreBatchExports: "CORE_BATCH_EXPORTS",
  evaluations: "EVALUATIONS",
  experiments: "EXPERIMENTS",
  datasetRunExports: "DATASET_RUN_EXPORTS",
  datasetRunIngestion: "DATASET_RUN_INGESTION",
  analyticsIntegrations: "ANALYTICS_INTEGRATIONS",
} as const satisfies Record<AnalyticsCapabilityName, AnalyticsCapability>;

const ROLE_TO_PRISMA = {
  capture: "CAPTURE",
  producer: "PRODUCER",
  consumer: "CONSUMER",
  recovery: "RECOVERY",
} as const satisfies Record<
  AnalyticsCapabilityRuntimeRole,
  AnalyticsRuntimeCapabilityRole
>;

export function toPrismaAnalyticsBackend(
  backend: AnalyticsBackend,
): AnalyticsBackendType {
  return BACKEND_TO_PRISMA[backend];
}

export function toPrismaAnalyticsRuntimeComponent(
  component: AnalyticsRuntimeComponentName,
): AnalyticsRuntimeComponent {
  return COMPONENT_TO_PRISMA[component];
}

export function toPrismaAnalyticsCapability(
  capability: AnalyticsCapabilityName,
): AnalyticsCapability {
  return CAPABILITY_TO_PRISMA[capability];
}

export function fromPrismaAnalyticsCapability(
  capability: AnalyticsCapability,
): AnalyticsCapabilityName {
  switch (capability) {
    case "CORE_BATCH_EXPORTS":
      return "coreBatchExports";
    case "EVALUATIONS":
      return "evaluations";
    case "EXPERIMENTS":
      return "experiments";
    case "DATASET_RUN_EXPORTS":
      return "datasetRunExports";
    case "DATASET_RUN_INGESTION":
      return "datasetRunIngestion";
    case "ANALYTICS_INTEGRATIONS":
      return "analyticsIntegrations";
  }
}

export function toPrismaAnalyticsRuntimeRole(
  role: AnalyticsCapabilityRuntimeRole,
): AnalyticsRuntimeCapabilityRole {
  return ROLE_TO_PRISMA[role];
}

export function fromPrismaAnalyticsRuntimeComponent(
  component: AnalyticsRuntimeComponent,
): AnalyticsRuntimeComponentName {
  switch (component) {
    case "WEB":
      return "web";
    case "WORKER":
      return "worker";
    case "CHECKPOINT":
      return "checkpoint";
  }
}

export function fromPrismaAnalyticsServingRuntimeComponent(
  component: AnalyticsRuntimeComponent,
): AnalyticsServingRuntimeComponentName {
  const mapped = fromPrismaAnalyticsRuntimeComponent(component);
  if (mapped === "checkpoint") {
    throw new Error("Checkpoint runtime is not a serving fleet member");
  }
  return mapped;
}
