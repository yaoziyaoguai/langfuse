import type { CommunityCapability } from "./communityAvailability";

export type CommunityParityManifestEntry = {
  readonly id: string;
  readonly scope: "included" | "excluded";
  readonly currentDorisStatus:
    | "available"
    | "explicitly-unavailable"
    | "storage-neutral"
    | "cloud-ee-excluded";
  readonly targetDorisStatus: "available" | "storage-neutral" | "excluded";
  readonly ownerUnit: `U${number}`;
  readonly activationClass:
    | "static-synchronous"
    | "durable-activation"
    | "none";
  readonly durableCapability?:
    | "coreBatchExports"
    | "evaluations"
    | "experiments"
    | "datasetRunExports"
    | "datasetRunIngestion"
    | "analyticsIntegrations";
  readonly gateCapability?: CommunityCapability;
  readonly rationale: string;
  readonly evidence: readonly string[];
};

/**
 * Test-only frozen product corpus. Runtime availability remains owned by the
 * feature routes and durable activation rows; this fixture must never become a
 * second routing registry.
 */
export const COMMUNITY_PARITY_MANIFEST = [
  {
    id: "backend-selection-readiness",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U0",
    activationClass: "none",
    rationale:
      "Deployment-level backend selection is required by every surface.",
    evidence: [
      "env:LANGFUSE_ANALYTICS_BACKEND",
      "public:/api/public/ready",
      "worker:/api/ready",
    ],
  },
  {
    id: "otel-v4-canonical-ingestion",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U0",
    activationClass: "static-synchronous",
    rationale: "Canonical telemetry ingestion is a Community foundation path.",
    evidence: ["public:/api/public/otel", "queue:analytics-ingestion-v2-queue"],
  },
  {
    id: "legacy-trace-observation-score-ingestion",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U6",
    activationClass: "static-synchronous",
    rationale:
      "Legacy trace, observation, and score ingestion uses the canonical backend-selected path; dataset-run children require the active durable ingestion capability.",
    evidence: ["public:/api/public/ingestion"],
  },
  {
    id: "trace-observation-score-session-user-reads",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U3",
    activationClass: "static-synchronous",
    rationale:
      "Community list, detail, search, filter, and query surfaces are reachable.",
    evidence: [
      "trpc:traces.all",
      "public:/api/public/traces",
      "public:/api/public/observations",
      "public:/api/public/scores",
      "mcp:listObservations",
    ],
  },
  {
    id: "trace-metrics-bulk-detail-export-source",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U1",
    activationClass: "static-synchronous",
    rationale:
      "Trace metrics and exact-ID bulk reads support public trace workflows and exports.",
    evidence: ["trpc:traces.metrics", "public:/api/public/v2/metrics"],
  },
  {
    id: "core-batch-exports",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U2",
    activationClass: "durable-activation",
    durableCapability: "coreBatchExports",
    gateCapability: "batchExports",
    rationale:
      "Doris core exports have durable admission, manifest, execution, recovery, and cleanup; external creation still requires an ACTIVE matching capability generation.",
    evidence: ["trpc:batchExport.create", "queue:batch-export-queue"],
  },
  {
    id: "monitors",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U3",
    activationClass: "static-synchronous",
    gateCapability: "monitors",
    rationale:
      "Community monitor pages and workers consume the shared QueryEngine contract.",
    evidence: [
      "page:/project/[projectId]/monitors",
      "trpc:monitors.all",
      "mcp:monitors",
    ],
  },
  {
    id: "custom-dashboards-widgets",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U3",
    activationClass: "static-synchronous",
    gateCapability: "customDashboards",
    rationale:
      "Community dashboard and widget authoring is registered and reachable.",
    evidence: [
      "page:/project/[projectId]/dashboards",
      "trpc:dashboard.createDashboard",
      "mcp:dashboardWidgets",
      "public:/api/public/unstable/dashboards",
      "public:/api/public/unstable/dashboard-widgets",
    ],
  },
  {
    id: "evaluator-execution",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U5",
    activationClass: "durable-activation",
    durableCapability: "evaluations",
    gateCapability: "evaluations",
    rationale:
      "Evaluation rules and executions are Community product surfaces.",
    evidence: [
      "page:/project/[projectId]/evals",
      "trpc:evals.createJob",
      "public:/api/public/unstable/evaluators",
      "mcp:evals",
    ],
  },
  {
    id: "experiment-execution",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U6",
    activationClass: "durable-activation",
    durableCapability: "experiments",
    gateCapability: "experiments",
    rationale:
      "Prompt and remote experiment execution is available to Community users in ClickHouse mode.",
    evidence: [
      "page:/project/[projectId]/experiments",
      "trpc:experiments.createExperiment",
    ],
  },
  {
    id: "dataset-run-ingestion-analytics",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U6",
    activationClass: "durable-activation",
    durableCapability: "datasetRunIngestion",
    gateCapability: "experiments",
    rationale:
      "Dataset-run children and projections are required by experiment surfaces.",
    evidence: [
      "public:/api/public/dataset-run-items",
      "mcp:createDatasetRunItem",
    ],
  },
  {
    id: "dataset-run-exports",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U6",
    activationClass: "durable-activation",
    durableCapability: "datasetRunExports",
    gateCapability: "batchExports",
    rationale:
      "Dataset-run export is part of the registered batch-export table contract.",
    evidence: ["trpc:batchExport.create:dataset_run_items"],
  },
  {
    id: "dataset-run-query-and-comparison",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U6",
    activationClass: "durable-activation",
    durableCapability: "datasetRunIngestion",
    gateCapability: "experiments",
    rationale:
      "Run list, detail, compare, and metrics pages consume dataset-run analytics.",
    evidence: [
      "trpc:datasets.runsByDatasetId",
      "trpc:datasets.countAllDatasetItems",
      "mcp:listDatasetRuns",
      "page:/project/[projectId]/datasets/[datasetId]/runs/[runId]",
    ],
  },
  {
    id: "analytics-integrations",
    scope: "included",
    currentDorisStatus: "available",
    targetDorisStatus: "available",
    ownerUnit: "U7",
    activationClass: "durable-activation",
    durableCapability: "analyticsIntegrations",
    gateCapability: "analyticsIntegrations",
    rationale:
      "PostHog, Mixpanel, and Blob export integrations are Community surfaces.",
    evidence: [
      "trpc:posthogIntegration.update",
      "trpc:mixpanelIntegration.update",
      "public:/api/public/integrations/blob-storage",
      "page:/project/[projectId]/settings/integrations/posthog",
    ],
  },
  {
    id: "postgres-control-plane",
    scope: "included",
    currentDorisStatus: "storage-neutral",
    targetDorisStatus: "storage-neutral",
    ownerUnit: "U0",
    activationClass: "none",
    rationale:
      "Projects, datasets, configs, prompts, and authorization remain in PostgreSQL.",
    evidence: ["postgres:projects,datasets,prompts,job-configurations"],
  },
  {
    id: "cloud-core-data-s3-export",
    scope: "excluded",
    currentDorisStatus: "cloud-ee-excluded",
    targetDorisStatus: "excluded",
    ownerUnit: "U0",
    activationClass: "none",
    rationale:
      "This is an explicitly Cloud operational export, not a Community product surface.",
    evidence: ["worker:CoreDataS3ExportQueue"],
  },
  {
    id: "enterprise-cloud-billing-and-operations",
    scope: "excluded",
    currentDorisStatus: "cloud-ee-excluded",
    targetDorisStatus: "excluded",
    ownerUnit: "U0",
    activationClass: "none",
    rationale:
      "Enterprise, billing, and Cloud-only operations are outside this Community baseline.",
    evidence: ["directory:ee", "worker:CloudUsageMeteringQueue"],
  },
] as const satisfies readonly CommunityParityManifestEntry[];
