export const ANALYTICS_CAPABILITY_NAMES = [
  "coreBatchExports",
  "evaluations",
  "experiments",
  "datasetRunExports",
  "datasetRunIngestion",
  "analyticsIntegrations",
] as const;

export type AnalyticsCapabilityName =
  (typeof ANALYTICS_CAPABILITY_NAMES)[number];

export type AnalyticsCapabilityRuntimeRole =
  | "capture"
  | "consumer"
  | "producer"
  | "recovery";

export type AnalyticsCapabilityActivationStatus =
  | "DISABLED"
  | "DARK"
  | "ACTIVE"
  | "DRAINING";

type AnalyticsCapabilityContract = {
  readonly ownerUnit: "U2" | "U5" | "U6" | "U7";
  readonly contractVersion: number;
  readonly minimumRuntimeContract: number;
  readonly initialDorisStatus: "DISABLED";
  readonly dependencies: readonly AnalyticsCapabilityName[];
  readonly components: Readonly<
    Record<"web" | "worker", readonly AnalyticsCapabilityRuntimeRole[]>
  >;
};

export const ANALYTICS_CAPABILITY_CATALOG = {
  coreBatchExports: {
    ownerUnit: "U2",
    contractVersion: 1,
    minimumRuntimeContract: 1,
    initialDorisStatus: "DISABLED",
    dependencies: [],
    components: {
      web: ["producer"],
      worker: ["consumer", "recovery"],
    },
  },
  evaluations: {
    ownerUnit: "U5",
    contractVersion: 1,
    minimumRuntimeContract: 1,
    initialDorisStatus: "DISABLED",
    dependencies: [],
    components: {
      web: ["producer"],
      worker: ["capture", "consumer", "recovery"],
    },
  },
  experiments: {
    ownerUnit: "U6",
    contractVersion: 1,
    minimumRuntimeContract: 1,
    initialDorisStatus: "DISABLED",
    dependencies: ["datasetRunIngestion"],
    components: {
      web: ["producer"],
      worker: ["consumer", "recovery"],
    },
  },
  datasetRunExports: {
    ownerUnit: "U6",
    contractVersion: 1,
    minimumRuntimeContract: 1,
    initialDorisStatus: "DISABLED",
    dependencies: ["coreBatchExports"],
    components: {
      web: ["producer"],
      worker: ["consumer", "recovery"],
    },
  },
  datasetRunIngestion: {
    ownerUnit: "U6",
    contractVersion: 1,
    minimumRuntimeContract: 1,
    initialDorisStatus: "DISABLED",
    dependencies: [],
    components: {
      web: ["producer"],
      worker: ["producer", "consumer", "recovery"],
    },
  },
  analyticsIntegrations: {
    ownerUnit: "U7",
    contractVersion: 1,
    minimumRuntimeContract: 1,
    initialDorisStatus: "DISABLED",
    dependencies: [],
    components: {
      web: ["producer"],
      worker: ["capture", "consumer", "recovery"],
    },
  },
} as const satisfies Readonly<
  Record<AnalyticsCapabilityName, AnalyticsCapabilityContract>
>;

export const STATIC_SYNCHRONOUS_ANALYTICS_FEATURES = [
  "coreIngestion",
  "coreReads",
  "queryEngine",
  "monitors",
  "customDashboards",
] as const;
