import { SeedError, type AnalyticsBackend } from "./scenarios/types";

const REQUIRED_ENV_VARS: Record<AnalyticsBackend, readonly string[]> = {
  clickhouse: [
    "DATABASE_URL",
    "CLICKHOUSE_URL",
    "CLICKHOUSE_USER",
    "CLICKHOUSE_PASSWORD",
  ],
  doris: ["DATABASE_URL"],
};

export const resolveSeederAnalyticsBackend = (
  value: string | undefined,
): AnalyticsBackend => {
  const backend = value ?? "clickhouse";
  if (backend === "clickhouse" || backend === "doris") return backend;
  throw new SeedError(
    `unsupported LANGFUSE_ANALYTICS_BACKEND="${backend}"`,
    "set LANGFUSE_ANALYTICS_BACKEND to clickhouse or doris",
  );
};

export const getSeederRequiredEnvVars = (
  backend: AnalyticsBackend,
): readonly string[] => REQUIRED_ENV_VARS[backend];
