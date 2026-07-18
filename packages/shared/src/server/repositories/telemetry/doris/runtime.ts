import { env } from "../../../../env";
import { DorisClientManager } from "../../../doris/client";
import { parseDorisQueryConfig } from "../../../doris/config";
import {
  createDorisTelemetryRepositories,
  type DorisTelemetryRepositories,
} from "./composition";

let repositories: DorisTelemetryRepositories | undefined;

export function isDorisAnalyticsBackend(): boolean {
  return env.LANGFUSE_ANALYTICS_BACKEND === "doris";
}

export function getDorisTelemetryRepositories(): DorisTelemetryRepositories {
  if (!isDorisAnalyticsBackend()) {
    throw new Error("Doris analytics repositories are not active");
  }
  repositories ??= createDorisTelemetryRepositories(
    DorisClientManager.getInstance().getClient(
      parseDorisQueryConfig(
        {
          DORIS_QUERY_URL: env.DORIS_QUERY_URL,
          DORIS_QUERY_USER: env.DORIS_QUERY_USER,
          DORIS_QUERY_PASSWORD: env.DORIS_QUERY_PASSWORD,
          DORIS_QUERY_TLS_ENABLED: env.DORIS_QUERY_TLS_ENABLED,
          DORIS_QUERY_TLS_CA_PATH: env.DORIS_QUERY_TLS_CA_PATH,
          DORIS_QUERY_MAX_CONNECTIONS: String(env.DORIS_QUERY_MAX_CONNECTIONS),
          DORIS_QUERY_CONNECT_TIMEOUT_MS: String(
            env.DORIS_QUERY_CONNECT_TIMEOUT_MS,
          ),
          DORIS_QUERY_TIMEOUT_MS: String(env.DORIS_QUERY_TIMEOUT_MS),
        },
        env.NODE_ENV,
      ),
    ),
  );
  return repositories;
}
