import { env } from "../../../../env";
import { DorisClientManager } from "../../../doris/client";
import {
  parseDorisQueryConfig,
  resolveDorisNodeEnv,
} from "../../../doris/config";
import {
  createDorisTelemetryRepositories,
  type DorisTelemetryRepositories,
} from "./composition";

let repositories: DorisTelemetryRepositories | undefined;
let executor: ReturnType<DorisClientManager["getClient"]> | undefined;

export function getDorisTelemetryRepositories(): DorisTelemetryRepositories {
  repositories ??= createDorisTelemetryRepositories(getDorisQueryExecutor());
  return repositories;
}

export function getDorisQueryExecutor(): ReturnType<
  DorisClientManager["getClient"]
> {
  executor ??= DorisClientManager.getInstance().getClient(
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
      resolveDorisNodeEnv(env.NODE_ENV, env.DORIS_LOCAL_DEV_MODE),
    ),
  );
  return executor;
}
