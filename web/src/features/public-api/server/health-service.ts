import { VERSION } from "@/src/constants";
import { env } from "@/src/env.mjs";
import { prisma } from "@langfuse/shared/src/db";
import {
  DorisClientManager,
  logger,
  parseDorisQueryConfig,
  resolveDorisNodeEnv,
  traceException,
} from "@langfuse/shared/src/server";

type HealthCheckInput = {
  failIfDatabaseUnavailable: boolean;
  failIfNoRecentEvents: boolean;
};

export type HealthCheckResult = {
  isHealthy: boolean;
  status: string;
  version: string;
};

export const runHealthCheck = async ({
  failIfDatabaseUnavailable,
  failIfNoRecentEvents,
}: HealthCheckInput): Promise<HealthCheckResult> => {
  const version = VERSION.replace("v", "");

  try {
    try {
      if (failIfDatabaseUnavailable) {
        await prisma.$queryRaw`SELECT 1;`;
      }
    } catch (error) {
      logger.error("Couldn't connect to database", error);
      traceException(error);
      return {
        isHealthy: false,
        status: "Database not available",
        version,
      };
    }

    try {
      if (failIfNoRecentEvents) {
        const client = DorisClientManager.getInstance().getClient(
          parseDorisQueryConfig(
            process.env,
            resolveDorisNodeEnv(env.NODE_ENV, env.DORIS_LOCAL_DEV_MODE),
          ),
        );
        const events = await client.query<{ span_id: string }>(
          `SELECT span_id
           FROM events_current
           WHERE start_time <= CURRENT_TIMESTAMP(6)
             AND start_time >= CURRENT_TIMESTAMP(6) - INTERVAL 3 MINUTE
           LIMIT 1`,
        );

        if (events.length === 0) {
          return {
            isHealthy: false,
            status: "No events within the last 3 minutes",
            version,
          };
        }
      }
    } catch (error) {
      logger.error("Couldn't fetch recent events", error);
      traceException(error);
      return {
        isHealthy: false,
        status: "Couldn't fetch recent events",
        version,
      };
    }
  } catch (error) {
    traceException(error);
    logger.error("Health check failed", error);
    return {
      isHealthy: false,
      status: "Health check failed",
      version,
    };
  }

  return {
    isHealthy: true,
    status: "OK",
    version,
  };
};
