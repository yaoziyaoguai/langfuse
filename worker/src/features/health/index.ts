import { prisma } from "@langfuse/shared/src/db";
import {
  checkAnalyticsReadiness,
  DorisClientManager,
  logger,
  parseDorisQueryConfig,
  PrismaAnalyticsCompatibilityControlState,
  redis,
  SUPPORTED_DORIS_CANONICALIZER_VERSIONS,
  SUPPORTED_DORIS_SCHEMA_VERSIONS,
} from "@langfuse/shared/src/server";
import { Response } from "express";

import { env } from "../../env";

type ContainerHealthOptions = {
  /** Fail (500) once a SIGTERM/SIGINT has been received (readiness only). */
  failOnSigterm: boolean;
};

/**
 * Check the health of the container.
 */
export const checkContainerHealth = async (
  res: Response,
  options: ContainerHealthOptions,
) => {
  const { failOnSigterm } = options;

  if (failOnSigterm && isSigtermReceived()) {
    logger.info(
      "Health check failed: SIGTERM / SIGINT received, shutting down.",
    );
    return res.status(500).json({
      status: "SIGTERM / SIGINT received, shutting down",
    });
  }

  //check database health
  await prisma.$queryRaw`SELECT 1;`;

  if (!redis) {
    throw new Error("Redis connection not available");
  }

  await Promise.race([
    redis?.ping(),
    new Promise((_, reject) =>
      setTimeout(
        () => reject(new Error("Redis ping timeout after 2 seconds")),
        2000,
      ),
    ),
  ]);

  if (failOnSigterm) {
    const client = DorisClientManager.getInstance().getClient(
      parseDorisQueryConfig(process.env, env.NODE_ENV),
    );
    const analytics = await checkAnalyticsReadiness({
      executor: client,
      controlState: new PrismaAnalyticsCompatibilityControlState(prisma),
      supportedCanonicalizerVersions: SUPPORTED_DORIS_CANONICALIZER_VERSIONS,
      supportedSchemaVersions: SUPPORTED_DORIS_SCHEMA_VERSIONS,
    });
    if (!analytics.ready) {
      return res.status(503).json({
        status: "Analytics readiness check failed",
        analytics: analytics.code,
        schemaVersion: analytics.schemaVersion,
      });
    }
  }

  res.json({
    status: "ok",
  });
};

let sigtermReceived = false;

export const setSigtermReceived = () => {
  logger.info("Set sigterm received to true");
  sigtermReceived = true;
};

export const isSigtermReceived = () => sigtermReceived;
