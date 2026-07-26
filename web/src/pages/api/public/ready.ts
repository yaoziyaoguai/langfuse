import { VERSION } from "@/src/constants";
import { cors, runMiddleware } from "@/src/features/public-api/server/cors";
import { telemetry } from "@/src/features/telemetry";
import { checkWebAnalyticsRuntimeReadiness } from "@/src/server/analyticsRuntime";
import { isSigtermReceived } from "@/src/utils/shutdown";
import { env } from "@/src/env.mjs";
import { prisma } from "@langfuse/shared/src/db";
import {
  checkAnalyticsReadiness,
  DorisClientManager,
  logger,
  parseDorisQueryConfig,
  resolveDorisNodeEnv,
  PrismaAnalyticsCompatibilityControlState,
  SUPPORTED_DORIS_CANONICALIZER_VERSIONS,
  SUPPORTED_DORIS_SCHEMA_VERSIONS,
  traceException,
} from "@langfuse/shared/src/server";
import { type NextApiRequest, type NextApiResponse } from "next";

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  try {
    await runMiddleware(req, res, cors);
    await telemetry();

    if (isSigtermReceived()) {
      logger.info(
        "Readiness check failed: SIGTERM / SIGINT received, shutting down.",
      );
      return res.status(500).json({
        status: "SIGTERM / SIGINT received, shutting down",
        version: VERSION.replace("v", ""),
      });
    }

    const expectedAnalyticsBackend = req.query.analyticsBackend;
    if (
      expectedAnalyticsBackend !== undefined &&
      expectedAnalyticsBackend !== "clickhouse" &&
      expectedAnalyticsBackend !== "doris"
    ) {
      return res.status(400).json({
        status: "Invalid analytics backend expectation",
        version: VERSION.replace("v", ""),
      });
    }
    if (
      expectedAnalyticsBackend !== undefined &&
      expectedAnalyticsBackend !== env.LANGFUSE_ANALYTICS_BACKEND
    ) {
      return res.status(503).json({
        status: "Expected analytics backend is not selected",
        version: VERSION.replace("v", ""),
      });
    }

    if (env.LANGFUSE_ANALYTICS_BACKEND === "doris") {
      const client = DorisClientManager.getInstance().getClient(
        parseDorisQueryConfig(
          process.env,
          resolveDorisNodeEnv(env.NODE_ENV, env.DORIS_LOCAL_DEV_MODE),
        ),
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
          version: VERSION.replace("v", ""),
        });
      }
    }

    if (!(await checkWebAnalyticsRuntimeReadiness())) {
      return res.status(503).json({
        status: "Analytics runtime readiness check failed",
        version: VERSION.replace("v", ""),
      });
    }
  } catch (e) {
    traceException(e);
    logger.warn("Readiness check failed: ", e);
    return res.status(503).json({
      status: "Readiness check failed",
      version: VERSION.replace("v", ""),
    });
  }
  return res.status(200).json({
    status: "OK",
    version: VERSION.replace("v", ""),
  });
}
