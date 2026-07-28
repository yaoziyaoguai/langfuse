// https://github.com/vercel/next.js/issues/51404
// There is no official best way to gracefully shutdown a Next.js app in Docker.
// This here is a workaround to handle SIGTERM and SIGINT signals.
// NEVER call process.exit() in this process. Kubernetes should kill the container: https://kostasbariotis.com/why-you-should-not-use-process-exit/
// We wait for 110 seconds to allow the app to finish processing requests. There is no native way to do this in Next.js.

import {
  ClickHouseClientManager,
  DorisClientManager,
  logger,
  redis,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";
import { RateLimitService } from "@/src/features/public-api/server/RateLimitService";
import { quiesceWebAnalyticsRuntime } from "@/src/server/analyticsRuntime";
import { env } from "@/src/env.mjs";
import type prexit from "prexit";

const TIMEOUT = 110_000;
type ShutdownSignal = Parameters<NonNullable<Parameters<typeof prexit>[1]>>[0];

const runShutdownStep = async (
  failureMessage: string,
  step: () => unknown | Promise<unknown>,
): Promise<boolean> => {
  try {
    await step();
    return true;
  } catch (error) {
    logger.error(failureMessage, error);
    return false;
  }
};

const performShutdown = async (): Promise<void> => {
  const requestsStopped = await runShutdownStep(
    "Failed to shut down rate limiting",
    () => RateLimitService.shutdown(),
  );

  const analyticsConnectionsClosed =
    env.LANGFUSE_ANALYTICS_BACKEND === "clickhouse"
      ? await runShutdownStep(
          "Failed to close ClickHouse analytics connections",
          () => ClickHouseClientManager.getInstance().closeAllConnections(),
        )
      : await runShutdownStep(
          "Failed to close Doris analytics connections",
          () => DorisClientManager.getInstance().closeAllConnections(),
        );

  if (requestsStopped && analyticsConnectionsClosed) {
    await runShutdownStep(
      "Failed to quiesce analytics runtime lease",
      async () => {
        if (!(await quiesceWebAnalyticsRuntime())) {
          throw new Error("Analytics runtime lease refused quiescence");
        }
      },
    );
  } else {
    logger.error(
      "Analytics runtime lease remains active because web drain did not complete",
    );
  }

  const redisConnection = redis;
  logger.info(`Redis status ${redisConnection?.status}`);
  if (!redisConnection) {
    logger.info("Redis connection not available");
  } else if (redisConnection.status === "end") {
    logger.info("Redis connection already closed");
  } else {
    await runShutdownStep("Failed to close Redis connection", () =>
      redisConnection.disconnect(),
    );
  }

  await runShutdownStep("Failed to disconnect Prisma", () =>
    prisma.$disconnect(),
  );
  logger.info("Shutdown complete");
};

declare global {
  var sigtermReceived: boolean | undefined;
}

globalThis.sigtermReceived = globalThis.sigtermReceived ?? false;

export const setSigtermReceived = () => {
  console.log("Set sigterm received to true");
  globalThis.sigtermReceived = true;
};

export const isSigtermReceived = () =>
  Boolean(process.env.NEXT_MANUAL_SIG_HANDLE) && globalThis.sigtermReceived;

export const shutdown = async (signal: ShutdownSignal) => {
  if (signal === "SIGTERM" || signal === "SIGINT") {
    console.log(
      `SIGTERM / SIGINT received. Shutting down in ${TIMEOUT / 1000} seconds.`,
    );
    setSigtermReceived();

    return await new Promise<void>((resolve) => {
      setTimeout(() => {
        performShutdown().then(resolve, (error) => {
          logger.error("Unexpected web shutdown failure", error);
          resolve();
        });
      }, TIMEOUT);
    });
  }
};
