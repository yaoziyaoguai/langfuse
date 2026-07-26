import {
  ClickHouseClientManager,
  DorisClientManager,
  logger,
} from "@langfuse/shared/src/server";
import { redis } from "@langfuse/shared/src/server";

import { ClickhouseWriter } from "../services/ClickhouseWriter";
import { setSigtermReceived } from "../features/health";
import { server } from "../index";
import { freeAllTokenizers } from "../features/tokenisation/usage";
import { getTokenCountWorkerManager } from "../features/tokenisation/async-usage";
import { WorkerManager } from "../queues/workerManager";
import { logInFlightBlobExportsOnShutdown } from "../features/blobstorage/inFlightExports";
import { prisma } from "@langfuse/shared/src/db";
import { stopWorkerWorkloads } from "../app";
import { env } from "../env";
import { isAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { quiesceWorkerAnalyticsRuntime } from "../analyticsRuntime";

const runShutdownStep = async (
  failureMessage: string,
  step: () => void | Promise<void>,
): Promise<boolean> => {
  try {
    await step();
    return true;
  } catch (error) {
    logger.error(failureMessage, error);
    return false;
  }
};

const performShutdown = async (signal: NodeJS.Signals): Promise<void> => {
  logger.info(`Received ${signal}, closing server...`);
  setSigtermReceived();

  // Stop accepting new connections
  server?.close();
  logger.info("Server has been closed.");

  // Before closeWorkers(), while the registry is still populated (LFE-10388).
  await runShutdownStep("Failed to inspect in-flight blob exports", () =>
    logInFlightBlobExportsOnShutdown(),
  );

  // Shutdown workers (https://docs.bullmq.io/guide/going-to-production#gracefully-shut-down-workers)
  const [workersStopped, workloadsStopped] = await Promise.all([
    runShutdownStep("Failed to drain worker workloads", () =>
      WorkerManager.closeWorkers(),
    ),
    runShutdownStep("Failed to drain worker workloads", () =>
      stopWorkerWorkloads(),
    ),
  ]);

  let writerStopped = true;
  if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "clickhouse")) {
    // Flush pending writes after the ClickHouse ingestion workers have stopped.
    writerStopped = await runShutdownStep(
      "Failed to shut down Clickhouse writer",
      async () => {
        await ClickhouseWriter.getInstance().shutdown();
        logger.info("Clickhouse writer has been shut down.");
      },
    );
  }

  const analyticsConnectionsClosed = await runShutdownStep(
    "Failed to close analytics connections",
    async () => {
      if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "clickhouse")) {
        await ClickHouseClientManager.getInstance().closeAllConnections();
      } else {
        await DorisClientManager.getInstance().closeAllConnections();
      }
    },
  );

  if (
    workersStopped &&
    workloadsStopped &&
    writerStopped &&
    analyticsConnectionsClosed
  ) {
    await runShutdownStep(
      "Failed to quiesce analytics runtime lease",
      async () => {
        if (!(await quiesceWorkerAnalyticsRuntime())) {
          throw new Error("Analytics runtime lease refused quiescence");
        }
      },
    );
  } else {
    logger.error(
      "Analytics runtime lease remains active because worker drain did not complete",
    );
  }

  await runShutdownStep("Failed to close Redis connection", () => {
    redis?.disconnect();
    logger.info("Redis connection has been closed.");
  });

  await runShutdownStep("Failed to disconnect Prisma", async () => {
    await prisma.$disconnect();
    logger.info("Prisma connection has been closed.");
  });

  // Shutdown tokenization worker threads
  await runShutdownStep(
    "Error terminating token count worker threads",
    async () => {
      await getTokenCountWorkerManager().terminate();
      logger.info("Token count worker threads have been terminated.");
    },
  );

  await runShutdownStep("Failed to clean up tokenizers", () => {
    freeAllTokenizers();
    logger.info("All tokenizers are cleaned up from memory.");
  });

  logger.info("Shutdown complete, exiting process...");
};

let shutdownOperation: Promise<void> | null = null;

export const onShutdown = (signal: NodeJS.Signals): Promise<void> => {
  if (shutdownOperation) return shutdownOperation;

  const operation = performShutdown(signal);
  shutdownOperation = operation;
  operation.then(
    () => {
      if (shutdownOperation === operation) shutdownOperation = null;
    },
    () => {
      if (shutdownOperation === operation) shutdownOperation = null;
    },
  );
  return operation;
};
