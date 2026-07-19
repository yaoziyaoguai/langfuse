import { DorisClientManager, logger } from "@langfuse/shared/src/server";
import { redis } from "@langfuse/shared/src/server";

import { setSigtermReceived } from "../features/health";
import { server } from "../index";
import { freeAllTokenizers } from "../features/tokenisation/usage";
import { getTokenCountWorkerManager } from "../features/tokenisation/async-usage";
import { WorkerManager } from "../queues/workerManager";
import { prisma } from "@langfuse/shared/src/db";
import { BackgroundMigrationManager } from "../backgroundMigrations/backgroundMigrationManager";
import {
  mediaRetentionCleaner,
  batchProjectMediaCleaner,
  batchTraceDeletionCleaner,
  traceDeleteBatchActionRunner,
  queueMetricsRunner,
  analyticsIngestionOutboxRunner,
} from "../app";

export const onShutdown: NodeJS.SignalsListener = async (signal) => {
  logger.info(`Received ${signal}, closing server...`);
  setSigtermReceived();

  // Stop accepting new connections
  server?.close();
  logger.info("Server has been closed.");

  // Stop media retention cleaner
  mediaRetentionCleaner?.stop();

  // Stop batch project media cleaner
  batchProjectMediaCleaner?.stop();

  // Stop batch trace deletion cleaner
  batchTraceDeletionCleaner?.stop();

  // Stop durable trace-delete batch action runner
  traceDeleteBatchActionRunner?.stop();

  // Stop queue metrics runner
  queueMetricsRunner?.stop();

  analyticsIngestionOutboxRunner?.stop();

  // Shutdown workers (https://docs.bullmq.io/guide/going-to-production#gracefully-shut-down-workers)
  await WorkerManager.closeWorkers();

  // Shutdown background migrations
  await BackgroundMigrationManager.close();

  redis?.disconnect();
  logger.info("Redis connection has been closed.");

  await prisma.$disconnect();
  logger.info("Prisma connection has been closed.");

  // Shutdown Doris query pools created by readiness or Doris-backed services.
  await DorisClientManager.getInstance().closeAllConnections();

  // Shutdown tokenization worker threads
  try {
    await getTokenCountWorkerManager().terminate();
    logger.info("Token count worker threads have been terminated.");
  } catch (error) {
    logger.error("Error terminating token count worker threads", error);
  }

  freeAllTokenizers();
  logger.info("All tokenizers are cleaned up from memory.");

  logger.info("Shutdown complete, exiting process...");
};
