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
import { BackgroundMigrationManager } from "../backgroundMigrations/backgroundMigrationManager";
import {
  batchProjectCleaners,
  batchDataRetentionCleaners,
  mediaRetentionCleaner,
  batchProjectMediaCleaner,
  batchProjectBlobCleaner,
  batchTraceDeletionCleaner,
  traceDeleteBatchActionRunner,
  deletedMaskCleaner,
  queueMetricsRunner,
  monitorRunners,
  analyticsIngestionOutboxRunner,
  analyticsDeletionRecoveryRunner,
  dorisGlobalRetentionRunner,
} from "../app";
import { env } from "../env";
import { isAnalyticsBackend } from "@langfuse/shared/analytics-backend";

export const onShutdown: NodeJS.SignalsListener = async (signal) => {
  logger.info(`Received ${signal}, closing server...`);
  setSigtermReceived();

  // Stop accepting new connections
  server?.close();
  logger.info("Server has been closed.");

  // Stop batch project cleaners
  for (const cleaner of batchProjectCleaners) {
    cleaner.stop();
  }

  // Stop batch data retention cleaners
  for (const cleaner of batchDataRetentionCleaners) {
    cleaner.stop();
  }

  // Stop media retention cleaner
  mediaRetentionCleaner?.stop();

  // Stop batch project media cleaner
  batchProjectMediaCleaner?.stop();

  // Stop batch project blob cleaner
  batchProjectBlobCleaner?.stop();

  // Stop batch trace deletion cleaner
  batchTraceDeletionCleaner?.stop();

  // Stop durable trace-delete batch action runner
  traceDeleteBatchActionRunner?.stop();

  // Stop deleted-mask cleaner
  deletedMaskCleaner?.stop();

  // Stop queue metrics runner
  queueMetricsRunner?.stop();

  // Stop monitor runners
  for (const runner of monitorRunners) {
    runner.stop();
  }

  analyticsIngestionOutboxRunner?.stop();

  analyticsDeletionRecoveryRunner?.stop();

  dorisGlobalRetentionRunner?.stop();

  // Before closeWorkers(), while the registry is still populated (LFE-10388).
  logInFlightBlobExportsOnShutdown();

  // Shutdown workers (https://docs.bullmq.io/guide/going-to-production#gracefully-shut-down-workers)
  await WorkerManager.closeWorkers();

  // Shutdown background migrations
  await BackgroundMigrationManager.close();

  if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "clickhouse")) {
    // Flush pending writes after the ClickHouse ingestion workers have stopped.
    await ClickhouseWriter.getInstance().shutdown();
    logger.info("Clickhouse writer has been shut down.");
  }

  redis?.disconnect();
  logger.info("Redis connection has been closed.");

  await prisma.$disconnect();
  logger.info("Prisma connection has been closed.");

  if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "clickhouse")) {
    await ClickHouseClientManager.getInstance().closeAllConnections();
  }

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
