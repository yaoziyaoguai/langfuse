import express from "express";
import cors from "cors";
import * as middlewares from "./middlewares";
import api from "./api";
import MessageResponse from "./interfaces/MessageResponse";

require("dotenv").config();

import { onShutdown } from "./utils/shutdown";
import helmet from "helmet";
import { cloudUsageMeteringQueueProcessor } from "./queues/cloudUsageMeteringQueue";
import { cloudSpendAlertQueueProcessor } from "./queues/cloudSpendAlertQueue";
import { cloudFreeTierUsageThresholdQueueProcessor } from "./queues/cloudFreeTierUsageThresholdQueue";
import { WorkerManager } from "./queues/workerManager";
import {
  QueueName,
  logger,
  DeadLetterRetryQueue,
  AnalyticsIngestionQueue,
  CloudFreeTierUsageThresholdQueue,
  CloudUsageMeteringQueue,
  BatchActionQueue,
  findRecoverableDeletionOperations,
  handoffLegacyAnalyticsIngestionOutbox,
} from "@langfuse/shared/src/server";
import type { AnalyticsDeletionScope } from "@prisma/client";
import { env } from "./env";
import { BackgroundMigrationManager } from "./backgroundMigrations/backgroundMigrationManager";
import { traceDeleteProcessor } from "./queues/traceDelete";
import { projectDeleteProcessor } from "./queues/projectDelete";
import { scoreDeleteProcessor } from "./queues/scoreDelete";
import { DlqRetryService } from "./services/dlq/dlqRetryService";
import { entityChangeQueueProcessor } from "./queues/entityChangeQueue";
import { webhookProcessor } from "./queues/webhooks";
import { datasetDeleteProcessor } from "./queues/datasetDelete";
import { notificationQueueProcessor } from "./queues/notificationQueue";
import { BatchTraceDeletionCleaner } from "./features/batch-trace-deletion-cleaner";
import { BatchProjectMediaCleaner } from "./features/batch-project-media-cleaner";
import { QueueMetricsRunner } from "./features/queue-metrics-runner";
import { TraceDeleteBatchActionRunner } from "./features/trace-delete-batch-action-runner";
import { createDorisAnalyticsPersistence } from "./services/dorisAnalyticsPersistence";
import { AnalyticsIngestionOutboxRunner } from "./features/analytics-ingestion-outbox-runner";
import { batchActionQueueProcessor } from "./queues/batchActionQueue";
import { assertDorisAnalyticsReady } from "./services/dorisAnalyticsReadiness";
import { AnalyticsDeletionRecoveryRunner } from "./features/analytics-deletion-recovery-runner";
import { processAnalyticsDeletionRecoveryOperation } from "./features/analytics-deletion-recovery-runner/processOperation";

const app = express();

app.use(helmet());
app.use(cors());
app.use(express.json());
app.get<{}, MessageResponse>("/", (req, res) => {
  res.json({
    message: "Langfuse Worker API 🚀",
  });
});

app.use("/api", api);

app.use(middlewares.notFound);
app.use(middlewares.errorHandler);

if (env.LANGFUSE_ENABLE_BACKGROUND_MIGRATIONS === "true") {
  // Will start background migrations without blocking the queue workers
  BackgroundMigrationManager.run().catch((err) => {
    logger.error("Error running background migrations", err);
  });
}

if (env.QUEUE_CONSUMER_TRACE_DELETE_QUEUE_IS_ENABLED === "true") {
  WorkerManager.register(QueueName.TraceDelete, traceDeleteProcessor, {
    concurrency: env.LANGFUSE_TRACE_DELETE_CONCURRENCY,
    // Same configuration as EvaluationExecution or
    // BlobStorageIntegrationProcessingQueue queue, see detailed comment there
    maxStalledCount: 3,
    lockDuration: 60000, // 60 seconds
    stalledInterval: 120000, // 120 seconds
    limiter: {
      // Process at most `max` delete jobs per 2 min
      max: env.LANGFUSE_TRACE_DELETE_CONCURRENCY,
      duration: env.LANGFUSE_ANALYTICS_TRACE_DELETION_RATE_LIMIT_WINDOW_MS,
    },
  });
}

if (env.QUEUE_CONSUMER_SCORE_DELETE_QUEUE_IS_ENABLED === "true") {
  WorkerManager.register(QueueName.ScoreDelete, scoreDeleteProcessor, {
    concurrency: env.LANGFUSE_SCORE_DELETE_CONCURRENCY,
    limiter: {
      // Process at most `max` delete jobs per 15 seconds
      max: env.LANGFUSE_SCORE_DELETE_CONCURRENCY,
      duration: env.LANGFUSE_ANALYTICS_TRACE_DELETION_RATE_LIMIT_WINDOW_MS,
    },
  });
}

if (env.QUEUE_CONSUMER_DATASET_DELETE_QUEUE_IS_ENABLED === "true") {
  WorkerManager.register(QueueName.DatasetDelete, datasetDeleteProcessor, {
    concurrency: env.LANGFUSE_DATASET_DELETE_CONCURRENCY,
    limiter: {
      max: env.LANGFUSE_DATASET_DELETE_CONCURRENCY,
      duration: env.LANGFUSE_ANALYTICS_DATASET_DELETION_RATE_LIMIT_WINDOW_MS,
    },
  });
}

if (env.QUEUE_CONSUMER_PROJECT_DELETE_QUEUE_IS_ENABLED === "true") {
  WorkerManager.register(QueueName.ProjectDelete, projectDeleteProcessor, {
    concurrency: env.LANGFUSE_PROJECT_DELETE_CONCURRENCY,
    limiter: {
      // Process at most `max` delete jobs per configured rate-limit window.
      max: env.LANGFUSE_PROJECT_DELETE_CONCURRENCY,
      duration: env.LANGFUSE_ANALYTICS_PROJECT_DELETION_RATE_LIMIT_WINDOW_MS,
    },
  });
}

const dorisAnalyticsPersistence = createDorisAnalyticsPersistence({});
const legacyIngestionHandoffEnabled =
  env.LANGFUSE_ANALYTICS_INGESTION_LEGACY_HANDOFF_ENABLED === "true";

export let analyticsIngestionOutboxRunner: AnalyticsIngestionOutboxRunner | null =
  null;

AnalyticsIngestionQueue.getInstance();
WorkerManager.register(
  QueueName.AnalyticsIngestionQueue,
  dorisAnalyticsPersistence.processor,
  {
    concurrency: env.LANGFUSE_ANALYTICS_INGESTION_WORKER_CONCURRENCY,
  },
);
analyticsIngestionOutboxRunner = new AnalyticsIngestionOutboxRunner({
  workerId: dorisAnalyticsPersistence.workerId,
  intervalMs: env.LANGFUSE_ANALYTICS_INGESTION_OUTBOX_INTERVAL_MS,
  batchSize: env.LANGFUSE_ANALYTICS_INGESTION_OUTBOX_BATCH_SIZE,
  // Enable only after every legacy ingestion producer and consumer has stopped.
  handoffLegacy: legacyIngestionHandoffEnabled
    ? handoffLegacyAnalyticsIngestionOutbox
    : undefined,
  assertReady: assertDorisAnalyticsReady,
});
analyticsIngestionOutboxRunner.start();

export let analyticsDeletionRecoveryRunner: AnalyticsDeletionRecoveryRunner | null =
  null;

const deletionRecoveryScopes: AnalyticsDeletionScope[] = [];
if (env.QUEUE_CONSUMER_TRACE_DELETE_QUEUE_IS_ENABLED === "true") {
  deletionRecoveryScopes.push("TRACE");
}
if (env.QUEUE_CONSUMER_PROJECT_DELETE_QUEUE_IS_ENABLED === "true") {
  deletionRecoveryScopes.push("PROJECT");
}
if (deletionRecoveryScopes.length > 0) {
  analyticsDeletionRecoveryRunner = new AnalyticsDeletionRecoveryRunner({
    intervalMs: env.LANGFUSE_ANALYTICS_DELETION_RECOVERY_INTERVAL_MS,
    batchSize: env.LANGFUSE_ANALYTICS_DELETION_RECOVERY_BATCH_SIZE,
    assertReady: assertDorisAnalyticsReady,
    findRecoverableOperations: (input) =>
      findRecoverableDeletionOperations({
        ...input,
        scopes: deletionRecoveryScopes,
      }),
    processOperation: processAnalyticsDeletionRecoveryOperation,
  });
  analyticsDeletionRecoveryRunner.start();
}

BatchActionQueue.getInstance();
WorkerManager.register(QueueName.BatchActionQueue, batchActionQueueProcessor, {
  concurrency: 1,
});

if (
  env.QUEUE_CONSUMER_CLOUD_USAGE_METERING_QUEUE_IS_ENABLED === "true" &&
  env.STRIPE_SECRET_KEY
) {
  // Instantiate the queue to trigger scheduled jobs
  CloudUsageMeteringQueue.getInstance();

  WorkerManager.register(
    QueueName.CloudUsageMeteringQueue,
    cloudUsageMeteringQueueProcessor,
    {
      concurrency: 1,
      limiter: {
        // Process at most `max` jobs per 30 seconds
        max: 1,
        duration: 30_000,
      },
    },
  );
}

// Cloud Spend Alert Queue: Only enable in cloud environment with Stripe
if (
  env.QUEUE_CONSUMER_CLOUD_SPEND_ALERT_QUEUE_IS_ENABLED === "true" &&
  env.STRIPE_SECRET_KEY
) {
  WorkerManager.register(
    QueueName.CloudSpendAlertQueue,
    cloudSpendAlertQueueProcessor,
    {
      concurrency: 20,
      limiter: {
        // Process at most 600 jobs per minute / 10 jobs per second for Stripe API rate limits
        // - stripe allows 100 ops / sec but we want to use a lower limit to account for 3 environments and other calls
        // - See: https://docs.stripe.com/rate-limits
        max: 900,
        duration: 60_000,
      },
    },
  );
}

// Free Tier Usage Threshold Queue: Only enable in cloud environment
if (
  env.QUEUE_CONSUMER_FREE_TIER_USAGE_THRESHOLD_QUEUE_IS_ENABLED === "true" &&
  env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION && // Only in cloud deployments
  env.STRIPE_SECRET_KEY
) {
  // Instantiate the queue to trigger scheduled jobs
  CloudFreeTierUsageThresholdQueue.getInstance();
  WorkerManager.register(
    QueueName.CloudFreeTierUsageThresholdQueue,
    cloudFreeTierUsageThresholdQueueProcessor,
    {
      concurrency: 1,
      limiter: {
        // Process at most `max` jobs per 30 seconds
        max: 1,
        duration: 30_000,
      },
    },
  );
}

if (env.QUEUE_CONSUMER_DEAD_LETTER_RETRY_QUEUE_IS_ENABLED === "true") {
  // Instantiate the queue to trigger scheduled jobs
  DeadLetterRetryQueue.getInstance();

  WorkerManager.register(
    QueueName.DeadLetterRetryQueue,
    DlqRetryService.retryDeadLetterQueue,
    {
      concurrency: 1,
    },
  );
}

if (env.QUEUE_CONSUMER_WEBHOOK_QUEUE_IS_ENABLED === "true") {
  WorkerManager.register(QueueName.WebhookQueue, webhookProcessor, {
    concurrency: env.LANGFUSE_WEBHOOK_QUEUE_PROCESSING_CONCURRENCY,
  });
}

if (env.QUEUE_CONSUMER_ENTITY_CHANGE_QUEUE_IS_ENABLED === "true") {
  WorkerManager.register(
    QueueName.EntityChangeQueue,
    entityChangeQueueProcessor,
    {
      concurrency: env.LANGFUSE_ENTITY_CHANGE_QUEUE_PROCESSING_CONCURRENCY,
    },
  );
}

if (env.QUEUE_CONSUMER_NOTIFICATION_QUEUE_IS_ENABLED === "true") {
  WorkerManager.register(
    QueueName.NotificationQueue,
    notificationQueueProcessor,
    {
      concurrency: 5, // Process up to 5 notification jobs concurrently
    },
  );
}

// Batch project media cleaner for S3 media cleanup of soft-deleted projects
export let batchProjectMediaCleaner: BatchProjectMediaCleaner | null = null;

if (
  env.LANGFUSE_BATCH_PROJECT_CLEANER_ENABLED === "true" &&
  env.LANGFUSE_S3_MEDIA_UPLOAD_BUCKET
) {
  batchProjectMediaCleaner = new BatchProjectMediaCleaner();
  batchProjectMediaCleaner.start();
}

// Batch trace deletion cleaner for supplementary trace deletion
export let batchTraceDeletionCleaner: BatchTraceDeletionCleaner | null = null;

if (env.LANGFUSE_BATCH_TRACE_DELETION_CLEANER_ENABLED === "true") {
  batchTraceDeletionCleaner = new BatchTraceDeletionCleaner();
  batchTraceDeletionCleaner.start();
}

// Durable trace-delete BatchAction runner
export let traceDeleteBatchActionRunner: TraceDeleteBatchActionRunner | null =
  null;

if (env.LANGFUSE_TRACE_DELETE_BATCH_ACTION_RUNNER_ENABLED === "true") {
  traceDeleteBatchActionRunner = new TraceDeleteBatchActionRunner();
  traceDeleteBatchActionRunner.start();
}

// Queue metrics background reporter
export let queueMetricsRunner: QueueMetricsRunner | null = null;

if (env.LANGFUSE_QUEUE_METRICS_ENABLED === "true") {
  queueMetricsRunner = new QueueMetricsRunner();
  queueMetricsRunner.start();
}

process.on("SIGINT", () => onShutdown("SIGINT"));
process.on("SIGTERM", () => onShutdown("SIGTERM"));

export default app;
