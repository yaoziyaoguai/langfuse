import {
  logger,
  QueueName,
  recordHistogram,
} from "@langfuse/shared/src/server";
import { getQueue } from "@langfuse/shared/src/server";
import { isAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { env } from "../../env";

export class DlqRetryService {
  private static readonly backendIndependentRetryQueues = [
    QueueName.ProjectDelete,
    QueueName.TraceDelete,
    QueueName.ScoreDelete,
    QueueName.BatchActionQueue,
  ] as const;

  static getRetryQueues(): Array<
    | (typeof DlqRetryService.backendIndependentRetryQueues)[number]
    | QueueName.DataRetentionProcessingQueue
  > {
    return isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "clickhouse")
      ? [
          ...DlqRetryService.backendIndependentRetryQueues,
          QueueName.DataRetentionProcessingQueue,
        ]
      : [...DlqRetryService.backendIndependentRetryQueues];
  }

  // called each 10 minutes, defined by the bull cron job
  public static async retryDeadLetterQueue() {
    logger.info(
      `Retrying dead letter queues for queues: ${DlqRetryService.getRetryQueues().join(
        ", ",
      )}`,
    );
    const retryQueues = DlqRetryService.getRetryQueues();
    for (const queueName of retryQueues) {
      const queue = getQueue(queueName);

      if (!queue) {
        logger.error(`Queue ${queueName} not found`);
        continue;
      }

      // Find failed jobs
      const failedJobs = await queue.getFailed();
      logger.info(
        `Found ${failedJobs.length} failed jobs in queue ${queueName}`,
      );
      for (const job of failedJobs) {
        try {
          const projectId = job.data.payload.projectId;
          const ts = job.data.timestamp;

          const dlxDelay = Date.now() - ts;

          recordHistogram("langfuse.dlq_retry_delay", dlxDelay, {
            unit: "milliseconds",
            projectId,
            queueName,
          });

          await job.retry();
          logger.info(
            `Retried job ${JSON.stringify(job)} in queue ${queueName}`,
          );
        } catch (error) {
          logger.error(
            `Failed to retry job ${JSON.stringify(job)} in queue ${queueName}:`,
            error,
          );
        }
      }
    }
  }
}
