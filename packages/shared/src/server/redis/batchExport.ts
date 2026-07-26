import { Queue } from "bullmq";
import { QueueName, TQueueJobTypes } from "../queues";
import { createBullMQQueueOptionsWithRedis } from "./redis";
import { logger } from "../logger";

export const BATCH_EXPORT_QUEUE_ATTEMPTS = 8;
export const BATCH_EXPORT_QUEUE_BACKOFF_DELAY_MS = 5_000;

export class BatchExportQueue {
  private static instance: Queue<TQueueJobTypes[QueueName.BatchExport]> | null =
    null;

  public static getInstance(): Queue<
    TQueueJobTypes[QueueName.BatchExport]
  > | null {
    if (BatchExportQueue.instance) return BatchExportQueue.instance;

    const queueOptionsWithRedis = createBullMQQueueOptionsWithRedis(
      QueueName.BatchExport,
    );
    BatchExportQueue.instance = queueOptionsWithRedis
      ? new Queue<TQueueJobTypes[QueueName.BatchExport]>(
          QueueName.BatchExport,
          {
            ...queueOptionsWithRedis,
            defaultJobOptions: {
              removeOnComplete: true,
              removeOnFail: 10_000,
              attempts: BATCH_EXPORT_QUEUE_ATTEMPTS,
              backoff: {
                type: "exponential",
                delay: BATCH_EXPORT_QUEUE_BACKOFF_DELAY_MS,
              },
            },
          },
        )
      : null;

    BatchExportQueue.instance?.on("error", (err) => {
      logger.error("BatchExportQueue error", err);
    });

    return BatchExportQueue.instance;
  }
}
