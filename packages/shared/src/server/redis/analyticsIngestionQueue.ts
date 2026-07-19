import { Queue } from "bullmq";

import { logger } from "../logger";
import { QueueName, type TQueueJobTypes } from "../queues";
import { createBullMQQueueOptionsWithRedis } from "./redis";

export class AnalyticsIngestionQueue {
  private static instance:
    | Queue<TQueueJobTypes[QueueName.AnalyticsIngestionQueue]>
    | null
    | undefined;

  static getInstance(): Queue<
    TQueueJobTypes[QueueName.AnalyticsIngestionQueue]
  > | null {
    if (this.instance !== undefined) return this.instance;
    const options = createBullMQQueueOptionsWithRedis(
      QueueName.AnalyticsIngestionQueue,
    );
    this.instance = options
      ? new Queue(QueueName.AnalyticsIngestionQueue, {
          ...options,
          defaultJobOptions: {
            removeOnComplete: { age: 7 * 24 * 60 * 60, count: 100_000 },
            removeOnFail: { age: 30 * 24 * 60 * 60, count: 100_000 },
            // Postgres outbox generations own retries. A BullMQ job executes
            // once so Redis cannot become a second, divergent retry ledger.
            attempts: 1,
          },
        })
      : null;
    this.instance?.on("error", (error) => {
      logger.error("AnalyticsIngestionQueue error", error);
    });
    return this.instance;
  }
}
