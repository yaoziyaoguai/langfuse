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
            attempts: 10,
            backoff: { type: "exponential", delay: 5_000 },
          },
        })
      : null;
    this.instance?.on("error", (error) => {
      logger.error("AnalyticsIngestionQueue error", error);
    });
    return this.instance;
  }
}
