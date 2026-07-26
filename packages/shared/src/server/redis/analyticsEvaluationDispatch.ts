import { Queue } from "bullmq";

import { logger } from "../logger";
import { QueueName, type TQueueJobTypes } from "../queues";
import { createBullMQQueueOptionsWithRedis } from "./redis";

export class AnalyticsEvaluationDispatchQueue {
  private static instance:
    | Queue<TQueueJobTypes[QueueName.AnalyticsEvaluationDispatch]>
    | null
    | undefined;

  static getInstance(): Queue<
    TQueueJobTypes[QueueName.AnalyticsEvaluationDispatch]
  > | null {
    if (this.instance !== undefined) return this.instance;
    const options = createBullMQQueueOptionsWithRedis(
      QueueName.AnalyticsEvaluationDispatch,
    );
    this.instance = options
      ? new Queue(QueueName.AnalyticsEvaluationDispatch, {
          ...options,
          defaultJobOptions: {
            removeOnComplete: { age: 7 * 24 * 60 * 60, count: 100_000 },
            removeOnFail: { age: 30 * 24 * 60 * 60, count: 100_000 },
            attempts: 1,
          },
        })
      : null;
    this.instance?.on("error", (error) => {
      logger.error("AnalyticsEvaluationDispatchQueue error", error);
    });
    return this.instance;
  }
}
