import { Queue } from "bullmq";
import { QueueName, TQueueJobTypes } from "../queues";
import {
  createAnalyticsQueuePublisherOptionsWithRedis,
  redisErrorForLogging,
} from "./redis";
import { logger } from "../logger";

export class BatchActionQueue {
  private static instance: Queue<
    TQueueJobTypes[QueueName.BatchActionQueue]
  > | null = null;

  public static getInstance(): Queue<
    TQueueJobTypes[QueueName.BatchActionQueue]
  > | null {
    if (BatchActionQueue.instance) return BatchActionQueue.instance;

    const queueOptionsWithRedis = createAnalyticsQueuePublisherOptionsWithRedis(
      QueueName.BatchActionQueue,
    );
    const queue = queueOptionsWithRedis
      ? new Queue<TQueueJobTypes[QueueName.BatchActionQueue]>(
          QueueName.BatchActionQueue,
          {
            ...queueOptionsWithRedis,
            defaultJobOptions: {
              removeOnComplete: true,
              removeOnFail: 10_000,
              attempts: 10,
              backoff: {
                type: "exponential",
                delay: 5000,
              },
            },
          },
        )
      : null;
    BatchActionQueue.instance = queue;

    queue?.on("error", (err) => {
      logger.error("BatchActionQueue error", redisErrorForLogging(err));
    });

    if (queue && queueOptionsWithRedis) {
      queueOptionsWithRedis.connection.on("end", () => {
        if (BatchActionQueue.instance === queue) {
          BatchActionQueue.instance = null;
        }
      });
    }

    return queue;
  }
}
