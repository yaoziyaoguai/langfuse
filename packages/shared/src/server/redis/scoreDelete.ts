import { QueueName, TQueueJobTypes } from "../queues";
import { Queue } from "bullmq";
import {
  createAnalyticsQueuePublisherOptionsWithRedis,
  redisErrorForLogging,
} from "./redis";
import { logger } from "../logger";

export class ScoreDeleteQueue {
  private static instance: Queue<TQueueJobTypes[QueueName.ScoreDelete]> | null =
    null;

  public static getInstance(): Queue<
    TQueueJobTypes[QueueName.ScoreDelete]
  > | null {
    if (ScoreDeleteQueue.instance) return ScoreDeleteQueue.instance;

    const queueOptionsWithRedis = createAnalyticsQueuePublisherOptionsWithRedis(
      QueueName.ScoreDelete,
    );
    const queue = queueOptionsWithRedis
      ? new Queue<TQueueJobTypes[QueueName.ScoreDelete]>(
          QueueName.ScoreDelete,
          {
            ...queueOptionsWithRedis,
            defaultJobOptions: {
              removeOnComplete: true,
              removeOnFail: 100_000,
              attempts: 2,
              backoff: {
                type: "exponential",
                delay: 30_000,
              },
            },
          },
        )
      : null;
    ScoreDeleteQueue.instance = queue;

    queue?.on("error", (err) => {
      logger.error("ScoreDeleteQueue error", redisErrorForLogging(err));
    });

    if (queue && queueOptionsWithRedis) {
      queueOptionsWithRedis.connection.on("end", () => {
        if (ScoreDeleteQueue.instance === queue) {
          ScoreDeleteQueue.instance = null;
        }
      });
    }

    return queue;
  }
}
