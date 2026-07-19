import type { PrismaClient } from "@prisma/client";
import { Queue, QueueEvents, Worker } from "bullmq";
import {
  QueueJobs,
  type QueueName,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { describe, expect, it, vi } from "vitest";
import waitForExpect from "wait-for-expect";

import { publishAnalyticsIngestionOutboxBatch } from "../analyticsIngestionQueue";

const redisUrl =
  process.env.ANALYTICS_INGESTION_REDIS_TEST_URL ??
  process.env.REDIS_CONNECTION_STRING;
const redisHost =
  process.env.REDIS_CLUSTER_ENABLED === "true"
    ? undefined
    : process.env.REDIS_HOST;

describe.skipIf(!redisUrl && !redisHost)(
  "analytics ingestion BullMQ delivery",
  () => {
    it("retries a retained failed stable job instead of orphaning the generation", async () => {
      const parsed = redisUrl ? new URL(redisUrl) : null;
      const connection = {
        host: parsed?.hostname ?? redisHost!,
        port: parsed
          ? Number(parsed.port || 6379)
          : Number(process.env.REDIS_PORT || 6379),
        username: parsed?.username
          ? decodeURIComponent(parsed.username)
          : undefined,
        password: parsed?.password
          ? decodeURIComponent(parsed.password)
          : process.env.REDIS_AUTH,
        db:
          parsed && parsed.pathname.length > 1
            ? Number(parsed.pathname.slice(1))
            : 0,
        tls:
          parsed?.protocol === "rediss:" ||
          process.env.REDIS_TLS_ENABLED === "true"
            ? {}
            : undefined,
      };
      const queueName = `analytics-ingestion-v2-test-${Date.now()}-${Math.random()
        .toString(16)
        .slice(2)}`;
      const queue = new Queue<
        TQueueJobTypes[QueueName.AnalyticsIngestionQueue]
      >(queueName, { connection });
      const queueEvents = new QueueEvents(queueName, { connection });
      let deliveries = 0;
      const worker = new Worker(
        queueName,
        async () => {
          deliveries += 1;
          if (deliveries === 1) throw new Error("first delivery failed");
        },
        { connection },
      );

      try {
        await Promise.all([
          worker.waitUntilReady(),
          queueEvents.waitUntilReady(),
        ]);
        const jobId = "operation-1-g1";
        const payload = {
          timestamp: new Date("2026-07-18T13:00:00.000Z"),
          id: "operation-1",
          payload: {
            operationId: "operation-1",
            projectId: "project-1",
            generation: 1,
          },
          name: QueueJobs.AnalyticsIngestionJob,
        };
        const failed = await queue.add(
          QueueJobs.AnalyticsIngestionJob,
          payload,
          {
            jobId,
            attempts: 1,
          },
        );
        await expect(
          failed.waitUntilFinished(queueEvents, 10_000),
        ).rejects.toThrow("first delivery failed");
        await expect(failed.getState()).resolves.toBe("failed");

        const markPublished = vi.fn(async () => true);
        await expect(
          publishAnalyticsIngestionOutboxBatch({
            client: {} as PrismaClient,
            queue,
            workerId: "publisher-a",
            now: new Date("2026-07-18T13:00:01.000Z"),
            claimOutbox: vi.fn(async () => [
              {
                operationId: "operation-1",
                generation: 1,
                attempts: 2,
                operation: { projectId: "project-1" },
              },
            ]) as never,
            markPublished: markPublished as never,
          }),
        ).resolves.toBe(1);

        await waitForExpect(async () => {
          await expect(failed.getState()).resolves.toBe("completed");
        }, 10_000);
        expect(deliveries).toBe(2);
        expect(markPublished).toHaveBeenCalledOnce();
      } finally {
        await worker.close();
        await queueEvents.close();
        await queue.obliterate({ force: true });
        await queue.close();
      }
    }, 30_000);
  },
);
