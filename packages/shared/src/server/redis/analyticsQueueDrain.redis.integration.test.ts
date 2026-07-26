import { createHash, randomUUID } from "node:crypto";

import { Queue } from "bullmq";
import Redis, { type Cluster, type RedisOptions } from "ioredis";
import { describe, expect, it, vi } from "vitest";

const ENABLED = process.env.ANALYTICS_QUEUE_DRAIN_REDIS_INTEGRATION === "1";

const SHARD_COUNT_ENV_KEYS = [
  "LANGFUSE_INGESTION_QUEUE_SHARD_COUNT",
  "LANGFUSE_INGESTION_SECONDARY_QUEUE_SHARD_COUNT",
  "LANGFUSE_OTEL_INGESTION_QUEUE_SHARD_COUNT",
  "LANGFUSE_OTEL_INGESTION_SECONDARY_QUEUE_SHARD_COUNT",
  "LANGFUSE_TRACE_UPSERT_QUEUE_SHARD_COUNT",
  "LANGFUSE_EVAL_EXECUTION_QUEUE_SHARD_COUNT",
  "LANGFUSE_EVAL_EXECUTION_SECONDARY_QUEUE_SHARD_COUNT",
  "LANGFUSE_LLM_AS_JUDGE_EXECUTION_QUEUE_SHARD_COUNT",
  "LANGFUSE_CODE_EVAL_EXECUTION_QUEUE_SHARD_COUNT",
] as const;

const TEST_ENV_KEYS = ["REDIS_KEY_PREFIX", ...SHARD_COUNT_ENV_KEYS] as const;

function createTestRedisClient(input: {
  readonly createNewRedisInstance: (
    options: RedisOptions,
  ) => Redis | Cluster | null;
}): Redis {
  const boundedOptions: RedisOptions = {
    enableOfflineQueue: false,
    maxRetriesPerRequest: 1,
    connectTimeout: 5_000,
    retryStrategy: (attempt) => (attempt <= 2 ? attempt * 100 : null),
  };
  const redis = input.createNewRedisInstance(boundedOptions);
  if (!(redis instanceof Redis)) {
    throw new Error(
      "A standalone Redis connection is required for the Redis integration test",
    );
  }
  return redis;
}

async function scanPhysicalKeys(
  redis: Redis,
  pattern: string,
): Promise<string[]> {
  const keys = new Set<string>();
  let cursor = "0";
  do {
    const [nextCursor, page] = await redis.scan(
      cursor,
      "MATCH",
      pattern,
      "COUNT",
      1_000,
    );
    page.forEach((key) => keys.add(key));
    cursor = nextCursor;
  } while (cursor !== "0");
  return [...keys].sort();
}

async function deletePhysicalKeys(redis: Redis, keys: string[]): Promise<void> {
  for (const key of keys) {
    await redis.del(key);
  }
}

describe.skipIf(!ENABLED)(
  "community analytics queue drain against Redis",
  () => {
    it("inspects all 28 queue families and every configured shard without creating repeat configuration", async () => {
      const originalEnv = Object.fromEntries(
        TEST_ENV_KEYS.map((key) => [key, process.env[key]]),
      );
      const prefix = `langfuse-analytics-queue-drain-it-${randomUUID()}`;
      const runtimeEnv: Record<string, string | undefined> = process.env;
      runtimeEnv.REDIS_KEY_PREFIX = prefix;
      for (const key of SHARD_COUNT_ENV_KEYS) runtimeEnv[key] = "1";
      runtimeEnv[SHARD_COUNT_ENV_KEYS[0]] = "8";

      vi.resetModules();
      const [inventoryModule, drainModule, queueModule, redisModule] =
        await Promise.all([
          import("./analyticsQueueInventory.js"),
          import("./analyticsScoreDeletionDrain.js"),
          import("../queues.js"),
          import("./redis.js"),
        ]);
      const redis = createTestRedisClient(redisModule);
      redis.on("error", () => undefined);
      const inventory =
        inventoryModule.configuredCommunityAnalyticsQueueInventory();
      const producerQueues = inventory.map(
        ({ name }) =>
          new Queue(name, {
            connection: redis,
            prefix,
          }),
      );
      const namespaceFingerprint = createHash("sha256")
        .update(prefix)
        .digest("hex");
      const probe = drainModule.createAnalyticsScoreDeletionDrainProbe({
        getQueueNamespaceFingerprint: () => namespaceFingerprint,
      });
      const prefixPattern = `${prefix}:*`;

      let testFailure: { readonly error: unknown } | null = null;
      try {
        expect(new Set(inventory.map(({ family }) => family))).toHaveProperty(
          "size",
          28,
        );
        expect(inventory).toHaveLength(35);
        expect(inventory).toContainEqual({
          family: queueModule.QueueName.IngestionQueue,
          name: `${queueModule.QueueName.IngestionQueue}-7`,
          shardIndex: 7,
        });

        await Promise.all(
          producerQueues.map((queue, index) =>
            queue.add("analytics-queue-drain-integration", {
              marker: prefix,
              physicalQueueIndex: index,
            }),
          ),
        );
        await expect(
          Promise.all(
            producerQueues.map((queue) => queue.getJobSchedulersCount()),
          ),
        ).resolves.toEqual(Array(inventory.length).fill(0));
        await expect(
          scanPhysicalKeys(redis, `${prefix}:*:repeat*`),
        ).resolves.toEqual([]);

        const scope = {
          backend: "clickhouse" as const,
          deploymentGeneration: 7n,
          workloadEpochFingerprint: "a".repeat(64),
        };
        const blockedEvidence = await probe.verify(scope);

        expect(blockedEvidence).toMatchObject({
          empty: false,
          pendingJobs: inventory.length,
        });
        expect(blockedEvidence.queues).toHaveLength(inventory.length);
        expect(
          blockedEvidence.queues.every((queue) => queue.counts.waiting === 1),
        ).toBe(true);
        expect(
          blockedEvidence.queues.find(
            ({ name }) => name === `${queueModule.QueueName.IngestionQueue}-7`,
          )?.counts.waiting,
        ).toBe(1);
        expect(
          blockedEvidence.queues.every((queue) => queue.counts.repeat === 0),
        ).toBe(true);
        await expect(
          scanPhysicalKeys(redis, `${prefix}:*:repeat*`),
        ).resolves.toEqual([]);

        await Promise.all(
          producerQueues.map((queue) => queue.obliterate({ force: true })),
        );
        const emptyEvidence = await probe.verify(scope);

        expect(emptyEvidence).toMatchObject({ empty: true, pendingJobs: 0 });
        expect(
          emptyEvidence.queues.every((queue) =>
            drainModule.ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES.every(
              (jobType) => queue.counts[jobType] === 0,
            ),
          ),
        ).toBe(true);
      } catch (error) {
        testFailure = { error };
      }

      const cleanupErrors: unknown[] = [];
      try {
        await probe.close();
      } catch (error) {
        cleanupErrors.push(error);
      }
      const queueCloseResults = await Promise.allSettled(
        producerQueues.map((queue) => queue.close()),
      );
      cleanupErrors.push(
        ...queueCloseResults.flatMap((result) =>
          result.status === "rejected" ? [result.reason] : [],
        ),
      );
      try {
        const ownedKeys = await scanPhysicalKeys(redis, prefixPattern);
        await deletePhysicalKeys(redis, ownedKeys);
        expect(await scanPhysicalKeys(redis, prefixPattern)).toEqual([]);
      } catch (error) {
        cleanupErrors.push(error);
      }
      try {
        await redis.quit();
      } catch (error) {
        cleanupErrors.push(error);
      }

      for (const key of TEST_ENV_KEYS) {
        const original = originalEnv[key];
        if (original === undefined) delete runtimeEnv[key];
        else runtimeEnv[key] = original;
      }
      const failures = [
        ...(testFailure ? [testFailure.error] : []),
        ...cleanupErrors,
      ];
      if (failures.length > 0) {
        throw new AggregateError(
          failures,
          "Redis queue-drain integration or cleanup failed",
        );
      }
    }, 60_000);
  },
);
