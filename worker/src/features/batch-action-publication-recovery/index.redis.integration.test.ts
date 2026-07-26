import { randomUUID } from "node:crypto";

import { BatchActionStatus, BatchTableNames } from "@langfuse/shared";
import { Queue, Worker } from "bullmq";
import Redis from "ioredis";
import { describe, expect, it, vi } from "vitest";
import waitForExpect from "wait-for-expect";

import { buildRecoveredBatchActionJob, publishRecoveredBatchAction } from ".";

const ENABLED =
  process.env.BATCH_ACTION_PUBLICATION_RECOVERY_REDIS_INTEGRATION === "1";

async function scanOwnedKeys(
  connection: Redis,
  prefix: string,
): Promise<string[]> {
  const keys = new Set<string>();
  let cursor = "0";
  do {
    const [nextCursor, page] = await connection.scan(
      cursor,
      "MATCH",
      `${prefix}:*`,
      "COUNT",
      1_000,
    );
    page.forEach((key) => keys.add(key));
    cursor = nextCursor;
  } while (cursor !== "0");
  return [...keys].sort();
}

describe.skipIf(!ENABLED)(
  "BatchAction publication recovery against Redis",
  () => {
    it("never deletes a retained failed job when a DLQ retry wins the race", async () => {
      const redisUrl = process.env.REDIS_CONNECTION_STRING;
      if (!redisUrl) {
        throw new Error(
          "REDIS_CONNECTION_STRING is required when the BatchAction recovery Redis integration test is enabled",
        );
      }

      const prefix = `langfuse-batch-action-recovery-it-${randomUUID()}`;
      const queueName = "batch-action-publication-recovery-it";
      let connection: Redis | undefined;
      let queue: Queue | undefined;
      let worker: Worker<unknown, void, string> | undefined;
      let testFailure: unknown;

      try {
        connection = new Redis(redisUrl, {
          lazyConnect: true,
          enableOfflineQueue: false,
          maxRetriesPerRequest: null,
          connectTimeout: 5_000,
          retryStrategy: (attempt) =>
            attempt <= 2 ? Math.min(attempt * 100, 500) : null,
        });
        connection.on("error", () => undefined);
        await connection.connect();
        await connection.ping();

        queue = new Queue(queueName, { connection, prefix });
        let deliveries = 0;
        const testWorker = new Worker<unknown, void, string>(
          queueName,
          async (): Promise<void> => {
            deliveries += 1;
            throw new Error("forced retained BatchAction failure");
          },
          { connection, prefix },
        );
        worker = testWorker;
        await testWorker.waitUntilReady();

        const action = {
          id: randomUUID(),
          projectId: randomUUID(),
          actionType: "observation-add-to-dataset",
          tableName: BatchTableNames.Events,
          status: BatchActionStatus.Queued,
          query: {
            filter: [],
            orderBy: { column: "startTime", order: "DESC" },
          },
          config: {
            datasetId: randomUUID(),
            datasetName: "Redis integration dataset",
            mapping: {
              input: { mode: "full" },
              expectedOutput: { mode: "full" },
              metadata: { mode: "full" },
            },
          },
          createdAt: new Date("2026-07-22T08:00:00.000Z"),
        };
        const recoveredJob = buildRecoveredBatchActionJob(action);
        const retainedJob = await queue.add(recoveredJob.name, recoveredJob, {
          jobId: action.id,
          attempts: 1,
          removeOnFail: false,
        });

        await waitForExpect(async () => {
          await expect(retainedJob.getState()).resolves.toBe("failed");
        }, 10_000);
        expect(deliveries).toBe(1);
        await expect(queue.getJob(action.id)).resolves.toBeDefined();
        await testWorker.close();
        worker = undefined;

        const findUnique = vi.fn(async (args: unknown) => {
          expect(args).toEqual({
            where: { id: action.id, projectId: action.projectId },
            select: { status: true, actionType: true },
          });
          return {
            status: BatchActionStatus.Queued,
            actionType: action.actionType,
          };
        });
        const updateMany = vi.fn().mockResolvedValue({ count: 1 });
        const transaction = { batchAction: { findUnique, updateMany } };
        const client = {
          $transaction: vi.fn(
            async (
              operation: (input: typeof transaction) => Promise<unknown>,
              options: unknown,
            ) => {
              expect(options).toEqual({
                maxWait: 120_000,
                timeout: 35 * 60_000,
              });
              return operation(transaction);
            },
          ),
        };
        const lockLegacyAdmission = vi.fn(async (input: unknown) => {
          expect(input).toBe(transaction);
        });
        const lockAdmission = vi.fn(async () => {
          throw new Error("managed admission must not be used by this test");
        });
        const queueAdd = queue.add.bind(queue);
        const racingQueue = {
          add: async (...args: Parameters<typeof queueAdd>) => {
            const publicationJob = await queueAdd(...args);
            const readState = publicationJob.getState.bind(publicationJob);
            publicationJob.getState = async () => {
              const observedState = await readState();
              expect(observedState).toBe("failed");
              // 真实执行 DlqRetryService 的核心原子操作，再把刚才读到的
              // failed snapshot 返回给 publisher，确定性覆盖命令间竞态。
              await publicationJob.retry("failed");
              return observedState;
            };
            return publicationJob;
          },
        };

        await expect(
          publishRecoveredBatchAction(
            {
              backend: "clickhouse",
              getAdmissionContext: () => null,
              job: recoveredJob,
              action,
            },
            {
              client: client as never,
              getQueue: () => racingQueue as never,
              lockAdmission,
              lockLegacyAdmission: lockLegacyAdmission as never,
            },
          ),
        ).rejects.toThrow();

        expect(client.$transaction).toHaveBeenCalledOnce();
        expect(findUnique).toHaveBeenCalledOnce();
        expect(updateMany).not.toHaveBeenCalled();
        expect(lockLegacyAdmission).toHaveBeenCalledOnce();
        expect(lockAdmission).not.toHaveBeenCalled();
        expect(deliveries).toBe(1);
        await expect(queue.getJob(action.id)).resolves.toBeDefined();
        await expect(retainedJob.getState()).resolves.toBe("waiting");
      } catch (error) {
        testFailure = error;
      }

      const cleanupErrors: unknown[] = [];
      if (worker) {
        try {
          await worker.close();
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
      if (queue) {
        try {
          await queue.close();
        } catch (error) {
          cleanupErrors.push(error);
        }
      }
      if (connection) {
        try {
          const keys = await scanOwnedKeys(connection, prefix);
          for (const key of keys) await connection.del(key);
          expect(await scanOwnedKeys(connection, prefix)).toEqual([]);
        } catch (error) {
          cleanupErrors.push(error);
        }
        try {
          if (connection.status === "ready") await connection.quit();
          else connection.disconnect();
        } catch (error) {
          cleanupErrors.push(error);
          connection.disconnect();
        }
      }

      const failures = [
        ...(testFailure === undefined ? [] : [testFailure]),
        ...cleanupErrors,
      ];
      if (failures.length > 0) {
        throw new AggregateError(
          failures,
          "BatchAction recovery Redis integration or cleanup failed",
        );
      }
    }, 30_000);
  },
);
