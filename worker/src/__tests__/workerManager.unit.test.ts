import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../env", () => ({
  env: { LANGFUSE_QUEUE_METRICS_SAMPLE_RATE: 0 },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  QueueName: {
    TraceDelete: "trace-delete",
    IngestionQueue: "ingestion-queue",
    ScoreDelete: "score-delete",
  },
  contextWithLangfuseProps: vi.fn(() => ({})),
  convertQueueNameToMetricName: vi.fn((queueName: string) =>
    queueName === "ingestion-queue"
      ? "langfuse.queue.ingestion"
      : `langfuse.queue.${queueName.replaceAll("-", "_")}`,
  ),
  createBullMQWorkerOptionsWithRedis: vi.fn(() => undefined),
  logger: { error: vi.fn(), info: vi.fn(), warn: vi.fn() },
  recordDistribution: vi.fn(),
  recordGauge: vi.fn(),
  recordHistogram: vi.fn(),
  recordIncrement: vi.fn(),
  traceException: vi.fn(),
}));
vi.mock("../queues/shardedQueueRegistry", () => ({
  resolveQueueInstance: vi.fn(),
  SHARDED_QUEUE_BASE_NAMES: ["ingestion-queue"],
}));

import { QueueName } from "@langfuse/shared/src/server";
import { WorkerManager } from "../queues/workerManager";

const extractProjectId = (data: unknown): string | undefined =>
  (
    WorkerManager as unknown as {
      extractProjectId(job: { data: unknown }): string | undefined;
    }
  ).extractProjectId({ data });

const resolveMetricInfo = (queueName: QueueName) =>
  (
    WorkerManager as unknown as {
      resolveMetricInfo(queueName: QueueName): {
        baseMetric: string;
      };
    }
  ).resolveMetricInfo(queueName);

describe("WorkerManager", () => {
  beforeEach(() => {
    const manager = WorkerManager as unknown as {
      workers: Record<string, { close: () => Promise<void> }>;
      registrationsFenced: boolean;
      closeOperation: Promise<void> | null;
    };
    manager.workers = {};
    manager.registrationsFenced = false;
    manager.closeOperation = null;
  });

  describe("extractProjectId", () => {
    it("extracts project ids from queue payloads", () => {
      expect(
        extractProjectId({
          payload: { projectId: "project-from-payload" },
        }),
      ).toBe("project-from-payload");
    });

    it("extracts ingestion project ids from payload auth scope", () => {
      expect(
        extractProjectId({
          payload: {
            authCheck: {
              scope: { projectId: "project-from-auth-scope" },
            },
          },
        }),
      ).toBe("project-from-auth-scope");
    });

    it("ignores non-contract top-level project ids", () => {
      expect(
        extractProjectId({
          projectId: "top-level-project",
        }),
      ).toBeUndefined();
    });

    it("ignores non-contract top-level auth scope project ids", () => {
      expect(
        extractProjectId({
          authCheck: {
            scope: { projectId: "top-level-auth-project" },
          },
        }),
      ).toBeUndefined();
    });
  });

  describe("resolveMetricInfo", () => {
    it("uses the base metric as the worker ClickHouse route", () => {
      expect(resolveMetricInfo(QueueName.TraceDelete).baseMetric).toBe(
        "langfuse.queue.trace_delete",
      );
    });

    it("uses the base metric as the worker ClickHouse route for sharded queues", () => {
      expect(
        resolveMetricInfo(`${QueueName.IngestionQueue}-1` as QueueName)
          .baseMetric,
      ).toBe("langfuse.queue.ingestion");
    });
  });

  it("shares an in-flight close and permanently rejects registration after fencing", async () => {
    let releaseClose!: () => void;
    const close = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          releaseClose = resolve;
        }),
    );
    const manager = WorkerManager as unknown as {
      workers: Record<string, { close: () => Promise<void> }>;
    };
    manager.workers[QueueName.TraceDelete] = { close };

    const firstClose = WorkerManager.closeWorkers();
    let fenceResolved = false;
    const fence = WorkerManager.fenceRegistrations().then(() => {
      fenceResolved = true;
    });
    await new Promise((resolve) => setImmediate(resolve));

    expect(close).toHaveBeenCalledOnce();
    expect(fenceResolved).toBe(false);

    releaseClose();
    await Promise.all([firstClose, fence]);
    WorkerManager.register(QueueName.ScoreDelete, async () => undefined);

    expect(close).toHaveBeenCalledOnce();
    expect(WorkerManager.getRegisteredQueueNames()).toEqual([]);
  });

  it("retains failed workers and retries only those closures", async () => {
    const failedClose = vi
      .fn<() => Promise<void>>()
      .mockRejectedValueOnce(new Error("transient close failure"))
      .mockResolvedValueOnce(undefined);
    const successfulClose = vi.fn(async () => undefined);
    const manager = WorkerManager as unknown as {
      workers: Record<string, { close: () => Promise<void> }>;
    };
    manager.workers[QueueName.TraceDelete] = { close: failedClose };
    manager.workers[QueueName.ScoreDelete] = { close: successfulClose };

    await expect(WorkerManager.closeWorkers()).rejects.toThrow(
      "Failed to close all workers",
    );
    expect(WorkerManager.getRegisteredQueueNames()).toEqual([
      QueueName.TraceDelete,
    ]);

    await expect(WorkerManager.closeWorkers()).resolves.toBeUndefined();
    expect(failedClose).toHaveBeenCalledTimes(2);
    expect(successfulClose).toHaveBeenCalledOnce();
    expect(WorkerManager.getRegisteredQueueNames()).toEqual([]);
  });
});
