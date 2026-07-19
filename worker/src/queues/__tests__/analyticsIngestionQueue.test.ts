import type { PrismaClient } from "@prisma/client";
import type { Job } from "bullmq";
import {
  AnalyticsPersistenceError,
  QueueJobs,
  QueueName,
  type CanonicalAnalyticsBatch,
  type TQueueJobTypes,
} from "@langfuse/shared/src/server";
import { describe, expect, it, vi } from "vitest";

import {
  analyticsIngestionQueueProcessorBuilder,
  publishAnalyticsIngestionOutboxBatch,
} from "../analyticsIngestionQueue";

const now = new Date("2026-07-18T13:00:00.000Z");

function queueJob(operationId: string, projectId: string) {
  return {
    data: {
      timestamp: now,
      id: operationId,
      payload: { operationId, projectId, generation: 1 },
      name: QueueJobs.AnalyticsIngestionJob,
    },
  } as Job<TQueueJobTypes[QueueName.AnalyticsIngestionQueue]>;
}

describe("analytics ingestion durable queue", () => {
  it("publishes a deterministic body-free job before marking the outbox row", async () => {
    const order: string[] = [];
    const delivery = {
      getState: vi.fn(async () => "waiting"),
      retry: vi.fn(async () => undefined),
    };
    const add = vi.fn(async () => {
      order.push("queue");
      return delivery;
    });
    const markPublished = vi.fn(async () => {
      order.push("outbox");
      return true;
    });

    await expect(
      publishAnalyticsIngestionOutboxBatch({
        client: {} as PrismaClient,
        queue: { add },
        workerId: "publisher-a",
        now,
        claimOutbox: vi.fn(async () => [
          {
            operationId: "operation-1",
            generation: 1,
            attempts: 1,
            operation: { projectId: "project-1" },
          },
        ]) as never,
        markPublished: markPublished as never,
      }),
    ).resolves.toBe(1);

    expect(order).toEqual(["queue", "outbox"]);
    expect(add).toHaveBeenCalledWith(
      QueueJobs.AnalyticsIngestionJob,
      {
        timestamp: now,
        id: "operation-1",
        payload: {
          operationId: "operation-1",
          projectId: "project-1",
          generation: 1,
        },
        name: QueueJobs.AnalyticsIngestionJob,
      },
      { jobId: "operation-1-g1", attempts: 1 },
    );
    expect(delivery.retry).not.toHaveBeenCalled();
  });

  it("does not mark an outbox row when BullMQ publication fails", async () => {
    const markPublished = vi.fn();
    await expect(
      publishAnalyticsIngestionOutboxBatch({
        client: {} as PrismaClient,
        queue: {
          add: vi.fn(async () => {
            throw new Error("redis unavailable");
          }),
        },
        workerId: "publisher-a",
        now,
        claimOutbox: vi.fn(async () => [
          {
            operationId: "operation-1",
            generation: 1,
            attempts: 1,
            operation: { projectId: "project-1" },
          },
        ]) as never,
        markPublished: markPublished as never,
      }),
    ).rejects.toThrow("redis unavailable");
    expect(markPublished).not.toHaveBeenCalled();
  });

  it("reuses the generation job id and retries a retained failed delivery", async () => {
    const delivery = {
      getState: vi.fn(async () => "failed"),
      retry: vi.fn(async () => undefined),
    };
    const add = vi.fn(async () => delivery);

    await expect(
      publishAnalyticsIngestionOutboxBatch({
        client: {} as PrismaClient,
        queue: { add },
        workerId: "publisher-a",
        now,
        claimOutbox: vi.fn(async () => [
          {
            operationId: "operation-1",
            generation: 3,
            attempts: 27,
            operation: { projectId: "project-1" },
          },
        ]) as never,
        markPublished: vi.fn(async () => true) as never,
      }),
    ).resolves.toBe(1);

    expect(add).toHaveBeenCalledWith(
      QueueJobs.AnalyticsIngestionJob,
      expect.objectContaining({
        payload: expect.objectContaining({ generation: 3 }),
      }),
      { jobId: "operation-1-g3", attempts: 1 },
    );
    expect(delivery.retry).toHaveBeenCalledWith("failed");
  });

  it("awaits canonical persistence and skips already-successful terminal jobs", async () => {
    const canonicalBatch = {
      operationId: "operation-1",
      projectId: "project-1",
    } as CanonicalAnalyticsBatch;
    const canonicalize = vi.fn(async () => canonicalBatch);
    const persist = vi.fn(async () => ({
      operationId: "operation-1",
      status: "VISIBLE" as const,
    }));
    const findOperation = vi.fn(async () => ({
      id: "operation-1",
      projectId: "project-1",
      rawObjectKey: "raw/operation-1.json",
      acceptedAtNanos: 1n,
      canonicalizerVersion: "1",
      schemaVersion: 3,
      terminalAt: null,
      status: "QUEUED",
      outboxV2: { generation: 1 },
    }));
    const processor = analyticsIngestionQueueProcessorBuilder({
      sink: { persist },
      canonicalize,
      client: {} as PrismaClient,
      findOperation: findOperation as never,
    });

    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).resolves.toBeUndefined();
    expect(canonicalize).toHaveBeenCalledOnce();
    expect(persist).toHaveBeenCalledWith(canonicalBatch);

    canonicalize.mockClear();
    persist.mockClear();
    findOperation.mockResolvedValue({
      id: "operation-1",
      projectId: "project-1",
      terminalAt: now,
      status: "VISIBLE",
      outboxV2: { generation: 1 },
    } as never);
    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).resolves.toBeUndefined();
    expect(canonicalize).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });

  it("terminalizes non-retryable persistence errors on the current delivery", async () => {
    const markTerminalFailure = vi.fn(async () => true);
    const processor = analyticsIngestionQueueProcessorBuilder({
      sink: { persist: vi.fn() },
      canonicalize: vi.fn(async () => {
        throw new AnalyticsPersistenceError(
          "ANALYTICS_VALIDATION_ERROR",
          false,
        );
      }),
      client: {} as PrismaClient,
      findOperation: vi.fn(async () => ({
        id: "operation-1",
        projectId: "project-1",
        terminalAt: null,
        status: "QUEUED",
        outboxV2: { generation: 1 },
      })) as never,
      markTerminalFailure: markTerminalFailure as never,
    });

    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).rejects.toMatchObject({ name: "UnrecoverableError" });
    expect(markTerminalFailure).toHaveBeenCalledWith(
      expect.objectContaining({
        operationId: "operation-1",
        projectId: "project-1",
        status: "UNRECOVERABLE",
        reasonCode: "ANALYTICS_VALIDATION_ERROR",
        expectedGeneration: 1,
      }),
    );
  });

  it("routes unexpected worker errors through durable retry bookkeeping", async () => {
    const resolveAttemptFailure = vi.fn(async () => "requeued" as const);
    const processor = analyticsIngestionQueueProcessorBuilder({
      sink: { persist: vi.fn() },
      canonicalize: vi.fn(async () => {
        throw new Error("unexpected dependency error");
      }),
      client: {} as PrismaClient,
      findOperation: vi.fn(async () => ({
        id: "operation-1",
        projectId: "project-1",
        terminalAt: null,
        status: "QUEUED",
        outboxV2: { generation: 1 },
      })) as never,
      resolveAttemptFailure: resolveAttemptFailure as never,
    });

    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).rejects.toMatchObject({ name: "UnrecoverableError" });
    expect(resolveAttemptFailure).toHaveBeenCalledWith(
      expect.objectContaining({
        operationId: "operation-1",
        projectId: "project-1",
        reasonCode: "ANALYTICS_UNAVAILABLE",
        expectedGeneration: 1,
      }),
    );
  });

  it("does not retry a previously terminalized partial failure", async () => {
    const canonicalize = vi.fn();
    const processor = analyticsIngestionQueueProcessorBuilder({
      sink: { persist: vi.fn() },
      canonicalize,
      client: {} as PrismaClient,
      findOperation: vi.fn(async () => ({
        id: "operation-1",
        projectId: "project-1",
        terminalAt: now,
        status: "PARTIAL_FAILED",
        outboxV2: { generation: 1 },
      })) as never,
    });

    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).rejects.toMatchObject({ name: "UnrecoverableError" });
    expect(canonicalize).not.toHaveBeenCalled();
  });

  it("does not canonicalize or persist while Doris readiness is closed", async () => {
    const canonicalize = vi.fn();
    const persist = vi.fn();
    const resolveAttemptFailure = vi.fn(async () => "requeued" as const);
    const processor = analyticsIngestionQueueProcessorBuilder({
      sink: { persist },
      canonicalize,
      assertReady: vi.fn(async () => {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true);
      }),
      client: {} as PrismaClient,
      findOperation: vi.fn(async () => ({
        id: "operation-1",
        projectId: "project-1",
        terminalAt: null,
        status: "QUEUED",
        outboxV2: { generation: 1 },
      })) as never,
      resolveAttemptFailure: resolveAttemptFailure as never,
    });

    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).rejects.toMatchObject({ name: "UnrecoverableError" });
    expect(canonicalize).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
    expect(resolveAttemptFailure).toHaveBeenCalledOnce();
  });

  it("durably requeues each retryable failure through a new outbox generation", async () => {
    const resolveAttemptFailure = vi.fn(async () => "requeued" as const);
    const processor = analyticsIngestionQueueProcessorBuilder({
      sink: { persist: vi.fn() },
      canonicalize: vi.fn(async () => {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true);
      }),
      client: {} as PrismaClient,
      findOperation: vi.fn(async () => ({
        id: "operation-1",
        projectId: "project-1",
        terminalAt: null,
        status: "QUEUED",
        outboxV2: { generation: 1 },
      })) as never,
      resolveAttemptFailure: resolveAttemptFailure as never,
    });
    const job = queueJob("operation-1", "project-1");

    await expect(processor(job, "token")).rejects.toMatchObject({
      name: "UnrecoverableError",
    });
    expect(resolveAttemptFailure).toHaveBeenCalledWith(
      expect.objectContaining({
        operationId: "operation-1",
        projectId: "project-1",
        reasonCode: "ANALYTICS_UNAVAILABLE",
        expectedGeneration: 1,
      }),
    );
  });

  it("reconciles unknown loads before reading an expired canonical artifact", async () => {
    const canonicalize = vi.fn();
    const persist = vi.fn();
    const processor = analyticsIngestionQueueProcessorBuilder({
      sink: { persist },
      canonicalize,
      reconcileUnresolved: vi.fn(async () => true),
      client: {} as PrismaClient,
      findOperation: vi.fn(async () => ({
        id: "operation-1",
        projectId: "project-1",
        terminalAt: null,
        status: "RETRYING",
        outboxV2: { generation: 1 },
      })) as never,
    });

    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).resolves.toBeUndefined();
    expect(canonicalize).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });

  it("ignores a late job from a superseded retry generation", async () => {
    const canonicalize = vi.fn();
    const persist = vi.fn();
    const processor = analyticsIngestionQueueProcessorBuilder({
      sink: { persist },
      canonicalize,
      client: {} as PrismaClient,
      findOperation: vi.fn(async () => ({
        id: "operation-1",
        projectId: "project-1",
        terminalAt: null,
        status: "RETRYING",
        outboxV2: { generation: 2 },
      })) as never,
    });

    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).resolves.toBeUndefined();
    expect(canonicalize).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });
});
