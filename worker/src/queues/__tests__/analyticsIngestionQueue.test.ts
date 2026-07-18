import type { PrismaClient } from "@prisma/client";
import type { Job } from "bullmq";
import {
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
      payload: { operationId, projectId },
      name: QueueJobs.AnalyticsIngestionJob,
    },
  } as Job<TQueueJobTypes[QueueName.AnalyticsIngestionQueue]>;
}

describe("analytics ingestion durable queue", () => {
  it("publishes a deterministic body-free job before marking the outbox row", async () => {
    const order: string[] = [];
    const add = vi.fn(async () => {
      order.push("queue");
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
        payload: { operationId: "operation-1", projectId: "project-1" },
        name: QueueJobs.AnalyticsIngestionJob,
      },
      { jobId: "operation-1" },
    );
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
            operation: { projectId: "project-1" },
          },
        ]) as never,
        markPublished: markPublished as never,
      }),
    ).rejects.toThrow("redis unavailable");
    expect(markPublished).not.toHaveBeenCalled();
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
    } as never);
    await expect(
      processor(queueJob("operation-1", "project-1"), "token"),
    ).resolves.toBeUndefined();
    expect(canonicalize).not.toHaveBeenCalled();
    expect(persist).not.toHaveBeenCalled();
  });
});
