import type { PrismaClient } from "@prisma/client";
import type { Job } from "bullmq";
import { describe, expect, it, vi } from "vitest";

import { processCommunityDataRetentionJob } from "./process";

function job(payload: unknown): Job {
  return {
    data: {
      id: "job-1",
      payload,
    },
  } as Job;
}

function client(retentionDays: number | null) {
  return {
    project: {
      findUnique: vi.fn(async () => ({ retentionDays })),
    },
  } as unknown as PrismaClient;
}

describe("processCommunityDataRetentionJob", () => {
  it("rejects malformed queue payloads before any deletion", async () => {
    const processDoris = vi.fn();

    await expect(
      processCommunityDataRetentionJob(job({ projectId: "", retention: 2 }), {
        client: client(7),
        isDoris: () => true,
        processDoris,
      }),
    ).rejects.toThrow("Invalid Community data retention job");
    expect(processDoris).not.toHaveBeenCalled();
  });

  it("runs the bounded Doris project-retention state machine", async () => {
    const processDoris = vi.fn(async () => ({ outcome: "idle" as const }));
    const prismaClient = client(30);
    const deleteDorisHeads = vi.fn(async () => undefined);

    await processCommunityDataRetentionJob(
      job({ projectId: "project-1", retention: 14 }),
      {
        client: prismaClient,
        isDoris: () => true,
        processDoris,
        deleteDorisHeads,
      },
    );

    expect(processDoris).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        queuedRetentionDays: 14,
      }),
      expect.objectContaining({
        client: prismaClient,
        deleteDorisHeads,
        onCutoffPublished: expect.any(Function),
        scheduleContinuation: expect.any(Function),
      }),
    );
    expect(prismaClient.project.findUnique).not.toHaveBeenCalled();
  });

  it("uses the current ClickHouse setting and deletes media before analytics data", async () => {
    const order: string[] = [];
    const deleteMedia = vi.fn(async () => {
      order.push("media");
    });
    const deleteClickHouseData = vi.fn(async () => {
      order.push("analytics");
    });

    await expect(
      processCommunityDataRetentionJob(
        job({ projectId: "project-1", retention: 7 }),
        {
          client: client(14),
          isDoris: () => false,
          now: () => Date.parse("2026-07-29T00:00:00.000Z"),
          deleteMedia,
          deleteClickHouseData,
        },
      ),
    ).resolves.toEqual({
      outcome: "completed",
      cutoffDate: new Date("2026-07-15T00:00:00.000Z"),
    });
    expect(order).toEqual(["media", "analytics"]);
  });

  it("does not delete ClickHouse data after retention is disabled", async () => {
    const deleteMedia = vi.fn();
    const deleteClickHouseData = vi.fn();

    await expect(
      processCommunityDataRetentionJob(
        job({ projectId: "project-1", retention: 7 }),
        {
          client: client(null),
          isDoris: () => false,
          deleteMedia,
          deleteClickHouseData,
        },
      ),
    ).resolves.toEqual({ outcome: "idle" });
    expect(deleteMedia).not.toHaveBeenCalled();
    expect(deleteClickHouseData).not.toHaveBeenCalled();
  });
});
