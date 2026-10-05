import type { PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import { scheduleCommunityDataRetention } from "./schedule";

function client(input: {
  projects: Array<{ id: string; retentionDays: number | null }>;
  states?: Array<{ id: string }>;
}) {
  return {
    project: {
      findMany: vi.fn(async () => input.projects),
    },
    analyticsRetentionState: {
      findMany: vi.fn(async () => input.states ?? []),
    },
  } as unknown as PrismaClient;
}

describe("scheduleCommunityDataRetention", () => {
  it("schedules every project with an active retention period", async () => {
    const prismaClient = client({
      projects: [{ id: "project-1", retentionDays: 14 }],
    });
    const queue = { addBulk: vi.fn(async () => []) };

    await expect(
      scheduleCommunityDataRetention({
        client: prismaClient,
        isDoris: () => false,
        queue,
        createId: () => "job-1",
        now: () => new Date("2026-07-29T00:00:00.000Z"),
      }),
    ).resolves.toEqual({ scheduled: 1 });

    expect(
      prismaClient.analyticsRetentionState.findMany,
    ).not.toHaveBeenCalled();
    expect(queue.addBulk).toHaveBeenCalledWith([
      {
        name: "data-retention-processing-job",
        data: {
          id: "job-1",
          name: "data-retention-processing-job",
          timestamp: new Date("2026-07-29T00:00:00.000Z"),
          payload: { projectId: "project-1", retention: 14 },
        },
      },
    ]);
  });

  it("continues an active Doris cutoff after retention is disabled", async () => {
    const prismaClient = client({
      projects: [{ id: "project-1", retentionDays: null }],
      states: [{ id: "project:project-1" }],
    });
    const queue = { addBulk: vi.fn(async () => []) };

    await scheduleCommunityDataRetention({
      client: prismaClient,
      isDoris: () => true,
      queue,
      createId: () => "job-1",
      now: () => new Date("2026-07-29T00:00:00.000Z"),
    });

    expect(prismaClient.project.findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          OR: [{ retentionDays: { gt: 0 } }, { id: { in: ["project-1"] } }],
        },
      }),
    );
    expect(queue.addBulk).toHaveBeenCalledWith([
      expect.objectContaining({
        data: expect.objectContaining({
          payload: { projectId: "project-1", retention: 3 },
        }),
      }),
    ]);
  });

  it("does not enqueue an empty batch", async () => {
    const queue = { addBulk: vi.fn(async () => []) };

    await expect(
      scheduleCommunityDataRetention({
        client: client({ projects: [] }),
        isDoris: () => false,
        queue,
      }),
    ).resolves.toEqual({ scheduled: 0 });
    expect(queue.addBulk).not.toHaveBeenCalled();
  });
});
