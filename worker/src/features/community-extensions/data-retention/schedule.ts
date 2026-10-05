import { randomUUID } from "node:crypto";

import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  analyticsProjectIdFromRetentionStateId,
  DataRetentionProcessingQueue,
  isDorisAnalyticsBackend,
  QueueJobs,
} from "@langfuse/shared/src/server";

type ProcessingQueue = {
  addBulk: (
    jobs: Array<{
      name: string;
      data: {
        id: string;
        name: string;
        timestamp: Date;
        payload: { projectId: string; retention: number };
      };
    }>,
  ) => Promise<unknown>;
};

type ScheduleDependencies = {
  client?: PrismaClient;
  isDoris?: () => boolean;
  queue?: ProcessingQueue | null;
  createId?: () => string;
  now?: () => Date;
};

export async function scheduleCommunityDataRetention(
  dependencies: ScheduleDependencies = {},
): Promise<{ scheduled: number }> {
  const client = dependencies.client ?? prisma;
  const isDoris = dependencies.isDoris ?? isDorisAnalyticsBackend;
  const activeProjectIds = isDoris()
    ? (
        await client.analyticsRetentionState.findMany({
          select: { id: true },
          where: {
            id: { startsWith: "project:" },
            activeRunId: { not: null },
          },
        })
      ).flatMap(({ id }) => {
        const projectId = analyticsProjectIdFromRetentionStateId(id);
        return projectId ? [projectId] : [];
      })
    : [];

  const projects = await client.project.findMany({
    select: { id: true, retentionDays: true },
    where: {
      OR: [
        { retentionDays: { gt: 0 } },
        ...(activeProjectIds.length > 0
          ? [{ id: { in: activeProjectIds } }]
          : []),
      ],
    },
  });

  const queue =
    dependencies.queue ?? DataRetentionProcessingQueue.getInstance();
  if (!queue) {
    throw new Error("DataRetentionProcessingQueue not initialized");
  }
  if (projects.length === 0) return { scheduled: 0 };

  const createId = dependencies.createId ?? randomUUID;
  const now = dependencies.now ?? (() => new Date());
  await queue.addBulk(
    projects.map((project) => ({
      name: QueueJobs.DataRetentionProcessingJob,
      data: {
        id: createId(),
        name: QueueJobs.DataRetentionProcessingJob,
        timestamp: now(),
        payload: {
          projectId: project.id,
          // 已发布的 Doris cutoff 必须在用户关闭 retention 后继续完成。
          retention:
            project.retentionDays && project.retentionDays >= 3
              ? project.retentionDays
              : 3,
        },
      },
    })),
  );

  return { scheduled: projects.length };
}
