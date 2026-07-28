import { prisma } from "@langfuse/shared/src/db";
import {
  analyticsProjectIdFromRetentionStateId,
  DataRetentionProcessingQueue,
  isDorisAnalyticsBackend,
  QueueJobs,
} from "@langfuse/shared/src/server";
import { randomUUID } from "crypto";

export const handleDataRetentionSchedule = async () => {
  const activeDorisStates = isDorisAnalyticsBackend()
    ? await prisma.analyticsRetentionState.findMany({
        select: { id: true },
        where: {
          id: { startsWith: "project:" },
          activeRunId: { not: null },
        },
      })
    : [];
  const activeProjectIds = activeDorisStates.flatMap(({ id }) => {
    const projectId = analyticsProjectIdFromRetentionStateId(id);
    return projectId ? [projectId] : [];
  });
  const projectsWithRetention = await prisma.project.findMany({
    select: {
      id: true,
      retentionDays: true,
    },
    where: {
      OR: [
        {
          retentionDays: {
            gt: 0, // Select all projects with a non-zero/non-null retention
          },
        },
        ...(activeProjectIds.length > 0
          ? [{ id: { in: activeProjectIds } }]
          : []),
      ],
    },
  });

  const dataRetentionProcessingQueue =
    DataRetentionProcessingQueue.getInstance();
  if (!dataRetentionProcessingQueue) {
    throw new Error("DataRetentionProcessingQueue not initialized");
  }

  await dataRetentionProcessingQueue.addBulk(
    projectsWithRetention.map((project) => ({
      name: QueueJobs.DataRetentionProcessingJob,
      data: {
        id: randomUUID(),
        name: QueueJobs.DataRetentionProcessingJob,
        timestamp: new Date(),
        payload: {
          projectId: project.id,
          // 已发布的 Doris cutoff 必须在用户关闭 retention 后继续收敛；
          // active run 会忽略这个 fallback，并使用持久化 cutoff。
          retention:
            project.retentionDays && project.retentionDays >= 3
              ? project.retentionDays
              : 3,
        },
      },
    })),
  );
};
