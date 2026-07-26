import { prisma } from "@langfuse/shared/src/db";
import {
  MixpanelIntegrationProcessingQueue,
  QueueJobs,
  logger,
} from "@langfuse/shared/src/server";
import { randomUUID } from "crypto";
import { env } from "../../env";
import { scheduleDorisAnalyticsIntegrations } from "../analytics-integrations/scheduleDorisAnalyticsIntegrations";

export const handleMixpanelIntegrationSchedule = async () => {
  const mixpanelIntegrationProjects = await prisma.mixpanelIntegration.findMany(
    {
      select: {
        lastSyncAt: true,
        projectId: true,
      },
      where: {
        enabled: true,
      },
    },
  );

  if (mixpanelIntegrationProjects.length === 0) {
    logger.info("[MIXPANEL] No Mixpanel integrations ready for sync");
    return;
  }

  const mixpanelIntegrationProcessingQueue =
    MixpanelIntegrationProcessingQueue.getInstance();
  if (!mixpanelIntegrationProcessingQueue) {
    throw new Error("MixpanelIntegrationProcessingQueue not initialized");
  }

  logger.info(
    `[MIXPANEL] Scheduling ${mixpanelIntegrationProjects.length} Mixpanel integrations for sync`,
  );

  if (env.LANGFUSE_ANALYTICS_BACKEND === "doris") {
    await scheduleDorisAnalyticsIntegrations({
      integrationType: "MIXPANEL",
      projectIds: mixpanelIntegrationProjects.map(({ projectId }) => projectId),
      queue: mixpanelIntegrationProcessingQueue,
      jobName: QueueJobs.MixpanelIntegrationProcessingJob,
    });
    return;
  }

  await mixpanelIntegrationProcessingQueue.addBulk(
    mixpanelIntegrationProjects.map(
      (integration: { projectId: string; lastSyncAt: Date | null }) => ({
        name: QueueJobs.MixpanelIntegrationProcessingJob,
        data: {
          id: randomUUID(),
          name: QueueJobs.MixpanelIntegrationProcessingJob,
          timestamp: new Date(),
          payload: {
            projectId: integration.projectId,
          },
        },
        opts: {
          // Deduplicate by projectId + lastSyncAt so the same project isn't queued
          // twice for the same sync window. removeOnFail ensures failed jobs are
          // immediately cleaned up so they don't block re-queuing on the next cycle.
          jobId: `${integration.projectId}-${integration.lastSyncAt?.toISOString() ?? ""}`,
          removeOnFail: true,
        },
      }),
    ),
  );
};
