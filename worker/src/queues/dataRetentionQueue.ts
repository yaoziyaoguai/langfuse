import { Processor } from "bullmq";
import {
  isCommunityExtensionEnabled,
  instrumentAsync,
  logger,
  QueueJobs,
} from "@langfuse/shared/src/server";
import { SpanKind } from "@opentelemetry/api";

const runDataRetentionSchedule = async () =>
  isCommunityExtensionEnabled()
    ? (
        await import("../features/community-extensions/data-retention/index.js")
      ).scheduleCommunityDataRetention()
    : (
        await import("../ee/dataRetention/handleDataRetentionSchedule.js")
      ).handleDataRetentionSchedule();

const runDataRetentionProcessingJob = async (job: Parameters<Processor>[0]) =>
  isCommunityExtensionEnabled()
    ? (
        await import("../features/community-extensions/data-retention/index.js")
      ).processCommunityDataRetentionJob(job)
    : (
        await import("../ee/dataRetention/handleDataRetentionProcessingJob.js")
      ).handleDataRetentionProcessingJob(job);

export const dataRetentionProcessor: Processor = async (job) => {
  if (job.name === QueueJobs.DataRetentionJob) {
    logger.info("Executing Data Retention Job");
    try {
      return await runDataRetentionSchedule();
    } catch (error) {
      logger.error("Error executing DataRetentionJob", error);
      throw error;
    }
  }
};

export const dataRetentionProcessingProcessor: Processor = async (job) => {
  if (job.name === QueueJobs.DataRetentionProcessingJob) {
    return await instrumentAsync(
      {
        name: "process data-retention-project",
        startNewTrace: true,
        spanKind: SpanKind.CONSUMER,
      },
      async () => {
        try {
          return await runDataRetentionProcessingJob(job);
        } catch (error) {
          logger.error("Error executing DataRetentionProcessingJob", error);
          throw error;
        }
      },
    );
  }
};
