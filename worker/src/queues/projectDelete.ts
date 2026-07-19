import { Job, Processor } from "bullmq";
import {
  getCurrentSpan,
  findLatestProjectDeletionOperation,
  logger,
  QueueName,
  TQueueJobTypes,
  scheduleProjectDeletionOperation,
} from "@langfuse/shared/src/server";
import { processAnalyticsProjectDelete } from "../features/projects/processAnalyticsProjectDelete";

export const projectDeleteProcessor: Processor = async (
  job: Job<TQueueJobTypes[QueueName.ProjectDelete]>,
): Promise<void> => {
  const { orgId, projectId } = job.data.payload;

  const span = getCurrentSpan();
  if (span) {
    span.setAttribute("messaging.bullmq.job.input.id", job.data.id);
    span.setAttribute(
      "messaging.bullmq.job.input.projectId",
      job.data.payload.projectId,
    );
    span.setAttribute(
      "messaging.bullmq.job.input.orgId",
      job.data.payload.orgId,
    );
  }

  logger.info(`Deleting ${projectId} in org ${orgId}`);

  const operation =
    job.data.payload.deletionOperationId && job.data.payload.deletionGeneration
      ? {
          id: job.data.payload.deletionOperationId,
          generation: BigInt(job.data.payload.deletionGeneration),
        }
      : ((await findLatestProjectDeletionOperation({
          projectId,
          organizationId: orgId,
        })) ??
        (await scheduleProjectDeletionOperation({
          projectId,
          organizationId: orgId,
          requester: {
            principalType: "system",
            principalId: "analytics-project-deletion-worker",
          },
        })));
  await processAnalyticsProjectDelete({
    projectId,
    organizationId: orgId,
    reference: {
      operationId: operation.id,
      generation: operation.generation,
    },
  });
};
