import { Job } from "bullmq";
import {
  claimExperimentExecution,
  completeExperimentExecution,
  ExperimentCreateQueue,
  failExperimentExecution,
  isManagedExperimentExecutionJob,
  QueueJobs,
  QueueName,
  renewExperimentExecutionClaim,
  TQueueJobTypes,
  classifyEvaluatorLlmError,
  logger,
  traceException,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";
import { retryLLMRateLimitError } from "../features/utils";
import { createExperimentJob } from "../features/experiments/experimentServiceClickhouse";
import { isUnrecoverableError } from "../errors/UnrecoverableError";
import { getWorkerAnalyticsAdmissionContext } from "../analyticsRuntime";

const EXPERIMENT_EXECUTION_LEASE_MS = 30 * 60_000;

export const experimentCreateQueueProcessor = async (
  job: Job<TQueueJobTypes[QueueName.ExperimentCreate]>,
) => {
  const managed = isManagedExperimentExecutionJob(job.data.payload);
  const admissionContext = managed
    ? getWorkerAnalyticsAdmissionContext()
    : null;
  if (managed && !admissionContext) {
    throw new Error("Doris experiment worker runtime is not admitted");
  }
  const leaseOwner = `experiment:${process.pid}:${job.id ?? job.data.id}`;
  const claim = managed
    ? await claimExperimentExecution({
        client: prisma,
        admissionContext: admissionContext!,
        job: job.data.payload,
        leaseOwner,
        leaseMs: EXPERIMENT_EXECUTION_LEASE_MS,
      })
    : null;
  if (claim && "completed" in claim) return true;

  try {
    await createExperimentJob({
      event: job.data.payload,
      ...(claim && "claimId" in claim
        ? {
            onItemProcessed: async () => {
              await renewExperimentExecutionClaim({
                client: prisma,
                admissionContext: admissionContext!,
                projectId: job.data.payload.projectId,
                runId: job.data.payload.runId,
                claimId: claim.claimId,
                generation: claim.generation,
                leaseOwner,
                leaseMs: EXPERIMENT_EXECUTION_LEASE_MS,
              });
            },
          }
        : {}),
    });
    if (claim && "claimId" in claim) {
      const completed = await completeExperimentExecution({
        client: prisma,
        admissionContext: admissionContext!,
        projectId: job.data.payload.projectId,
        runId: job.data.payload.runId,
        claimId: claim.claimId,
        generation: claim.generation,
        leaseOwner,
      });
      if (!completed) {
        throw new Error("Doris experiment execution completion was fenced");
      }
    }
    return true;
  } catch (e) {
    if (claim && "claimId" in claim) {
      await failExperimentExecution({
        client: prisma,
        admissionContext: admissionContext!,
        projectId: job.data.payload.projectId,
        runId: job.data.payload.runId,
        claimId: claim.claimId,
        generation: claim.generation,
        leaseOwner,
        failureCode: e instanceof Error ? e.name : "UNKNOWN_ERROR",
      }).catch((failure) => {
        logger.error("Failed to persist Doris experiment execution failure", {
          runId: job.data.payload.runId,
          failure,
        });
      });
    }
    const llmError = classifyEvaluatorLlmError(e);

    if (llmError?.isRetryable) {
      const retryResult = await retryLLMRateLimitError(job, {
        table: "dataset_runs",
        idField: "runId",
        queue: ExperimentCreateQueue.getInstance(),
        queueName: QueueName.ExperimentCreate,
        jobName: QueueJobs.ExperimentCreateJob,
      });

      if (retryResult.outcome === "scheduled") return;
      if (retryResult.outcome === "queue_unavailable") throw e;
    }

    if (llmError || isUnrecoverableError(e)) return;

    logger.error(
      `Failed to process experiment create job for project: ${job.data.payload.projectId}`,
      e,
    );
    traceException(e);

    // Retry job by rethrowing error
    throw e;
  }
};
