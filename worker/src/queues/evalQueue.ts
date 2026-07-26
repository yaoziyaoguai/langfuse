import { Job, Processor } from "bullmq";
import { EvalTemplateType, JobExecutionStatus } from "@langfuse/shared";
import { prisma } from "@langfuse/shared/src/db";
import {
  QueueName,
  TQueueJobTypes,
  logger,
  traceException,
  EvalExecutionQueue,
  SecondaryEvalExecutionQueue,
  LLMAsJudgeExecutionQueue,
  QueueJobs,
  getCurrentSpan,
  classifyEvaluatorLlmError,
} from "@langfuse/shared/src/server";
import { createEvalJobs, evaluate } from "../features/evaluation/evalService";
import { processObservationEval } from "../features/evaluation/observationEval";
import { createW3CTraceId, retryLLMRateLimitError } from "../features/utils";
import { isUnrecoverableError } from "../errors/UnrecoverableError";
import { retryObservationNotFound } from "../features/evaluation/retryObservationNotFound";
import { isObservationNotFoundError } from "../errors/ObservationNotFoundError";
import { env } from "../env";
import { resolveAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { assertEvaluationExecutionAdmission } from "../features/evaluation/evaluationExecutionAdmission";

type SafeEvaluationFailure = {
  readonly code:
    | "EVALUATION_MODEL_AUTH_INVALID"
    | "EVALUATION_MODEL_BILLING_EXHAUSTED"
    | "EVALUATION_MODEL_UNAVAILABLE"
    | "EVALUATION_MODEL_ENDPOINT_UNREACHABLE"
    | "EVALUATION_MODEL_ACCOUNT_NOT_READY"
    | "EVALUATION_MODEL_RATE_LIMITED"
    | "EVALUATION_MODEL_TEMPORARY_FAILURE"
    | "EVALUATION_MODEL_REQUEST_FAILED"
    | "EVALUATION_INVALID_REQUEST"
    | "EVALUATION_INTERNAL_ERROR";
  readonly message: string;
  readonly retryable: boolean;
};

function safeEvaluationFailure(
  error: unknown,
  classification: ReturnType<typeof classifyEvaluatorLlmError>,
): SafeEvaluationFailure {
  switch (classification?.blockReason) {
    case "LLM_CONNECTION_AUTH_INVALID":
      return {
        code: "EVALUATION_MODEL_AUTH_INVALID",
        message: "The evaluation model credentials are invalid.",
        retryable: false,
      };
    case "LLM_CONNECTION_BILLING_EXHAUSTED":
      return {
        code: "EVALUATION_MODEL_BILLING_EXHAUSTED",
        message: "The evaluation model account has no available credits.",
        retryable: false,
      };
    case "EVAL_MODEL_UNAVAILABLE":
      return {
        code: "EVALUATION_MODEL_UNAVAILABLE",
        message: "The configured evaluation model is unavailable.",
        retryable: false,
      };
    case "LLM_CONNECTION_ENDPOINT_UNREACHABLE":
      return {
        code: "EVALUATION_MODEL_ENDPOINT_UNREACHABLE",
        message: "The evaluation model endpoint is unreachable.",
        retryable: false,
      };
    case "PROVIDER_ACCOUNT_NOT_READY":
      return {
        code: "EVALUATION_MODEL_ACCOUNT_NOT_READY",
        message: "The evaluation model account is not ready.",
        retryable: false,
      };
  }
  if (classification?.statusCode === 429) {
    return {
      code: "EVALUATION_MODEL_RATE_LIMITED",
      message: "The evaluation model is temporarily rate limited.",
      retryable: true,
    };
  }
  if (classification?.isRetryable) {
    return {
      code: "EVALUATION_MODEL_TEMPORARY_FAILURE",
      message: "The evaluation model is temporarily unavailable.",
      retryable: true,
    };
  }
  if (classification) {
    return {
      code: "EVALUATION_MODEL_REQUEST_FAILED",
      message: "The evaluation model request failed.",
      retryable: false,
    };
  }
  if (isUnrecoverableError(error)) {
    return {
      code: "EVALUATION_INVALID_REQUEST",
      message: "The evaluation could not be completed.",
      retryable: false,
    };
  }
  return {
    code: "EVALUATION_INTERNAL_ERROR",
    message: "An internal error occurred",
    retryable: true,
  };
}

function safeEvaluationException(failure: SafeEvaluationFailure): Error {
  const error = new Error(failure.message);
  error.name = "EvaluationExecutionError";
  return error;
}

function safeEvaluationSchedulingException(): Error {
  const error = new Error("Evaluation scheduling failed");
  error.name = "EvaluationSchedulingError";
  return error;
}

export const evalJobTraceCreatorQueueProcessor = async (
  job: Job<TQueueJobTypes[QueueName.TraceUpsert]>,
) => {
  try {
    await createEvalJobs({
      sourceEventType: "trace-upsert",
      event: job.data.payload,
      jobTimestamp: job.data.timestamp,
      enforcedJobTimeScope: "NEW", // we must not execute evals which are intended for existing data only.
    });
    return true;
  } catch {
    const safeError = safeEvaluationSchedulingException();
    logger.error("Failed to schedule trace evaluation", {
      projectId: job.data.payload.projectId,
      traceId: job.data.payload.traceId,
      errorCode: "EVALUATION_SCHEDULING_FAILED",
    });
    traceException(safeError, undefined, "EVALUATION_SCHEDULING_FAILED");
    throw safeError;
  }
};

export const evalJobDatasetCreatorQueueProcessor = async (
  job: Job<TQueueJobTypes[QueueName.DatasetRunItemUpsert]>,
) => {
  try {
    await createEvalJobs({
      sourceEventType: "dataset-run-item-upsert",
      event: job.data.payload,
      jobTimestamp: job.data.timestamp,
      enforcedJobTimeScope: "NEW", // we must not execute evals which are intended for existing data only.
    });
    return true;
  } catch (e) {
    // Handle observation-not-found errors with manual retry
    if (isObservationNotFoundError(e)) {
      const shouldRetry = await retryObservationNotFound(e, {
        data: {
          projectId: job.data.payload.projectId,
          datasetItemId: job.data.payload.datasetItemId,
          traceId: job.data.payload.traceId,
          observationId: job.data.payload.observationId,
          retryBaggage: job.data.retryBaggage,
        },
      });

      if (shouldRetry) {
        // Retry was scheduled, complete this job successfully
        return true;
      }

      // Max attempts reached, log warning and complete successfully
      logger.warn(
        `Observation not found after max retries. Completing job without creating eval.`,
        {
          projectId: job.data.payload.projectId,
          datasetItemId: job.data.payload.datasetItemId,
          observationId: job.data.payload.observationId,
          traceId: job.data.payload.traceId,
        },
      );
      return true;
    }

    // All other errors should be logged and propagated for BullMQ retry
    const safeError = safeEvaluationSchedulingException();
    logger.error("Failed to schedule dataset item evaluation", {
      projectId: job.data.payload.projectId,
      datasetItemId: job.data.payload.datasetItemId,
      traceId: job.data.payload.traceId,
      observationId: job.data.payload.observationId,
      errorCode: "EVALUATION_SCHEDULING_FAILED",
    });
    traceException(safeError, undefined, "EVALUATION_SCHEDULING_FAILED");
    throw safeError;
  }
};

export const evalJobCreatorQueueProcessor = async (
  job: Job<TQueueJobTypes[QueueName.CreateEvalQueue]>,
) => {
  try {
    await createEvalJobs({
      sourceEventType: "ui-create-eval",
      event: job.data.payload,
      jobTimestamp: job.data.timestamp,
    });
    return true;
  } catch {
    const safeError = safeEvaluationSchedulingException();
    logger.error("Failed to create historical evaluation jobs", {
      projectId: job.data.payload.projectId,
      traceId: job.data.payload.traceId,
      configId: job.data.payload.configId,
      errorCode: "EVALUATION_SCHEDULING_FAILED",
    });
    traceException(safeError, undefined, "EVALUATION_SCHEDULING_FAILED");
    throw safeError;
  }
};

export const evalJobExecutorQueueProcessorBuilder = (
  enableRedirectToSecondaryQueue: boolean,
  queueName: string,
): Processor => {
  const projectIdsToRedirectToSecondaryQueue =
    env.LANGFUSE_SECONDARY_EVAL_EXECUTION_QUEUE_ENABLED_PROJECT_IDS?.split(
      ",",
    ) ?? [];

  return async (job: Job<TQueueJobTypes[QueueName.EvaluationExecution]>) => {
    try {
      await assertEvaluationExecutionAdmission({
        backend: resolveAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND),
        projectId: job.data.payload.projectId,
        jobExecutionId: job.data.payload.jobExecutionId,
        analyticsEvaluationDispatch:
          job.data.payload.analyticsEvaluationDispatch,
      });
      logger.info("Executing Evaluation Execution Job", {
        projectId: job.data.payload.projectId,
        jobExecutionId: job.data.payload.jobExecutionId,
      });

      // Redirect selected projects to the secondary queue from the primary consumer.
      if (enableRedirectToSecondaryQueue) {
        const projectId = job.data.payload.projectId;
        const shouldRedirectToSecondaryQueue =
          projectIdsToRedirectToSecondaryQueue.includes(projectId);

        if (shouldRedirectToSecondaryQueue) {
          logger.debug(
            `Redirecting evaluation execution job to secondary queue for project ${projectId}`,
          );
          const shardingKey = `${projectId}-${job.data.payload.jobExecutionId}`;
          const secondaryQueue = SecondaryEvalExecutionQueue.getInstance({
            shardingKey,
          });
          if (!secondaryQueue) {
            throw new Error(
              "Secondary evaluation execution queue is not available",
            );
          }

          await secondaryQueue.add(
            QueueName.EvaluationExecutionSecondaryQueue,
            job.data,
          );
          return;
        }
      }

      const span = getCurrentSpan();

      if (span) {
        span.setAttribute(
          "messaging.bullmq.job.input.jobExecutionId",
          job.data.payload.jobExecutionId,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.projectId",
          job.data.payload.projectId,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.retryBaggage.attempt",
          job.data.retryBaggage?.attempt ?? 0,
        );
      }

      await evaluate({ event: job.data.payload });
      return true;
    } catch (e) {
      const llmError = classifyEvaluatorLlmError(e);
      const safeFailure = safeEvaluationFailure(e, llmError);
      // ┌─────────────────────────┐
      // │   Job Fails with Error  │
      // └───────────┬─────────────┘
      //             │
      //             ▼
      // ┌────────────────────────────────────────┐
      // │ Is it a retryable native AI SDK        │
      // │ provider error?                        │
      // └─────┬──────────────────────────────┬───┘
      //       │ Yes                          │ No
      //       ▼                              ▼
      // ┌──────────────────┐       ┌───────────────────────┐
      // │ Is job inside its│       │ Is it retryable?      │
      // │ retry budget?    │       │ (shouldRetryJob)      │
      // └─────┬──────┬─────┘       └─────┬─────────────┬───┘
      //   Yes │      │ No             Yes│             │No
      //       ▼      ▼                Yes│             │No
      // ┌─────────┐ ┌────────┐          ▼             ▼
      // │Set:     │ │Set:    │    ┌─────────┐  ┌──────────┐
      // │DELAYED  │ │ERROR   │    │BullMQ   │  │Set:      │
      // │Retry by │ │Stop    │    │retry    │  │ERROR     │
      // │120 min  │ │        │    │w/ exp.  │  │Done      │
      // └─────────┘ └────────┘    │backoff  │  └──────────┘
      //                           └─────────┘

      const executionTraceId = createW3CTraceId(
        job.data.payload.jobExecutionId,
      );

      if (llmError?.isRetryable) {
        const queue = queueName.startsWith(
          QueueName.EvaluationExecutionSecondaryQueue,
        )
          ? SecondaryEvalExecutionQueue.getInstance({ shardName: queueName })
          : EvalExecutionQueue.getInstance({ shardName: queueName });

        const retryResult = await retryLLMRateLimitError(job, {
          table: "job_executions",
          idField: "jobExecutionId",
          queue,
          queueName,
          jobName: QueueJobs.EvaluationExecution,
        });

        if (retryResult.outcome === "scheduled") {
          // Use the deterministic execution trace ID to update the job execution
          await prisma.jobExecution.update({
            where: {
              id: job.data.payload.jobExecutionId,
              projectId: job.data.payload.projectId,
            },
            data: {
              status: JobExecutionStatus.DELAYED,
              executionTraceId,
            },
          });

          // Return early as we have already scheduled a delayed retry
          return;
        }
      }

      // At this point only terminal LLM failures and application errors remain.
      await prisma.jobExecution.update({
        where: {
          id: job.data.payload.jobExecutionId,
          projectId: job.data.payload.projectId,
        },
        data: {
          status: JobExecutionStatus.ERROR,
          endTime: new Date(),
          // Show user-facing error messages (LLM and config errors)
          error: safeFailure.message,
          executionTraceId,
        },
      });

      logger.error("Evaluation execution failed", {
        projectId: job.data.payload.projectId,
        jobExecutionId: job.data.payload.jobExecutionId,
        errorCode: safeFailure.code,
        retryable: safeFailure.retryable,
      });
      if (llmError || isUnrecoverableError(e)) return;

      const safeError = safeEvaluationException(safeFailure);
      traceException(safeError, undefined, safeFailure.code);

      // Retry job by rethrowing error
      throw safeError;
    }
  };
};

export const llmAsJudgeExecutionQueueProcessorBuilder =
  (queueName: string): Processor =>
  async (job: Job<TQueueJobTypes[QueueName.LLMAsJudgeExecution]>) => {
    try {
      await assertEvaluationExecutionAdmission({
        backend: resolveAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND),
        projectId: job.data.payload.projectId,
        jobExecutionId: job.data.payload.jobExecutionId,
        analyticsEvaluationDispatch:
          job.data.payload.analyticsEvaluationDispatch,
      });
      logger.debug("Executing LLM-as-Judge Observation Evaluation Job", {
        projectId: job.data.payload.projectId,
        jobExecutionId: job.data.payload.jobExecutionId,
      });

      const span = getCurrentSpan();

      if (span) {
        span.setAttribute(
          "messaging.bullmq.job.input.jobExecutionId",
          job.data.payload.jobExecutionId,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.projectId",
          job.data.payload.projectId,
        );
        span.setAttribute(
          "messaging.bullmq.job.input.retryBaggage.attempt",
          job.data.retryBaggage?.attempt ?? 0,
        );
      }

      await processObservationEval({
        event: job.data.payload,
        executionType: EvalTemplateType.LLM_AS_JUDGE,
      });
      return true;
    } catch (e) {
      const llmError = classifyEvaluatorLlmError(e);
      const safeFailure = safeEvaluationFailure(e, llmError);
      const executionTraceId = createW3CTraceId(
        job.data.payload.jobExecutionId,
      );

      if (llmError?.isRetryable) {
        const queue = LLMAsJudgeExecutionQueue.getInstance({
          shardName: queueName,
        });
        const retryResult = await retryLLMRateLimitError(job, {
          table: "job_executions",
          idField: "jobExecutionId",
          queue,
          queueName,
          jobName: QueueJobs.LLMAsJudgeExecution,
        });

        if (retryResult.outcome === "scheduled") {
          await prisma.jobExecution.update({
            where: {
              id: job.data.payload.jobExecutionId,
              projectId: job.data.payload.projectId,
            },
            data: {
              status: JobExecutionStatus.DELAYED,
              executionTraceId,
            },
          });

          return;
        }
      }

      await prisma.jobExecution.update({
        where: {
          id: job.data.payload.jobExecutionId,
          projectId: job.data.payload.projectId,
        },
        data: {
          status: JobExecutionStatus.ERROR,
          endTime: new Date(),
          error: safeFailure.message,
          executionTraceId,
        },
      });

      logger.error("LLM-as-Judge execution failed", {
        projectId: job.data.payload.projectId,
        jobExecutionId: job.data.payload.jobExecutionId,
        errorCode: safeFailure.code,
        retryable: safeFailure.retryable,
      });
      if (llmError || isUnrecoverableError(e)) return;

      const safeError = safeEvaluationException(safeFailure);
      traceException(safeError, undefined, safeFailure.code);

      throw safeError;
    }
  };
