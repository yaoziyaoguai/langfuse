import { Job, Processor } from "bullmq";
import { EvalTemplateType, JobExecutionStatus } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  CodeEvalExecutionError,
  getCodeEvalUserVisibleError,
  getCurrentSpan,
  logger,
  QueueName,
  TQueueJobTypes,
  traceException,
} from "@langfuse/shared/src/server";
import { processObservationEval } from "../features/evaluation/observationEval";
import { createW3CTraceId } from "../features/utils";
import { isUnrecoverableError } from "../errors/UnrecoverableError";
import { resolveAnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { env } from "../env";
import { assertEvaluationExecutionAdmission } from "../features/evaluation/evaluationExecutionAdmission";

export const codeEvalExecutionQueueProcessorBuilder = (
  _queueName: string,
): Processor => {
  return async (job: Job<TQueueJobTypes[QueueName.CodeEvalExecution]>) => {
    try {
      await assertEvaluationExecutionAdmission({
        backend: resolveAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND),
        projectId: job.data.payload.projectId,
        jobExecutionId: job.data.payload.jobExecutionId,
        analyticsEvaluationDispatch:
          job.data.payload.analyticsEvaluationDispatch,
      });
      logger.debug("Executing Code Evaluation Observation Job", {
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
      }

      await processObservationEval({
        event: job.data.payload,
        executionType: EvalTemplateType.CODE,
      });

      return true;
    } catch (e) {
      const executionTraceId = createW3CTraceId(
        job.data.payload.jobExecutionId,
      );

      const isTerminalError = isUnrecoverableError(e);
      const totalAttempts = job.opts.attempts ?? 1;
      const isFinalAttempt = job.attemptsMade + 1 >= totalAttempts;

      // Only persist the terminal ERROR state when there will be no more
      // retries; otherwise observationEvalProcessor would short-circuit the
      // retry attempts because it skips jobs already in ERROR status.
      if (isTerminalError || isFinalAttempt) {
        await prisma.jobExecution.update({
          where: {
            id: job.data.payload.jobExecutionId,
            projectId: job.data.payload.projectId,
          },
          data: {
            status: JobExecutionStatus.ERROR,
            endTime: new Date(),
            error: getJobExecutionErrorMessage(e),
            executionTraceId,
          },
        });
      }

      if (isTerminalError) return;

      const visibleError =
        e instanceof CodeEvalExecutionError
          ? {
              code: e.code,
              message: e.message,
              retryable: e.retryable,
            }
          : getCodeEvalUserVisibleError(e);
      const safeError = new CodeEvalExecutionError(visibleError);
      traceException(safeError, undefined, visibleError.code);
      logger.error("Failed code eval execution job", {
        projectId: job.data.payload.projectId,
        jobExecutionId: job.data.payload.jobExecutionId,
        errorCode: visibleError.code,
        retryable: visibleError.retryable,
      });

      throw safeError;
    }
  };
};

function getJobExecutionErrorMessage(e: unknown): string {
  if (e instanceof CodeEvalExecutionError) return e.message;

  return getCodeEvalUserVisibleError(e).message;
}
