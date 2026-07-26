import { JobExecutionStatus } from "@prisma/client";
import {
  logger,
  traceException,
  type CodeEvalScoreWithName,
} from "@langfuse/shared/src/server";
import { buildEvalScoreWritePayloads } from "./evalScoreEvent";
import { type EvalExecutionDeps } from "./evalExecutionDeps";

/**
 * Common result returned by every evaluator executor (LLM-as-judge,
 * code-based, future executors). The first persisted score payload is treated
 * as primary and is persisted on `JobExecution.jobOutputScoreId`.
 */
export type EvalExecutionResult = {
  scores: CodeEvalScoreWithName[];
  executionTraceId: string;
  metadata: Record<string, string>;
};

export async function completeEvalExecution({
  projectId,
  jobExecutionId,
  result,
  traceId,
  observationId,
  environment,
  scoreTimestamp,
  deps,
}: {
  projectId: string;
  jobExecutionId: string;
  result: EvalExecutionResult;
  traceId: string | null;
  observationId: string | null;
  environment: string;
  scoreTimestamp: Date;
  deps: EvalExecutionDeps;
}): Promise<{ scoreCount: number }> {
  const scoreWritePayloads = buildEvalScoreWritePayloads({
    scores: result.scores,
    jobExecutionId,
    traceId,
    observationId,
    environment,
    executionTraceId: result.executionTraceId,
    executionMetadata: result.metadata,
    timestamp: scoreTimestamp,
  });
  const [firstScorePayload] = scoreWritePayloads;

  if (!firstScorePayload) {
    throw new Error(`Evaluation job ${jobExecutionId} returned no scores`);
  }
  const jobOutputScoreId = firstScorePayload.scoreId;

  try {
    if (deps.persistScoreBatch) {
      await deps.persistScoreBatch({
        projectId,
        jobExecutionId,
        scoreWritePayloads,
      });
    } else {
      await Promise.all(
        scoreWritePayloads.map(async ({ scoreId, eventId, event }) => {
          await deps.uploadScore({
            projectId,
            scoreId,
            eventId,
            event,
          });

          await deps.enqueueScoreIngestion({
            projectId,
            scoreId,
            eventId,
          });
        }),
      );
    }
  } catch (e) {
    logger.error("Failed to persist evaluator score", {
      projectId,
      jobExecutionId,
      scoreId: jobOutputScoreId,
      errorType: e instanceof Error ? e.name : "UnknownError",
    });
    traceException(
      new Error("Evaluator score persistence failed"),
      undefined,
      "EVALUATION_SCORE_PERSISTENCE_FAILED",
    );
    throw new Error(
      `Failed to make score ${jobOutputScoreId} visible in analytics storage`,
    );
  }

  logger.debug(
    `Persisted ${scoreWritePayloads.length} score(s) for job ${jobExecutionId}`,
  );

  await deps.updateJobExecution({
    id: jobExecutionId,
    projectId,
    data: {
      status: JobExecutionStatus.COMPLETED,
      endTime: new Date(),
      jobOutputScoreId,
      executionTraceId: result.executionTraceId,
    },
  });

  return { scoreCount: scoreWritePayloads.length };
}
