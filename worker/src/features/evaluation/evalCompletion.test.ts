import { describe, expect, it, vi } from "vitest";
import { JobExecutionStatus } from "@prisma/client";
import { ScoreDataTypeEnum } from "@langfuse/shared";

const observabilityMocks = vi.hoisted(() => ({
  error: vi.fn(),
  traceException: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@langfuse/shared/src/server")>();

  return {
    ...actual,
    logger: {
      debug: vi.fn(),
      error: observabilityMocks.error,
    },
    traceException: observabilityMocks.traceException,
  };
});

import { completeEvalExecution } from "./evalCompletion";
import { createMockEvalExecutionDeps } from "./evalExecutionDeps";

const input = {
  projectId: "project-1",
  jobExecutionId: "job-1",
  result: {
    scores: [
      {
        name: "quality",
        value: 0.9,
        dataType: ScoreDataTypeEnum.NUMERIC,
      },
    ],
    executionTraceId: "execution-trace-1",
    metadata: { job_execution_id: "job-1" },
  },
  traceId: "trace-1",
  observationId: null,
  environment: "production",
  scoreTimestamp: new Date("2026-07-23T10:00:00.000Z"),
};

describe("completeEvalExecution", () => {
  it("marks the job complete only after backend-owned score persistence", async () => {
    const order: string[] = [];
    const persistScoreBatch = vi.fn(async () => {
      order.push("score-visible");
    });
    const updateJobExecution = vi.fn(async () => {
      order.push("job-complete");
    });
    const uploadScore = vi.fn();
    const enqueueScoreIngestion = vi.fn();

    await completeEvalExecution({
      ...input,
      deps: createMockEvalExecutionDeps({
        persistScoreBatch,
        updateJobExecution,
        uploadScore,
        enqueueScoreIngestion,
      }),
    });

    expect(order).toEqual(["score-visible", "job-complete"]);
    expect(uploadScore).not.toHaveBeenCalled();
    expect(enqueueScoreIngestion).not.toHaveBeenCalled();
    expect(persistScoreBatch).toHaveBeenCalledWith({
      projectId: "project-1",
      jobExecutionId: "job-1",
      scoreWritePayloads: [
        expect.objectContaining({
          eventId: expect.any(String),
          scoreId: expect.any(String),
          event: expect.objectContaining({
            id: expect.any(String),
            timestamp: "2026-07-23T10:00:00.000Z",
          }),
        }),
      ],
    });
    expect(updateJobExecution).toHaveBeenCalledWith({
      id: "job-1",
      projectId: "project-1",
      data: expect.objectContaining({
        status: JobExecutionStatus.COMPLETED,
        jobOutputScoreId: expect.any(String),
      }),
    });
  });

  it("does not complete the job when score visibility fails", async () => {
    const updateJobExecution = vi.fn();
    const secret =
      "postgresql://admin:password@db Authorization=Bearer token prompt=secret";

    await expect(
      completeEvalExecution({
        ...input,
        deps: createMockEvalExecutionDeps({
          persistScoreBatch: vi.fn().mockRejectedValue(new Error(secret)),
          updateJobExecution,
        }),
      }),
    ).rejects.toThrow("Failed to make score");

    expect(updateJobExecution).not.toHaveBeenCalled();
    expect(JSON.stringify(observabilityMocks.error.mock.calls)).not.toContain(
      secret,
    );
    expect(observabilityMocks.traceException).toHaveBeenCalledWith(
      expect.objectContaining({
        message: "Evaluator score persistence failed",
      }),
      undefined,
      "EVALUATION_SCORE_PERSISTENCE_FAILED",
    );
    expect(
      JSON.stringify(observabilityMocks.traceException.mock.calls),
    ).not.toContain(secret);
  });
});
