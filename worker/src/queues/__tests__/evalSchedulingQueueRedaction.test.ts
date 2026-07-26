import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Job } from "bullmq";

vi.mock("@langfuse/shared", () => ({
  removeEmptyEnvVariables: <T>(value: T) => value,
  EvalTemplateType: {
    CODE: "CODE",
    LLM_AS_JUDGE: "LLM_AS_JUDGE",
  },
  JobExecutionStatus: {
    DELAYED: "DELAYED",
    ERROR: "ERROR",
  },
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    jobExecution: {
      update: vi.fn(),
    },
  },
}));

vi.mock("@langfuse/shared/src/server", () => ({
  QueueName: {
    TraceUpsert: "trace-upsert",
    DatasetRunItemUpsert: "dataset-run-item-upsert",
    CreateEvalQueue: "create-eval",
    EvaluationExecutionSecondaryQueue: "evaluation-execution-secondary",
  },
  QueueJobs: {
    EvaluationExecution: "evaluation-execution",
  },
  logger: {
    debug: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
  },
  traceException: vi.fn(),
  EvalExecutionQueue: {
    getInstance: vi.fn(),
  },
  SecondaryEvalExecutionQueue: {
    getInstance: vi.fn(),
  },
  LLMAsJudgeExecutionQueue: {
    getInstance: vi.fn(),
  },
  getCurrentSpan: vi.fn(),
  classifyEvaluatorLlmError: vi.fn(),
}));

vi.mock("../../features/evaluation/evalService", () => ({
  createEvalJobs: vi.fn(),
  evaluate: vi.fn(),
}));

vi.mock("../../features/evaluation/observationEval", () => ({
  processObservationEval: vi.fn(),
}));

vi.mock("../../features/evaluation/evaluationExecutionAdmission", () => ({
  assertEvaluationExecutionAdmission: vi.fn(),
}));

vi.mock("../../features/utils", () => ({
  createW3CTraceId: vi.fn(),
  retryLLMRateLimitError: vi.fn(),
}));

vi.mock("../../features/evaluation/retryObservationNotFound", () => ({
  retryObservationNotFound: vi.fn(),
}));

vi.mock("../../errors/ObservationNotFoundError", () => ({
  isObservationNotFoundError: vi.fn().mockReturnValue(false),
}));

vi.mock("../../errors/UnrecoverableError", () => ({
  isUnrecoverableError: vi.fn().mockReturnValue(false),
}));

import { logger, traceException } from "@langfuse/shared/src/server";
import { createEvalJobs } from "../../features/evaluation/evalService";
import {
  evalJobCreatorQueueProcessor,
  evalJobDatasetCreatorQueueProcessor,
  evalJobTraceCreatorQueueProcessor,
} from "../evalQueue";

const secretError = new Error(
  "postgresql://admin:password@db/private Authorization=Bearer token SQL=select * from traces prompt=secret input=private output=private",
);

function serializedObservability(): string {
  return JSON.stringify([
    vi.mocked(logger.error).mock.calls,
    vi.mocked(traceException).mock.calls,
  ]);
}

describe("evaluation scheduling queue redaction", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(createEvalJobs).mockRejectedValue(secretError);
  });

  it("masks trace scheduling failures in logs, spans, and BullMQ errors", async () => {
    const job = {
      data: {
        timestamp: new Date(),
        payload: {
          projectId: "project-1",
          traceId: "trace-1",
        },
      },
    } as Job<any>;

    await expect(evalJobTraceCreatorQueueProcessor(job)).rejects.toThrow(
      "Evaluation scheduling failed",
    );

    expect(serializedObservability()).not.toContain("password");
    expect(serializedObservability()).not.toContain("Bearer token");
    expect(serializedObservability()).not.toContain("prompt=secret");
  });

  it("masks dataset scheduling failures in logs, spans, and BullMQ errors", async () => {
    const job = {
      data: {
        timestamp: new Date(),
        payload: {
          projectId: "project-1",
          datasetItemId: "dataset-item-1",
          traceId: "trace-1",
          observationId: "observation-1",
        },
      },
    } as Job<any>;

    await expect(evalJobDatasetCreatorQueueProcessor(job)).rejects.toThrow(
      "Evaluation scheduling failed",
    );

    expect(serializedObservability()).not.toContain("password");
    expect(serializedObservability()).not.toContain("select * from traces");
    expect(serializedObservability()).not.toContain("input=private");
  });

  it("masks historical scheduling failures in logs, spans, and BullMQ errors", async () => {
    const job = {
      data: {
        timestamp: new Date(),
        payload: {
          projectId: "project-1",
          traceId: "trace-1",
          configId: "config-1",
        },
      },
    } as Job<any>;

    await expect(evalJobCreatorQueueProcessor(job)).rejects.toThrow(
      "Evaluation scheduling failed",
    );

    expect(serializedObservability()).not.toContain("password");
    expect(serializedObservability()).not.toContain("output=private");
    expect(serializedObservability()).not.toContain("Authorization");
  });
});
