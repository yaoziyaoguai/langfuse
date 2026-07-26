import { beforeEach, describe, expect, it, vi, type Mock } from "vitest";
import { Job } from "bullmq";

vi.mock("@langfuse/shared/src/server", () => ({
  ExperimentCreateQueue: {
    getInstance: vi.fn().mockReturnValue({
      add: vi.fn(),
    }),
  },
  QueueJobs: {
    ExperimentCreateJob: "experiment-create-job",
  },
  QueueName: {
    ExperimentCreate: "experiment-create-queue",
  },
  classifyEvaluatorLlmError: vi.fn(),
  isManagedExperimentExecutionJob: vi.fn(() => false),
  claimExperimentExecution: vi.fn(),
  renewExperimentExecutionClaim: vi.fn(),
  completeExperimentExecution: vi.fn(),
  failExperimentExecution: vi.fn(),
  logger: {
    error: vi.fn(),
  },
  traceException: vi.fn(),
}));

vi.mock("../../features/utils", () => ({
  retryLLMRateLimitError: vi.fn(),
}));

vi.mock("../../features/experiments/experimentServiceClickhouse", () => ({
  createExperimentJob: vi.fn(),
}));
vi.mock("../../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(() => ({
    runtimeLeaseId: "runtime-1",
    backend: "doris",
    deploymentGeneration: 1n,
  })),
}));
vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));

vi.mock("../../errors/UnrecoverableError", async () => {
  const actual = await vi.importActual("../../errors/UnrecoverableError");
  return {
    ...actual,
    isUnrecoverableError: vi.fn(),
  };
});

import {
  claimExperimentExecution,
  classifyEvaluatorLlmError,
  completeExperimentExecution,
  isManagedExperimentExecutionJob,
  renewExperimentExecutionClaim,
} from "@langfuse/shared/src/server";
import { createExperimentJob } from "../../features/experiments/experimentServiceClickhouse";
import { retryLLMRateLimitError } from "../../features/utils";
import { experimentCreateQueueProcessor } from "../experimentQueue";
import { isUnrecoverableError } from "../../errors/UnrecoverableError";

describe("experimentCreateQueueProcessor", () => {
  const createMockJob = (): Job<any> =>
    ({
      data: {
        payload: {
          projectId: "project-id",
          runId: "run-id",
        },
      },
    }) as Job<any>;

  beforeEach(() => {
    vi.clearAllMocks();
    (classifyEvaluatorLlmError as Mock).mockReturnValue(null);
    (isUnrecoverableError as Mock).mockReturnValue(false);
    (isManagedExperimentExecutionJob as Mock).mockReturnValue(false);
  });

  it("rethrows retryable LLM errors when the retry queue is unavailable", async () => {
    const llmError = new Error("Rate limit exceeded");
    (createExperimentJob as Mock).mockRejectedValue(llmError);
    (classifyEvaluatorLlmError as Mock).mockReturnValue({
      kind: "provider",
      message: llmError.message,
      isRetryable: true,
      error: llmError,
      blockReason: null,
    });
    (retryLLMRateLimitError as Mock).mockResolvedValue({
      outcome: "queue_unavailable",
    });

    await expect(
      experimentCreateQueueProcessor(createMockJob()),
    ).rejects.toThrow("Rate limit exceeded");

    expect(retryLLMRateLimitError).toHaveBeenCalledWith(
      expect.objectContaining({
        data: expect.objectContaining({
          payload: expect.objectContaining({
            projectId: "project-id",
            runId: "run-id",
          }),
        }),
      }),
      expect.objectContaining({
        table: "dataset_runs",
        idField: "runId",
      }),
    );
  });

  it("claims and completes a managed Doris experiment", async () => {
    (isManagedExperimentExecutionJob as Mock).mockReturnValue(true);
    (claimExperimentExecution as Mock).mockResolvedValue({
      claimId: "claim-1",
      generation: 2n,
      leaseExpiresAt: new Date(Date.now() + 60_000),
    });
    (renewExperimentExecutionClaim as Mock).mockResolvedValue(
      new Date(Date.now() + 120_000),
    );
    (completeExperimentExecution as Mock).mockResolvedValue(true);
    (createExperimentJob as Mock).mockImplementation(
      async ({ onItemProcessed }) => {
        await onItemProcessed();
        return { success: true };
      },
    );
    const job = createMockJob();
    job.data.payload = {
      projectId: "project-id",
      datasetId: "dataset-id",
      runId: "run-id",
      dispatchGeneration: 1,
      analyticsBackend: "DORIS",
      deploymentGeneration: "1",
      workloadEpochFingerprint: "e".repeat(64),
      runtimeContractVersion: 1,
      capabilityActivationGeneration: "1",
      capabilityContractVersion: 1,
    };

    await expect(experimentCreateQueueProcessor(job)).resolves.toBe(true);

    expect(claimExperimentExecution).toHaveBeenCalledWith(
      expect.objectContaining({
        job: job.data.payload,
        leaseOwner: expect.stringContaining("experiment:"),
      }),
    );
    expect(renewExperimentExecutionClaim).toHaveBeenCalledWith(
      expect.objectContaining({ claimId: "claim-1", generation: 2n }),
    );
    expect(completeExperimentExecution).toHaveBeenCalledWith(
      expect.objectContaining({ claimId: "claim-1", generation: 2n }),
    );
  });
});
