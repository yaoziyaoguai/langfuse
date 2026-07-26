import { describe, expect, it, vi } from "vitest";

import { ExperimentExecutionProvenanceError } from "@langfuse/shared/src/server";
import { publishExperimentExecutionDispatchBatch } from ".";

const admissionContext = {
  runtimeLeaseId: "worker-runtime",
  backend: "doris" as const,
  deploymentGeneration: 2n,
};
const candidate = {
  projectId: "project-1",
  runId: "run-1",
  generation: 3,
};

describe("publishExperimentExecutionDispatchBatch", () => {
  it("publishes with a stable job id and revives a failed delivery", async () => {
    const retry = vi.fn().mockResolvedValue(undefined);
    const queue = {
      add: vi.fn().mockResolvedValue({
        getState: vi.fn().mockResolvedValue("failed"),
        retry,
      }),
    };
    const publishDispatch = vi.fn(async (input) => {
      await input.publish({
        projectId: candidate.projectId,
        datasetId: "dataset-1",
        runId: candidate.runId,
        dispatchGeneration: candidate.generation,
        analyticsBackend: "DORIS",
        deploymentGeneration: "2",
        workloadEpochFingerprint: "e".repeat(64),
        runtimeContractVersion: 1,
        capabilityActivationGeneration: "4",
        capabilityContractVersion: 1,
      });
      return true;
    });

    await expect(
      publishExperimentExecutionDispatchBatch({
        admissionContext,
        queue,
        findPending: vi.fn().mockResolvedValue([candidate]),
        publishDispatch,
      }),
    ).resolves.toBe(1);

    expect(queue.add).toHaveBeenCalledWith(
      "experiment-create-queue",
      expect.objectContaining({
        id: "run-1-g3",
        payload: expect.objectContaining({
          runId: "run-1",
          dispatchGeneration: 3,
        }),
      }),
      { jobId: "run-1-g3" },
    );
    expect(retry).toHaveBeenCalledWith("failed");
  });

  it("quarantines provenance failures and defers transient failures", async () => {
    const quarantineDispatch = vi.fn().mockResolvedValue(true);
    const deferDispatch = vi.fn().mockResolvedValue(true);

    await expect(
      publishExperimentExecutionDispatchBatch({
        admissionContext,
        queue: { add: vi.fn() },
        findPending: vi
          .fn()
          .mockResolvedValue([candidate, { ...candidate, runId: "run-2" }]),
        publishDispatch: vi
          .fn()
          .mockRejectedValueOnce(
            new ExperimentExecutionProvenanceError("stale"),
          )
          .mockRejectedValueOnce(new Error("redis unavailable")),
        quarantineDispatch,
        deferDispatch,
      }),
    ).resolves.toBe(0);

    expect(quarantineDispatch).toHaveBeenCalledWith(
      expect.objectContaining({
        runId: "run-1",
        failureCode: "EXPERIMENT_DISPATCH_PROVENANCE_MISMATCH",
      }),
    );
    expect(deferDispatch).toHaveBeenCalledWith(
      expect.objectContaining({ runId: "run-2", expectedGeneration: 3 }),
    );
  });
});
