import { describe, expect, it } from "vitest";

import {
  claimExperimentExecution,
  completeExperimentExecution,
  failExperimentExecution,
} from "./experimentExecutions";

const base = {
  client: {} as never,
  admissionContext: {
    runtimeLeaseId: "worker-lease",
    backend: "doris" as const,
    deploymentGeneration: 1n,
  },
};

describe("experiment execution input validation", () => {
  it("rejects non-integer claim leases before opening a transaction", async () => {
    await expect(
      claimExperimentExecution({
        ...base,
        job: {
          projectId: "project-1",
          datasetId: "dataset-1",
          runId: "run-1",
        },
        leaseOwner: "worker-1",
        leaseMs: Number.POSITIVE_INFINITY,
      }),
    ).rejects.toThrow(TypeError);
  });

  it("rejects invalid completion identities and failure outcomes", async () => {
    await expect(
      completeExperimentExecution({
        ...base,
        projectId: "project-1",
        runId: "run-1",
        claimId: "",
        generation: 1n,
        leaseOwner: "worker-1",
      }),
    ).rejects.toThrow(TypeError);

    await expect(
      failExperimentExecution({
        ...base,
        projectId: "project-1",
        runId: "run-1",
        claimId: "claim-1",
        generation: 1n,
        leaseOwner: "worker-1",
      }),
    ).rejects.toThrow(TypeError);
  });
});
