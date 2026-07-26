import { describe, expect, it, vi } from "vitest";

import type { AnalyticsEvaluationDispatchEventType } from "@langfuse/shared/src/server";

import { assertEvaluationExecutionAdmission } from "./evaluationExecutionAdmission";

vi.mock("../../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(() => null),
}));

const envelope: AnalyticsEvaluationDispatchEventType = {
  dispatchId: "dispatch-1",
  dispatchGeneration: 1,
  projectId: "project-1",
  operationId: "operation-1",
  targetType: "TRACE_UPSERT",
  targetId: "trace-1",
  analyticsBackend: "DORIS",
  deploymentGeneration: "1",
  workloadEpochFingerprint: "a".repeat(64),
  runtimeContractVersion: 1,
  capabilityActivationGeneration: "1",
  capabilityContractVersion: 1,
};

describe("assertEvaluationExecutionAdmission", () => {
  it("keeps legacy ClickHouse evaluation execution unchanged", async () => {
    const validate = vi.fn();

    await assertEvaluationExecutionAdmission(
      {
        backend: "clickhouse",
        projectId: "project-1",
        jobExecutionId: "job-1",
      },
      {
        getAdmissionContext: vi.fn(),
        validate,
      },
    );

    expect(validate).not.toHaveBeenCalled();
  });

  it("rejects legacy queue work before execution in Doris mode", async () => {
    const validate = vi.fn();

    await expect(
      assertEvaluationExecutionAdmission(
        {
          backend: "doris",
          projectId: "project-1",
          jobExecutionId: "job-1",
        },
        {
          getAdmissionContext: vi.fn(),
          validate,
        },
      ),
    ).rejects.toThrow("Doris evaluation execution requires");

    expect(validate).not.toHaveBeenCalled();
  });

  it("validates the authoritative dispatch link and admitted runtime", async () => {
    const admissionContext = {
      runtimeLeaseId: "worker-lease",
      backend: "doris" as const,
      deploymentGeneration: 1n,
    };
    const validate = vi.fn().mockResolvedValue(undefined);

    await assertEvaluationExecutionAdmission(
      {
        backend: "doris",
        projectId: "project-1",
        jobExecutionId: "job-1",
        analyticsEvaluationDispatch: envelope,
      },
      {
        getAdmissionContext: vi.fn(() => admissionContext),
        validate,
      },
    );

    expect(validate).toHaveBeenCalledWith({
      client: expect.anything(),
      admissionContext,
      envelope,
      jobExecutionId: "job-1",
    });
  });

  it("rejects an envelope whose project does not match the execution", async () => {
    const validate = vi.fn();

    await expect(
      assertEvaluationExecutionAdmission(
        {
          backend: "doris",
          projectId: "other-project",
          jobExecutionId: "job-1",
          analyticsEvaluationDispatch: envelope,
        },
        {
          getAdmissionContext: vi.fn(() => ({
            runtimeLeaseId: "worker-lease",
            backend: "doris",
            deploymentGeneration: 1n,
          })),
          validate,
        },
      ),
    ).rejects.toThrow("project does not match");

    expect(validate).not.toHaveBeenCalled();
  });

  it("rejects Doris-managed work on a ClickHouse runtime", async () => {
    await expect(
      assertEvaluationExecutionAdmission(
        {
          backend: "clickhouse",
          projectId: "project-1",
          jobExecutionId: "job-1",
          analyticsEvaluationDispatch: envelope,
        },
        {
          getAdmissionContext: vi.fn(),
          validate: vi.fn(),
        },
      ),
    ).rejects.toThrow("cannot execute on ClickHouse");
  });
});
