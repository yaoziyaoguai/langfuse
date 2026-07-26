import { describe, expect, it, vi } from "vitest";

vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));
vi.mock("../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(),
}));
vi.mock("../features/evaluation/evalService", () => ({
  createEvalJobs: vi.fn(),
}));
vi.mock("../features/evaluation/observationEval", () => ({
  createObservationEvalSchedulerDeps: vi.fn(),
  fetchObservationEvalConfigs: vi.fn(),
  isObservationAllowedForQueuedObservationEvals: vi.fn(() => true),
  scheduleObservationEvals: vi.fn(),
}));
vi.mock("../features/evaluation/analyticsEvaluationTargetRuntime", () => ({
  getAnalyticsEvaluationTargetSource: vi.fn(),
}));

import { AnalyticsEvaluationDispatchProvenanceError } from "@langfuse/shared/src/server";
import { analyticsEvaluationDispatchQueueProcessorBuilder } from "./analyticsEvaluationDispatchQueue";

const envelope = {
  dispatchId: "dispatch-1",
  dispatchGeneration: 1,
  projectId: "project-1",
  operationId: "operation-1",
  targetType: "TRACE_UPSERT" as const,
  targetId: "trace-1",
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "1",
  workloadEpochFingerprint: "e".repeat(64),
  runtimeContractVersion: 1,
  capabilityActivationGeneration: "7",
  capabilityContractVersion: 1,
};

const job = (payload = envelope) =>
  ({
    data: {
      id: "dispatch-1-g1",
      name: "analytics-evaluation-dispatch-job",
      timestamp: new Date("2026-07-23T06:00:00.000Z"),
      payload,
    },
  }) as never;

const dispatch = {
  id: "dispatch-1",
  projectId: "project-1",
  traceId: "trace-1",
  observationId: null,
  datasetItemId: null,
  datasetItemValidFrom: null,
  targetTimestamp: new Date("2026-07-23T05:59:00.000Z"),
  traceEnvironment: "production",
  targetType: "TRACE_UPSERT",
  dispatchGeneration: 1,
};

describe("analytics evaluation dispatch consumer", () => {
  it("validates the strict envelope before claiming or reading a target", async () => {
    const claim = vi.fn();
    const getTargetSource = vi.fn();
    const processor = analyticsEvaluationDispatchQueueProcessorBuilder({
      claim: claim as never,
      getTargetSource,
      getAdmissionContext: () => ({
        runtimeLeaseId: "worker-1",
        backend: "doris",
        deploymentGeneration: 1n,
      }),
    });

    await expect(
      processor(
        job({ ...envelope, projectId: "", untrusted: "payload" } as never),
        "token",
      ),
    ).rejects.toThrow();
    expect(claim).not.toHaveBeenCalled();
    expect(getTargetSource).not.toHaveBeenCalled();
  });

  it("creates trace jobs from the authoritative claimed row and completes it", async () => {
    const createJobs = vi.fn().mockResolvedValue(undefined);
    const complete = vi.fn().mockResolvedValue(true);
    const processor = analyticsEvaluationDispatchQueueProcessorBuilder({
      claim: vi.fn().mockResolvedValue(dispatch) as never,
      createJobs: createJobs as never,
      complete: complete as never,
      leaseOwner: () => "consumer-a",
      getAdmissionContext: () => ({
        runtimeLeaseId: "worker-1",
        backend: "doris",
        deploymentGeneration: 1n,
      }),
    });

    await expect(processor(job(), "token")).resolves.toBe(true);
    expect(createJobs).toHaveBeenCalledWith(
      expect.objectContaining({
        sourceEventType: "trace-upsert",
        event: expect.objectContaining({
          projectId: "project-1",
          traceId: "trace-1",
        }),
        analyticsEvaluationDispatch: envelope,
      }),
    );
    expect(complete).toHaveBeenCalledWith(
      expect.objectContaining({
        dispatchId: "dispatch-1",
        expectedGeneration: 1,
        leaseOwner: "consumer-a",
      }),
    );
  });

  it("quarantines a missing observation target without scheduling work", async () => {
    const quarantine = vi.fn().mockResolvedValue(true);
    const scheduleObservation = vi.fn();
    const processor = analyticsEvaluationDispatchQueueProcessorBuilder({
      claim: vi.fn().mockResolvedValue({
        ...dispatch,
        targetType: "OBSERVATION_UPSERT",
        targetId: "span-1",
        observationId: "span-1",
      }) as never,
      getTargetSource: () =>
        ({
          getObservationForEvaluation: vi.fn().mockResolvedValue(undefined),
        }) as never,
      quarantine: quarantine as never,
      scheduleObservation: scheduleObservation as never,
      leaseOwner: () => "consumer-a",
      getAdmissionContext: () => ({
        runtimeLeaseId: "worker-1",
        backend: "doris",
        deploymentGeneration: 1n,
      }),
    });

    await expect(
      processor(
        job({
          ...envelope,
          targetType: "OBSERVATION_UPSERT",
          targetId: "span-1",
        }),
        "token",
      ),
    ).resolves.toBe(true);
    expect(quarantine).toHaveBeenCalledWith(
      expect.objectContaining({
        dispatchId: "dispatch-1",
        expectedGeneration: 1,
        leaseOwner: "consumer-a",
        failureCode: "EVALUATION_TARGET_NOT_FOUND",
      }),
    );
    expect(scheduleObservation).not.toHaveBeenCalled();
  });

  it("creates a managed historical trace job for the selected evaluator", async () => {
    const createJobs = vi.fn().mockResolvedValue(undefined);
    const complete = vi.fn().mockResolvedValue(true);
    const historicalEnvelope = {
      ...envelope,
      targetType: "HISTORICAL" as const,
      targetId: "trace-1",
    };
    const processor = analyticsEvaluationDispatchQueueProcessorBuilder({
      claim: vi.fn().mockResolvedValue({
        ...dispatch,
        targetType: "HISTORICAL",
        jobConfigurationId: "config-1",
      }) as never,
      createJobs: createJobs as never,
      complete: complete as never,
      leaseOwner: () => "consumer-a",
      getAdmissionContext: () => ({
        runtimeLeaseId: "worker-1",
        backend: "doris",
        deploymentGeneration: 1n,
      }),
    });

    await expect(processor(job(historicalEnvelope), "token")).resolves.toBe(
      true,
    );
    expect(createJobs).toHaveBeenCalledWith({
      sourceEventType: "ui-create-eval",
      event: {
        projectId: "project-1",
        traceId: "trace-1",
        configId: "config-1",
        timestamp: dispatch.targetTimestamp,
        exactTimestamp: dispatch.targetTimestamp,
        traceEnvironment: "production",
      },
      jobTimestamp: new Date("2026-07-23T06:00:00.000Z"),
      analyticsEvaluationDispatch: historicalEnvelope,
      executionMode: "MANUAL",
    });
    expect(complete).toHaveBeenCalledOnce();
  });

  it("schedules only the selected historical observation evaluator in manual mode", async () => {
    const scheduleObservation = vi.fn().mockResolvedValue(undefined);
    const createObservationScheduler = vi.fn().mockReturnValue({
      marker: "scheduler",
    });
    const complete = vi.fn().mockResolvedValue(true);
    const fetchObservationConfigs = vi
      .fn()
      .mockResolvedValue([{ id: "config-other" }, { id: "config-selected" }]);
    const observation = {
      id: "span-1",
      traceId: "trace-1",
    };
    const historicalEnvelope = {
      ...envelope,
      targetType: "HISTORICAL" as const,
      targetId: "span-1",
    };
    const processor = analyticsEvaluationDispatchQueueProcessorBuilder({
      claim: vi.fn().mockResolvedValue({
        ...dispatch,
        targetType: "HISTORICAL",
        targetId: "span-1",
        observationId: "span-1",
        jobConfigurationId: "config-selected",
      }) as never,
      getTargetSource: () =>
        ({
          getObservationForEvaluation: vi.fn().mockResolvedValue(observation),
        }) as never,
      fetchObservationConfigs: fetchObservationConfigs as never,
      createObservationScheduler: createObservationScheduler as never,
      scheduleObservation: scheduleObservation as never,
      complete: complete as never,
      leaseOwner: () => "consumer-a",
      getAdmissionContext: () => ({
        runtimeLeaseId: "worker-1",
        backend: "doris",
        deploymentGeneration: 1n,
      }),
    });

    await expect(processor(job(historicalEnvelope), "token")).resolves.toBe(
      true,
    );
    expect(createObservationScheduler).toHaveBeenCalledWith({
      analyticsEvaluationDispatch: historicalEnvelope,
    });
    expect(fetchObservationConfigs).toHaveBeenCalledWith("project-1", {
      includeInactive: true,
      jobConfigurationId: "config-selected",
    });
    expect(scheduleObservation).toHaveBeenCalledWith({
      observation,
      configs: [{ id: "config-selected" }],
      schedulerDeps: { marker: "scheduler" },
      executionMode: "MANUAL",
      failOnConfigError: true,
      jobIdentitySeed: "dispatch-1",
    });
    expect(complete).toHaveBeenCalledOnce();
  });

  it("requeues transient scheduling failures and terminalizes provenance failures", async () => {
    const requeue = vi.fn().mockResolvedValue(true);
    const logError = vi.fn();
    const transientProcessor = analyticsEvaluationDispatchQueueProcessorBuilder(
      {
        claim: vi.fn().mockResolvedValue(dispatch) as never,
        createJobs: vi
          .fn()
          .mockRejectedValue(
            new Error(
              "Authorization: Bearer secret prompt=private sql=SELECT *",
            ),
          ) as never,
        requeue: requeue as never,
        logError,
        leaseOwner: () => "consumer-a",
        getAdmissionContext: () => ({
          runtimeLeaseId: "worker-1",
          backend: "doris",
          deploymentGeneration: 1n,
        }),
      },
    );
    await expect(transientProcessor(job(), "token")).resolves.toBe(true);
    expect(requeue).toHaveBeenCalledWith(
      expect.objectContaining({ dispatchId: "dispatch-1" }),
    );
    expect(JSON.stringify(logError.mock.calls)).not.toContain("Bearer secret");
    expect(JSON.stringify(logError.mock.calls)).not.toContain("prompt=private");
    expect(JSON.stringify(logError.mock.calls)).not.toContain("SELECT");

    const quarantine = vi.fn().mockResolvedValue(true);
    const invalidProcessor = analyticsEvaluationDispatchQueueProcessorBuilder({
      claim: vi.fn().mockResolvedValue(dispatch) as never,
      createJobs: vi
        .fn()
        .mockRejectedValue(
          new AnalyticsEvaluationDispatchProvenanceError("tampered"),
        ) as never,
      quarantine: quarantine as never,
      leaseOwner: () => "consumer-a",
      getAdmissionContext: () => ({
        runtimeLeaseId: "worker-1",
        backend: "doris",
        deploymentGeneration: 1n,
      }),
    });
    await expect(invalidProcessor(job(), "token")).rejects.toThrow("tampered");
    expect(quarantine).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedGeneration: 1,
        leaseOwner: "consumer-a",
        failureCode: "EVALUATION_DISPATCH_PROVENANCE_MISMATCH",
      }),
    );
  });
});
