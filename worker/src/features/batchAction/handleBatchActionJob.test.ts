import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  buildObservationQuery: vi.fn(),
  buildTraceQuery: vi.fn(),
  createHistoricalDispatches: vi.fn(),
  getAdmissionContext: vi.fn(),
  observationScan: vi.fn(),
  traceScan: vi.fn(),
  jobConfigurationFindFirst: vi.fn(),
  jobConfigurationFindMany: vi.fn(),
  datasetRunItemsFindMany: vi.fn(),
  batchActionUpdate: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", () => ({
  buildDorisObservationReadQuery: mocks.buildObservationQuery,
  buildDorisTraceReadQuery: mocks.buildTraceQuery,
  createHistoricalAnalyticsEvaluationDispatches:
    mocks.createHistoricalDispatches,
  findDatasetIdsForBatchDeletion: vi.fn(),
  getDorisTelemetryRepositories: () => ({
    observations: { scan: mocks.observationScan },
    traces: { scanEvaluationTargets: mocks.traceScan },
  }),
  getObservationsWithModelDataFromEventsTable: vi.fn(),
  getScoresUiTableFromEvents: vi.fn(),
  getSessionsTable: vi.fn(),
  getTraceIdentifiers: vi.fn(),
  logger: { info: vi.fn() },
  QueueName: { BatchActionQueue: "batch-action-queue" },
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    jobConfiguration: {
      findFirst: mocks.jobConfigurationFindFirst,
      findMany: mocks.jobConfigurationFindMany,
    },
    datasetRunItems: { findMany: mocks.datasetRunItemsFindMany },
    batchAction: { update: mocks.batchActionUpdate },
  },
}));

vi.mock("../../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: mocks.getAdmissionContext,
}));

vi.mock("../../env", () => ({
  env: { LANGFUSE_MAX_HISTORIC_EVAL_CREATION_LIMIT: 10_000 },
}));

vi.mock("../scores/processAnalyticsScoreDelete", () => ({
  processAnalyticsScoreDelete: vi.fn(),
}));
vi.mock("./processAddObservationsToDataset", () => ({
  processAddObservationsToDataset: vi.fn(),
}));
vi.mock("./processAddToQueue", () => ({
  processAddToAnnotationQueue: vi.fn(),
}));
vi.mock("./processDeleteDatasets", () => ({
  processDeleteDatasets: vi.fn(),
}));

import {
  ActionId,
  BatchEvalSourceTable,
  BatchActionStatus,
  EvalTargetObject,
} from "@langfuse/shared";
import { EvalTemplateType } from "@prisma/client";

import { handleBatchActionJob } from "./handleBatchActionJob";

const admissionContext = {
  runtimeLeaseId: "worker-runtime",
  backend: "doris" as const,
  deploymentGeneration: 7n,
};

describe("Doris batch action evaluation dispatch", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.getAdmissionContext.mockReturnValue(admissionContext);
    mocks.createHistoricalDispatches.mockImplementation(
      ({ targets }: { targets: readonly unknown[] }) =>
        Promise.resolve({ created: targets.length, missing: 0 }),
    );
    mocks.batchActionUpdate.mockResolvedValue({});
  });

  it("scans matching Doris traces and persists selected historical evaluator dispatches", async () => {
    mocks.jobConfigurationFindFirst.mockResolvedValue({
      id: "config-1",
      evalTemplate: { type: EvalTemplateType.LLM_AS_JUDGE },
    });
    mocks.buildTraceQuery.mockResolvedValue({
      impossible: false,
      range: {
        from: new Date("2026-01-01T00:00:00.000Z"),
        to: new Date("2026-07-24T00:00:00.000Z"),
      },
      filters: [],
    });
    mocks.traceScan.mockImplementation(async function* () {
      yield {
        id: "trace-1",
        timestamp: new Date("2026-01-02T00:00:00.000Z"),
        environment: "production",
      };
      yield {
        id: "trace-2",
        timestamp: new Date("2026-01-03T00:00:00.000Z"),
        environment: "staging",
      };
    });

    await handleBatchActionJob({
      id: "batch-job-1",
      name: "batch-action-processing-job",
      timestamp: new Date("2026-07-24T00:00:00.000Z"),
      payload: {
        projectId: "project-1",
        actionId: "eval-create",
        configId: "config-1",
        cutoffCreatedAt: new Date("2026-07-24T00:00:00.000Z"),
        targetObject: EvalTargetObject.TRACE,
        query: {
          filter: [],
          orderBy: { column: "timestamp", order: "DESC" },
        },
      },
    } as never);

    expect(mocks.traceScan).toHaveBeenCalledWith(
      expect.objectContaining({ projectId: "project-1", limit: 10_000 }),
    );
    expect(mocks.createHistoricalDispatches).toHaveBeenCalledWith({
      admissionContext,
      projectId: "project-1",
      targets: [
        expect.objectContaining({
          requestId: "batch-job-1:config-1",
          jobConfigurationId: "config-1",
          targetId: "trace-1",
          traceId: "trace-1",
        }),
        expect.objectContaining({
          requestId: "batch-job-1:config-1",
          targetId: "trace-2",
          traceId: "trace-2",
        }),
      ],
    });
  });

  it("creates one durable historical dispatch per selected observation evaluator", async () => {
    mocks.jobConfigurationFindMany.mockResolvedValue([
      { id: "config-a" },
      { id: "config-b" },
    ]);
    mocks.buildObservationQuery.mockReturnValue({
      range: {
        from: new Date("2026-07-01T00:00:00.000Z"),
        to: new Date("2026-07-24T00:00:00.000Z"),
      },
      filters: [],
    });
    mocks.observationScan.mockResolvedValue({
      items: [
        {
          id: "span-1",
          traceId: "trace-1",
          startTime: new Date("2026-07-02T00:00:00.000Z"),
          environment: "production",
        },
      ],
      nextCursor: null,
    });

    await handleBatchActionJob({
      id: "batch-job-2",
      name: "batch-action-processing-job",
      timestamp: new Date("2026-07-24T00:00:00.000Z"),
      payload: {
        actionId: ActionId.ObservationBatchEvaluation,
        batchActionId: "batch-action-2",
        projectId: "project-1",
        cutoffCreatedAt: new Date("2026-07-24T00:00:00.000Z"),
        query: {
          filter: [],
          orderBy: { column: "startTime", order: "DESC" },
        },
        evaluatorIds: ["config-a", "config-b"],
      },
    } as never);

    expect(mocks.createHistoricalDispatches).toHaveBeenCalledWith({
      admissionContext,
      projectId: "project-1",
      targets: [
        expect.objectContaining({
          requestId: "batch-job-2:config-a",
          jobConfigurationId: "config-a",
          targetId: "span-1",
          observationId: "span-1",
        }),
        expect.objectContaining({
          requestId: "batch-job-2:config-b",
          jobConfigurationId: "config-b",
          targetId: "span-1",
          observationId: "span-1",
        }),
      ],
    });
    expect(mocks.jobConfigurationFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({ targetObject: "EVENT" }),
      }),
    );
    expect(mocks.batchActionUpdate).toHaveBeenLastCalledWith({
      where: { id: "batch-action-2", projectId: "project-1" },
      data: expect.objectContaining({
        status: BatchActionStatus.Completed,
        totalCount: 1,
        processedCount: 1,
        failedCount: 0,
      }),
    });
  });

  it("dispatches selected experiment evaluators from Doris root observations", async () => {
    mocks.jobConfigurationFindMany.mockResolvedValue([{ id: "config-exp" }]);
    mocks.buildObservationQuery.mockReturnValue({
      range: {
        from: new Date("2026-07-01T00:00:00.000Z"),
        to: new Date("2026-07-24T00:00:00.000Z"),
      },
      filters: [
        {
          type: "boolean",
          column: "isExperimentItemRootSpan",
          operator: "=",
          value: true,
        },
      ],
    });
    mocks.observationScan.mockResolvedValue({
      items: [
        {
          id: "experiment-root-1",
          traceId: "trace-1",
          startTime: new Date("2026-07-02T00:00:00.000Z"),
          environment: "production",
        },
      ],
      nextCursor: null,
    });

    await handleBatchActionJob({
      id: "batch-job-experiment",
      name: "batch-action-processing-job",
      timestamp: new Date("2026-07-24T00:00:00.000Z"),
      payload: {
        actionId: ActionId.ObservationBatchEvaluation,
        batchActionId: "batch-action-experiment",
        projectId: "project-1",
        cutoffCreatedAt: new Date("2026-07-24T00:00:00.000Z"),
        query: {
          filter: [
            {
              type: "boolean",
              column: "isExperimentItemRootSpan",
              operator: "=",
              value: true,
            },
          ],
          orderBy: { column: "startTime", order: "DESC" },
        },
        evaluatorIds: ["config-exp"],
        sourceTable: BatchEvalSourceTable.EXPERIMENTS,
      },
    } as never);

    expect(mocks.jobConfigurationFindMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: expect.objectContaining({ targetObject: "EXPERIMENT" }),
      }),
    );
    expect(mocks.createHistoricalDispatches).toHaveBeenCalledWith({
      admissionContext,
      projectId: "project-1",
      targets: [
        expect.objectContaining({
          requestId: "batch-job-experiment:config-exp",
          jobConfigurationId: "config-exp",
          targetId: "experiment-root-1",
          observationId: "experiment-root-1",
        }),
      ],
    });
  });

  it("fails before durable writes without an admitted worker runtime", async () => {
    mocks.getAdmissionContext.mockReturnValue(null);
    mocks.jobConfigurationFindFirst.mockResolvedValue({
      id: "config-1",
      evalTemplate: { type: EvalTemplateType.LLM_AS_JUDGE },
    });
    mocks.buildTraceQuery.mockResolvedValue({
      impossible: false,
      range: {
        from: new Date("2026-01-01T00:00:00.000Z"),
        to: new Date("2026-07-24T00:00:00.000Z"),
      },
      filters: [],
    });
    mocks.traceScan.mockImplementation(async function* () {
      yield {
        id: "trace-1",
        timestamp: new Date("2026-01-02T00:00:00.000Z"),
        environment: "production",
      };
    });

    await expect(
      handleBatchActionJob({
        id: "batch-job-3",
        name: "batch-action-processing-job",
        timestamp: new Date("2026-07-24T00:00:00.000Z"),
        payload: {
          projectId: "project-1",
          actionId: "eval-create",
          configId: "config-1",
          cutoffCreatedAt: new Date("2026-07-24T00:00:00.000Z"),
          targetObject: EvalTargetObject.TRACE,
          query: {
            filter: [],
            orderBy: { column: "timestamp", order: "DESC" },
          },
        },
      } as never),
    ).rejects.toThrow("runtime is not admitted");
    expect(mocks.createHistoricalDispatches).not.toHaveBeenCalled();
  });
});
