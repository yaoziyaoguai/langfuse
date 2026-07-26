import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ backend: "clickhouse" }));
const provenance = {
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "7",
  workloadEpochFingerprint: "a".repeat(64),
  runtimeContractVersion: 3,
  producerRuntimeLeaseId: "runtime-producer",
};
const mocks = vi.hoisted(() => ({
  clickhouseBatchAction: vi.fn(),
  dorisBatchAction: vi.fn(),
  clickhouseScoreDelete: vi.fn(),
  dorisScoreDelete: vi.fn(),
  clickhouseDatasetDelete: vi.fn(),
  dorisDatasetDelete: vi.fn(),
  durableWorkFence: vi.fn(
    async ({ run }: { run: () => Promise<unknown> }) => await run(),
  ),
  getAdmissionContext: vi.fn(() => ({
    runtimeLeaseId: "runtime-current",
    backend: state.backend,
    deploymentGeneration: 7n,
  })),
}));

vi.mock("../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: mocks.getAdmissionContext,
}));
vi.mock("../features/analytics-deletion/analyticsDeletionWorkFence", () => ({
  withAnalyticsDurableWorkFence: mocks.durableWorkFence,
}));

vi.mock("../env", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return state.backend;
    },
  },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  logger: { info: vi.fn(), error: vi.fn() },
  traceException: vi.fn(),
  shouldSkipDeletionFor: vi.fn().mockResolvedValue(false),
}));
vi.mock("../features/batchAction/handleBatchActionJob", () => ({
  handleBatchActionJob: mocks.dorisBatchAction,
}));
vi.mock("../features/batchAction/handleClickhouseBatchActionJob", () => ({
  handleClickhouseBatchActionJob: mocks.clickhouseBatchAction,
}));
vi.mock("../features/scores/processClickhouseScoreDelete", () => ({
  processClickhouseScoreDelete: mocks.clickhouseScoreDelete,
}));
vi.mock("../features/scores/processAnalyticsScoreDelete", () => ({
  processAnalyticsScoreDelete: mocks.dorisScoreDelete,
}));
vi.mock("../features/datasets/processClickhouseDatasetDelete", () => ({
  processClickhouseDatasetDelete: mocks.clickhouseDatasetDelete,
}));
vi.mock("../features/datasets/processDatasetDelete", () => ({
  processDatasetDelete: mocks.dorisDatasetDelete,
}));

import { batchActionQueueProcessor } from "./batchActionQueue";
import { datasetDeleteProcessor } from "./datasetDelete";
import { scoreDeleteProcessor } from "./scoreDelete";

describe("analytics queue backend routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("keeps managed ClickHouse score mutations on the legacy processors", async () => {
    state.backend = "clickhouse";

    await batchActionQueueProcessor({
      id: "batch-job",
      data: {
        id: "batch-job-data",
        payload: { actionId: "score-delete" },
      },
    } as never);
    await scoreDeleteProcessor({
      data: {
        id: "score-job-data",
        payload: { projectId: "project-1", scoreIds: ["score-1"] },
      },
    } as never);

    expect(mocks.clickhouseBatchAction).toHaveBeenCalledOnce();
    expect(mocks.clickhouseScoreDelete).toHaveBeenCalledOnce();
    expect(mocks.dorisBatchAction).not.toHaveBeenCalled();
    expect(mocks.dorisScoreDelete).not.toHaveBeenCalled();
    expect(mocks.durableWorkFence).toHaveBeenCalledTimes(2);
    expect(mocks.durableWorkFence).toHaveBeenCalledWith(
      expect.objectContaining({
        selectedBackend: "clickhouse",
        serializedProvenance: undefined,
        resourceIdentity: "batch-job",
      }),
    );
    expect(mocks.durableWorkFence).toHaveBeenCalledWith(
      expect.objectContaining({
        selectedBackend: "clickhouse",
        serializedProvenance: undefined,
        resourceIdentity: "score-job-data",
      }),
    );
  });

  it("routes managed Doris score mutations behind their durable fence", async () => {
    state.backend = "doris";

    await batchActionQueueProcessor({
      id: "batch-job",
      data: {
        id: "batch-job",
        payload: {
          actionId: "score-delete",
          deletionOperationId: "batch-job",
          deletionGeneration: "7",
          analyticsProvenance: provenance,
        },
      },
    } as never);
    await scoreDeleteProcessor({
      data: {
        id: "score-job-data",
        payload: {
          projectId: "project-1",
          scoreIds: ["score-1"],
          deletionOperationId: "score-job-data",
          deletionGeneration: "7",
          analyticsProvenance: provenance,
        },
      },
    } as never);

    expect(mocks.dorisBatchAction).toHaveBeenCalledOnce();
    expect(mocks.dorisScoreDelete).toHaveBeenCalledWith("project-1", [
      "score-1",
    ]);
    expect(mocks.clickhouseBatchAction).not.toHaveBeenCalled();
    expect(mocks.clickhouseScoreDelete).not.toHaveBeenCalled();
    expect(mocks.durableWorkFence).toHaveBeenCalledTimes(2);
    expect(mocks.durableWorkFence).toHaveBeenCalledWith(
      expect.objectContaining({
        selectedBackend: "doris",
        serializedProvenance: provenance,
        resourceIdentity: "batch-job",
      }),
    );
    expect(mocks.durableWorkFence).toHaveBeenCalledWith(
      expect.objectContaining({
        selectedBackend: "doris",
        serializedProvenance: provenance,
        resourceIdentity: "score-job-data",
      }),
    );
  });

  it("rejects an incomplete Doris score deletion before its durable fence", async () => {
    state.backend = "doris";

    await expect(
      scoreDeleteProcessor({
        data: {
          id: "score-job-data",
          payload: {
            projectId: "project-1",
            scoreIds: ["score-1"],
            deletionOperationId: "score-job-data",
          },
        },
      } as never),
    ).rejects.toThrow("does not match the queue delivery");

    expect(mocks.durableWorkFence).not.toHaveBeenCalled();
    expect(mocks.dorisScoreDelete).not.toHaveBeenCalled();
  });

  it("keeps non-score mutations backend-routed", async () => {
    for (const backend of ["clickhouse", "doris"] as const) {
      state.backend = backend;
      await datasetDeleteProcessor({
        data: {
          payload: {
            projectId: "project-1",
            datasetId: "dataset-1",
            deletionType: "dataset",
          },
        },
      } as never);
    }
    expect(mocks.clickhouseDatasetDelete).toHaveBeenCalledOnce();
    expect(mocks.dorisDatasetDelete).toHaveBeenCalledOnce();
  });
});
