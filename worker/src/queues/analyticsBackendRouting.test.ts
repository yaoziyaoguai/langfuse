import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ backend: "clickhouse" }));
const mocks = vi.hoisted(() => ({
  clickhouseBatchAction: vi.fn(),
  dorisBatchAction: vi.fn(),
  clickhouseScoreDelete: vi.fn(),
  dorisScoreDelete: vi.fn(),
  clickhouseDatasetDelete: vi.fn(),
  dorisDatasetDelete: vi.fn(),
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

  it.each(["clickhouse", "doris"] as const)(
    "routes mutations only to %s",
    async (backend) => {
      state.backend = backend;

      await batchActionQueueProcessor({
        id: "batch-job",
        data: { payload: { actionId: "score-delete" } },
      } as never);
      await scoreDeleteProcessor({
        data: {
          payload: { projectId: "project-1", scoreIds: ["score-1"] },
        },
      } as never);
      await datasetDeleteProcessor({
        data: {
          payload: {
            projectId: "project-1",
            datasetId: "dataset-1",
            deletionType: "dataset",
          },
        },
      } as never);

      const selected =
        backend === "clickhouse"
          ? [
              mocks.clickhouseBatchAction,
              mocks.clickhouseScoreDelete,
              mocks.clickhouseDatasetDelete,
            ]
          : [
              mocks.dorisBatchAction,
              mocks.dorisScoreDelete,
              mocks.dorisDatasetDelete,
            ];
      const unselected =
        backend === "clickhouse"
          ? [
              mocks.dorisBatchAction,
              mocks.dorisScoreDelete,
              mocks.dorisDatasetDelete,
            ]
          : [
              mocks.clickhouseBatchAction,
              mocks.clickhouseScoreDelete,
              mocks.clickhouseDatasetDelete,
            ];

      for (const handler of selected) expect(handler).toHaveBeenCalledOnce();
      for (const handler of unselected) expect(handler).not.toHaveBeenCalled();
    },
  );
});
