import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  referenceMatches: vi.fn(),
  findOperation: vi.fn(),
  claimOperation: vi.fn(),
  markBarrierVisible: vi.fn(),
  completeOperation: vi.fn(),
  clickhouseDelete: vi.fn(),
  dorisDelete: vi.fn(),
}));

vi.mock("../env", () => ({
  env: { LANGFUSE_ANALYTICS_BACKEND: "clickhouse" },
}));
vi.mock("@langfuse/shared/analytics-backend", () => ({
  isAnalyticsBackend: vi.fn(() => false),
}));
vi.mock("@langfuse/shared/src/server", () => ({
  analyticsDatasetDeletionReferenceMatches: mocks.referenceMatches,
  claimAnalyticsDatasetDeletionOperation: mocks.claimOperation,
  completeAnalyticsDatasetDeletionOperation: mocks.completeOperation,
  findAnalyticsDatasetDeletionOperation: mocks.findOperation,
  markAnalyticsDatasetDeletionBarrierVisible: mocks.markBarrierVisible,
}));
vi.mock("../features/datasets/processClickhouseDatasetDelete", () => ({
  processClickhouseDatasetDelete: mocks.clickhouseDelete,
}));
vi.mock("../features/datasets/processDatasetDelete", () => ({
  processDatasetDelete: mocks.dorisDelete,
}));

import { datasetDeleteProcessor } from "./datasetDelete";

const operation = {
  id: "operation-1",
  scope: "DATASET_RUNS" as const,
  projectId: "project-1",
  datasetId: "dataset-1",
  datasetRunIds: ["run-1"],
  datasetGeneration: null,
  runGenerations: { "run-1": "2" },
  status: "SCHEDULED" as const,
  phase: "visibility_barrier",
  workerFence: 0n,
  leaseOwner: null,
  leaseExpiresAt: null,
  attempts: 0,
  lastErrorCode: null,
  logicallyInvisible: false,
  completedAt: null,
  createdAt: new Date("2026-07-23T09:00:00.000Z"),
  updatedAt: new Date("2026-07-23T09:00:00.000Z"),
};

const payload = {
  deletionType: "dataset-runs" as const,
  projectId: "project-1",
  datasetId: "dataset-1",
  datasetRunIds: ["run-1"],
  analyticsDeletion: {
    operationId: "operation-1",
    datasetGeneration: null,
    runGenerations: { "run-1": "2" },
  },
};

describe("datasetDeleteProcessor ClickHouse durable contract", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.findOperation.mockResolvedValue(operation);
    mocks.referenceMatches.mockReturnValue(true);
    mocks.claimOperation.mockResolvedValue({
      ...operation,
      workerFence: 1n,
      leaseOwner: "clickhouse-worker",
    });
    mocks.markBarrierVisible.mockResolvedValue(true);
    mocks.completeOperation.mockResolvedValue(true);
    mocks.clickhouseDelete.mockResolvedValue(undefined);
  });

  it("rejects tampered generation provenance before physical deletion", async () => {
    mocks.referenceMatches.mockReturnValue(false);

    await expect(
      datasetDeleteProcessor({ data: { payload } } as never),
    ).rejects.toThrow("contract mismatch");
    expect(mocks.clickhouseDelete).not.toHaveBeenCalled();
    expect(mocks.claimOperation).not.toHaveBeenCalled();
  });

  it("completes a valid managed ClickHouse deletion under its fenced lease", async () => {
    await expect(
      datasetDeleteProcessor({ data: { payload } } as never),
    ).resolves.toBeUndefined();
    expect(mocks.clickhouseDelete).toHaveBeenCalledWith(payload);
    expect(mocks.markBarrierVisible).toHaveBeenCalledWith(
      expect.objectContaining({
        operationId: "operation-1",
        lease: expect.objectContaining({ fence: 1n }),
      }),
    );
    expect(mocks.completeOperation).toHaveBeenCalledOnce();
  });
});
