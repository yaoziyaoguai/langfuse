import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  markPublished: vi.fn(),
}));

vi.mock("../redis/datasetDelete", () => ({
  DatasetDeleteQueue: {
    getInstance: vi.fn(() => ({ add: mocks.add })),
  },
}));
vi.mock("../redis/redis", () => ({ redis: {} }));
vi.mock("../repositories/analyticsDatasetDeletionOperations", () => ({
  markAnalyticsDatasetDeletionOutboxPublished: mocks.markPublished,
}));

import { addToDeleteDatasetQueue } from "./addToDeleteQueue";

describe("addToDeleteDatasetQueue", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.add.mockResolvedValue({});
    mocks.markPublished.mockResolvedValue(true);
  });

  it("uses the durable operation ID as the BullMQ deduplication key", async () => {
    await expect(
      addToDeleteDatasetQueue({
        deletionType: "dataset-runs",
        projectId: "project-1",
        datasetId: "dataset-1",
        datasetRunIds: ["run-1"],
        analyticsDeletion: {
          operationId: "operation-1",
          datasetGeneration: null,
          runGenerations: { "run-1": "2" },
        },
      }),
    ).resolves.toBe(true);

    expect(mocks.add).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        payload: expect.objectContaining({
          analyticsDeletion: expect.objectContaining({
            operationId: "operation-1",
          }),
        }),
      }),
      { jobId: "operation-1" },
    );
    expect(mocks.markPublished).toHaveBeenCalledWith({
      operationId: "operation-1",
    });
  });

  it("does not mark the outbox published when enqueue fails", async () => {
    mocks.add.mockRejectedValueOnce(new Error("redis unavailable"));

    await expect(
      addToDeleteDatasetQueue({
        deletionType: "dataset",
        projectId: "project-1",
        datasetId: "dataset-1",
        analyticsDeletion: {
          operationId: "operation-1",
          datasetGeneration: "3",
          runGenerations: {},
        },
      }),
    ).rejects.toThrow("redis unavailable");
    expect(mocks.markPublished).not.toHaveBeenCalled();
  });
});
