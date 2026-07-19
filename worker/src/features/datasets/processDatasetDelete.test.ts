import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  deleteDatasetMediaLinksByDatasetId: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", () => ({
  deleteDatasetMediaLinksByDatasetId: mocks.deleteDatasetMediaLinksByDatasetId,
}));

import { processDatasetDelete } from "./processDatasetDelete";

describe("processDatasetDelete", () => {
  beforeEach(() => vi.clearAllMocks());

  it("removes uncascaded media links when a dataset is deleted", async () => {
    await processDatasetDelete({
      deletionType: "dataset",
      projectId: "project-1",
      datasetId: "dataset-1",
    });

    expect(mocks.deleteDatasetMediaLinksByDatasetId).toHaveBeenCalledWith({
      projectId: "project-1",
      datasetId: "dataset-1",
    });
  });

  it("does not create R1B analytics work when only runs are deleted", async () => {
    await processDatasetDelete({
      deletionType: "dataset-runs",
      projectId: "project-1",
      datasetId: "dataset-1",
      datasetRunIds: ["run-1"],
    });

    expect(mocks.deleteDatasetMediaLinksByDatasetId).not.toHaveBeenCalled();
  });
});
