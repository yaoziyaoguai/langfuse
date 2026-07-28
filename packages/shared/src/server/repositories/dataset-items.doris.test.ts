import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  queryClickhouse: vi.fn(),
  versionTimestamps: vi.fn(),
}));

vi.mock("../../env", () => ({
  env: {
    LANGFUSE_DATASET_SERVICE_READ_FROM_VERSIONED_IMPLEMENTATION: "true",
    LANGFUSE_DATASET_SERVICE_WRITE_TO_VERSIONED_IMPLEMENTATION: "true",
  },
}));

vi.mock("./clickhouse", () => ({
  parseClickhouseUTCDateTimeFormat: (value: string) => new Date(`${value}Z`),
  queryClickhouse: mocks.queryClickhouse,
}));

vi.mock("./telemetry/doris", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => ({
    datasetRunItems: {
      versionTimestamps: mocks.versionTimestamps,
    },
  }),
}));

import { getDatasetVersionForRun } from "./dataset-items";

describe("dataset item repository Doris routing", () => {
  beforeEach(() => vi.clearAllMocks());

  it("resolves a versioned experiment run without querying ClickHouse", async () => {
    const datasetItemVersion = new Date("2026-07-23T08:00:00.000Z");
    mocks.versionTimestamps.mockResolvedValue({
      maxCreatedAt: new Date("2026-07-24T10:00:00.000Z"),
      maxDatasetItemVersion: datasetItemVersion,
    });

    await expect(
      getDatasetVersionForRun({
        projectId: "project-1",
        datasetId: "dataset-1",
        runId: "run-1",
      }),
    ).resolves.toEqual(datasetItemVersion);

    expect(mocks.versionTimestamps).toHaveBeenCalledWith({
      projectId: "project-1",
      datasetId: "dataset-1",
      datasetRunId: "run-1",
    });
    expect(mocks.queryClickhouse).not.toHaveBeenCalled();
  });
});
