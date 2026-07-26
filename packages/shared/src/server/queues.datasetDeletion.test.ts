import { describe, expect, it } from "vitest";

import { DatasetQueueEventSchema } from "./queues";
import { analyticsDatasetDeletionReferenceMatches } from "./repositories/analyticsDatasetDeletionOperations";

describe("DatasetQueueEventSchema", () => {
  it("accepts a complete durable dataset deletion reference", () => {
    expect(
      DatasetQueueEventSchema.parse({
        deletionType: "dataset",
        projectId: "project-1",
        datasetId: "dataset-1",
        analyticsDeletion: {
          operationId: "operation-1",
          datasetGeneration: "2",
          runGenerations: { "run-1": "3" },
        },
      }),
    ).toMatchObject({
      analyticsDeletion: {
        datasetGeneration: "2",
        runGenerations: { "run-1": "3" },
      },
    });
  });

  it("rejects malformed generation references", () => {
    expect(
      DatasetQueueEventSchema.safeParse({
        deletionType: "dataset-runs",
        projectId: "project-1",
        datasetId: "dataset-1",
        datasetRunIds: ["run-1"],
        analyticsDeletion: {
          operationId: "operation-1",
          datasetGeneration: null,
          runGenerations: { "run-1": "0" },
        },
      }).success,
    ).toBe(false);
  });
});

describe("analyticsDatasetDeletionReferenceMatches", () => {
  const operation = {
    id: "operation-1",
    datasetGeneration: null,
    runGenerations: { "run-1": "2", "run-2": "3" },
  };

  it("matches generation maps independently of serialized key order", () => {
    expect(
      analyticsDatasetDeletionReferenceMatches({
        operation,
        reference: {
          operationId: "operation-1",
          datasetGeneration: null,
          runGenerations: { "run-2": "3", "run-1": "2" },
        },
      }),
    ).toBe(true);
  });

  it("rejects a tampered dataset or run generation", () => {
    expect(
      analyticsDatasetDeletionReferenceMatches({
        operation,
        reference: {
          operationId: "operation-1",
          datasetGeneration: "1",
          runGenerations: { "run-1": "2", "run-2": "4" },
        },
      }),
    ).toBe(false);
  });
});
