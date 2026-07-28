import { beforeEach, describe, expect, it, vi } from "vitest";
import type { FilterCondition } from "@langfuse/shared";

const mocks = vi.hoisted(() => ({
  applyCommentFilters: vi.fn(),
  buildDorisObservationReadQuery: vi.fn(),
  getObservationsWithModelDataFromEventsTable: vi.fn(),
  observationScan: vi.fn(),
  processAddObservationsToDataset: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    batchAction: { update: vi.fn().mockResolvedValue(undefined) },
    jobConfiguration: {
      findMany: vi.fn().mockResolvedValue([{ id: "evaluator-1" }]),
    },
  },
}));

vi.mock("../env", () => ({
  env: {
    LANGFUSE_MAX_HISTORIC_EVAL_CREATION_LIMIT: 50_000,
  },
}));

vi.mock("../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", () => ({
  applyCommentFilters: mocks.applyCommentFilters,
  buildDorisObservationReadQuery: mocks.buildDorisObservationReadQuery,
  buildDorisTraceReadQuery: vi.fn(),
  createHistoricalAnalyticsEvaluationDispatches: vi.fn(),
  findDatasetIdsForBatchDeletion: vi.fn(),
  getDorisTelemetryRepositories: vi.fn(() => ({
    observations: { scan: mocks.observationScan },
  })),
  getObservationsWithModelDataFromEventsTable:
    mocks.getObservationsWithModelDataFromEventsTable,
  getScoresUiTableFromEvents: vi.fn(),
  getSessionsTable: vi.fn(),
  getTraceIdentifiers: vi.fn(),
  logger: { info: vi.fn() },
  QueueName: { BatchActionQueue: "batch-action-queue" },
}));

vi.mock("../features/batchAction/processAddObservationsToDataset", () => ({
  processAddObservationsToDataset: mocks.processAddObservationsToDataset,
}));

vi.mock("../features/batchAction/processAddToQueue", () => ({
  processAddToAnnotationQueue: vi.fn(),
}));

vi.mock("../features/batchAction/processDeleteDatasets", () => ({
  processDeleteDatasets: vi.fn(),
}));

vi.mock("../features/scores/processAnalyticsScoreDelete", () => ({
  processAnalyticsScoreDelete: vi.fn(),
}));

import { handleBatchActionJob } from "../features/batchAction/handleBatchActionJob";

const rawCommentFilter: FilterCondition = {
  type: "string",
  column: "commentContent",
  operator: "contains",
  value: "review me",
};

const resolvedFilter: FilterCondition[] = [
  {
    type: "stringOptions",
    column: "id",
    operator: "any of",
    value: ["observation-1"],
  },
];

const basePayload = {
  projectId: "project-1",
  cutoffCreatedAt: new Date("2026-07-28T00:00:00.000Z"),
  query: {
    filter: [rawCommentFilter],
    orderBy: null,
  },
  batchActionId: "batch-action-1",
};

describe("Doris batch-action comment filter wiring", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.applyCommentFilters.mockResolvedValue({
      filterState: resolvedFilter,
      hasNoMatches: false,
      matchingIds: ["observation-1"],
    });
    mocks.getObservationsWithModelDataFromEventsTable.mockResolvedValue([]);
    mocks.buildDorisObservationReadQuery.mockReturnValue({
      range: {
        from: new Date("2026-07-27T00:00:00.000Z"),
        to: new Date("2026-07-28T00:00:00.000Z"),
      },
      filters: resolvedFilter,
      impossible: false,
    });
    mocks.observationScan.mockResolvedValue({
      items: [],
      nextCursor: null,
    });
  });

  it("resolves observation comments before querying Doris annotation targets", async () => {
    await handleBatchActionJob({
      payload: {
        ...basePayload,
        actionId: "observation-add-to-annotation-queue",
        targetId: "queue-1",
      },
    } as never);

    expect(mocks.applyCommentFilters).toHaveBeenCalledWith({
      filterState: [
        rawCommentFilter,
        expect.objectContaining({
          type: "datetime",
          column: "startTime",
          operator: "<",
        }),
      ],
      objectType: "OBSERVATION",
      prisma: expect.anything(),
      projectId: "project-1",
    });
    expect(
      mocks.getObservationsWithModelDataFromEventsTable,
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        filter: resolvedFilter,
        projectId: "project-1",
      }),
    );
  });

  it("encodes an empty Doris selection when no comments match", async () => {
    mocks.applyCommentFilters.mockResolvedValue({
      filterState: [],
      hasNoMatches: true,
      matchingIds: [],
    });

    await handleBatchActionJob({
      payload: {
        ...basePayload,
        actionId: "observation-add-to-dataset",
        config: {
          datasetId: "dataset-1",
          datasetName: "Dataset",
          mapping: {
            input: { mode: "full" },
            expectedOutput: { mode: "full" },
            metadata: { mode: "none" },
          },
        },
      },
    } as never);

    expect(
      mocks.getObservationsWithModelDataFromEventsTable,
    ).toHaveBeenCalledWith(
      expect.objectContaining({
        filter: [
          {
            type: "stringOptions",
            column: "id",
            operator: "any of",
            value: [],
          },
        ],
      }),
    );
  });

  it("resolves observation comments before building a Doris evaluation scan", async () => {
    await handleBatchActionJob({
      payload: {
        ...basePayload,
        actionId: "observation-run-batched-evaluation",
        evaluatorIds: ["evaluator-1"],
        sourceTable: "events",
      },
    } as never);

    expect(mocks.buildDorisObservationReadQuery).toHaveBeenCalledWith(
      resolvedFilter,
    );
    expect(mocks.observationScan).toHaveBeenCalledWith(
      expect.objectContaining({
        filters: resolvedFilter,
        projectId: "project-1",
      }),
    );
  });
});
