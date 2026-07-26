import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  backend: "doris" as "clickhouse" | "doris",
}));
const mocks = vi.hoisted(() => ({
  findMany: vi.fn(),
  processBatch: vi.fn(),
  shouldSkip: vi.fn(),
  updateMany: vi.fn(),
  fence: vi.fn(async ({ run }) => run()),
  clickhouseDelete: vi.fn(),
  postgresDelete: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    pendingDeletion: {
      findMany: mocks.findMany,
      updateMany: mocks.updateMany,
    },
  },
}));
vi.mock("@langfuse/shared/src/server", () => ({
  getCurrentSpan: vi.fn(),
  isDorisAnalyticsBackend: vi.fn(() => state.backend === "doris"),
  logger: { debug: vi.fn(), error: vi.fn() },
  shouldSkipDeletionFor: mocks.shouldSkip,
}));
vi.mock("../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(() => ({
    runtimeLeaseId: "runtime-current",
    backend: state.backend,
    deploymentGeneration: 7n,
  })),
}));
vi.mock("../env", () => ({
  env: { LANGFUSE_DELETE_BATCH_SIZE: 2 },
}));
vi.mock("../features/analytics-deletion/analyticsDeletionWorkFence", () => ({
  withAnalyticsDeletionWorkFence: mocks.fence,
}));
vi.mock("../features/traces/processAnalyticsTraceDeletionBatch", () => ({
  processAnalyticsTraceDeletionBatch: mocks.processBatch,
}));
vi.mock("../features/traces/processClickhouseTraceDelete", () => ({
  processClickhouseTraceDelete: mocks.clickhouseDelete,
}));
vi.mock("../features/traces/processPostgresTraceDelete", () => ({
  processPostgresTraceDelete: mocks.postgresDelete,
}));

import { traceDeleteProcessor } from "./traceDelete";

describe("traceDeleteProcessor managed deletion batching", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    state.backend = "doris";
    mocks.findMany.mockResolvedValue([]);
    mocks.processBatch.mockResolvedValue(undefined);
    mocks.shouldSkip.mockResolvedValue(false);
    mocks.updateMany.mockResolvedValue({ count: 0 });
  });

  it("slices trace IDs and authoritative deletion references in lockstep", async () => {
    const deletionOperations = ["trace-1", "trace-2", "trace-3"].map(
      (traceId, index) => ({
        operationId: `operation-${index + 1}`,
        traceId,
        generation: "1",
      }),
    );

    await traceDeleteProcessor({
      data: {
        payload: {
          projectId: "project-1",
          traceIds: deletionOperations.map(({ traceId }) => traceId),
          deletionOperations,
        },
      },
    } as never);

    expect(mocks.processBatch).toHaveBeenCalledWith({
      projectId: "project-1",
      traceIds: ["trace-1", "trace-2"],
      deletionOperations: deletionOperations.slice(0, 2),
    });
  });

  it("runs an unstamped managed ClickHouse delivery through the legacy processors", async () => {
    state.backend = "clickhouse";

    await traceDeleteProcessor({
      data: {
        id: "trace-job-1",
        payload: { projectId: "project-1", traceIds: ["trace-1"] },
      },
    } as never);

    expect(mocks.fence).toHaveBeenCalledWith(
      expect.objectContaining({
        operation: null,
        selectedBackend: "clickhouse",
        serializedProvenance: undefined,
        admissionContext: expect.objectContaining({ backend: "clickhouse" }),
      }),
    );
    expect(mocks.clickhouseDelete).toHaveBeenCalledWith("project-1", [
      "trace-1",
    ]);
    expect(mocks.postgresDelete).toHaveBeenCalledWith("project-1", ["trace-1"]);
    expect(mocks.processBatch).not.toHaveBeenCalled();
  });

  it("rejects a durable trace reference on ClickHouse", async () => {
    state.backend = "clickhouse";

    await expect(
      traceDeleteProcessor({
        data: {
          payload: {
            projectId: "project-1",
            traceIds: ["trace-1"],
            deletionOperations: [
              {
                operationId: "operation-1",
                traceId: "trace-1",
                generation: "1",
              },
            ],
          },
        },
      } as never),
    ).rejects.toThrow("legacy queue contract");
    expect(mocks.fence).not.toHaveBeenCalled();
    expect(mocks.processBatch).not.toHaveBeenCalled();
  });
});
