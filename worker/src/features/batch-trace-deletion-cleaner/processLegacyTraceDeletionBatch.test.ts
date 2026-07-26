import { describe, expect, it, vi } from "vitest";

vi.mock("@langfuse/shared/src/db", () => ({ prisma: {} }));
vi.mock("../../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(),
}));
vi.mock("../analytics-deletion/analyticsDeletionWorkFence", () => ({
  withAnalyticsDeletionWorkFence: vi.fn(),
}));
vi.mock("../traces/processClickhouseTraceDelete", () => ({
  processClickhouseTraceDelete: vi.fn(),
}));
vi.mock("../traces/processPostgresTraceDelete", () => ({
  processPostgresTraceDelete: vi.fn(),
}));

import { processLegacyTraceDeletionBatch } from "./processLegacyTraceDeletionBatch";

describe("processLegacyTraceDeletionBatch", () => {
  it("performs no Postgres or ClickHouse IO when the legacy fence rejects", async () => {
    const processPostgres = vi.fn();
    const processClickhouse = vi.fn();
    const fenceError = new Error("Legacy work fenced by deployment");
    const withFence = vi.fn().mockRejectedValue(fenceError);

    await expect(
      processLegacyTraceDeletionBatch(
        { projectId: "project-1", traceIds: ["trace-1"] },
        { processPostgres, processClickhouse, withFence },
      ),
    ).resolves.toEqual([
      { backend: "admission", errorName: "Error", reason: fenceError },
    ]);

    expect(withFence).toHaveBeenCalledOnce();
    expect(processPostgres).not.toHaveBeenCalled();
    expect(processClickhouse).not.toHaveBeenCalled();
  });

  it("starts both deletion paths only from inside the admitted fence", async () => {
    const processPostgres = vi.fn().mockResolvedValue(undefined);
    const processClickhouse = vi.fn().mockResolvedValue(undefined);
    const withFence = vi.fn(async (run: () => Promise<void>) => {
      expect(processPostgres).not.toHaveBeenCalled();
      expect(processClickhouse).not.toHaveBeenCalled();
      await run();
    });

    await expect(
      processLegacyTraceDeletionBatch(
        { projectId: "project-1", traceIds: ["trace-1"] },
        { processPostgres, processClickhouse, withFence },
      ),
    ).resolves.toEqual([]);

    expect(processPostgres).toHaveBeenCalledWith("project-1", ["trace-1"]);
    expect(processClickhouse).toHaveBeenCalledWith("project-1", ["trace-1"]);
  });

  it("reports every deletion backend failure after admission", async () => {
    const postgresError = new TypeError("Postgres failed");
    const clickhouseError = "ClickHouse failed";
    const processPostgres = vi.fn().mockRejectedValue(postgresError);
    const processClickhouse = vi.fn().mockRejectedValue(clickhouseError);
    const withFence = vi.fn((run: () => Promise<void>) => run());

    await expect(
      processLegacyTraceDeletionBatch(
        { projectId: "project-1", traceIds: ["trace-1"] },
        { processPostgres, processClickhouse, withFence },
      ),
    ).resolves.toEqual([
      { backend: "postgres", errorName: "TypeError", reason: postgresError },
      { backend: "clickhouse", errorName: "string", reason: clickhouseError },
    ]);
  });
});
