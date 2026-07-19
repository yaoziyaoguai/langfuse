import { beforeEach, describe, expect, it, vi } from "vitest";

const { executeDoris, queryClickhouse, executor } = vi.hoisted(() => ({
  executeDoris: vi.fn(),
  queryClickhouse: vi.fn(),
  executor: { query: vi.fn() },
}));

vi.mock("../../../server/repositories/clickhouse", () => ({ queryClickhouse }));

vi.mock("../../../env", () => ({ env: {} }));

vi.mock("../../../server/repositories/telemetry/doris/runtime", () => ({
  getDorisQueryExecutor: () => executor,
  isDorisAnalyticsBackend: () => true,
}));

vi.mock("./adapters/doris/DorisAnalyticsQueryEngine", () => ({
  executeDorisAnalyticsQuery: executeDoris,
}));

import type { QueryType } from "../types";
import { executeQuery } from "./queryExecutor";

const query: QueryType = {
  view: "observations",
  dimensions: [{ field: "providedModelName" }],
  metrics: [{ measure: "totalCost", aggregation: "sum" }],
  filters: [],
  timeDimension: { granularity: "day" },
  fromTimestamp: "2026-07-17T00:00:00.000Z",
  toTimestamp: "2026-07-18T00:00:00.000Z",
  orderBy: null,
};

describe("executeQuery Doris composition", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    executeDoris.mockResolvedValue([{ sum_totalCost: 1.5 }]);
  });

  it("routes the shared dashboard/API/MCP engine without compiling ClickHouse", async () => {
    await expect(executeQuery("project-1", query, "v2", true)).resolves.toEqual(
      [{ sum_totalCost: 1.5 }],
    );

    expect(executeDoris).toHaveBeenCalledWith({
      executor,
      projectId: "project-1",
      query,
      version: "v2",
    });
    expect(queryClickhouse).not.toHaveBeenCalled();
  });
});
