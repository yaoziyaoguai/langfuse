import { beforeEach, describe, expect, it, vi } from "vitest";

const { build, executeDoris, queryClickhouse } = vi.hoisted(() => ({
  build: vi.fn(),
  executeDoris: vi.fn(),
  queryClickhouse: vi.fn(),
}));

vi.mock("../../../server/repositories/clickhouse", () => ({ queryClickhouse }));
vi.mock("../../../server/clickhouse/measureAndReturn", () => ({
  measureAndReturn: vi.fn(),
}));
vi.mock("../../../env", () => ({
  env: {
    LANGFUSE_ENABLE_SINGLE_LEVEL_QUERY_OPTIMIZATION: "false",
    CLICKHOUSE_USE_QUERY_CONDITION_CACHE: "false",
    CLICKHOUSE_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY: 32_000_000_000,
  },
}));
vi.mock("../../../server/repositories/telemetry/doris/runtime", () => ({
  getDorisQueryExecutor: vi.fn(),
  isDorisAnalyticsBackend: () => false,
}));
vi.mock("./adapters/doris/DorisAnalyticsQueryEngine", () => ({
  executeDorisAnalyticsQuery: executeDoris,
}));
vi.mock("./queryBuilder", () => ({
  QueryBuilder: class {
    build = build;
  },
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

describe("executeQuery ClickHouse composition", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    build.mockResolvedValue({
      query: "SELECT sum(total_cost) FROM observations",
      parameters: { projectId: "project-1" },
    });
    queryClickhouse.mockResolvedValue([{ sum_totalCost: 1.5 }]);
  });

  it("routes through ClickHouse without creating a Doris executor", async () => {
    await expect(executeQuery("project-1", query, "v2", true)).resolves.toEqual(
      [{ sum_totalCost: 1.5 }],
    );

    expect(queryClickhouse).toHaveBeenCalledOnce();
    expect(executeDoris).not.toHaveBeenCalled();
  });
});
