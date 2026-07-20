import { beforeEach, describe, expect, it, vi } from "vitest";

const { build, queryWithProgress } = vi.hoisted(() => ({
  build: vi.fn(),
  queryWithProgress: vi.fn(),
}));

vi.mock("../../../../../env", () => ({
  env: {
    LANGFUSE_ENABLE_SINGLE_LEVEL_QUERY_OPTIMIZATION: "false",
    CLICKHOUSE_USE_QUERY_CONDITION_CACHE: "false",
    CLICKHOUSE_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY: 32_000_000_000,
  },
}));

vi.mock("../../../../../server/clickhouse/measureAndReturn", () => ({
  measureAndReturn: vi.fn(),
}));

vi.mock("../../../../../server/repositories/clickhouse", () => {
  class ClickHouseResourceError extends Error {
    static wrapIfResourceError(error: Error): Error {
      return /memory limit/i.test(error.message)
        ? new ClickHouseResourceError(error.message)
        : error;
    }
  }

  return {
    ClickHouseResourceError,
    isProgressRow: (event: object) => "progress" in event,
    isRow: (event: object) => "row" in event,
    isException: (event: object) => "exception" in event,
    queryClickhouse: vi.fn(),
    queryClickhouseWithProgress: queryWithProgress,
  };
});

vi.mock("../../queryBuilder", () => ({
  QueryBuilder: class {
    build = build;
  },
}));

import type { QueryType } from "../../../types";
import { createClickHouseAnalyticsQueryEngine } from "./ClickHouseAnalyticsQueryEngine";

const query: QueryType = {
  view: "observations",
  dimensions: [],
  metrics: [{ measure: "count", aggregation: "count" }],
  filters: [],
  timeDimension: null,
  fromTimestamp: "2026-07-17T00:00:00.000Z",
  toTimestamp: "2026-07-18T00:00:00.000Z",
  orderBy: null,
};

async function collect<T>(iterable: AsyncIterable<T>): Promise<T[]> {
  const values: T[] = [];
  for await (const value of iterable) values.push(value);
  return values;
}

describe("ClickHouseAnalyticsQueryEngine", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    build.mockResolvedValue({
      query: "SELECT count() AS count_count FROM observations",
      parameters: { projectId: "project-1" },
    });
  });

  it("maps the native progress protocol to backend-neutral events", async () => {
    const signal = new AbortController().signal;
    queryWithProgress.mockImplementationOnce(async function* () {
      yield { progress: { read_rows: "1" } };
      yield { row: { count_count: 1 } };
    });

    await expect(
      collect(
        createClickHouseAnalyticsQueryEngine().stream({
          projectId: "project-1",
          query,
          version: "v2",
          signal,
        }),
      ),
    ).resolves.toEqual([
      { type: "progress", progress: { read_rows: "1" } },
      { type: "row", row: { count_count: 1 } },
    ]);
    expect(queryWithProgress).toHaveBeenCalledWith(
      expect.objectContaining({ abortSignal: signal }),
    );
  });

  it("normalizes ClickHouse resource failures", async () => {
    queryWithProgress.mockImplementationOnce(async function* () {
      yield { exception: "Code: 241. Memory limit exceeded" };
    });

    await expect(
      collect(
        createClickHouseAnalyticsQueryEngine().stream({
          projectId: "project-1",
          query,
          version: "v2",
        }),
      ),
    ).rejects.toMatchObject({
      name: "AnalyticsQueryError",
      code: "RESOURCE_EXHAUSTED",
    });
  });
});
