import { beforeEach, describe, expect, it, vi } from "vitest";

const { executeDoris } = vi.hoisted(() => ({ executeDoris: vi.fn() }));

vi.mock("./DorisAnalyticsQueryEngine", () => ({
  executeDorisAnalyticsQuery: executeDoris,
}));

import type { DorisQueryExecutor } from "../../../../../server/doris/client";
import type { QueryType } from "../../../types";
import { createDorisAnalyticsQueryEngine } from "./DorisAnalyticsQueryAdapter";

const executor: DorisQueryExecutor = { query: vi.fn() };
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

describe("DorisAnalyticsQueryAdapter", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("exposes Doris batch results through the shared stream contract", async () => {
    executeDoris.mockResolvedValueOnce([{ count_count: 2 }]);

    await expect(
      collect(
        createDorisAnalyticsQueryEngine(executor).stream({
          projectId: "project-1",
          query,
          version: "v2",
        }),
      ),
    ).resolves.toEqual([{ type: "row", row: { count_count: 2 } }]);
    expect(executeDoris).toHaveBeenCalledWith({
      executor,
      projectId: "project-1",
      query,
      version: "v2",
    });
  });

  it("does not start a Doris query after cancellation", async () => {
    const controller = new AbortController();
    controller.abort();

    await expect(
      collect(
        createDorisAnalyticsQueryEngine(executor).stream({
          projectId: "project-1",
          query,
          version: "v2",
          signal: controller.signal,
        }),
      ),
    ).resolves.toEqual([]);
    expect(executeDoris).not.toHaveBeenCalled();
  });

  it("propagates cancellation to an in-flight Doris query", async () => {
    const signal = new AbortController().signal;
    executeDoris.mockResolvedValueOnce([]);

    await collect(
      createDorisAnalyticsQueryEngine(executor).stream({
        projectId: "project-1",
        query,
        version: "v2",
        signal,
      }),
    );

    expect(executeDoris).toHaveBeenCalledWith({
      executor,
      projectId: "project-1",
      query,
      version: "v2",
      signal,
    });
  });
});
