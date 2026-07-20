import { describe, expect, it, vi } from "vitest";

vi.mock("../../../env", () => ({
  env: {
    LANGFUSE_ANALYTICS_BACKEND: "clickhouse",
    LANGFUSE_ENABLE_SINGLE_LEVEL_QUERY_OPTIMIZATION: "false",
    CLICKHOUSE_USE_QUERY_CONDITION_CACHE: "false",
    CLICKHOUSE_MAX_BYTES_BEFORE_EXTERNAL_GROUP_BY: 32_000_000_000,
  },
}));

import type { AnalyticsQueryEngine } from "./AnalyticsQueryEngine";
import { createAnalyticsQueryEngine } from "./analyticsQueryRuntime";

const engine = (): AnalyticsQueryEngine => ({
  execute: vi.fn(),
  stream: vi.fn(),
});

describe("createAnalyticsQueryEngine", () => {
  it("creates only the selected ClickHouse adapter", () => {
    const clickhouse = engine();
    const createClickHouse = vi.fn(() => clickhouse);
    const createDoris = vi.fn(() => engine());

    expect(
      createAnalyticsQueryEngine({
        backend: "clickhouse",
        createClickHouse,
        createDoris,
      }),
    ).toBe(clickhouse);
    expect(createClickHouse).toHaveBeenCalledOnce();
    expect(createDoris).not.toHaveBeenCalled();
  });

  it("creates only the selected Doris adapter", () => {
    const doris = engine();
    const createClickHouse = vi.fn(() => engine());
    const createDoris = vi.fn(() => doris);

    expect(
      createAnalyticsQueryEngine({
        backend: "doris",
        createClickHouse,
        createDoris,
      }),
    ).toBe(doris);
    expect(createDoris).toHaveBeenCalledOnce();
    expect(createClickHouse).not.toHaveBeenCalled();
  });
});
