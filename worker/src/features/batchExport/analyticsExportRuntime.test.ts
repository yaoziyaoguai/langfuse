import { describe, expect, it, vi } from "vitest";

vi.mock("../../env", () => ({
  env: { LANGFUSE_ANALYTICS_BACKEND: "clickhouse" },
}));
vi.mock("./analyticsExportSourceFactories", () => ({
  createClickHouseAnalyticsExportSource: vi.fn(),
  createDorisAnalyticsExportSource: vi.fn(),
}));

import type { AnalyticsExportSource } from "./AnalyticsExportSource";
import { createAnalyticsExportSource } from "./analyticsExportRuntime";

const source = (): AnalyticsExportSource => ({ open: vi.fn() });

describe("createAnalyticsExportSource", () => {
  it.each(["clickhouse", "doris"] as const)(
    "creates only the selected %s adapter",
    (backend) => {
      const clickhouse = source();
      const doris = source();
      const createClickHouse = vi.fn(() => clickhouse);
      const createDoris = vi.fn(() => doris);

      expect(
        createAnalyticsExportSource({
          backend,
          createClickHouse,
          createDoris,
        }),
      ).toBe(backend === "doris" ? doris : clickhouse);
      expect(createClickHouse).toHaveBeenCalledTimes(
        backend === "clickhouse" ? 1 : 0,
      );
      expect(createDoris).toHaveBeenCalledTimes(backend === "doris" ? 1 : 0);
    },
  );
});
