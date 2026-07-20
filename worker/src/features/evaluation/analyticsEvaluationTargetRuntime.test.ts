import { describe, expect, it, vi } from "vitest";

vi.mock("../../env", () => ({
  env: { LANGFUSE_ANALYTICS_BACKEND: "clickhouse" },
}));
vi.mock("./evaluationTargetSourceFactories", () => ({
  createClickHouseEvaluationTargetSource: vi.fn(),
  createDorisEvaluationTargetSource: vi.fn(),
}));

import type { AnalyticsEvaluationTargetSource } from "./AnalyticsEvaluationTargetSource";
import { createAnalyticsEvaluationTargetSource } from "./analyticsEvaluationTargetRuntime";

const source = (): AnalyticsEvaluationTargetSource => ({
  getTrace: vi.fn(),
  getObservationsByName: vi.fn(),
});

describe("createAnalyticsEvaluationTargetSource", () => {
  it.each(["clickhouse", "doris"] as const)(
    "creates only the selected %s adapter",
    (backend) => {
      const clickhouse = source();
      const doris = source();
      const createClickHouse = vi.fn(() => clickhouse);
      const createDoris = vi.fn(() => doris);

      expect(
        createAnalyticsEvaluationTargetSource({
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
