import type * as SharedServer from "@langfuse/shared/src/server";

const { executeQuery, legacyCount, legacyGenerate } = vi.hoisted(() => ({
  executeQuery: vi.fn(),
  legacyCount: vi.fn(),
  legacyGenerate: vi.fn(),
}));

vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const original = await importOriginal<typeof SharedServer>();
  return {
    ...original,
    convertApiProvidedFilterToClickhouseFilter: vi.fn(),
    generateDailyMetrics: legacyGenerate,
    getDailyMetricsCount: legacyCount,
    isDorisAnalyticsBackend: () => true,
  };
});

vi.mock("@langfuse/shared/query/server", () => ({ executeQuery }));

import {
  generateDailyMetrics,
  getDailyMetricsCount,
} from "@/src/features/public-api/server/dailyMetrics";

describe("daily metrics Doris routing", () => {
  it("builds the legacy daily response from the shared Doris query engine", async () => {
    executeQuery.mockImplementation(
      (_projectId, query: { view: "traces" | "observations" }) =>
        query.view === "traces"
          ? Promise.resolve([
              {
                time_dimension: "2026-07-17T00:00:00.000Z",
                count_count: 3,
              },
            ])
          : Promise.resolve([
              {
                time_dimension: "2026-07-17T00:00:00.000Z",
                providedModelName: "model-a",
                count_count: 2,
                uniq_traceId: 2,
                sum_inputTokens: 20,
                sum_outputTokens: 10,
                sum_totalTokens: 30,
                sum_totalCost: 0.25,
              },
              {
                time_dimension: "2026-07-17T00:00:00.000Z",
                providedModelName: null,
                count_count: 1,
                uniq_traceId: 1,
                sum_inputTokens: 3,
                sum_outputTokens: 2,
                sum_totalTokens: 5,
                sum_totalCost: 0,
              },
            ]),
    );
    const props = {
      projectId: "project-1",
      page: 1,
      limit: 10,
      traceName: "checkout",
      tags: ["prod"],
      traceEnvironment: "production",
      observationEnvironment: "production",
      fromTimestamp: "2026-07-17T00:00:00.000Z",
      toTimestamp: "2026-07-18T00:00:00.000Z",
    };

    await expect(generateDailyMetrics(props)).resolves.toEqual([
      {
        date: "2026-07-17",
        countTraces: 3,
        countObservations: 3,
        totalCost: 0.25,
        usage: [
          {
            model: "model-a",
            inputUsage: 20,
            outputUsage: 10,
            totalUsage: 30,
            countObservations: 2,
            countTraces: 2,
            totalCost: 0.25,
          },
          {
            model: null,
            inputUsage: 3,
            outputUsage: 2,
            totalUsage: 5,
            countObservations: 1,
            countTraces: 1,
            totalCost: 0,
          },
        ],
      },
    ]);
    await expect(getDailyMetricsCount(props)).resolves.toBe(1);

    expect(executeQuery).toHaveBeenCalledTimes(2);
    expect(executeQuery.mock.calls[0]?.[1]).toEqual(
      expect.objectContaining({
        view: "traces",
        filters: expect.arrayContaining([
          expect.objectContaining({ column: "traceName", value: "checkout" }),
          expect.objectContaining({ column: "tags", value: ["prod"] }),
        ]),
      }),
    );
    expect(legacyGenerate).not.toHaveBeenCalled();
    expect(legacyCount).not.toHaveBeenCalled();
  });
});
