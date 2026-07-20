import { describe, expect, it, vi } from "vitest";

import { ClickHouseEvaluationTargetSource } from "./ClickHouseEvaluationTargetSource";
import { DorisEvaluationTargetSource } from "./DorisEvaluationTargetSource";

describe("evaluation target source adapters", () => {
  it("preserves the ClickHouse evaluator target readers", async () => {
    const trace = { id: "trace-1" };
    const observation = { id: "span-1" };
    const getTrace = vi.fn().mockResolvedValue(trace);
    const getObservationsByName = vi.fn().mockResolvedValue([observation]);
    const source = new ClickHouseEvaluationTargetSource({
      getTrace,
      getObservationsByName,
    });

    await expect(
      source.getTrace({ projectId: "project-1", traceId: "trace-1" }),
    ).resolves.toBe(trace);
    await expect(
      source.getObservationsByName({
        projectId: "project-1",
        traceId: "trace-1",
        name: "generation",
      }),
    ).resolves.toEqual([observation]);
  });

  it("maps Doris trace and named observation targets into evaluator domains", async () => {
    const getTrace = vi.fn().mockResolvedValue({
      id: "trace-1",
      projectId: "project-1",
      timestamp: new Date("2026-07-20T00:00:00.000Z"),
      endTime: new Date("2026-07-20T00:00:01.000Z"),
      name: "trace",
      environment: "production",
      userId: null,
      sessionId: null,
      release: null,
      version: null,
      tags: [],
      inputPreview: "input",
      outputPreview: "output",
      rootObservationId: "span-1",
      fallbackObservationId: "span-1",
      incomplete: false,
      observationCount: 1,
      totalInputTokens: 1,
      totalOutputTokens: 1,
      totalUsage: 2,
      totalCost: 0.1,
      latency: 1,
    });
    const listForTrace = vi.fn().mockResolvedValue({
      items: [
        {
          id: "span-1",
          traceId: "trace-1",
          projectId: "project-1",
          type: "GENERATION",
          name: "generation",
          environment: "production",
          startTime: new Date("2026-07-20T00:00:00.000Z"),
          endTime: null,
          createdAt: new Date("2026-07-20T00:00:00.000Z"),
          updatedAt: new Date("2026-07-20T00:00:00.000Z"),
          input: { prompt: "hello" },
          tags: [],
          usageDetails: {},
          costDetails: {},
          providedUsageDetails: {},
          providedCostDetails: {},
          totalUsage: 0,
        },
      ],
      nextCursor: null,
    });
    const source = new DorisEvaluationTargetSource({
      repositories: () =>
        ({
          traces: { get: getTrace },
          observations: { listForTrace },
        }) as never,
      toTraceDomain: (trace, control) =>
        ({
          ...trace,
          input: trace.inputPreview,
          output: trace.outputPreview,
          metadata: {},
          bookmarked: control.bookmarked,
          public: control.public,
        }) as never,
      toObservationDomain: (observation) => observation as never,
      getTraceControl: vi
        .fn()
        .mockResolvedValue({ bookmarked: true, public: false }),
    });

    await expect(
      source.getTrace({
        projectId: "project-1",
        traceId: "trace-1",
        excludeInputOutput: true,
      }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "trace-1",
        input: null,
        bookmarked: true,
        public: false,
      }),
    );
    await expect(
      source.getObservationsByName({
        projectId: "project-1",
        traceId: "trace-1",
        name: "generation",
        fetchWithInputOutput: true,
      }),
    ).resolves.toEqual([
      expect.objectContaining({ id: "span-1", input: { prompt: "hello" } }),
    ]);
    expect(listForTrace).toHaveBeenCalledWith(
      expect.objectContaining({
        traceId: "trace-1",
        includeFullContent: true,
        filters: [
          {
            type: "string",
            column: "name",
            operator: "=",
            value: "generation",
          },
        ],
      }),
    );
  });
});
