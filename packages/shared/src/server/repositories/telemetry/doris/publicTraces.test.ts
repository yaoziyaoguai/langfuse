import { describe, expect, it, vi } from "vitest";

import {
  getDorisTracesCountForPublicApi,
  getDorisTracesForPublicApi,
} from "./publicTraces";

const trace = {
  id: "trace-1",
  projectId: "project-1",
  timestamp: new Date("2026-07-17T10:00:00.000Z"),
  endTime: new Date("2026-07-17T10:00:02.000Z"),
  name: "trace",
  environment: "production",
  userId: "user-1",
  sessionId: "session-1",
  release: null,
  version: null,
  tags: ["prod"],
  inputPreview: "input preview",
  outputPreview: "output preview",
  rootObservationId: "span-1",
  fallbackObservationId: "span-1",
  incomplete: false,
  observationCount: 1,
  totalInputTokens: 10,
  totalOutputTokens: 5,
  totalUsage: 15,
  totalCost: 0.125,
  latency: 2,
};

function dependencies() {
  return {
    repository: {
      list: vi.fn().mockResolvedValue({ items: [trace], nextCursor: null }),
      count: vi.fn().mockResolvedValue(1),
    },
    findTraceControls: vi
      .fn()
      .mockResolvedValue([
        { traceId: "trace-1", bookmarked: true, public: false },
      ]),
    findObservationIds: vi
      .fn()
      .mockResolvedValue(new Map([["trace-1", ["span-1"]]])),
  };
}

describe("Doris public trace reads", () => {
  it("maps event-derived traces, Postgres controls, and requested extras", async () => {
    const deps = dependencies();

    const result = await getDorisTracesForPublicApi(
      {
        projectId: "project-1",
        page: 1,
        limit: 10,
        fromTimestamp: "2026-07-17T00:00:00.000Z",
        toTimestamp: "2026-07-18T00:00:00.000Z",
        environment: "production",
        fields: ["core", "observations", "metrics"],
      },
      deps,
    );

    expect(result).toEqual([
      expect.objectContaining({
        id: "trace-1",
        bookmarked: true,
        public: false,
        observations: ["span-1"],
        scores: [],
        totalCost: 0.125,
        latency: 2,
        htmlPath: "/project/project-1/traces/trace-1",
      }),
    ]);
    expect(deps.repository.list).toHaveBeenCalledWith(
      expect.objectContaining({
        filters: expect.arrayContaining([
          expect.objectContaining({ column: "environment" }),
        ]),
      }),
    );
  });

  it("uses the same bounded filters for count", async () => {
    const deps = dependencies();

    await expect(
      getDorisTracesCountForPublicApi(
        {
          projectId: "project-1",
          page: 1,
          limit: 10,
          fromTimestamp: "2026-07-17T00:00:00.000Z",
          toTimestamp: "2026-07-18T00:00:00.000Z",
          userId: "user-1",
        },
        deps,
      ),
    ).resolves.toBe(1);
    expect(deps.repository.count).toHaveBeenCalledWith(
      expect.objectContaining({
        filters: expect.arrayContaining([
          expect.objectContaining({ column: "userId" }),
        ]),
      }),
    );
  });

  it("loads full trace content only when the public projection requests io", async () => {
    const deps = dependencies();
    deps.repository.list.mockResolvedValueOnce({
      items: [
        {
          ...trace,
          input: { question: "full input" },
          output: { answer: "full output" },
        },
      ],
      nextCursor: null,
    });

    const result = await getDorisTracesForPublicApi(
      {
        projectId: "project-1",
        page: 1,
        limit: 10,
        fromTimestamp: "2026-07-17T00:00:00.000Z",
        toTimestamp: "2026-07-18T00:00:00.000Z",
        fields: ["core", "io"],
      },
      deps,
    );

    expect(deps.repository.list).toHaveBeenCalledWith(
      expect.objectContaining({ includeFullContent: true }),
    );
    expect(result[0]).toEqual(
      expect.objectContaining({
        input: { question: "full input" },
        output: { answer: "full output" },
      }),
    );
  });
});
