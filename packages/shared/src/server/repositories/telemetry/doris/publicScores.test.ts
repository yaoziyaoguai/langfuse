import { describe, expect, it, vi } from "vitest";

import type { ScoreDomain } from "../../../../domain/scores";
import { readDorisScoresForPublicApi } from "./publicScores";

const score = {
  id: "score-1",
  projectId: "project-1",
  environment: "production",
  name: "quality",
  value: 0.9,
  source: "API",
  authorUserId: null,
  comment: null,
  metadata: {},
  configId: null,
  queueId: null,
  executionTraceId: null,
  createdAt: new Date("2026-07-17T10:00:03.000Z"),
  updatedAt: new Date("2026-07-17T10:00:03.000Z"),
  timestamp: new Date("2026-07-17T10:00:02.000Z"),
  traceId: "trace-1",
  sessionId: null,
  datasetRunId: null,
  observationId: null,
  longStringValue: "",
  dataType: "NUMERIC",
  stringValue: null,
} satisfies ScoreDomain;

describe("Doris public score reads", () => {
  it("preserves score pagination and trace projection", async () => {
    const list = vi
      .fn()
      .mockResolvedValue({ items: [score], nextCursor: null });
    const count = vi.fn().mockResolvedValue(1);
    const getTrace = vi.fn().mockResolvedValue({
      userId: "user-1",
      tags: ["prod"],
      environment: "production",
      sessionId: "session-1",
    });

    await expect(
      readDorisScoresForPublicApi(
        {
          projectId: "project-1",
          page: 1,
          limit: 10,
          fields: ["score", "trace"],
        },
        "v2",
        { list, count, getTrace },
      ),
    ).resolves.toEqual({
      items: [
        {
          ...score,
          trace: {
            userId: "user-1",
            tags: ["prod"],
            environment: "production",
            sessionId: "session-1",
          },
        },
      ],
      count: 1,
    });
    expect(list).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        limit: 10,
        offset: 0,
      }),
    );
  });

  it("filters trace properties before applying API pagination", async () => {
    const second = { ...score, id: "score-2", traceId: "trace-2" };
    const list = vi.fn().mockResolvedValue({
      items: [score, second],
      nextCursor: null,
    });
    const count = vi.fn().mockResolvedValue(2);
    const getTrace = vi.fn().mockImplementation(({ traceId }) =>
      Promise.resolve({
        name: traceId === "trace-2" ? "target-trace" : "other-trace",
        userId: traceId === "trace-2" ? "target" : "other",
        tags: [],
        environment: "production",
        sessionId: null,
      }),
    );

    await expect(
      readDorisScoresForPublicApi(
        {
          projectId: "project-1",
          page: 1,
          limit: 1,
          userId: "target",
        },
        "v2",
        { list, count, getTrace },
      ),
    ).resolves.toEqual({
      items: [expect.objectContaining({ id: "score-2" })],
      count: 1,
    });
  });

  it("keeps trace-backed advanced filters out of score SQL", async () => {
    const second = { ...score, id: "score-2", traceId: "trace-2" };
    const list = vi.fn().mockResolvedValue({
      items: [score, second],
      nextCursor: null,
    });
    const getTrace = vi.fn().mockImplementation(({ traceId }) =>
      Promise.resolve({
        name: traceId === "trace-2" ? "target-trace" : "other-trace",
        userId: null,
        tags: [],
        environment: "production",
        sessionId: null,
      }),
    );

    await expect(
      readDorisScoresForPublicApi(
        {
          projectId: "project-1",
          page: 1,
          limit: 10,
          fields: ["score"],
          advancedFilters: [
            {
              type: "string",
              column: "traceName",
              operator: "contains",
              value: "target",
            },
          ],
        },
        "v2",
        { list, count: vi.fn(), getTrace },
      ),
    ).resolves.toEqual({
      items: [expect.objectContaining({ id: "score-2" })],
      count: 1,
    });
    expect(list.mock.calls[0]?.[0].filters).not.toEqual(
      expect.arrayContaining([
        expect.objectContaining({ column: "traceName" }),
      ]),
    );
    expect(getTrace).toHaveBeenCalledTimes(2);
  });
});
