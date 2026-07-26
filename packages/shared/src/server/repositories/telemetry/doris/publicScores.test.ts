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
    const list = vi.fn().mockResolvedValue({
      items: [
        {
          ...score,
          trace: {
            name: null,
            userId: "user-1",
            tags: ["prod"],
            environment: "production",
            sessionId: "session-1",
          },
        },
      ],
      nextCursor: null,
    });
    const count = vi.fn().mockResolvedValue(1);

    await expect(
      readDorisScoresForPublicApi(
        {
          projectId: "project-1",
          page: 1,
          limit: 10,
          fields: ["score", "trace"],
        },
        "v2",
        { list, count },
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
        includeTraceContext: true,
      }),
    );
  });

  it("pushes trace properties down before applying API pagination", async () => {
    const second = { ...score, id: "score-2", traceId: "trace-2" };
    const list = vi.fn().mockResolvedValue({
      items: [
        {
          ...second,
          trace: {
            name: "target-trace",
            userId: "target",
            tags: [],
            environment: "production",
            sessionId: null,
          },
        },
      ],
      nextCursor: null,
    });
    const count = vi.fn().mockResolvedValue(1);

    await expect(
      readDorisScoresForPublicApi(
        {
          projectId: "project-1",
          page: 1,
          limit: 1,
          userId: "target",
        },
        "v2",
        { list, count },
      ),
    ).resolves.toEqual({
      items: [expect.objectContaining({ id: "score-2" })],
      count: 1,
    });
    expect(list.mock.calls[0]?.[0].filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ column: "userId", value: "target" }),
      ]),
    );
    expect(count).toHaveBeenCalledWith(
      expect.objectContaining({
        filters: expect.arrayContaining([
          expect.objectContaining({ column: "userId", value: "target" }),
        ]),
      }),
    );
  });

  it("keeps trace-backed advanced filters in score SQL", async () => {
    const second = { ...score, id: "score-2", traceId: "trace-2" };
    const list = vi.fn().mockResolvedValue({
      items: [{ ...second, trace: null }],
      nextCursor: null,
    });
    const count = vi.fn().mockResolvedValue(1);

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
        { list, count },
      ),
    ).resolves.toEqual({
      items: [expect.objectContaining({ id: "score-2" })],
      count: 1,
    });
    expect(list.mock.calls[0]?.[0].filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ column: "traceName" }),
      ]),
    );
  });

  it("matches ClickHouse environment scoping for trace-backed API filters", async () => {
    const list = vi
      .fn()
      .mockResolvedValue({ items: [score], nextCursor: null });
    const count = vi.fn().mockResolvedValue(1);

    await readDorisScoresForPublicApi(
      {
        projectId: "project-1",
        page: 1,
        limit: 10,
        userId: "user-1",
        environment: ["production"],
        fields: ["score"],
      },
      "v2",
      { list, count },
    );

    expect(list).toHaveBeenCalledWith(
      expect.objectContaining({
        includeTraceContext: false,
        filters: expect.arrayContaining([
          expect.objectContaining({
            column: "environment",
            value: ["production"],
          }),
          expect.objectContaining({
            column: "traceEnvironment",
            value: ["production"],
          }),
        ]),
      }),
    );
    expect(count).toHaveBeenCalledWith(
      expect.objectContaining({
        filters: expect.arrayContaining([
          expect.objectContaining({ column: "traceEnvironment" }),
        ]),
      }),
    );
  });

  it("pushes dataset-run identity into Doris score reads", async () => {
    const list = vi
      .fn()
      .mockResolvedValue({ items: [score], nextCursor: null });
    const count = vi.fn().mockResolvedValue(1);

    await readDorisScoresForPublicApi(
      {
        projectId: "project-1",
        page: 1,
        limit: 10,
        datasetRunId: "run-1",
        fields: ["score"],
      },
      "v2",
      { list, count },
    );

    const expectedFilter = expect.objectContaining({
      type: "string",
      column: "datasetRunId",
      operator: "=",
      value: "run-1",
    });
    expect(list).toHaveBeenCalledWith(
      expect.objectContaining({
        filters: expect.arrayContaining([expectedFilter]),
      }),
    );
    expect(count).toHaveBeenCalledWith(
      expect.objectContaining({
        filters: expect.arrayContaining([expectedFilter]),
      }),
    );
  });
});
