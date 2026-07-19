import { describe, expect, it, vi } from "vitest";

import { DorisScoresRepository } from "./scores";

const range = {
  from: new Date("2026-07-17T00:00:00.000Z"),
  to: new Date("2026-07-18T00:00:00.000Z"),
};

const row = {
  project_id: "project-1",
  score_date: "2026-07-17",
  score_id: "score-1",
  trace_id: "trace-1",
  observation_id: "span-1",
  session_id: null,
  name: "quality",
  source: "API",
  data_type: "NUMERIC",
  value: "0.9",
  string_value: null,
  long_string_value: null,
  boolean_value: null,
  comment: "good",
  author_user_id: null,
  config_id: "config-1",
  queue_id: null,
  environment: "production",
  metadata: '{"region":"eu"}',
  timestamp: "2026-07-17 10:00:02.000000",
  created_at: "2026-07-17 10:00:03.000000",
  updated_at: "2026-07-17 10:00:03.000000",
};

describe("Doris scores repository", () => {
  it("lists visible project scores with bounded stable pagination", async () => {
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisScoresRepository({ query });

    await expect(
      repository.list({
        projectId: "project-1",
        range,
        filters: [
          {
            type: "string",
            column: "name",
            operator: "=",
            value: "quality",
          },
        ],
        limit: 10,
      }),
    ).resolves.toEqual({
      items: [
        expect.objectContaining({
          id: "score-1",
          projectId: "project-1",
          dataType: "NUMERIC",
          value: 0.9,
          traceId: "trace-1",
          observationId: "span-1",
          metadata: { region: "eu" },
        }),
      ],
      nextCursor: null,
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM scores_current s");
    expect(sql).toContain("LEFT JOIN trace_tombstones");
    expect(sql).toContain("LEFT JOIN project_tombstones");
    expect(sql).toContain("s.project_id = ?");
    expect(sql).toContain("s.score_date >= ?");
    expect(sql).toContain("s.`timestamp` >= ?");
    expect(sql).toContain("ORDER BY s.`timestamp` DESC, s.score_id DESC");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["project-1", "quality", 11]),
    );
  });

  it("resolves point detail through the immutable Postgres locator", async () => {
    const locateScore = vi
      .fn()
      .mockResolvedValue([{ partitionDate: "2026-07-17", scoreId: "score-1" }]);
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisScoresRepository({ query, locateScore });

    await expect(
      repository.get({ projectId: "project-1", scoreId: "score-1" }),
    ).resolves.toEqual(expect.objectContaining({ id: "score-1" }));

    expect(locateScore).toHaveBeenCalledWith({
      projectId: "project-1",
      scoreId: "score-1",
    });
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["2026-07-17", "score-1"]),
    );
  });

  it("counts through the same visibility and filter scope", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "3" }]);
    const repository = new DorisScoresRepository({ query });

    await expect(
      repository.count({
        projectId: "project-1",
        range,
        filters: [
          {
            type: "stringOptions",
            column: "dataType",
            operator: "any of",
            value: ["NUMERIC", "BOOLEAN"],
          },
        ],
      }),
    ).resolves.toBe(3);
    expect(query.mock.calls[0]?.[0]).toContain("COUNT(*) AS count");
  });

  it("allows only catalogued UI ordering expressions", async () => {
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisScoresRepository({ query });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      limit: 10,
      orderBy: { column: "name", order: "ASC" },
    });
    expect(query.mock.calls[0]?.[0]).toContain(
      "ORDER BY s.`name` ASC, s.`timestamp` DESC, s.score_id DESC",
    );

    await expect(
      repository.list({
        projectId: "project-1",
        range,
        filters: [],
        limit: 10,
        orderBy: { column: "unsafe_sql", order: "ASC" },
      }),
    ).rejects.toThrow("Unsupported Doris score order column");
  });

  it("groups only catalogued score dimensions inside the visible scope", async () => {
    const query = vi
      .fn()
      .mockResolvedValue([
        { name: "quality", dataType: "NUMERIC", count: "2" },
      ]);
    const repository = new DorisScoresRepository({ query });

    await expect(
      repository.aggregateGroups({
        projectId: "project-1",
        range,
        filters: [],
        columns: ["name", "dataType"],
        limit: 200,
      }),
    ).resolves.toEqual([{ name: "quality", dataType: "NUMERIC", count: 2 }]);
    expect(query.mock.calls[0]?.[0]).toContain(
      "GROUP BY s.`name`, s.data_type",
    );
    await expect(
      repository.aggregateGroups({
        projectId: "project-1",
        range,
        filters: [],
        columns: ["unsafe_sql"],
        limit: 10,
      }),
    ).rejects.toThrow("Invalid Doris score grouping request");
  });
});
