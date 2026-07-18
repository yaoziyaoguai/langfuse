import { describe, expect, it, vi } from "vitest";

import { DorisSessionsRepository } from "./sessions";

const range = {
  from: new Date("2026-07-17T00:00:00.000Z"),
  to: new Date("2026-07-18T00:00:00.000Z"),
};

describe("Doris sessions repository", () => {
  it("returns event-derived session aggregates with deterministic arrays", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        project_id: "project-1",
        session_id: "session-1",
        min_timestamp: "2026-07-17 10:00:00.000000",
        max_timestamp: "2026-07-17 10:00:03.000000",
        trace_ids: '["trace-b","trace-a"]',
        user_ids: '["user-b","user-a"]',
        environments: '["production"]',
        trace_tag_sets: '[["prod","shared"],["shared","api"]]',
        trace_count: "2",
        observation_count: "3",
        total_input_tokens: "12",
        total_output_tokens: "6",
        total_cost: "0.5",
      },
    ]);
    const repository = new DorisSessionsRepository({ query });

    const page = await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      limit: 10,
    });

    expect(page.items).toEqual([
      expect.objectContaining({
        id: "session-1",
        traceIds: ["trace-a", "trace-b"],
        userIds: ["user-a", "user-b"],
        tags: ["api", "prod", "shared"],
        traceCount: 2,
        observationCount: 3,
        totalUsage: 18,
        duration: 3,
      }),
    ]);
    const sql = query.mock.calls[0]?.[0] as string;
    expect(sql).toContain("GROUP BY e.project_id, e.session_id");
    expect(sql.match(/LEFT JOIN trace_tombstones/g)).toHaveLength(2);
    expect(sql).not.toContain("e.input AS input");
  });

  it("uses a stable created-at/session cursor and bound identifier search", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([
        {
          project_id: "project-1",
          session_id: "session-1",
          min_timestamp: "2026-07-17 10:00:00.000000",
          max_timestamp: "2026-07-17 10:00:03.000000",
          trace_ids: "[]",
          user_ids: "[]",
          environments: "[]",
          trace_tag_sets: "[]",
          trace_count: "1",
          observation_count: "1",
          total_input_tokens: "0",
          total_output_tokens: "0",
          total_cost: null,
        },
        {
          project_id: "project-1",
          session_id: "session-0",
          min_timestamp: "2026-07-17 09:00:00.000000",
          max_timestamp: "2026-07-17 09:00:01.000000",
          trace_ids: "[]",
          user_ids: "[]",
          environments: "[]",
          trace_tag_sets: "[]",
          trace_count: "1",
          observation_count: "1",
          total_input_tokens: "0",
          total_output_tokens: "0",
          total_cost: null,
        },
      ])
      .mockResolvedValueOnce([]);
    const repository = new DorisSessionsRepository({ query });
    const first = await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      identifierQuery: "50%_OFF\\",
      limit: 1,
    });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      cursor: first.nextCursor ?? undefined,
      limit: 1,
    });

    expect(first.nextCursor).not.toBeNull();
    expect(query.mock.calls[0]?.[0]).toContain("LIKE ? ESCAPE '\\\\'");
    expect(query.mock.calls[0]?.[1]).toContain("%50\\%\\_off\\\\%");
    expect(query.mock.calls[1]?.[0]).toContain("min_timestamp < ?");
    expect(query.mock.calls[1]?.[1]).toEqual(
      expect.arrayContaining([
        new Date("2026-07-17T10:00:00.000Z"),
        "session-1",
      ]),
    );
  });

  it("requires a bounded range for session detail", async () => {
    const repository = new DorisSessionsRepository({ query: vi.fn() });

    await expect(
      repository.get({
        projectId: "project-1",
        sessionId: "session-1",
        range: null,
      }),
    ).rejects.toEqual(
      expect.objectContaining({ code: "InvalidTimeRange", maxDays: 30 }),
    );
  });

  it("supports ascending created-at pagination with the same tie breaker", async () => {
    const query = vi.fn().mockResolvedValue([]);
    const repository = new DorisSessionsRepository({ query });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      order: "ASC",
      limit: 10,
    });

    expect(query.mock.calls[0]?.[0]).toContain(
      "ORDER BY min_timestamp ASC, session_id ASC",
    );
  });

  it("counts matching sessions through the bounded visibility scope", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "3" }]);
    const repository = new DorisSessionsRepository({ query });

    await expect(
      repository.count({ projectId: "project-1", range, filters: [] }),
    ).resolves.toBe(3);
    expect(query.mock.calls[0]?.[0]).toContain(
      "SELECT COUNT(*) AS count\nFROM aggregated_sessions",
    );
  });

  it("binds aggregate arrays, metadata, and negative session filters", async () => {
    const query = vi.fn().mockResolvedValue([]);
    const repository = new DorisSessionsRepository({ query });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      sessionFilters: [
        {
          type: "arrayOptions",
          column: "userIds",
          operator: "all of",
          value: ["user-a", "user-b"],
        },
        {
          type: "arrayOptions",
          column: "traceTags",
          operator: "none of",
          value: ["internal"],
        },
        {
          type: "stringObject",
          column: "metadata",
          key: "region",
          operator: "=",
          value: "eu",
        },
      ],
      limit: 10,
    });

    const sql = query.mock.calls[0]?.[0] as string;
    expect(sql).toContain("ARRAY_CONTAINS(user_ids, ?)");
    expect(sql).toContain("NOT (ARRAY_CONTAINS(trace_tags, ?))");
    expect(sql).toContain("ELEMENT_AT(CAST(metadata_json AS VARIANT), ?)");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["user-a", "user-b", "internal", "region", "eu"]),
    );
  });

  it("counts after applying aggregate filters", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "1" }]);
    const repository = new DorisSessionsRepository({ query });

    await repository.count({
      projectId: "project-1",
      range,
      filters: [],
      sessionFilters: [
        {
          type: "stringOptions",
          column: "id",
          operator: "none of",
          value: ["excluded"],
        },
      ],
    });

    expect(query.mock.calls[0]?.[0]).toContain(
      "SELECT COUNT(*) AS count\nFROM aggregated_sessions",
    );
    expect(query.mock.calls[0]?.[0]).toContain("session_id NOT IN (?)");
  });
});
