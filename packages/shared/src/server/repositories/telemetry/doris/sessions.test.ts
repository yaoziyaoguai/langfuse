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

  it("uses a stable max-timestamp/session cursor and bound identifier search", async () => {
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
    expect(query.mock.calls[1]?.[0]).toContain("max_timestamp < ?");
    expect(query.mock.calls[1]?.[1]).toEqual(
      expect.arrayContaining([
        new Date("2026-07-17T10:00:03.000Z"),
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
});
