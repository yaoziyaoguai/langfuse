import { describe, expect, it, vi } from "vitest";

import { DorisUsersRepository } from "./users";

const range = {
  from: new Date("2026-07-17T00:00:00.000Z"),
  to: new Date("2026-07-18T00:00:00.000Z"),
};

describe("Doris users repository", () => {
  it("returns bounded event-derived user metrics", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        project_id: "project-1",
        user_id: "user-1",
        min_timestamp: "2026-07-17 10:00:00.000000",
        max_timestamp: "2026-07-17 10:00:03.000000",
        session_ids: '["session-b","session-a"]',
        environments: '["production"]',
        trace_count: "2",
        session_count: "2",
        observation_count: "3",
        total_input_tokens: "12",
        total_output_tokens: "6",
        total_cost: "0.5",
      },
    ]);
    const repository = new DorisUsersRepository({ query });

    const page = await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      limit: 10,
    });

    expect(page.items).toEqual([
      expect.objectContaining({
        id: "user-1",
        sessionIds: ["session-a", "session-b"],
        traceCount: 2,
        sessionCount: 2,
        observationCount: 3,
        totalUsage: 18,
        totalCost: 0.5,
      }),
    ]);
    const sql = query.mock.calls[0]?.[0] as string;
    expect(sql).toContain("GROUP BY e.project_id, e.user_id");
    expect(sql.match(/LEFT JOIN project_tombstones/g)).toHaveLength(1);
    expect(sql).toContain("ORDER BY trace_count DESC, user_id DESC");
  });

  it("keeps search values bound and paginates by last-seen/user id", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([
        {
          project_id: "project-1",
          user_id: "user-1",
          min_timestamp: "2026-07-17 10:00:00.000000",
          max_timestamp: "2026-07-17 10:00:03.000000",
          session_ids: "[]",
          environments: "[]",
          trace_count: "1",
          session_count: "0",
          observation_count: "1",
          total_input_tokens: "0",
          total_output_tokens: "0",
          total_cost: null,
        },
        {
          project_id: "project-1",
          user_id: "user-0",
          min_timestamp: "2026-07-17 09:00:00.000000",
          max_timestamp: "2026-07-17 09:00:01.000000",
          session_ids: "[]",
          environments: "[]",
          trace_count: "1",
          session_count: "0",
          observation_count: "1",
          total_input_tokens: "0",
          total_output_tokens: "0",
          total_cost: null,
        },
      ])
      .mockResolvedValueOnce([]);
    const repository = new DorisUsersRepository({ query });
    const first = await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      identifierQuery: "x' OR 1=1 --",
      limit: 1,
    });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      cursor: first.nextCursor ?? undefined,
      limit: 1,
    });

    expect(query.mock.calls[0]?.[0]).not.toContain("x' OR 1=1 --");
    expect(query.mock.calls[0]?.[1]).toContain("%x' or 1=1 --%");
    expect(query.mock.calls[1]?.[0]).toContain("trace_count < ?");
    expect(query.mock.calls[1]?.[1]).toEqual(
      expect.arrayContaining([1, "user-1"]),
    );
    expect(first.nextCursor).not.toBeNull();
  });

  it("aggregates only the events selected by user filters", async () => {
    const query = vi.fn().mockResolvedValue([]);
    const repository = new DorisUsersRepository({ query });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [
        {
          type: "stringOptions",
          column: "environment",
          operator: "any of",
          value: ["production"],
        },
      ],
      limit: 10,
    });

    const sql = query.mock.calls[0]?.[0] as string;
    expect(sql.match(/FROM events_current/g)).toHaveLength(1);
    expect(sql).toContain("e.environment IN (?)");
  });

  it("requires a bounded range for user detail", async () => {
    const repository = new DorisUsersRepository({ query: vi.fn() });

    await expect(
      repository.get({ projectId: "project-1", userId: "user-1", range: null }),
    ).rejects.toEqual(
      expect.objectContaining({ code: "InvalidTimeRange", maxDays: 30 }),
    );
  });

  it("counts matching users through the bounded visibility scope", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "5" }]);
    const repository = new DorisUsersRepository({ query });

    await expect(
      repository.count({ projectId: "project-1", range, filters: [] }),
    ).resolves.toBe(5);
    expect(query.mock.calls[0]?.[0]).toContain(
      "COUNT(DISTINCT e.user_id) AS count",
    );
  });
});
