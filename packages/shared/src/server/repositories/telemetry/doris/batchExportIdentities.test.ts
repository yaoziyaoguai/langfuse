import { describe, expect, it, vi } from "vitest";

import { DorisObservationsRepository } from "./observations";
import { DorisScoresRepository } from "./scores";
import { DorisSessionsRepository } from "./sessions";
import { DorisTracesRepository } from "./traces";

async function collect<T>(values: AsyncIterable<T>): Promise<T[]> {
  const rows: T[] = [];
  for await (const value of values) rows.push(value);
  return rows;
}

const range = {
  from: new Date("2026-07-01T00:00:00.000Z"),
  to: new Date("2026-07-24T00:00:00.000Z"),
};

describe("Doris batch export identity scans", () => {
  it("streams canonical trace identities in one statement without OFFSET", async () => {
    const query = vi.fn();
    const streamQuery = vi.fn(async function* (
      _sql: string,
      _params?: readonly unknown[],
    ) {
      yield { trace_id: "trace-a" };
      yield { trace_id: "trace-b" };
    });
    const repository = new DorisTracesRepository({
      query,
      streamQuery: streamQuery as never,
    });

    await expect(
      collect(
        repository.scanIdentities({
          projectId: "project-1",
          range,
          filters: [],
          limit: 7,
        }),
      ),
    ).resolves.toEqual([{ id: "trace-a" }, { id: "trace-b" }]);
    expect(query).not.toHaveBeenCalled();
    const [sql, params] = streamQuery.mock.calls[0]!;
    expect(sql).toContain("SELECT DISTINCT e.trace_id");
    expect(sql).toContain("ORDER BY e.trace_id ASC");
    expect(sql).not.toContain("OFFSET");
    expect(params?.at(-1)).toBe(7);
  });

  it("keeps observation identities composite and canonically ordered", async () => {
    const streamQuery = vi.fn(async function* (
      _sql: string,
      _params?: readonly unknown[],
    ) {
      yield { trace_id: "trace-a", span_id: "span-a" };
    });
    const repository = new DorisObservationsRepository({
      query: vi.fn(),
      streamQuery: streamQuery as never,
    });

    await expect(
      collect(
        repository.scanIdentities({
          projectId: "project-1",
          range,
          filters: [],
          limit: 8,
        }),
      ),
    ).resolves.toEqual([{ id: "span-a", traceId: "trace-a" }]);
    expect(streamQuery.mock.calls[0]![0]).toContain(
      "ORDER BY e.trace_id ASC, e.span_id ASC",
    );
  });

  it("streams canonical score and session identities", async () => {
    const scoreStream = vi.fn(async function* (
      _sql: string,
      _params?: readonly unknown[],
    ) {
      yield { score_id: "score-a" };
    });
    const sessionStream = vi.fn(async function* (
      _sql: string,
      _params?: readonly unknown[],
    ) {
      yield { session_id: "session-a" };
    });
    const scores = new DorisScoresRepository({
      query: vi.fn(),
      streamQuery: scoreStream as never,
    });
    const sessions = new DorisSessionsRepository({
      query: vi.fn(),
      streamQuery: sessionStream as never,
    });

    await expect(
      collect(
        scores.scanIdentities({
          projectId: "project-1",
          range,
          filters: [],
          limit: 9,
        }),
      ),
    ).resolves.toEqual([{ id: "score-a" }]);
    await expect(
      collect(
        sessions.scanIdentities({
          projectId: "project-1",
          range,
          filters: [],
          sessionFilters: [],
          limit: 10,
        }),
      ),
    ).resolves.toEqual([{ id: "session-a" }]);
    expect(scoreStream.mock.calls[0]![0]).toContain("ORDER BY s.score_id ASC");
    expect(sessionStream.mock.calls[0]![0]).toContain(
      "ORDER BY session_id ASC",
    );
  });
});
