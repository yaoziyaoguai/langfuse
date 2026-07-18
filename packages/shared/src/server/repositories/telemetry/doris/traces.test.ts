import { describe, expect, it, vi } from "vitest";

import { DorisTracesRepository } from "./traces";

const range = {
  from: new Date("2026-07-17T00:00:00.000Z"),
  to: new Date("2026-07-18T00:00:00.000Z"),
};

describe("Doris traces repository", () => {
  it("builds real-root and deterministic fallback summaries", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        project_id: "project-1",
        trace_id: "trace-root",
        trace_timestamp: "2026-07-17 10:00:00.000000",
        trace_end_time: "2026-07-17 10:00:03.000000",
        representative_span_id: "root-span",
        representative_is_root: 1,
        name: "root name",
        environment: "production",
        user_id: "user-1",
        session_id: "session-1",
        release: null,
        version: null,
        tags: '["prod"]',
        input_preview: "root input",
        output_preview: "root output",
        observation_count: "2",
        total_input_tokens: "12",
        total_output_tokens: "6",
        total_cost: "0.5",
      },
      {
        project_id: "project-1",
        trace_id: "trace-fallback",
        trace_timestamp: "2026-07-17 09:00:00.000000",
        trace_end_time: "2026-07-17 09:00:01.000000",
        representative_span_id: "first-span",
        representative_is_root: 0,
        name: "first event",
        environment: "production",
        user_id: null,
        session_id: null,
        release: null,
        version: null,
        tags: "[]",
        input_preview: null,
        output_preview: null,
        observation_count: "1",
        total_input_tokens: "0",
        total_output_tokens: "0",
        total_cost: null,
      },
    ]);
    const repository = new DorisTracesRepository({ query });

    const page = await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      limit: 10,
    });

    expect(page.items).toEqual([
      expect.objectContaining({
        id: "trace-root",
        rootObservationId: "root-span",
        incomplete: false,
        observationCount: 2,
        totalUsage: 18,
        latency: 3,
      }),
      expect.objectContaining({
        id: "trace-fallback",
        rootObservationId: null,
        fallbackObservationId: "first-span",
        incomplete: true,
      }),
    ]);
    const sql = query.mock.calls[0]?.[0] as string;
    expect(sql).toContain("ROW_NUMBER() OVER");
    expect(sql).toContain("e.is_app_root DESC");
    expect(sql.match(/LEFT JOIN trace_tombstones/g)).toHaveLength(2);
    expect(sql).not.toContain("e.input AS input");
  });

  it("uses a stable trace timestamp/id cursor", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([
        {
          project_id: "project-1",
          trace_id: "trace-b",
          trace_timestamp: "2026-07-17 10:00:00.000000",
          trace_end_time: "2026-07-17 10:00:01.000000",
          representative_span_id: "span-b",
          representative_is_root: 0,
          name: "b",
          environment: "production",
          user_id: null,
          session_id: null,
          release: null,
          version: null,
          tags: "[]",
          input_preview: null,
          output_preview: null,
          observation_count: "1",
          total_input_tokens: "0",
          total_output_tokens: "0",
          total_cost: null,
        },
        {
          project_id: "project-1",
          trace_id: "trace-a",
          trace_timestamp: "2026-07-17 10:00:00.000000",
          trace_end_time: "2026-07-17 10:00:01.000000",
          representative_span_id: "span-a",
          representative_is_root: 0,
          name: "a",
          environment: "production",
          user_id: null,
          session_id: null,
          release: null,
          version: null,
          tags: "[]",
          input_preview: null,
          output_preview: null,
          observation_count: "1",
          total_input_tokens: "0",
          total_output_tokens: "0",
          total_cost: null,
        },
      ])
      .mockResolvedValueOnce([]);
    const repository = new DorisTracesRepository({ query });
    const first = await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      limit: 1,
    });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      cursor: first.nextCursor ?? undefined,
      limit: 1,
    });

    expect(first.items.map(({ id }) => id)).toEqual(["trace-b"]);
    expect(first.nextCursor).not.toBeNull();
    expect(query.mock.calls[1]?.[0]).toContain("trace_timestamp < ?");
    expect(query.mock.calls[1]?.[1]).toEqual(
      expect.arrayContaining([new Date("2026-07-17T10:00:00.000Z"), "trace-b"]),
    );
  });

  it("applies an allowlisted trace order with stable offset pagination", async () => {
    const query = vi.fn().mockResolvedValue([]);
    const repository = new DorisTracesRepository({ query });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      orderBy: { column: "name", order: "ASC" },
      offset: 100,
      limit: 50,
    });

    expect(query.mock.calls[0]?.[0]).toContain(
      "ORDER BY name ASC, trace_timestamp ASC, trace_id ASC",
    );
    expect(query.mock.calls[0]?.[0]).toContain("LIMIT ? OFFSET ?");
    expect(query.mock.calls[0]?.[1].slice(-2)).toEqual([51, 100]);
  });

  it("derives an exact multi-partition detail bound from entity heads", async () => {
    const locateTrace = vi.fn().mockResolvedValue([
      {
        partitionDate: "2026-07-18",
        traceId: "trace-midnight",
        observationId: "post",
      },
      {
        partitionDate: "2026-07-17",
        traceId: "trace-midnight",
        observationId: "pre",
      },
    ]);
    const query = vi.fn().mockResolvedValue([
      {
        project_id: "project-1",
        trace_id: "trace-midnight",
        trace_timestamp: "2026-07-17 23:55:00.000000",
        trace_end_time: "2026-07-18 00:05:00.000000",
        representative_span_id: "pre",
        representative_is_root: 0,
        name: "pre",
        environment: "production",
        user_id: null,
        session_id: null,
        release: null,
        version: null,
        tags: "[]",
        input_preview: null,
        output_preview: null,
        input: '{"question":"price"}',
        output: '{"answer":"ok"}',
        metadata: '{"region":"eu"}',
        observation_count: "2",
        total_input_tokens: "0",
        total_output_tokens: "0",
        total_cost: null,
      },
    ]);
    const repository = new DorisTracesRepository({ query, locateTrace });

    await expect(
      repository.get({ projectId: "project-1", traceId: "trace-midnight" }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "trace-midnight",
        observationCount: 2,
        incomplete: true,
        input: { question: "price" },
        metadata: { region: "eu" },
      }),
    );
    expect(locateTrace).toHaveBeenCalledWith({
      projectId: "project-1",
      traceId: "trace-midnight",
    });
    expect(query.mock.calls[0]?.[0]).toContain("e.partition_date IN (?, ?)");
    expect(query.mock.calls[0]?.[0]).toContain("e.input");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["2026-07-17", "2026-07-18", "trace-midnight"]),
    );
  });

  it("counts distinct matching traces through the bounded visibility scope", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "4" }]);
    const repository = new DorisTracesRepository({ query });

    await expect(
      repository.count({ projectId: "project-1", range, filters: [] }),
    ).resolves.toBe(4);
    expect(query.mock.calls[0]?.[0]).toContain(
      "COUNT(DISTINCT e.trace_id) AS count",
    );
    expect(query.mock.calls[0]?.[0]).toContain(
      "project_deletion.project_id IS NULL",
    );
  });

  it("rejects full-content list projections over the 30-day cap", async () => {
    const repository = new DorisTracesRepository({ query: vi.fn() });

    await expect(
      repository.list({
        projectId: "project-1",
        range: {
          from: new Date("2026-01-01T00:00:00.000Z"),
          to: new Date("2026-02-01T00:00:00.001Z"),
        },
        filters: [],
        limit: 10,
        includeFullContent: true,
      }),
    ).rejects.toEqual(
      expect.objectContaining({ code: "InvalidTimeRange", maxDays: 30 }),
    );
  });

  it("returns bounded representative trace filter options", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([{ value: "root name", count: "3" }])
      .mockResolvedValueOnce([{ value: "prod", count: "2" }]);
    const repository = new DorisTracesRepository({ query });

    await expect(
      repository.filterOptionValues({
        projectId: "project-1",
        range,
        filters: [],
        column: "name",
        limit: 100,
      }),
    ).resolves.toEqual([{ value: "root name", count: 3 }]);
    await expect(
      repository.filterOptionValues({
        projectId: "project-1",
        range,
        filters: [],
        column: "tags",
        limit: 100,
      }),
    ).resolves.toEqual([{ value: "prod", count: 2 }]);

    expect(query.mock.calls[0]?.[0]).toContain("matched_trace_ids AS");
    expect(query.mock.calls[0]?.[0]).toContain("representative_rank = 1");
    expect(query.mock.calls[0]?.[0]).toContain("r.name AS value");
    expect(query.mock.calls[1]?.[0]).toContain(
      "LATERAL VIEW explode(r.tags) exploded AS value",
    );
  });
});
