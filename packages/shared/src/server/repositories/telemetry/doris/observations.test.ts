import { describe, expect, it, vi } from "vitest";

import { DorisObservationsRepository } from "./observations";

const range = {
  from: new Date("2026-07-17T00:00:00.000Z"),
  to: new Date("2026-07-18T00:00:00.000Z"),
};

const row = {
  project_id: "project-1",
  partition_date: "2026-07-17",
  trace_id: "trace-1",
  span_id: "span-1",
  parent_span_id: null,
  version_token: "1784282400000000000",
  type: "GENERATION",
  name: "generation",
  environment: "production",
  user_id: "user-1",
  session_id: "session-1",
  trace_name: "trace name",
  release: "release-1",
  version: "version-1",
  level: "DEFAULT",
  status_message: null,
  is_app_root: 1,
  bookmarked: 0,
  public: 0,
  start_time: "2026-07-17 10:00:00.000000",
  end_time: "2026-07-17 10:00:02.500000",
  completion_start_time: "2026-07-17 10:00:00.500000",
  created_at: "2026-07-17 10:00:00.000000",
  updated_at: "2026-07-17 10:00:02.500000",
  provided_model_name: "gpt-test",
  internal_model_id: "model-1",
  prompt_id: "prompt-1",
  prompt_name: "support",
  prompt_version: 2,
  total_input_tokens: "10",
  total_output_tokens: "5",
  total_cost: "0.125",
  tags: '["prod","canary"]',
  usage_details: '{"input":10,"output":5,"total":15}',
  cost_details: '{"input":0.1,"output":0.025,"total":0.125}',
  provided_usage_details: "{}",
  provided_cost_details: "{}",
  tool_definitions_count: "1",
  tool_calls_count: "1",
  input_preview: '{"question":"price"}',
  output_preview: '{"answer":"ok"}',
};

describe("Doris observations repository", () => {
  it("returns a narrow stable page with a canonical cursor", async () => {
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisObservationsRepository({ query });

    const page = await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      limit: 2,
    });

    expect(page.nextCursor).toBeNull();
    expect(page.items).toEqual([
      expect.objectContaining({
        id: "span-1",
        traceId: "trace-1",
        projectId: "project-1",
        inputPreview: '{"question":"price"}',
        outputPreview: '{"answer":"ok"}',
        latency: 2.5,
        timeToFirstToken: 0.5,
        totalUsage: 15,
        totalCost: 0.125,
      }),
    ]);
    expect(query).toHaveBeenCalledWith(
      expect.stringContaining("LEFT JOIN trace_tombstones"),
      expect.arrayContaining(["project-1", 3]),
    );
    expect(query.mock.calls[0]?.[0]).not.toContain("e.input AS input");
  });

  it("resolves an ID-only detail through Postgres before its exact Doris scan", async () => {
    const locateObservation = vi.fn().mockResolvedValue([
      {
        partitionDate: "2026-07-17",
        traceId: "trace-1",
        observationId: "span-1",
      },
    ]);
    const query = vi.fn().mockResolvedValue([
      {
        ...row,
        input: '{"question":"price"}',
        output: '"true"',
        metadata: '{"region":"eu"}',
        model_parameters: '{"temperature":0}',
        tool_definitions: '{"search":"{}"}',
        tool_calls: '["search"]',
        tool_call_names: '["search"]',
      },
    ]);
    const repository = new DorisObservationsRepository({
      query,
      locateObservation,
    });

    await expect(
      repository.get({ projectId: "project-1", observationId: "span-1" }),
    ).resolves.toEqual(
      expect.objectContaining({
        id: "span-1",
        input: { question: "price" },
        output: "true",
        metadata: { region: "eu" },
        toolDefinitions: { search: "{}" },
      }),
    );
    expect(locateObservation).toHaveBeenCalledWith({
      projectId: "project-1",
      observationId: "span-1",
      traceId: undefined,
    });
    expect(query.mock.calls[0]?.[0]).toContain("e.partition_date >= ?");
    expect(query.mock.calls[0]?.[0]).toContain("e.span_id IN (?)");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["project-1", "2026-07-17", "span-1"]),
    );
  });

  it("rejects an ambiguous ID-only lookup instead of selecting by arrival order", async () => {
    const locateObservation = vi.fn().mockResolvedValue([
      {
        partitionDate: "2026-07-18",
        traceId: "trace-b",
        observationId: "shared-span",
      },
      {
        partitionDate: "2026-07-17",
        traceId: "trace-a",
        observationId: "shared-span",
      },
    ]);
    const query = vi.fn();
    const repository = new DorisObservationsRepository({
      query,
      locateObservation,
    });

    await expect(
      repository.get({
        projectId: "project-1",
        observationId: "shared-span",
      }),
    ).rejects.toMatchObject({ httpCode: 409 });
    expect(query).not.toHaveBeenCalled();
  });

  it("resolves trace-only list scope to exact immutable partitions", async () => {
    const locateTrace = vi.fn().mockResolvedValue([
      {
        partitionDate: "2026-07-17",
        traceId: "trace-1",
        observationId: "span-1",
      },
      {
        partitionDate: "2026-07-18",
        traceId: "trace-1",
        observationId: "span-2",
      },
    ]);
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisObservationsRepository({ query, locateTrace });

    await repository.listForTrace({
      projectId: "project-1",
      traceId: "trace-1",
      filters: [],
      limit: 10,
    });

    expect(locateTrace).toHaveBeenCalledWith({
      projectId: "project-1",
      traceId: "trace-1",
    });
    expect(query.mock.calls[0]?.[0]).toContain("e.partition_date IN (?, ?)");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["2026-07-17", "2026-07-18", "trace-1"]),
    );
  });

  it("enforces the 30-day cap when a list projects full content", async () => {
    const repository = new DorisObservationsRepository({ query: vi.fn() });

    await expect(
      repository.list({
        projectId: "project-1",
        range: {
          from: new Date("2026-01-01T00:00:00.000Z"),
          to: new Date("2026-02-01T00:00:00.001Z"),
        },
        filters: [],
        includeFullContent: true,
        limit: 10,
      }),
    ).rejects.toEqual(
      expect.objectContaining({ code: "InvalidTimeRange", maxDays: 30 }),
    );
  });

  it("treats a trace-scoped full-content read as point detail", async () => {
    const locateTrace = vi.fn().mockResolvedValue([
      {
        partitionDate: "2026-01-01",
        traceId: "trace-long",
        observationId: "span-1",
      },
      {
        partitionDate: "2026-02-02",
        traceId: "trace-long",
        observationId: "span-2",
      },
    ]);
    const query = vi.fn().mockResolvedValue([]);
    const repository = new DorisObservationsRepository({ query, locateTrace });

    await expect(
      repository.listForTrace({
        projectId: "project-1",
        traceId: "trace-long",
        filters: [],
        includeFullContent: true,
        limit: 10,
      }),
    ).resolves.toEqual({ items: [], nextCursor: null });
    expect(query.mock.calls[0]?.[0]).toContain("e.input AS input");
  });

  it("applies a requested order and offset", async () => {
    const query = vi.fn().mockResolvedValue([]);
    const repository = new DorisObservationsRepository({ query });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [],
      orderBy: { column: "totalCost", order: "ASC" },
      offset: 40,
      limit: 20,
    });

    expect(query.mock.calls[0]?.[0]).toContain(
      "ORDER BY e.total_cost ASC, e.start_time ASC, e.trace_id ASC, e.span_id ASC",
    );
    expect(query.mock.calls[0]?.[1].slice(-2)).toEqual([21, 40]);
  });

  it("counts through the same bounded visibility scope", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "7" }]);
    const repository = new DorisObservationsRepository({ query });

    await expect(
      repository.count({ projectId: "project-1", range, filters: [] }),
    ).resolves.toBe(7);
    expect(query.mock.calls[0]?.[0]).toContain("SELECT COUNT(*) AS count");
    expect(query.mock.calls[0]?.[0]).toContain(
      "trace_deletion.trace_id IS NULL",
    );
  });

  it("counts observations and distinct traces through one scope", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "7", trace_count: "3" }]);
    const repository = new DorisObservationsRepository({ query });

    await expect(
      repository.counts({ projectId: "project-1", range, filters: [] }),
    ).resolves.toEqual({ totalCount: 7, uniqueTraceCount: 3 });
    expect(query.mock.calls[0]?.[0]).toContain(
      "COUNT(DISTINCT e.trace_id) AS trace_count",
    );
  });

  it("returns bounded scalar and array filter options from allowlisted columns", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([{ value: "generation", count: "4" }])
      .mockResolvedValueOnce([{ value: "prod", count: "2" }]);
    const repository = new DorisObservationsRepository({ query });

    await expect(
      repository.filterOptionValues({
        projectId: "project-1",
        range,
        filters: [],
        column: "name",
        limit: 10,
        offset: 3,
      }),
    ).resolves.toEqual([{ column: "name", value: "generation", count: 4 }]);
    await expect(
      repository.filterOptionValues({
        projectId: "project-1",
        range,
        filters: [],
        column: "traceTags",
        limit: 10,
      }),
    ).resolves.toEqual([{ column: "traceTags", value: "prod", count: 2 }]);

    expect(query.mock.calls[0]?.[0]).toContain("e.name AS value");
    expect(query.mock.calls[0]?.[0]).toContain("e.partition_date >= ?");
    expect(query.mock.calls[0]?.[0]).toContain(
      "ORDER BY count DESC, value ASC",
    );
    expect(query.mock.calls[0]?.[1].slice(-2)).toEqual([10, 3]);
    expect(query.mock.calls[1]?.[0]).toContain(
      "LATERAL VIEW explode(scoped.facet_values) exploded AS value",
    );
  });

  it("computes bounded numeric filter statistics from an allowlisted expression", async () => {
    const query = vi
      .fn()
      .mockResolvedValue([{ min: "0.5", max: "4", avg: "2.25", count: "8" }]);
    const repository = new DorisObservationsRepository({ query });

    await expect(
      repository.numericStats({
        projectId: "project-1",
        range,
        filters: [],
        column: "latency",
      }),
    ).resolves.toEqual({ min: 0.5, max: 4, avg: 2.25, count: 8 });
    expect(query.mock.calls[0]?.[0]).toContain(
      "TIMESTAMPDIFF(MICROSECOND, e.start_time, e.end_time)",
    );
    expect(query.mock.calls[0]?.[0]).toContain("e.project_id = ?");
  });

  it("enforces the full-content range cap for tool facets and statistics", async () => {
    const query = vi.fn().mockResolvedValue([]);
    const repository = new DorisObservationsRepository({ query });
    const overlongRange = {
      from: new Date("2026-01-01T00:00:00.000Z"),
      to: new Date("2026-02-01T00:00:00.001Z"),
    };

    await expect(
      repository.filterOptionValues({
        projectId: "project-1",
        range: overlongRange,
        filters: [],
        column: "toolNames",
        limit: 10,
      }),
    ).rejects.toMatchObject({ code: "InvalidTimeRange", maxDays: 30 });
    await expect(
      repository.numericStats({
        projectId: "project-1",
        range: overlongRange,
        filters: [],
        column: "toolCalls",
      }),
    ).rejects.toMatchObject({ code: "InvalidTimeRange", maxDays: 30 });
    expect(query).not.toHaveBeenCalled();
  });

  it("rejects inactive experiment facets before issuing Doris SQL", async () => {
    const query = vi.fn();
    const repository = new DorisObservationsRepository({ query });

    await expect(
      repository.filterOptionValues({
        projectId: "project-1",
        range,
        filters: [],
        column: "experimentId" as "name",
        limit: 10,
      }),
    ).rejects.toMatchObject({ httpCode: 400 });
    expect(query).not.toHaveBeenCalled();
  });

  it("reads recent SDK attribution through the bounded visible event scope", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        ingestion_sdk_name: "js",
        ingestion_sdk_version: "4.1.0",
        telemetry_sdk_language: "javascript",
      },
    ]);
    const repository = new DorisObservationsRepository({ query });

    await expect(
      repository.latestSdkMetadata({
        projectId: "project-1",
        range,
      }),
    ).resolves.toEqual({
      isOtel: true,
      name: "js",
      version: "4.1.0",
      language: "javascript",
    });
    expect(query.mock.calls[0]?.[0]).toContain("e.`source` LIKE 'otel%'");
    expect(query.mock.calls[0]?.[0]).toContain("ORDER BY e.start_time DESC");
    expect(query.mock.calls[0]?.[0]).toContain("e.project_id = ?");
  });
});
