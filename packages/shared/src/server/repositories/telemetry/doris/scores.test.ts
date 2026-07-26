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
  dataset_run_id: "run-1",
  execution_trace_id: "execution-trace-1",
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
          datasetRunId: "run-1",
          executionTraceId: "execution-trace-1",
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
    expect(sql).toContain(
      "COALESCE(s.metadata_json, CAST(s.metadata AS STRING)) AS metadata",
    );
    expect(sql).toContain("ORDER BY s.`timestamp` DESC, s.score_id DESC");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["project-1", "quality", 11]),
    );
  });

  it("filters experiment scores by dataset run id", async () => {
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisScoresRepository({ query });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [
        {
          type: "stringOptions",
          column: "datasetRunId",
          operator: "any of",
          value: ["run-1"],
        },
      ],
      limit: 10,
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("s.dataset_run_id IN (?)");
    expect(sql).toContain("s.execution_trace_id");
    expect(query.mock.calls[0]?.[1]).toEqual(expect.arrayContaining(["run-1"]));
  });

  it("supports score-table dataset and experiment filters without duplicating scores", async () => {
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisScoresRepository({ query });

    await repository.list({
      projectId: "project-1",
      range,
      filters: [
        {
          type: "stringOptions",
          column: "datasetRunIds",
          operator: "any of",
          value: ["direct-run-1"],
        },
        {
          type: "stringOptions",
          column: "datasetRunItemRunIds",
          operator: "any of",
          value: ["run-1"],
        },
        {
          type: "stringOptions",
          column: "datasetId",
          operator: "any of",
          value: ["dataset-1"],
        },
        {
          type: "stringOptions",
          column: "datasetItemIds",
          operator: "any of",
          value: ["item-1"],
        },
        {
          type: "stringOptions",
          column: "experimentIds",
          operator: "any of",
          value: ["experiment-1"],
        },
      ],
      limit: 10,
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("s.dataset_run_id IN (?)");
    expect(sql).toContain("EXISTS (");
    expect(sql).toContain("FROM dataset_run_items_current dri");
    expect(sql).toContain("dri.project_id = s.project_id");
    expect(sql).toContain("dri.trace_id = s.trace_id");
    expect(sql).toContain("dri.dataset_run_id IN (?)");
    expect(sql).toContain("dri.dataset_id IN (?)");
    expect(sql).toContain("dri.dataset_item_id IN (?)");
    expect(sql).toContain("FROM dataset_tombstones dataset_deletion");
    expect(sql).toContain("FROM dataset_run_tombstones run_deletion");
    expect(sql).not.toContain("JOIN dataset_run_items_current dri");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining([
        "direct-run-1",
        "run-1",
        "dataset-1",
        "item-1",
        "experiment-1",
      ]),
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
      "ORDER BY s.`name` IS NULL ASC, s.`name` ASC, s.`timestamp` DESC, s.score_id DESC",
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

  it("rejects score filters that exceed the shared Doris resource budget", async () => {
    const query = vi.fn().mockResolvedValue([]);
    const repository = new DorisScoresRepository({ query });

    await expect(
      repository.list({
        projectId: "project-1",
        range,
        filters: Array.from({ length: 101 }, (_, index) => ({
          type: "string" as const,
          column: "name",
          operator: "=" as const,
          value: `score-${index}`,
        })),
        limit: 10,
      }),
    ).rejects.toThrow("too many filters");
    expect(query).not.toHaveBeenCalled();
  });

  it("pushes trace-backed filters and ordering into one project-scoped query", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        ...row,
        context_trace_id: "trace-1",
        trace_name: "checkout",
        trace_user_id: "user-1",
        trace_tags: ["prod"],
        trace_environment: "production",
        trace_session_id: "session-1",
      },
    ]);
    const repository = new DorisScoresRepository({ query });

    await expect(
      repository.list({
        projectId: "project-1",
        range,
        filters: [
          {
            type: "string",
            column: "traceName",
            operator: "contains",
            value: "check",
          },
          {
            type: "arrayOptions",
            column: "trace_tags",
            operator: "all of",
            value: ["prod"],
          },
        ],
        limit: 10,
        orderBy: { column: "traceName", order: "ASC" },
        includeTraceContext: true,
      }),
    ).resolves.toEqual({
      items: [
        expect.objectContaining({
          id: "score-1",
          trace: {
            name: "checkout",
            userId: "user-1",
            tags: ["prod"],
            environment: "production",
            sessionId: "session-1",
          },
        }),
      ],
      nextCursor: null,
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM events_current trace_event");
    expect(sql).toContain("FROM scores_current candidate_score");
    expect(sql).toContain("SELECT DISTINCT candidate_score.trace_id");
    expect(sql).toContain("candidate_score.score_date >= ?");
    expect(sql).toContain("candidate_score.`timestamp` >= ?");
    expect(sql).toContain("trace_event.project_id = ?");
    expect(sql).toContain("ROW_NUMBER() OVER (");
    expect(sql).toContain("ORDER BY trace_event.is_app_root DESC");
    expect(sql).toContain("trace_event.start_time ASC");
    expect(sql).toContain("WHERE representative_rank = 1");
    expect(sql).not.toContain("MAX(NULLIF(trace_event.user_id, ''))");
    expect(sql).not.toContain("ANY_VALUE(trace_event.tags)");
    expect(sql).toContain(") trace_ctx");
    expect(sql).toContain("LOCATE(?, COALESCE(trace_ctx.trace_name, '')) > 0");
    expect(sql).toContain("ARRAY_CONTAINS(trace_ctx.trace_tags, ?)");
    expect(sql).toContain(
      "ORDER BY trace_ctx.trace_name IS NULL ASC, trace_ctx.trace_name ASC, s.`timestamp` DESC, s.score_id DESC",
    );
    expect(sql).not.toContain("check");
    expect(query.mock.calls[0]?.[1]).toEqual(
      expect.arrayContaining(["project-1", "check", "prod"]),
    );
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

  it("matches dataset-run scores by their complete attachment identity", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        score1_count: "2",
        score2_count: "3",
        matched_count: "1",
      },
    ]);
    const repository = new DorisScoresRepository({ query });

    await expect(
      repository.comparisonCounts({
        projectId: "project-1",
        range,
        score1: {
          name: "quality",
          source: "API",
          dataType: "NUMERIC",
        },
        score2: {
          name: "correctness",
          source: "ANNOTATION",
          dataType: "NUMERIC",
        },
        objectType: "dataset_run",
      }),
    ).resolves.toEqual({
      score1Count: 2,
      score2Count: 3,
      matchedCount: 1,
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("s.dataset_run_id IS NOT NULL");
    expect(sql).toContain("s.trace_id IS NULL");
    expect(sql).toContain("s.observation_id IS NULL");
    expect(sql).toContain("s.session_id IS NULL");
    expect(sql).toContain(
      "SELECT s.trace_id, s.observation_id, s.session_id, s.dataset_run_id",
    );
    expect(sql).toContain(
      "COALESCE(a.dataset_run_id, '') = COALESCE(b.dataset_run_id, '')",
    );
  });

  it("samples dataset-run analytics with deterministic attachment ordering", async () => {
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisScoresRepository({ query });

    await repository.analyticsRows({
      projectId: "project-1",
      range,
      score: {
        name: "quality",
        source: "API",
        dataType: "NUMERIC",
      },
      objectType: "dataset_run",
      limit: 100,
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("s.dataset_run_id IS NOT NULL");
    expect(sql).toContain(
      "ORDER BY COALESCE(s.trace_id, ''), COALESCE(s.observation_id, ''), COALESCE(s.session_id, ''), COALESCE(s.dataset_run_id, ''), s.score_id ASC",
    );
  });
});
