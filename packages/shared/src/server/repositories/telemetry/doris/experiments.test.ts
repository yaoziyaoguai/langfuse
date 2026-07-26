import { describe, expect, it, vi } from "vitest";

import { DorisExperimentsRepository } from "./experiments";

describe("Doris experiments repository", () => {
  it("lists experiment summaries from the visible dataset-run projection", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        experiment_id: "run-1",
        experiment_name: "quality check",
        experiment_description: "description",
        experiment_dataset_id: "dataset-1",
        start_time: "2026-07-24 09:59:00.000000",
        item_count: "2",
        error_count: "1",
        prompts: '[["support-prompt",3]]',
        experiment_metadata:
          '{"experiment_name":"quality check","model":"gpt-4.1"}',
      },
    ]);
    const repository = new DorisExperimentsRepository({ query });

    await expect(
      repository.list({
        projectId: "project-1",
        filters: [
          {
            type: "string",
            column: "id",
            operator: "=",
            value: "run-1",
          },
        ],
        limit: 10,
        page: 0,
      }),
    ).resolves.toEqual([
      {
        id: "run-1",
        name: "quality check",
        description: "description",
        datasetId: "dataset-1",
        startTime: new Date("2026-07-24T09:59:00.000Z"),
        itemCount: 2,
        errorCount: 1,
        prompts: [["support-prompt", 3]],
        metadata: {
          experiment_name: "quality check",
          model: "gpt-4.1",
        },
      },
    ]);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM dataset_run_items_current dri");
    expect(sql).toContain("JSON_EXTRACT_STRING");
    expect(sql).toContain("COUNT(DISTINCT dri.dataset_item_id)");
    expect(sql).toContain("dri.dataset_run_id = ?");
    expect(sql).toContain("event_row.trace_id = dri.trace_id");
    expect(sql).not.toContain("event_row.experiment_id = dri.dataset_run_id");
    expect(sql).not.toContain("project-1");
    expect(query.mock.calls[0]?.[1]).toEqual(["project-1", "run-1", 10]);
  });

  it("returns root experiment item metrics in stable item order", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([
        { dataset_item_id: "item-1" },
        { dataset_item_id: "item-2" },
      ])
      .mockResolvedValueOnce([
        {
          item_id: "item-1",
          experiment_id: "run-1",
          level: "DEFAULT",
          start_time: "2026-07-24 10:00:00.000000",
          total_cost: "0.2",
          latency_ms: "150",
          observation_id: "span-1",
          trace_id: "trace-1",
        },
      ]);
    const repository = new DorisExperimentsRepository({ query });

    await expect(
      repository.items({
        projectId: "project-1",
        baseExperimentId: "run-1",
        compExperimentIds: ["run-2"],
        filtersByExperiment: [],
        requireBaselinePresence: true,
        limit: 20,
        offset: 0,
      }),
    ).resolves.toEqual([
      {
        itemId: "item-1",
        experiments: [
          {
            experimentId: "run-1",
            level: "DEFAULT",
            startTime: new Date("2026-07-24T10:00:00.000Z"),
            totalCost: 0.2,
            latencyMs: 150,
            observationId: "span-1",
            traceId: "trace-1",
          },
        ],
      },
      { itemId: "item-2", experiments: [] },
    ]);

    const sql = String(query.mock.calls[1]?.[0]);
    expect(sql).toContain("FROM dataset_run_items_current dri");
    expect(sql).toContain("JOIN events_current event_row");
    expect(sql).toContain("event_row.trace_id = dri.trace_id");
    expect(sql).toContain("event_row.span_id = dri.observation_id");
    expect(sql).toContain("event_row.is_app_root = TRUE");
    expect(sql).not.toContain("event_row.experiment_id IN");
    expect(sql).toContain(
      "MICROSECONDS_DIFF(event_row.end_time, event_row.start_time) / 1000.0",
    );
    expect(sql).not.toContain("TIMESTAMPDIFF(MICROSECOND");
    expect(sql).toContain("dri.observation_id IS NULL");
    expect(sql).toContain("ROW_NUMBER() OVER");
  });

  it("returns truncated baseline IO and outputs from canonical rows", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([
        {
          item_id: "item-1",
          experiment_id: "run-1",
          input: '{"question":"hello"}',
          expected_output: '{"answer":"world"}',
        },
      ])
      .mockResolvedValueOnce([
        {
          item_id: "item-1",
          experiment_id: "run-1",
          output: '{"answer":"world"}',
        },
      ]);
    const repository = new DorisExperimentsRepository({ query });

    await expect(
      repository.batchIO({
        projectId: "project-1",
        itemIds: ["item-1"],
        baseExperimentId: "run-1",
        compExperimentIds: [],
      }),
    ).resolves.toEqual([
      {
        itemId: "item-1",
        input: '{"question":"hello"}',
        expectedOutput: '{"answer":"world"}',
        outputs: [{ experimentId: "run-1", output: '{"answer":"world"}' }],
      },
    ]);
    expect(query.mock.calls[0]?.[0]).toContain(
      "LEFT(dri.dataset_item_input, ?)",
    );
    expect(query.mock.calls[1]?.[0]).toContain("LEFT(event_row.output, ?)");
    expect(query.mock.calls[0]?.[0]).toContain(
      "LEFT JOIN dataset_run_tombstones",
    );
    expect(query.mock.calls[1]?.[0]).toContain(
      "FROM dataset_run_items_current dri",
    );
    expect(query.mock.calls[1]?.[0]).toContain(
      "event_row.trace_id = dri.trace_id",
    );
    expect(query.mock.calls[1]?.[0]).not.toContain(
      "event_row.experiment_id IN",
    );
    expect(query.mock.calls[1]?.[0]).toContain("LEFT JOIN trace_tombstones");
  });

  it("applies experiment-level observation score filters in Doris", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "1" }]);
    const repository = new DorisExperimentsRepository({ query });

    await expect(
      repository.count({
        projectId: "project-1",
        filters: [
          {
            type: "numberObject",
            column: "obs_scores_avg",
            key: "quality",
            operator: ">=",
            value: 0.8,
          },
        ],
      }),
    ).resolves.toBe(1);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM scores_current score_filter");
    expect(sql).toContain("score_relation.dataset_run_id = dri.dataset_run_id");
    expect(sql).toContain("experiment_event.trace_id = score_dri.trace_id");
    expect(sql).not.toContain("experiment_event.experiment_id");
    expect(sql).toContain(
      "score_relation.observation_id = score_filter.observation_id",
    );
    expect(sql).toContain("HAVING AVG(COALESCE(");
    expect(query.mock.calls[0]?.[1]).toEqual(["project-1", "quality", 0.8]);
  });

  it("returns score filter options without mixing experiment score levels", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        name: "quality",
        source: "EVAL",
        data_type: "NUMERIC",
        categorical_value: null,
      },
      {
        name: "approved",
        source: "ANNOTATION",
        data_type: "BOOLEAN",
        categorical_value: null,
      },
      {
        name: "label",
        source: "API",
        data_type: "CATEGORICAL",
        categorical_value: "good",
      },
    ]);
    const repository = new DorisExperimentsRepository({ query });

    await expect(
      repository.scoreFilterOptions({
        projectId: "project-1",
        experimentIds: ["run-1", "run-1"],
        level: "observation",
      }),
    ).resolves.toEqual({
      numeric: ["quality", "approved"],
      boolean: ["approved"],
      categorical: [{ label: "label", values: ["good"] }],
      scoreColumns: [
        { name: "quality", dataType: "NUMERIC", source: "EVAL" },
        { name: "approved", dataType: "BOOLEAN", source: "ANNOTATION" },
        { name: "label", dataType: "CATEGORICAL", source: "API" },
      ],
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM dataset_run_items_current score_dri");
    expect(sql).toContain("score_dri.trace_id = s.trace_id");
    expect(sql).toContain("experiment_event.span_id = s.observation_id");
    expect(sql).not.toContain("experiment_event.experiment_id");
    expect(sql).not.toContain("s.dataset_run_id IN");
    expect(query.mock.calls[0]?.[1]).toEqual(["project-1", "run-1"]);
  });

  it("counts qualified experiment items in Doris without materializing ids", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "4" }]);
    const repository = new DorisExperimentsRepository({ query });

    await expect(
      repository.itemsCount({
        projectId: "project-1",
        baseExperimentId: "run-1",
        compExperimentIds: ["run-2"],
        filtersByExperiment: [],
        requireBaselinePresence: true,
      }),
    ).resolves.toBe(4);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("SELECT COUNT(*) AS count");
    expect(sql).toContain("FROM (");
  });

  it("lists public experiment summaries with a backend-stable cursor", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        experiment_id: "run-1",
        experiment_name: "quality",
        experiment_description: null,
        experiment_dataset_id: "dataset-1",
        start_time: "2026-07-24 09:00:00.000000",
        end_time: "2026-07-24 10:00:00.000000",
        cursor_time: "2026-07-24 10:00:00.000000",
        cursor_trace_id: "trace-1",
        cursor_span_id: "span-1",
        item_count: "2",
        experiment_metadata: '{"model":"gpt-4.1"}',
      },
    ]);
    const repository = new DorisExperimentsRepository({ query });

    await expect(
      repository.publicSummaries({
        projectId: "project-1",
        fromTime: new Date("2026-07-01T00:00:00.000Z"),
        toTime: new Date("2026-08-01T00:00:00.000Z"),
        id: ["run-1"],
        advancedFilters: [
          {
            type: "stringOptions",
            column: "name",
            operator: "any of",
            value: ["quality"],
          },
        ],
        cursor: {
          lastTime: "2026-07-25 00:00:00.000000",
          lastTraceId: "trace-2",
          lastId: "span-2",
          lastExperimentId: "run-2",
        },
        includeMetadata: true,
        limit: 11,
      }),
    ).resolves.toEqual([
      {
        experiment_id: "run-1",
        experiment_name: "quality",
        experiment_description: null,
        experiment_dataset_id: "dataset-1",
        start_time: "2026-07-24 09:00:00.000000",
        end_time: "2026-07-24 10:00:00.000000",
        cursor_time: "2026-07-24 10:00:00.000000",
        cursor_trace_id: "trace-1",
        cursor_span_id: "span-1",
        item_count: 2,
        experiment_metadata: { model: "gpt-4.1" },
      },
    ]);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM dataset_run_items_current dri");
    expect(sql).toContain("e.trace_id = dri.trace_id");
    expect(sql).toContain("ROW_NUMBER() OVER");
    expect(sql).toContain("GROUP BY experiment_id");
    expect(sql).toContain("cursor_time < ?");
    expect(sql).toContain("dri.dataset_run_id IN (?)");
    expect(sql).toContain("JSON_EXTRACT_STRING");
    expect(sql).not.toContain("e.experiment_id IS NOT NULL");
    expect(sql).not.toContain("e.experiment_name IN (?)");
    expect(sql).not.toContain("project-1");
  });

  it("projects only requested public experiment-item field groups", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        span_id: "span-1",
        trace_id: "trace-1",
        start_time: "2026-07-24 10:00:00.000000",
        end_time: null,
        level: "DEFAULT",
        environment: "langfuse-prompt-experiment",
        experiment_id: "run-1",
        experiment_name: "quality",
        experiment_item_id: "item-1",
        input: '{"question":"hello"}',
        output: '{"answer":"world"}',
        experiment_item_expected_output: '{"answer":"world"}',
      },
    ]);
    const repository = new DorisExperimentsRepository({ query });

    await expect(
      repository.publicItems({
        projectId: "project-1",
        fromTime: new Date("2026-07-01T00:00:00.000Z"),
        toTime: new Date("2026-08-01T00:00:00.000Z"),
        experimentId: ["run-1"],
        includeDataset: false,
        includeIo: true,
        includeMetadata: false,
        includeItemMetadata: false,
        includeExperimentMetadata: false,
        limit: 11,
      }),
    ).resolves.toEqual([
      {
        id: "span-1",
        trace_id: "trace-1",
        start_time: "2026-07-24 10:00:00.000000",
        end_time: null,
        level: "DEFAULT",
        environment: "langfuse-prompt-experiment",
        experiment_id: "run-1",
        experiment_name: "quality",
        experiment_item_id: "item-1",
        input: { question: "hello" },
        output: { answer: "world" },
        experiment_item_expected_output: { answer: "world" },
      },
    ]);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM dataset_run_items_current dri");
    expect(sql).toContain("e.trace_id = dri.trace_id");
    expect(sql).toContain("dri.dataset_run_id AS experiment_id");
    expect(sql).toContain("e.input");
    expect(sql).not.toContain("AS metadata");
    expect(sql).not.toContain("e.experiment_item_version");
    expect(sql).not.toContain("e.experiment_id IS NOT NULL");
    expect(sql).toContain("e.span_id = dri.observation_id");
  });
});
