import { describe, expect, it, vi } from "vitest";

import { DorisDatasetRunItemsRepository } from "./datasetRunItems";

const row = {
  project_id: "project-1",
  run_item_id: "run-item-1",
  dataset_run_id: "run-1",
  dataset_item_id: "item-1",
  dataset_id: "dataset-1",
  trace_id: "trace-1",
  observation_id: "span-1",
  error: null,
  created_at: "2026-07-24 10:00:00.000000",
  updated_at: "2026-07-24 10:00:01.000000",
  dataset_run_name: "prompt experiment",
  dataset_run_description: "description",
  dataset_run_metadata: '{"model":"gpt-4.1"}',
  dataset_run_created_at: "2026-07-24 09:59:00.000000",
  dataset_item_version: "2026-07-23 08:00:00.000000",
  dataset_item_input: '{"question":"hello"}',
  dataset_item_expected_output: '{"answer":"world"}',
  dataset_item_metadata: '{"split":"test"}',
};

describe("Doris dataset run items repository", () => {
  it("lists visible project-scoped items and decodes the canonical projection", async () => {
    const query = vi.fn().mockResolvedValue([row]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    await expect(
      repository.list({
        projectId: "project-1",
        datasetId: "dataset-1",
        filters: [
          {
            type: "stringOptions",
            column: "datasetRunId",
            operator: "any of",
            value: ["run-1"],
          },
        ],
        orderBy: { column: "createdAt", order: "DESC" },
        limit: 20,
        offset: 0,
      }),
    ).resolves.toEqual([
      {
        id: "run-item-1",
        projectId: "project-1",
        datasetRunId: "run-1",
        datasetItemId: "item-1",
        datasetId: "dataset-1",
        traceId: "trace-1",
        observationId: "span-1",
        error: null,
        createdAt: new Date("2026-07-24T10:00:00.000Z"),
        updatedAt: new Date("2026-07-24T10:00:01.000Z"),
        datasetRunName: "prompt experiment",
        datasetRunDescription: "description",
        datasetRunMetadata: { model: "gpt-4.1" },
        datasetRunCreatedAt: new Date("2026-07-24T09:59:00.000Z"),
        datasetItemVersion: new Date("2026-07-23T08:00:00.000Z"),
        datasetItemInput: { question: "hello" },
        datasetItemExpectedOutput: { answer: "world" },
        datasetItemMetadata: { split: "test" },
      },
    ]);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM dataset_run_items_current dri");
    expect(sql).toContain("LEFT JOIN dataset_tombstones");
    expect(sql).toContain("LEFT JOIN dataset_run_tombstones");
    expect(sql).toContain("LEFT JOIN project_tombstones");
    expect(sql).toContain("dri.project_id = ?");
    expect(sql).toContain("dri.dataset_run_id IN (?)");
    expect(sql).toContain("ORDER BY dri.created_at DESC, dri.run_item_id DESC");
    expect(sql).not.toContain("project-1");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "project-1",
      "dataset-1",
      "run-1",
      20,
    ]);
  });

  it("counts through the identical visibility and score-filter scope", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "2" }]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    await expect(
      repository.count({
        projectId: "project-1",
        datasetId: "dataset-1",
        filters: [
          {
            type: "numberObject",
            column: "agg_scores_avg",
            key: "quality",
            operator: ">=",
            value: 0.8,
          },
        ],
      }),
    ).resolves.toBe(2);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM scores_current score_filter");
    expect(sql).toContain("HAVING AVG(COALESCE(");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "project-1",
      "dataset-1",
      "quality",
      0.8,
    ]);
  });

  it("keeps observation and trace score filters on their native levels", async () => {
    const query = vi.fn().mockResolvedValue([{ count: "1" }]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    await repository.count({
      projectId: "project-1",
      filters: [
        {
          type: "numberObject",
          column: "obs_scores_avg",
          key: "root-quality",
          operator: ">",
          value: 0.5,
        },
        {
          type: "booleanObject",
          column: "trace_score_booleans",
          key: "approved",
          operator: "=",
          value: true,
        },
      ],
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("score_filter.observation_id IN (");
    expect(sql).toContain("experiment_event.trace_id = dri.trace_id");
    expect(sql).not.toContain("experiment_event.experiment_id");
    expect(sql).toContain("score_filter.observation_id IS NULL");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "project-1",
      "root-quality",
      0.5,
      "approved",
      true,
    ]);
  });

  it("returns stable existing dataset-item identities for experiment retry dedupe", async () => {
    const query = vi.fn().mockResolvedValue([{ dataset_item_id: "item-1" }]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    await expect(
      repository.existingDatasetItemIds({
        projectId: "project-1",
        datasetId: "dataset-1",
        datasetRunId: "run-1",
      }),
    ).resolves.toEqual(new Set(["item-1"]));

    expect(query.mock.calls[0]?.[1]).toEqual([
      "project-1",
      "dataset-1",
      "run-1",
    ]);
  });

  it("resolves visible run version timestamps without crossing project scope", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        max_created_at: "2026-07-24 10:00:00.000000",
        max_dataset_item_version: "2026-07-23 08:00:00.000000",
      },
    ]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    await expect(
      repository.versionTimestamps({
        projectId: "project-1",
        datasetId: "dataset-1",
        datasetRunId: "run-1",
      }),
    ).resolves.toEqual({
      maxCreatedAt: new Date("2026-07-24T10:00:00.000Z"),
      maxDatasetItemVersion: new Date("2026-07-23T08:00:00.000Z"),
    });

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("FROM dataset_run_items_current dri");
    expect(sql).toContain("LEFT JOIN dataset_tombstones");
    expect(sql).toContain("LEFT JOIN dataset_run_tombstones");
    expect(sql).toContain("LEFT JOIN project_tombstones");
    expect(sql).toContain("dri.project_id = ?");
    expect(sql).toContain("dri.dataset_id = ?");
    expect(sql).toContain("dri.dataset_run_id = ?");
    expect(sql).not.toContain("project-1");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "project-1",
      "dataset-1",
      "run-1",
    ]);
  });

  it("applies per-run filters before selecting comparison item identities", async () => {
    const query = vi.fn().mockResolvedValue([{ dataset_item_id: "item-1" }]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    await expect(
      repository.qualifyingDatasetItemIds({
        projectId: "project-1",
        datasetId: "dataset-1",
        runIds: ["run-1", "run-2"],
        filtersByRun: [
          {
            runId: "run-1",
            filters: [
              {
                type: "numberObject",
                column: "agg_scores_avg",
                key: "quality",
                operator: ">",
                value: 0.8,
              },
            ],
          },
        ],
        limit: 25,
        offset: 0,
      }),
    ).resolves.toEqual(["item-1"]);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("dri.dataset_run_id = ?");
    expect(sql).toContain("COUNT(DISTINCT dri.dataset_run_id) = ?");
    expect(sql).toContain("HAVING");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "project-1",
      "dataset-1",
      "run-1",
      "quality",
      0.8,
      "run-2",
      2,
      25,
    ]);
  });

  it("loads comparison rows without exposing item input or metadata", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        ...row,
        dataset_run_metadata: undefined,
        dataset_item_input: undefined,
        dataset_item_expected_output: undefined,
        dataset_item_metadata: undefined,
      },
    ]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    const items = await repository.listWithoutIOByItemIds({
      projectId: "project-1",
      datasetId: "dataset-1",
      runIds: ["run-1"],
      datasetItemIds: ["item-1"],
    });

    expect(items).toEqual([
      expect.not.objectContaining({ datasetItemInput: expect.anything() }),
    ]);
    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).not.toContain("dataset_item_input");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "project-1",
      "dataset-1",
      "run-1",
      "item-1",
    ]);
  });

  it("resolves dataset targets for a trace without crossing project scope", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        dataset_item_id: "item-1",
        dataset_id: "dataset-1",
        observation_id: "span-1",
      },
    ]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    await expect(
      repository.datasetItemIdsByTraceId({
        projectId: "project-1",
        traceId: "trace-1",
        filters: [],
      }),
    ).resolves.toEqual([
      {
        id: "item-1",
        datasetId: "dataset-1",
        observationId: "span-1",
      },
    ]);

    expect(query.mock.calls[0]?.[1]).toEqual(["project-1", "trace-1"]);
  });

  it("aggregates run metrics from visible run items and canonical events", async () => {
    const query = vi.fn().mockResolvedValue([
      {
        dataset_run_id: "run-1",
        dataset_run_name: "prompt experiment",
        project_id: "project-1",
        dataset_id: "dataset-1",
        count_run_items: "2",
        avg_latency_seconds: "1.25",
        avg_total_cost: "0.4",
        total_cost: "0.8",
      },
    ]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    const metrics = await repository.runMetrics({
      projectId: "project-1",
      datasetId: "dataset-1",
      runIds: ["run-1"],
      filters: [],
    });

    expect(metrics).toEqual([
      expect.objectContaining({
        id: "run-1",
        name: "prompt experiment",
        countRunItems: 2,
        avgLatency: 1.25,
      }),
    ]);
    expect(metrics[0]?.avgTotalCost.toNumber()).toBe(0.4);
    expect(metrics[0]?.totalCost.toNumber()).toBe(0.8);

    const sql = String(query.mock.calls[0]?.[0]);
    expect(sql).toContain("visible_run_items AS");
    expect(sql).toContain("FROM events_current event_row");
    expect(sql).toContain("trace_metrics AS");
    expect(sql).toContain("MICROSECONDS_DIFF(MAX(end_time), MIN(start_time))");
    expect(sql).not.toContain("TIMESTAMPDIFF(MICROSECOND");
    expect(query.mock.calls[0]?.[1]).toEqual([
      "project-1",
      "dataset-1",
      "run-1",
    ]);
  });

  it("lists and counts distinct dataset runs through one filter scope", async () => {
    const query = vi
      .fn()
      .mockResolvedValueOnce([
        {
          dataset_run_id: "run-1",
          dataset_run_name: "prompt experiment",
          project_id: "project-1",
          dataset_id: "dataset-1",
          dataset_run_created_at: "2026-07-24 09:59:00.000000",
          dataset_run_description: "description",
          dataset_run_metadata: '{"model":"gpt-4.1"}',
        },
      ])
      .mockResolvedValueOnce([{ count: "1" }]);
    const repository = new DorisDatasetRunItemsRepository({ query });

    await expect(
      repository.runRows({
        projectId: "project-1",
        datasetId: "dataset-1",
        filters: [],
        limit: 10,
        offset: 0,
      }),
    ).resolves.toEqual([
      {
        id: "run-1",
        name: "prompt experiment",
        projectId: "project-1",
        datasetId: "dataset-1",
        createdAt: new Date("2026-07-24T09:59:00.000Z"),
        description: "description",
        metadata: '{"model":"gpt-4.1"}',
      },
    ]);
    await expect(
      repository.runCount({
        projectId: "project-1",
        datasetId: "dataset-1",
        filters: [],
      }),
    ).resolves.toBe(1);
    expect(query.mock.calls[1]?.[0]).toContain(
      "COUNT(DISTINCT dri.dataset_run_id)",
    );
  });

  it("rejects uncatalogued filters and order expressions before analytics IO", async () => {
    const query = vi.fn();
    const repository = new DorisDatasetRunItemsRepository({ query });

    await expect(
      repository.list({
        projectId: "project-1",
        filters: [
          {
            type: "string",
            column: "unsafe_sql",
            operator: "=",
            value: "x",
          },
        ],
      }),
    ).rejects.toThrow("Unsupported Doris dataset-run-item filter");

    await expect(
      repository.list({
        projectId: "project-1",
        filters: [],
        orderBy: { column: "unsafe_sql", order: "ASC" },
      }),
    ).rejects.toThrow("Unsupported Doris dataset-run-item order column");
    expect(query).not.toHaveBeenCalled();
  });
});
