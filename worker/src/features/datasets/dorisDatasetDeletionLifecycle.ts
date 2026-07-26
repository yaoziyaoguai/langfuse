import { createHash } from "node:crypto";

import {
  DorisError,
  getDorisQueryExecutor,
  type AnalyticsDatasetDeletionLease,
} from "@langfuse/shared/src/server";

import { createDorisLifecycleTransport } from "../../services/dorisAnalyticsLifecycle";

type DatasetDeletionBarrier = {
  readonly operationId: string;
  readonly projectId: string;
  readonly datasetId: string;
  readonly datasetGeneration: bigint | null;
  readonly runGenerations: Readonly<Record<string, bigint>>;
  readonly lease: AnalyticsDatasetDeletionLease;
  readonly createdAt: Date;
};

function label(
  input: DatasetDeletionBarrier,
  kind: "dataset" | "runs",
): string {
  const digest = createHash("sha256")
    .update(
      [
        "langfuse-dataset-delete-v1",
        input.operationId,
        input.lease.fence.toString(),
        kind,
      ].join("\0"),
    )
    .digest("hex");
  return `lf_dataset_delete_${digest}`;
}

async function loadVisible(input: {
  readonly table: "dataset_tombstones" | "dataset_run_tombstones";
  readonly label: string;
  readonly columns: readonly string[];
  readonly rows: readonly Record<string, string>[];
}): Promise<void> {
  if (input.rows.length === 0) return;
  const transport = createDorisLifecycleTransport();
  const result = await transport.load({
    table: input.table,
    label: input.label,
    columns: input.columns,
    ndjsonBody: `${input.rows.map((row) => JSON.stringify(row)).join("\n")}\n`,
  });
  let visible = result.committed && !result.requiresReconciliation;
  if (result.requiresReconciliation) {
    visible = (await transport.reconcile({ label: input.label })).visible;
  }
  if (
    !visible ||
    result.numberFilteredRows !== 0 ||
    result.numberTotalRows !== input.rows.length
  ) {
    throw new DorisError(
      result.numberFilteredRows > 0 ? "FILTERED_ROWS" : "ANALYTICS_UNAVAILABLE",
      result.numberFilteredRows === 0,
    );
  }
}

export async function writeDorisDatasetDeletionBarrier(
  input: DatasetDeletionBarrier,
): Promise<void> {
  const createdAt = input.createdAt.toISOString().replace("Z", "");
  if (input.datasetGeneration !== null) {
    await loadVisible({
      table: "dataset_tombstones",
      label: label(input, "dataset"),
      columns: [
        "project_id",
        "dataset_id",
        "deletion_generation",
        "created_at",
      ],
      rows: [
        {
          project_id: input.projectId,
          dataset_id: input.datasetId,
          deletion_generation: input.datasetGeneration.toString(),
          created_at: createdAt,
        },
      ],
    });
  }
  await loadVisible({
    table: "dataset_run_tombstones",
    label: label(input, "runs"),
    columns: [
      "project_id",
      "dataset_run_id",
      "dataset_id",
      "deletion_generation",
      "created_at",
    ],
    rows: Object.entries(input.runGenerations).map(
      ([datasetRunId, generation]) => ({
        project_id: input.projectId,
        dataset_run_id: datasetRunId,
        dataset_id: input.datasetId,
        deletion_generation: generation.toString(),
        created_at: createdAt,
      }),
    ),
  });
}

export async function cleanupDorisDatasetRunItems(input: {
  readonly projectId: string;
  readonly datasetId: string;
  readonly datasetRunIds: readonly string[];
  readonly deleteDataset: boolean;
}): Promise<void> {
  const executor = getDorisQueryExecutor();
  if (input.deleteDataset) {
    await executor.query(
      "DELETE FROM dataset_run_items_current WHERE project_id = ? AND dataset_id = ?",
      [input.projectId, input.datasetId],
    );
    return;
  }
  const runIds = [...new Set(input.datasetRunIds)];
  for (let offset = 0; offset < runIds.length; offset += 500) {
    const chunk = runIds.slice(offset, offset + 500);
    await executor.query(
      `DELETE FROM dataset_run_items_current WHERE project_id = ? AND dataset_run_id IN (${chunk
        .map(() => "?")
        .join(", ")})`,
      [input.projectId, ...chunk],
    );
  }
}
