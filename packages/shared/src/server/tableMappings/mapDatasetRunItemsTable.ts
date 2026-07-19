import {
  matchesUiColumnMapping,
  UiColumnMappings,
} from "../../tableDefinitions";
import { DatasetRunItemDomain } from "../../domain/dataset-run-items";

export const datasetRunItemsTableUiColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "Dataset Run ID",
    uiTableId: "datasetRunId",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: 'dri."dataset_run_id"',
  },
  {
    uiTableName: "Created At",
    uiTableId: "createdAt",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: 'dri."created_at"',
  },
  {
    uiTableName: "Event Timestamp",
    uiTableId: "eventTs",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: 'dri."event_ts"',
  },
  {
    uiTableName: "Dataset Item ID",
    uiTableId: "datasetItemId",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: 'dri."dataset_item_id"',
  },
  {
    uiTableName: "Dataset",
    uiTableId: "datasetId",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: 'dri."dataset_id"',
  },
  {
    uiTableName: "Scores (numeric)",
    uiTableId: "agg_scores_avg",
    analyticsTableName: "scores",
    analyticsSelect: "sa.scores_avg",
  },
  {
    uiTableName: "Scores (categorical)",
    uiTableId: "agg_score_categories",
    analyticsTableName: "scores",
    analyticsSelect: "sa.score_categories",
  },
  {
    uiTableName: "Scores (boolean)",
    uiTableId: "agg_score_booleans",
    analyticsTableName: "scores",
    analyticsSelect: "sa.score_booleans",
  },
];

export const mapDatasetRunItemFilterColumn = (
  dataset: Pick<DatasetRunItemDomain, "id" | "datasetId">,
  column: string,
): unknown => {
  const columnDef = datasetRunItemsTableUiColumnDefinitions.find(
    (col) =>
      matchesUiColumnMapping(col, column) || col.analyticsSelect === column,
  );
  if (!columnDef) {
    throw new Error(`Unhandled column for dataset run items filter: ${column}`);
  }
  switch (columnDef.uiTableId) {
    case "id":
      return dataset.id;
    case "datasetId":
      return dataset.datasetId;
    default:
      throw new Error(
        `Unhandled column in dataset run items filter mapping: ${column}`,
      );
  }
};
