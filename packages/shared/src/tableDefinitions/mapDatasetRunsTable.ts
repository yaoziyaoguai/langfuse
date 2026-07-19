import { UiColumnMappings } from "./types";

export const datasetRunsTableUiColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "Dataset Run ID",
    uiTableId: "id",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: "drm.dataset_run_id",
  },
  {
    uiTableName: "Created At",
    uiTableId: "createdAt",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: "drm.dataset_run_created_at",
  },
  {
    uiTableName: "Scores (numeric)",
    uiTableId: "agg_scores_avg",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: "sa.scores_avg",
  },
  {
    uiTableName: "Scores (categorical)",
    uiTableId: "agg_score_categories",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: "sa.score_categories",
  },
  {
    uiTableName: "Scores (boolean)",
    uiTableId: "agg_score_booleans",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: "sa.score_booleans",
  },
];
