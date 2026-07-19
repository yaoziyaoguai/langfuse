import { UiColumnMappings } from "../../tableDefinitions";

export const experimentItemsTableNativeUiColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "Scores (numeric)",
    uiTableId: "obs_scores_avg",
    analyticsTableName: "scores",
    analyticsSelect: "s.scores_avg",
  },
  {
    uiTableName: "Scores (categorical)",
    uiTableId: "obs_score_categories",
    analyticsTableName: "scores",
    analyticsSelect: "s.score_categories",
  },
  {
    uiTableName: "Scores (boolean)",
    uiTableId: "obs_score_booleans",
    analyticsTableName: "scores",
    analyticsSelect: "s.score_booleans",
  },
  {
    uiTableName: "Trace Scores (numeric)",
    uiTableId: "trace_scores_avg",
    analyticsTableName: "scores",
    analyticsSelect: "ts.scores_avg",
  },
  {
    uiTableName: "Trace Scores (categorical)",
    uiTableId: "trace_score_categories",
    analyticsTableName: "scores",
    analyticsSelect: "ts.score_categories",
  },
  {
    uiTableName: "Trace Scores (boolean)",
    uiTableId: "trace_score_booleans",
    analyticsTableName: "scores",
    analyticsSelect: "ts.score_booleans",
  },
  {
    uiTableName: "Item Metadata",
    uiTableId: "itemMetadata",
    analyticsTableName: "events_proto",
    analyticsSelect: "experiment_item_metadata",
    queryPrefix: "e",
  },
  {
    uiTableName: "Metadata",
    uiTableId: "eventMetadata",
    analyticsTableName: "events_proto",
    analyticsSelect: "metadata",
    queryPrefix: "e",
  },
];
