import { UiColumnMappings } from "../../tableDefinitions";
import { SCORE_BOOLEAN_VALUE_SQL } from "./mapScoresTable";

export const scoresColumnsTableUiColumnDefinitions: UiColumnMappings = [
  // scores native columns
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    analyticsTableName: "scores",
    analyticsSelect: "timestamp",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    analyticsTableName: "scores",
    analyticsSelect: 's."session_id"',
  },
  {
    uiTableName: "Dataset Run IDs",
    uiTableId: "datasetRunIds",
    analyticsTableName: "scores",
    analyticsSelect: 's."dataset_run_id"',
  },
  {
    uiTableName: "Boolean Value",
    uiTableId: "booleanValue",
    analyticsTableName: "scores",
    analyticsSelect: SCORE_BOOLEAN_VALUE_SQL,
    emptyEqualsNull: true,
  },
  {
    uiTableName: "Observation ID",
    uiTableId: "observationId",
    analyticsTableName: "scores",
    analyticsSelect: 's."observation_id"',
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    analyticsTableName: "scores",
    analyticsSelect: 's."trace_id"',
  },
  // require join of scores with dataset_run_items_rmt via trace_id and project_id
  {
    uiTableName: "Dataset Run Item Run IDs",
    uiTableId: "datasetRunItemRunIds",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: 'dri."dataset_run_id"',
  },
  {
    uiTableName: "Dataset ID",
    uiTableId: "datasetId",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: 'dri."dataset_id"',
  },
  {
    uiTableName: "Dataset Item IDs",
    uiTableId: "datasetItemIds",
    analyticsTableName: "dataset_run_items_rmt",
    analyticsSelect: 'dri."dataset_item_id"',
  },
  // require join of scores with experiments via trace_id and project_id
  {
    uiTableName: "Experiment IDs",
    uiTableId: "experimentIds",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."experiment_id"',
  },
];
