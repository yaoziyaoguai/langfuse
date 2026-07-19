import { UiColumnMappings } from "../../tableDefinitions";

export const sessionCols: UiColumnMappings = [
  // we do not access the traces scores in Doris. We default back to the trace timestamps.

  {
    uiTableName: "⭐️",
    uiTableId: "bookmarked",
    analyticsTableName: "traces",
    analyticsSelect: "bookmarked",
  },
  {
    uiTableName: "Created At",
    uiTableId: "createdAt",
    analyticsTableName: "traces",
    analyticsSelect: "min_timestamp",
  },
  {
    uiTableName: "User IDs",
    uiTableId: "userIds",
    analyticsTableName: "traces",
    analyticsSelect: "user_ids",
  },
  {
    uiTableName: "Environment",
    uiTableId: "environment",
    analyticsTableName: "traces",
    analyticsSelect: "environment",
  },
  {
    uiTableName: "Session Duration",
    uiTableId: "sessionDuration",
    analyticsTableName: "traces",
    analyticsSelect: "duration",
    // If we use the default of Decimal64(12), we cannot filter for more than ~40min due to an overflow
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Count Traces",
    uiTableId: "countTraces",
    analyticsTableName: "traces",
    analyticsSelect: "trace_count",
  },
  {
    uiTableName: "Session Input Cost",
    uiTableId: "inputCost",
    analyticsTableName: "traces",
    analyticsSelect: "session_input_cost",
  },
  {
    uiTableName: "Session Output Cost",
    uiTableId: "outputCost",
    analyticsTableName: "traces",
    analyticsSelect: "session_output_cost",
  },
  {
    uiTableName: "Session Total Cost",
    uiTableId: "totalCost",
    analyticsTableName: "traces",
    analyticsSelect: "session_total_cost",
  },
  {
    uiTableName: "Input Tokens",
    uiTableId: "inputTokens",
    analyticsTableName: "traces",
    analyticsSelect: "session_input_usage",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Output Tokens",
    uiTableId: "outputTokens",
    analyticsTableName: "traces",
    analyticsSelect: "session_output_usage",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Total Tokens",
    uiTableId: "totalTokens",
    analyticsTableName: "traces",
    analyticsSelect: "session_total_usage",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Usage",
    uiTableId: "totalTokens",
    analyticsTableName: "traces",
    analyticsSelect: "session_total_usage",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Session Total Usage",
    uiTableId: "usage",
    analyticsTableName: "traces",
    analyticsSelect: "session_total_usage",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Session Duration (s)",
    uiTableId: "sessionDuration",
    analyticsTableName: "traces",
    analyticsSelect: "duration",
    // If we use the default of Decimal64(12), we cannot filter for more than ~40min due to an overflow
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Traces Count",
    uiTableId: "tracesCount",
    analyticsTableName: "traces",
    analyticsSelect: "trace_count",
  },
  {
    uiTableName: "Input Cost ($)",
    uiTableId: "inputCost",
    analyticsTableName: "traces",
    analyticsSelect: "session_input_cost",
  },
  {
    uiTableName: "Output Cost ($)",
    uiTableId: "outputCost",
    analyticsTableName: "traces",
    analyticsSelect: "session_output_cost",
  },
  {
    uiTableName: "Total Cost ($)",
    uiTableId: "totalCost",
    analyticsTableName: "traces",
    analyticsSelect: "session_total_cost",
  },
  {
    uiTableName: "Trace Tags",
    uiTableId: "traceTags",
    analyticsTableName: "traces",
    analyticsSelect: "trace_tags",
  },
  {
    uiTableName: "ID",
    uiTableId: "id",
    analyticsTableName: "traces",
    analyticsSelect: "session_id",
  },
  {
    uiTableName: "Scores (numeric)",
    uiTableId: "scores_avg",
    analyticsTableName: "scores",
    analyticsSelect: "scores_avg",
  },
  {
    uiTableName: "Scores (categorical)",
    uiTableId: "score_categories",
    analyticsTableName: "scores",
    analyticsSelect: "score_categories",
  },
  {
    uiTableName: "Scores (boolean)",
    uiTableId: "score_booleans",
    analyticsTableName: "scores",
    analyticsSelect: "score_booleans",
  },
];

export const sessionEventsCols: UiColumnMappings = sessionCols.concat({
  uiTableName: "Metadata",
  uiTableId: "metadata",
  analyticsTableName: "events_proto",
  analyticsSelect: "metadata",
  queryPrefix: "s",
});

export const sessionEventsOrderByCols: UiColumnMappings =
  sessionEventsCols.filter((column) => column.uiTableId !== "metadata");
