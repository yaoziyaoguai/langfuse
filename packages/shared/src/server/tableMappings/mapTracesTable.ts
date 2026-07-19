import { UiColumnMappings } from "../../tableDefinitions";

export const tracesTableUiColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "⭐️",
    uiTableId: "bookmarked",
    analyticsTableName: "traces",
    analyticsSelect: "t.bookmarked",
  },
  {
    uiTableName: "Level",
    uiTableId: "level",
    analyticsTableName: "observations",
    analyticsSelect: "aggregated_level",
    queryPrefix: "o",
  },
  {
    uiTableName: "ID",
    uiTableId: "id",
    analyticsTableName: "traces",
    analyticsSelect: "id",
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    analyticsTableName: "traces",
    analyticsSelect: "id",
  },
  {
    uiTableName: "Name",
    uiTableId: "name",
    analyticsTableName: "traces",
    analyticsSelect: "name",
    queryPrefix: "t",
  },
  {
    // Alias for name - allows traceName filter (used in evals) to work on traces table
    // this happens in the v4 beta if someone filters for traceName in beta mode and then switches back to non-beta
    // TODO: remove after beta v4 is concluded
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    analyticsTableName: "traces",
    analyticsSelect: "name",
    queryPrefix: "t",
  },
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    analyticsTableName: "traces",
    analyticsSelect: "timestamp",
    queryPrefix: "t",
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    analyticsTableName: "traces",
    analyticsSelect: "user_id",
    queryPrefix: "t",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    analyticsTableName: "traces",
    analyticsSelect: "session_id",
    queryPrefix: "t",
  },
  {
    uiTableName: "Metadata",
    uiTableId: "metadata",
    analyticsTableName: "traces",
    analyticsSelect: "metadata",
    queryPrefix: "t",
  },
  {
    uiTableName: "Version",
    uiTableId: "version",
    analyticsTableName: "traces",
    analyticsSelect: "version",
    queryPrefix: "t",
  },
  {
    uiTableName: "Release",
    uiTableId: "release",
    analyticsTableName: "traces",
    analyticsSelect: "release",
    queryPrefix: "t",
  },
  {
    uiTableName: "Environment",
    uiTableId: "environment",
    analyticsTableName: "traces",
    analyticsSelect: "environment",
    queryPrefix: "t",
  },
  {
    uiTableName: "Tags",
    uiTableId: "tags",
    analyticsTableName: "traces",
    analyticsSelect: "tags",
    queryPrefix: "t",
  },
  {
    // Alias for tags so canonical traceTags filters work on the traces table too.
    uiTableName: "Trace Tags",
    uiTableId: "traceTags",
    analyticsTableName: "traces",
    analyticsSelect: "tags",
    queryPrefix: "t",
  },
  {
    uiTableName: "Warning Level Count",
    uiTableId: "warningCount",
    analyticsTableName: "observations",
    analyticsSelect: "warning_count",
    queryPrefix: "o",
  },
  {
    uiTableName: "Error Level Count",
    uiTableId: "errorCount",
    analyticsTableName: "observations",
    analyticsSelect: "error_count",
    queryPrefix: "o",
  },
  {
    uiTableName: "Default Level Count",
    uiTableId: "defaultCount",
    analyticsTableName: "observations",
    analyticsSelect: "default_count",
    queryPrefix: "o",
  },
  {
    uiTableName: "Debug Level Count",
    uiTableId: "debugCount",
    analyticsTableName: "observations",
    analyticsSelect: "debug_count",
    queryPrefix: "o",
  },
  {
    uiTableName: "Input Tokens",
    uiTableId: "inputTokens",
    analyticsTableName: "observations",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, o.usage_details)))",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Output Tokens",
    uiTableId: "outputTokens",
    analyticsTableName: "observations",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, o.usage_details)))",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Total Tokens",
    uiTableId: "totalTokens",
    analyticsTableName: "observations",
    analyticsSelect:
      "if(mapExists((k, v) -> (k = 'total'), o.usage_details), o.usage_details['total'], NULL)",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Tokens",
    uiTableId: "tokens",
    analyticsTableName: "observations",
    analyticsSelect:
      "if(mapExists((k, v) -> (k = 'total'), o.usage_details), o.usage_details['total'], NULL)",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  // Scores column duplicated to allow renaming column name. Will be removed once session storage cache is outdated
  // Column names are cached in user sessions - changing them breaks existing filters
  {
    uiTableName: "Scores",
    uiTableId: "scores",
    analyticsTableName: "scores",
    analyticsSelect: "s.scores_avg",
  },
  {
    uiTableName: "Scores (numeric)",
    uiTableId: "scores_avg",
    analyticsTableName: "scores",
    analyticsSelect: "s.scores_avg",
  },
  {
    uiTableName: "Scores (categorical)",
    uiTableId: "score_categories",
    analyticsTableName: "scores",
    analyticsSelect: "s.score_categories",
  },
  {
    uiTableName: "Scores (boolean)",
    uiTableId: "score_booleans",
    analyticsTableName: "scores",
    analyticsSelect: "s.score_booleans",
  },
  {
    uiTableName: "Latency (s)",
    uiTableId: "latency",
    analyticsTableName: "observations",
    queryPrefix: "o",
    analyticsSelect: "latency_milliseconds / 1000",
    // If we use the default of Decimal64(12), we cannot filter for more than ~40min due to an overflow
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Input Cost ($)",
    uiTableId: "inputCost",
    analyticsTableName: "observations",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, o.cost_details)))",
  },
  {
    uiTableName: "Output Cost ($)",
    uiTableId: "outputCost",
    analyticsTableName: "observations",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, o.cost_details)))",
  },
  {
    uiTableName: "Total Cost ($)",
    uiTableId: "totalCost",
    analyticsTableName: "observations",
    queryPrefix: "o",
    analyticsSelect: "cost_details['total']",
  },
];
