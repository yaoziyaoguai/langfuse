// This structure is maintained to relate the frontend table definitions with the Doris table definitions.
// The frontend only sends the column names to the backend. This needs to be changed in the future to send column IDs.

import { UiColumnMappings } from "../../tableDefinitions";

export const observationsTableTraceUiColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "Trace Tags",
    uiTableId: "traceTags",
    analyticsTableName: "traces",
    analyticsSelect: "t.tags",
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    analyticsTableName: "traces",
    analyticsSelect: 't."user_id"',
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    analyticsTableName: "traces",
    analyticsSelect: 't."session_id"',
  },
  {
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    analyticsTableName: "traces",
    analyticsSelect: 't."name"',
  },
  {
    uiTableName: "Trace Environment",
    uiTableId: "traceEnvironment",
    analyticsTableName: "traces",
    analyticsSelect: 't."environment"',
  },
];

export const observationsTableUiColumnDefinitions: UiColumnMappings = [
  ...observationsTableTraceUiColumnDefinitions,
  {
    uiTableName: "Environment",
    uiTableId: "environment",
    analyticsTableName: "observations",
    analyticsSelect: 'o."environment"',
  },
  {
    uiTableName: "type",
    uiTableId: "type",
    analyticsTableName: "observations",
    analyticsSelect: 'o."type"',
  },
  {
    uiTableName: "ID",
    uiTableId: "id",
    analyticsTableName: "observations",
    analyticsSelect: 'o."id"',
  },
  {
    uiTableName: "Type",
    uiTableId: "type",
    analyticsTableName: "observations",
    analyticsSelect: 'o."type"',
  },
  {
    uiTableName: "Name",
    uiTableId: "name",
    analyticsTableName: "observations",
    analyticsSelect: 'o."name"',
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    analyticsTableName: "observations",
    analyticsSelect: 'o."trace_id"',
  },
  {
    uiTableName: "Parent Observation ID",
    uiTableId: "parentObservationId",
    analyticsTableName: "observations",
    analyticsSelect: 'o."parent_observation_id"',
  },

  {
    uiTableName: "Start Time",
    uiTableId: "startTime",
    analyticsTableName: "observations",
    analyticsSelect: 'o."start_time"',
  },
  {
    uiTableName: "End Time",
    uiTableId: "endTime",
    analyticsTableName: "observations",
    analyticsSelect: 'o."end_time"',
  },
  {
    uiTableName: "Time To First Token (s)",
    uiTableId: "timeToFirstToken",
    analyticsTableName: "observations",
    analyticsSelect:
      "if(isNull(completion_start_time), NULL,  date_diff('millisecond', start_time, completion_start_time) / 1000)",
    // If we use the default of Decimal64(12), we cannot filter for more than ~40min due to an overflow
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Latency (s)",
    uiTableId: "latency",
    analyticsTableName: "observations",
    analyticsSelect:
      "if(isNull(end_time), NULL, date_diff('millisecond', start_time, end_time) / 1000)",
    // If we use the default of Decimal64(12), we cannot filter for more than ~40min due to an overflow
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Tokens per second",
    uiTableId: "tokensPerSecond",
    analyticsTableName: "observations",
    analyticsSelect:
      "(arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, usage_details))) / (date_diff('millisecond', start_time, end_time) / 1000))",
  },
  {
    uiTableName: "Input Cost ($)",
    uiTableId: "inputCost",
    analyticsTableName: "observations",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, cost_details)))",
  },
  {
    uiTableName: "Output Cost ($)",
    uiTableId: "outputCost",
    analyticsTableName: "observations",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, cost_details)))",
  },
  {
    uiTableName: "Total Cost ($)",
    uiTableId: "totalCost",
    analyticsTableName: "observations",
    analyticsSelect:
      "if(mapExists((k, v) -> (k = 'total'), cost_details), cost_details['total'], NULL)",
  },
  {
    uiTableName: "Level",
    uiTableId: "level",
    analyticsTableName: "observations",
    analyticsSelect: 'o."level"',
  },
  {
    uiTableName: "Status Message",
    uiTableId: "statusMessage",
    analyticsTableName: "observations",
    analyticsSelect: 'o."status_message"',
  },
  {
    uiTableName: "Model",
    uiTableId: "model",
    analyticsTableName: "observations",
    analyticsSelect: 'o."provided_model_name"',
  },
  {
    uiTableName: "Model ID",
    uiTableId: "modelId",
    analyticsTableName: "observations",
    analyticsSelect: 'o."internal_model_id"',
  },
  {
    uiTableName: "Input Tokens",
    uiTableId: "inputTokens",
    analyticsTableName: "observations",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, usage_details)))",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Output Tokens",
    uiTableId: "outputTokens",
    analyticsTableName: "observations",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, usage_details)))",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Total Tokens",
    uiTableId: "totalTokens",
    analyticsTableName: "observations",
    analyticsSelect:
      "if(mapExists((k, v) -> (k = 'total'), usage_details), usage_details['total'], NULL)",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Tokens",
    uiTableId: "tokens",
    analyticsTableName: "observations",
    analyticsSelect:
      "if(mapExists((k, v) -> (k = 'total'), usage_details), usage_details['total'], NULL)",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Metadata",
    uiTableId: "metadata",
    analyticsTableName: "observations",
    analyticsSelect: 'o."metadata"',
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
    uiTableName: "Version",
    uiTableId: "version",
    analyticsTableName: "observations",
    analyticsSelect: 'o."version"',
  },
  {
    uiTableName: "Prompt Name",
    uiTableId: "promptName",
    analyticsTableName: "observations",
    analyticsSelect: "o.prompt_name",
  },
  {
    uiTableName: "Prompt Version",
    uiTableId: "promptVersion",
    analyticsTableName: "observations",
    analyticsSelect: "o.prompt_version",
  },
  {
    uiTableName: "Available Tools",
    uiTableId: "toolDefinitions",
    analyticsTableName: "observations",
    analyticsSelect: "length(mapKeys(o.tool_definitions))",
  },
  {
    uiTableName: "Tool Calls",
    uiTableId: "toolCalls",
    analyticsTableName: "observations",
    analyticsSelect: "length(o.tool_calls)",
  },
  {
    uiTableName: "Tool Names",
    uiTableId: "toolNames",
    analyticsTableName: "observations",
    analyticsSelect: "mapKeys(o.tool_definitions)",
  },
  {
    uiTableName: "Called Tool Names",
    uiTableId: "calledToolNames",
    analyticsTableName: "observations",
    analyticsSelect: "o.tool_call_names",
  },
];
