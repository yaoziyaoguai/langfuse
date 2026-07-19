// This structure is maintained to relate the frontend table definitions with the Doris table definitions.
// The frontend only sends the column names to the backend. This needs to be changed in the future to send column IDs.

import { UiColumnMappings } from "../../tableDefinitions";
import {
  eventsTableHasParentObservationSql,
  eventsTableIsRootObservationSql,
  eventsTableHasInputSql,
  eventsTableHasOutputSql,
} from "../../eventsTable";

export const eventsTableNativeUiColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "Environment",
    uiTableId: "environment",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."environment"',
  },
  {
    uiTableName: "Type",
    uiTableId: "type",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."type"',
  },
  {
    uiTableName: "ID",
    uiTableId: "id",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."span_id"',
  },
  {
    uiTableName: "Name",
    uiTableId: "name",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."name"',
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."trace_id"',
  },

  {
    uiTableName: "Start Time",
    uiTableId: "startTime",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."start_time"',
  },
  {
    uiTableName: "End Time",
    uiTableId: "endTime",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."end_time"',
  },
  {
    uiTableName: "Time To First Token (s)",
    uiTableId: "timeToFirstToken",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "if(isNull(e.completion_start_time), NULL,  date_diff('millisecond', e.start_time, e.completion_start_time) / 1000)",
    // If we use the default of Decimal64(12), we cannot filter for more than ~40min due to an overflow
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Latency (s)",
    uiTableId: "latency",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "if(isNull(e.end_time), NULL, date_diff('millisecond', e.start_time, e.end_time) / 1000)",
    // If we use the default of Decimal64(12), we cannot filter for more than ~40min due to an overflow
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Tokens per second",
    uiTableId: "tokensPerSecond",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "(arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, usage_details))) / (date_diff('millisecond', start_time, end_time) / 1000))",
  },
  {
    uiTableName: "Input Cost ($)",
    uiTableId: "inputCost",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, cost_details)))",
  },
  {
    uiTableName: "Output Cost ($)",
    uiTableId: "outputCost",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, cost_details)))",
  },
  {
    uiTableName: "Total Cost ($)",
    uiTableId: "totalCost",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "if(mapExists((k, v) -> (k = 'total'), cost_details), cost_details['total'], NULL)",
  },
  {
    uiTableName: "Level",
    uiTableId: "level",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."level"',
  },
  {
    uiTableName: "Status Message",
    uiTableId: "statusMessage",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."status_message"',
  },
  {
    uiTableName: "Model",
    uiTableId: "model",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."provided_model_name"',
  },
  {
    uiTableName: "Provided Model Name",
    uiTableId: "providedModelName",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."provided_model_name"',
  },
  {
    uiTableName: "Model ID",
    uiTableId: "modelId",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."model_id"',
  },
  {
    uiTableName: "Input Tokens",
    uiTableId: "inputTokens",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'input') > 0, usage_details)))",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Output Tokens",
    uiTableId: "outputTokens",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "arraySum(mapValues(mapFilter(x -> positionCaseInsensitive(x.1, 'output') > 0, usage_details)))",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Total Tokens",
    uiTableId: "totalTokens",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "if(mapExists((k, v) -> (k = 'total'), usage_details), usage_details['total'], NULL)",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Tokens",
    uiTableId: "tokens",
    analyticsTableName: "events_proto",
    analyticsSelect:
      "if(mapExists((k, v) -> (k = 'total'), usage_details), usage_details['total'], NULL)",
    analyticsTypeOverwrite: "Decimal64(3)",
  },
  {
    uiTableName: "Metadata",
    uiTableId: "metadata",
    analyticsTableName: "events_proto",
    analyticsSelect: "metadata",
    queryPrefix: "e",
  },
  {
    uiTableName: "Version",
    uiTableId: "version",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."version"',
  },
  {
    uiTableName: "Prompt Name",
    uiTableId: "promptName",
    analyticsTableName: "events_proto",
    analyticsSelect: "e.prompt_name",
  },
  {
    uiTableName: "Prompt Version",
    uiTableId: "promptVersion",
    analyticsTableName: "events_proto",
    analyticsSelect: "e.prompt_version",
  },
  {
    uiTableName: "Input",
    uiTableId: "input",
    analyticsTableName: "events_proto",
    analyticsSelect: "e.input",
  },
  {
    uiTableName: "Output",
    uiTableId: "output",
    analyticsTableName: "events_proto",
    analyticsSelect: "e.output",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."session_id"',
  },
  {
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."trace_name"',
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."user_id"',
  },
  {
    uiTableName: "Trace Tags",
    uiTableId: "traceTags",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."tags"',
  },
  {
    uiTableName: "Tags",
    uiTableId: "tags",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."tags"',
  },
  {
    uiTableName: "Trace Environment",
    uiTableId: "traceEnvironment",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."environment"',
  },
  {
    uiTableName: "Has Parent Observation",
    uiTableId: "hasParentObservation",
    analyticsTableName: "events_proto",
    analyticsSelect: eventsTableHasParentObservationSql,
  },
  {
    uiTableName: "Is Root Observation",
    uiTableId: "isRootObservation",
    analyticsTableName: "events_proto",
    analyticsSelect: eventsTableIsRootObservationSql,
  },
  {
    uiTableName: "Has Input",
    uiTableId: "hasInput",
    analyticsTableName: "events_proto",
    analyticsSelect: eventsTableHasInputSql,
  },
  {
    uiTableName: "Has Output",
    uiTableId: "hasOutput",
    analyticsTableName: "events_proto",
    analyticsSelect: eventsTableHasOutputSql,
  },
  {
    uiTableName: "Parent Observation ID",
    uiTableId: "parentObservationId",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."parent_span_id"',
    emptyEqualsNull: true,
  },
  {
    uiTableName: "Experiment Dataset ID",
    uiTableId: "experimentDatasetId",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."experiment_dataset_id"',
  },
  {
    uiTableName: "Experiment ID",
    uiTableId: "experimentId",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."experiment_id"',
  },
  {
    uiTableName: "Experiment Name",
    uiTableId: "experimentName",
    analyticsTableName: "events_proto",
    analyticsSelect: 'e."experiment_name"',
  },
  {
    uiTableName: "Is Experiment Item Root Span",
    uiTableId: "isExperimentItemRootSpan",
    analyticsTableName: "events_proto",
    analyticsSelect: "e.experiment_item_root_span_id = e.span_id",
  },
  {
    uiTableName: "Available Tools",
    uiTableId: "toolDefinitions",
    analyticsTableName: "events_proto",
    analyticsSelect: "length(mapKeys(e.tool_definitions))",
  },
  {
    uiTableName: "Tool Calls",
    uiTableId: "toolCalls",
    analyticsTableName: "events_proto",
    analyticsSelect: "length(e.tool_calls)",
  },
  {
    uiTableName: "Tool Names",
    uiTableId: "toolNames",
    analyticsTableName: "events_proto",
    analyticsSelect: "mapKeys(e.tool_definitions)",
  },
  {
    uiTableName: "Called Tool Names",
    uiTableId: "calledToolNames",
    analyticsTableName: "events_proto",
    analyticsSelect: "e.tool_call_names",
  },
];

export const eventsTableUiColumnDefinitions: UiColumnMappings = [
  ...eventsTableNativeUiColumnDefinitions,
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
    uiTableName: "Comment Count",
    uiTableId: "commentCount",
    analyticsTableName: "comments",
    analyticsSelect: "", // handled by comment filter helpers
  },
  {
    uiTableName: "Comment Content",
    uiTableId: "commentContent",
    analyticsTableName: "comments",
    analyticsSelect: "", // handled by comment filter helpers
  },
];
