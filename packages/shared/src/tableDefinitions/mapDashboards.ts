import { UiColumnMappings } from "./types";

// Make sure to update web/src/features/dashboard/lib/dashboardUiTableToViewMapping.ts if you make changes

export const dashboardColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    analyticsTableName: "traces",
    analyticsSelect: 't."name"',
  },
  {
    uiTableName: "Tags",
    uiTableId: "traceTags",
    analyticsTableName: "traces",
    analyticsSelect: 't."tags"',
  },
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    analyticsTableName: "traces",
    analyticsSelect: 't."timestamp"',
  },
  {
    analyticsTableName: "scores",
    analyticsSelect: "name",
    uiTableId: "scoreName",
    uiTableName: "Score Name",
  },
  {
    analyticsTableName: "scores",
    analyticsSelect: "timestamp",
    uiTableId: "scoreTimestamp",
    uiTableName: "Score Timestamp",
  },
  {
    analyticsTableName: "scores",
    analyticsSelect: "source",
    uiTableId: "scoreSource",
    uiTableName: "Score Source",
  },
  {
    analyticsTableName: "scores",
    analyticsSelect: "data_type",
    uiTableId: "scoreDataType",
    uiTableName: "Scores Data Type",
  },
  {
    analyticsTableName: "scores",
    analyticsSelect: "value",
    uiTableId: "value",
    uiTableName: "value",
  },
  {
    analyticsTableName: "observations",
    analyticsSelect: "o.start_time",
    uiTableId: "startTime",
    uiTableName: "Start Time",
  },
  {
    analyticsTableName: "observations",
    analyticsSelect: "o.end_time",
    uiTableId: "endTime",
    uiTableName: "End Time",
  },
  {
    analyticsTableName: "observations",
    analyticsSelect: "o.type",
    uiTableId: "type",
    uiTableName: "Type",
  },
  {
    analyticsTableName: "observations",
    analyticsSelect: "o.level",
    uiTableId: "level",
    uiTableName: "Level",
  },
  {
    analyticsTableName: "traces",
    analyticsSelect: "t.user_id",
    uiTableId: "userId",
    uiTableName: "User",
  },
  {
    analyticsTableName: "traces",
    analyticsSelect: "t.release",
    uiTableId: "release",
    uiTableName: "Release",
  },
  {
    analyticsTableName: "traces",
    analyticsSelect: "t.version",
    uiTableId: "version",
    uiTableName: "Version",
  },
  {
    analyticsTableName: "observations",
    analyticsSelect: "provided_model_name",
    uiTableId: "model",
    uiTableName: "Model",
  },
  {
    analyticsTableName: "observations",
    analyticsSelect: "mapKeys(tool_definitions)",
    uiTableId: "toolNames",
    uiTableName: "Tool Names (Available)",
    aliases: ["Tool Names"],
  },
  {
    analyticsTableName: "observations",
    analyticsSelect: "tool_call_names",
    uiTableId: "calledToolNames",
    uiTableName: "Tool Names (Called)",
  },
  {
    analyticsTableName: "traces",
    analyticsSelect: "environment",
    uiTableId: "environment",
    uiTableName: "Environment",
  },
];
