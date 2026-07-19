import { UiColumnMappings } from "../../tableDefinitions";

// Lowercased boolean string_value ('true'/'false', '' for non-boolean rows) —
// matches the lowercase options offered by the scores view's Boolean Value
// facet. Shared with mapScoresColumnsTable so the two mappings cannot drift.
export const SCORE_BOOLEAN_VALUE_SQL =
  "if(s.data_type = 'BOOLEAN' AND notEmpty(s.string_value), lowerUTF8(s.string_value), '')";

export const scoresTableUiColumnDefinitions: UiColumnMappings = [
  {
    uiTableName: "ID",
    uiTableId: "id",
    analyticsTableName: "scores",
    analyticsSelect: "id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Timestamp",
    uiTableId: "timestamp",
    analyticsTableName: "scores",
    analyticsSelect: "timestamp",
    queryPrefix: "s",
  },
  {
    uiTableName: "Environment",
    uiTableId: "environment",
    analyticsTableName: "scores",
    analyticsSelect: "environment",
    queryPrefix: "s",
  },
  {
    uiTableName: "Trace ID",
    uiTableId: "traceId",
    analyticsTableName: "scores",
    analyticsSelect: "trace_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Observation ID",
    uiTableId: "observationId",
    analyticsTableName: "scores",
    analyticsSelect: "observation_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Session ID",
    uiTableId: "sessionId",
    analyticsTableName: "scores",
    analyticsSelect: "session_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Name",
    uiTableId: "name",
    analyticsTableName: "scores",
    analyticsSelect: "name",
    queryPrefix: "s",
  },
  {
    uiTableName: "Value",
    uiTableId: "value",
    analyticsTableName: "scores",
    analyticsSelect: "value",
    queryPrefix: "s",
  },
  {
    uiTableName: "Boolean Value",
    uiTableId: "booleanValue",
    analyticsTableName: "scores",
    analyticsSelect: SCORE_BOOLEAN_VALUE_SQL,
    emptyEqualsNull: true,
  },
  {
    uiTableName: "Source",
    uiTableId: "source",
    analyticsTableName: "scores",
    analyticsSelect: "source",
    queryPrefix: "s",
  },
  {
    uiTableName: "Comment",
    uiTableId: "comment",
    analyticsTableName: "scores",
    analyticsSelect: "comment",
    queryPrefix: "s",
  },
  {
    uiTableName: "Author User ID",
    uiTableId: "authorUserId",
    analyticsTableName: "scores",
    analyticsSelect: "author_user_id",
    queryPrefix: "s",
  },
  {
    uiTableName: "Data Type",
    uiTableId: "dataType",
    analyticsTableName: "scores",
    analyticsSelect: "data_type",
    queryPrefix: "s",
  },
  {
    uiTableName: "String Value",
    uiTableId: "stringValue",
    analyticsTableName: "scores",
    analyticsSelect: "string_value",
    queryPrefix: "s",
  },
  {
    uiTableName: "Metadata",
    uiTableId: "metadata",
    analyticsTableName: "scores",
    analyticsSelect: "metadata",
    queryPrefix: "s",
  },
  {
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    analyticsTableName: "traces",
    analyticsSelect: "t.name",
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    analyticsTableName: "traces",
    analyticsSelect: "t.user_id",
  },
  {
    uiTableName: "Trace Tags",
    uiTableId: "trace_tags",
    analyticsTableName: "traces",
    analyticsSelect: "t.tags",
  },
];

/**
 * v4 column definitions for scores table — trace columns reference the traces
 * CTE built from a flat EventsQueryBuilder. The CTE is joined as alias "e".
 */
export const scoresTableUiColumnDefinitionsFromEvents: UiColumnMappings = [
  // All scores-native columns are identical to v3
  ...scoresTableUiColumnDefinitions.filter(
    (c) => c.analyticsTableName === "scores",
  ),
  {
    uiTableName: "Trace Name",
    uiTableId: "traceName",
    analyticsTableName: "traces",
    analyticsSelect: "name",
    queryPrefix: "e",
  },
  {
    uiTableName: "User ID",
    uiTableId: "userId",
    analyticsTableName: "traces",
    analyticsSelect: "user_id",
    queryPrefix: "e",
  },
  {
    uiTableName: "Trace Tags",
    uiTableId: "trace_tags",
    analyticsTableName: "traces",
    analyticsSelect: "tags",
    queryPrefix: "e",
  },
];
