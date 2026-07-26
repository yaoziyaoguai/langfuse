-- VARIANT stores JSON by path and therefore cannot round-trip literal dotted
-- keys such as OTEL's "service.name". Keep VARIANT for path predicates and a
-- parallel raw JSON string for backend-neutral API readback.

ALTER TABLE events_current ADD COLUMN metadata_json STRING NULL;
ALTER TABLE events_current ADD COLUMN usage_details_json STRING NULL;
ALTER TABLE events_current ADD COLUMN cost_details_json STRING NULL;
ALTER TABLE events_current ADD COLUMN provided_usage_details_json STRING NULL;
ALTER TABLE events_current ADD COLUMN provided_cost_details_json STRING NULL;
ALTER TABLE events_current ADD COLUMN model_parameters_json STRING NULL;
ALTER TABLE events_current ADD COLUMN tool_definitions_json STRING NULL;

ALTER TABLE scores_current ADD COLUMN metadata_json STRING NULL;
