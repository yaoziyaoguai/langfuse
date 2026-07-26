CREATE INDEX CONCURRENTLY "analytics_ingestion_operations_project_status_acceptance_idx"
ON "analytics_ingestion_operations"("project_id", "status", "acceptance_sequence");
