CREATE INDEX CONCURRENTLY "analytics_ingestion_operations_backend_generation_status_idx"
ON "analytics_ingestion_operations"("analytics_backend", "deployment_generation", "status");
