CREATE INDEX CONCURRENTLY "analytics_deletion_operations_backend_generation_status_idx"
ON "analytics_deletion_operations"("analytics_backend", "deployment_generation", "status");
