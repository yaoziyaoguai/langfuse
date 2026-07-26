CREATE INDEX CONCURRENTLY "analytics_retention_runs_backend_generation_status_idx"
ON "analytics_retention_runs"("analytics_backend", "deployment_generation", "status");
