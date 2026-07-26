CREATE INDEX CONCURRENTLY "analytics_checkpoint_generations_backend_generation_status_idx"
ON "analytics_checkpoint_generations"("analytics_backend", "deployment_generation", "status");
