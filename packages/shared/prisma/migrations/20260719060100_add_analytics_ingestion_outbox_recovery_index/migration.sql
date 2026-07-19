CREATE INDEX CONCURRENTLY "analytics_ingestion_outbox_v2_status_updated_at_idx"
ON "analytics_ingestion_outbox_v2"("status", "updated_at");
