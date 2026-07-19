CREATE INDEX CONCURRENTLY "analytics_ingestion_outbox_updated_at_id_idx"
ON "analytics_ingestion_outbox"("updated_at", "id");
