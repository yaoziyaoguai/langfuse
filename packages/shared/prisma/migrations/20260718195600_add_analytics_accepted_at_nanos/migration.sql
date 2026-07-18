-- Preserve the exact canonical acceptance timestamp used by immutable
-- artifacts; TIMESTAMP(3) remains for indexed operational/status queries.
ALTER TABLE "analytics_ingestion_operations"
ADD COLUMN "accepted_at_nanos" BIGINT;

UPDATE "analytics_ingestion_operations"
SET "accepted_at_nanos" = (
  EXTRACT(EPOCH FROM "accepted_at")::numeric * 1000000000
)::bigint;

ALTER TABLE "analytics_ingestion_operations"
ALTER COLUMN "accepted_at_nanos" SET NOT NULL;
