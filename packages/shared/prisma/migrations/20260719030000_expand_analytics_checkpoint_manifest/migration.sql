ALTER TABLE "analytics_checkpoint_generations"
  ADD COLUMN "operation_high_watermark_accepted_at_nanos" BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN "deletion_high_watermark_created_at" TIMESTAMP(3) NOT NULL DEFAULT TIMESTAMP '1970-01-01 00:00:00',
  ADD COLUMN "postgres_wal_lsn" TEXT,
  ADD COLUMN "manifest" JSONB,
  ADD COLUMN "external_anchor_ref" TEXT;

ALTER TABLE "analytics_ingestion_operations"
  ADD COLUMN "checkpoint_generation" BIGINT NOT NULL DEFAULT 0;

ALTER TABLE "analytics_deletion_operations"
  ADD COLUMN "checkpoint_generation" BIGINT NOT NULL DEFAULT 0;

ALTER TABLE "analytics_checkpoint_generations"
  ALTER COLUMN "operation_high_watermark_accepted_at_nanos" DROP DEFAULT,
  ALTER COLUMN "deletion_high_watermark_created_at" DROP DEFAULT;
