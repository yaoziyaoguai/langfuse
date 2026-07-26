ALTER TYPE "AnalyticsEntityType" ADD VALUE 'DATASET_RUN_ITEM';

CREATE TYPE "AnalyticsDatasetDeletionScope" AS ENUM ('DATASET', 'DATASET_RUNS');
CREATE TYPE "AnalyticsDatasetDeletionStatus" AS ENUM ('SCHEDULED', 'RETRYING', 'NEEDS_ATTENTION', 'COMPLETED');

ALTER TABLE "analytics_ingestion_candidates"
  ADD COLUMN "owning_dataset_id" TEXT,
  ADD COLUMN "owning_dataset_run_id" TEXT,
  ADD COLUMN "dataset_deletion_generation" BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN "run_deletion_generation" BIGINT NOT NULL DEFAULT 0;

ALTER TABLE "analytics_entity_heads"
  ADD COLUMN "owning_dataset_id" TEXT,
  ADD COLUMN "owning_dataset_run_id" TEXT,
  ADD COLUMN "dataset_deletion_generation" BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN "run_deletion_generation" BIGINT NOT NULL DEFAULT 0;

CREATE INDEX "analytics_ingestion_candidates_dataset_run_disposition_idx"
  ON "analytics_ingestion_candidates"("project_id", "owning_dataset_id", "owning_dataset_run_id", "disposition");
CREATE INDEX "analytics_entity_heads_dataset_run_type_idx"
  ON "analytics_entity_heads"("project_id", "owning_dataset_id", "owning_dataset_run_id", "entity_type");

CREATE TABLE "analytics_dataset_deletion_generations" (
  "project_id" TEXT NOT NULL,
  "dataset_id" TEXT NOT NULL,
  "generation" BIGINT NOT NULL,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "analytics_dataset_deletion_generations_pkey" PRIMARY KEY ("project_id", "dataset_id")
);

CREATE TABLE "analytics_dataset_run_deletion_generations" (
  "project_id" TEXT NOT NULL,
  "dataset_run_id" TEXT NOT NULL,
  "dataset_id" TEXT NOT NULL,
  "generation" BIGINT NOT NULL,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "analytics_dataset_run_deletion_generations_pkey" PRIMARY KEY ("project_id", "dataset_run_id")
);
CREATE INDEX "analytics_dataset_run_deletion_generations_project_id_dataset_id_idx"
  ON "analytics_dataset_run_deletion_generations"("project_id", "dataset_id");

CREATE TABLE "analytics_dataset_deletion_operations" (
  "id" TEXT NOT NULL,
  "scope" "AnalyticsDatasetDeletionScope" NOT NULL,
  "project_id" TEXT NOT NULL,
  "dataset_id" TEXT NOT NULL,
  "dataset_run_ids" TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
  "dataset_generation" BIGINT,
  "run_generations" JSONB NOT NULL DEFAULT '{}'::JSONB,
  "status" "AnalyticsDatasetDeletionStatus" NOT NULL DEFAULT 'SCHEDULED',
  "phase" TEXT NOT NULL DEFAULT 'visibility_barrier',
  "worker_fence" BIGINT NOT NULL DEFAULT 0,
  "lease_owner" TEXT,
  "lease_expires_at" TIMESTAMP(3),
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "last_error_code" TEXT,
  "logically_invisible" BOOLEAN NOT NULL DEFAULT false,
  "completed_at" TIMESTAMP(3),
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "analytics_dataset_deletion_operations_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "analytics_dataset_deletion_operations_claim_idx"
  ON "analytics_dataset_deletion_operations"("status", "lease_expires_at", "updated_at");
CREATE INDEX "analytics_dataset_deletion_operations_project_id_dataset_id_status_idx"
  ON "analytics_dataset_deletion_operations"("project_id", "dataset_id", "status");

CREATE TABLE "analytics_dataset_deletion_outbox" (
  "id" TEXT NOT NULL,
  "operation_id" TEXT NOT NULL,
  "status" "AnalyticsOutboxStatus" NOT NULL DEFAULT 'PENDING',
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "locked_by" TEXT,
  "locked_until" TIMESTAMP(3),
  "published_at" TIMESTAMP(3),
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "analytics_dataset_deletion_outbox_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "analytics_dataset_deletion_outbox_operation_id_key"
  ON "analytics_dataset_deletion_outbox"("operation_id");
CREATE INDEX "analytics_dataset_deletion_outbox_dispatch_idx"
  ON "analytics_dataset_deletion_outbox"("status", "next_attempt_at", "locked_until");
ALTER TABLE "analytics_dataset_deletion_outbox"
  ADD CONSTRAINT "analytics_dataset_deletion_outbox_operation_id_fkey"
  FOREIGN KEY ("operation_id") REFERENCES "analytics_dataset_deletion_operations"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;
