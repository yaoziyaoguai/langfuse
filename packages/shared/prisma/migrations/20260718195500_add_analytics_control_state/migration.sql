-- CreateEnum
CREATE TYPE "AnalyticsIngestionOperationStatus" AS ENUM ('ACCEPTED', 'QUEUED', 'PERSISTED', 'VISIBLE', 'RETRYING', 'PARTIAL_FAILED', 'QUARANTINED', 'UNRECOVERABLE', 'CANCELLED_BY_DELETION', 'COMPLETED_WITH_CANCELLATIONS');

-- CreateEnum
CREATE TYPE "AnalyticsManifestState" AS ENUM ('PENDING', 'CANDIDATE_PUBLISHED', 'FROZEN');

-- CreateEnum
CREATE TYPE "AnalyticsCandidateDisposition" AS ENUM ('PENDING', 'LOAD_REQUIRED', 'NOOP', 'QUARANTINED', 'CANCELLED_BY_DELETION');

-- CreateEnum
CREATE TYPE "AnalyticsOutboxStatus" AS ENUM ('PENDING', 'PUBLISHED');

-- CreateEnum
CREATE TYPE "AnalyticsLoadBatchStatus" AS ENUM ('PENDING', 'LOADING', 'VISIBLE', 'UNKNOWN', 'FAILED', 'CANCELLED_BY_DELETION');

-- CreateEnum
CREATE TYPE "AnalyticsEntityType" AS ENUM ('EVENT', 'SCORE', 'FILE_REFERENCE');

-- CreateEnum
CREATE TYPE "AnalyticsDeletionScope" AS ENUM ('TRACE', 'PROJECT');

-- CreateEnum
CREATE TYPE "AnalyticsDeletionStatus" AS ENUM ('SCHEDULED', 'RETRYING', 'NEEDS_ATTENTION', 'COMPLETED');

-- CreateEnum
CREATE TYPE "AnalyticsCheckpointStatus" AS ENUM ('PREPARING', 'SEALED', 'ABORTED');

-- CreateEnum
CREATE TYPE "AnalyticsBackgroundMigrationRetirementStatus" AS ENUM ('OBSERVING', 'DRAINING', 'DRAINED', 'TERMINALIZED');

-- CreateTable
CREATE TABLE "analytics_ingestion_operations" (
    "id" TEXT NOT NULL,
    "project_id" TEXT NOT NULL,
    "source_operation_id" TEXT NOT NULL,
    "source_checksum" TEXT NOT NULL,
    "raw_object_key" TEXT NOT NULL,
    "accepted_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "canonicalizer_version" TEXT NOT NULL,
    "schema_version" INTEGER NOT NULL,
    "canonicalization_fence" BIGINT NOT NULL DEFAULT 0,
    "canonicalization_lease_owner" TEXT,
    "canonicalization_lease_until" TIMESTAMP(3),
    "canonicalization_attempts" INTEGER NOT NULL DEFAULT 0,
    "reserved_canonical_object_key" TEXT,
    "canonical_object_key" TEXT,
    "canonical_artifact_checksum" TEXT,
    "manifest_state" "AnalyticsManifestState" NOT NULL DEFAULT 'PENDING',
    "candidate_manifest" JSONB,
    "frozen_manifest" JSONB,
    "status" "AnalyticsIngestionOperationStatus" NOT NULL DEFAULT 'ACCEPTED',
    "cancellation_reason_code" TEXT,
    "last_error_code" TEXT,
    "recoverable_until" TIMESTAMP(3) NOT NULL,
    "status_expires_at" TIMESTAMP(3) NOT NULL,
    "visible_at" TIMESTAMP(3),
    "terminal_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_ingestion_operations_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "analytics_ingestion_candidates" (
    "id" TEXT NOT NULL,
    "operation_id" TEXT NOT NULL,
    "project_id" TEXT NOT NULL,
    "candidate_key" TEXT NOT NULL,
    "entity_type" "AnalyticsEntityType" NOT NULL,
    "entity_key" TEXT NOT NULL,
    "owning_trace_id" TEXT,
    "partition_date" DATE,
    "source_version" BIGINT NOT NULL,
    "canonical_payload_hash" TEXT NOT NULL,
    "disposition" "AnalyticsCandidateDisposition" NOT NULL DEFAULT 'PENDING',
    "load_batch_id" TEXT,
    "reason_code" TEXT,
    "quarantine_expires_at" TIMESTAMP(3),
    "trace_deletion_generation" BIGINT NOT NULL DEFAULT 0,
    "project_deletion_generation" BIGINT NOT NULL DEFAULT 0,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_ingestion_candidates_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "analytics_ingestion_outbox" (
    "id" TEXT NOT NULL,
    "operation_id" TEXT NOT NULL,
    "status" "AnalyticsOutboxStatus" NOT NULL DEFAULT 'PENDING',
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "locked_by" TEXT,
    "locked_until" TIMESTAMP(3),
    "published_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_ingestion_outbox_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "analytics_load_batches" (
    "id" TEXT NOT NULL,
    "operation_id" TEXT NOT NULL,
    "project_id" TEXT NOT NULL,
    "database_name" TEXT NOT NULL,
    "target_table" TEXT NOT NULL,
    "logical_batch_id" TEXT NOT NULL,
    "attempt" INTEGER NOT NULL DEFAULT 0,
    "fence_generation" BIGINT NOT NULL,
    "lease_owner" TEXT,
    "lease_expires_at" TIMESTAMP(3),
    "label" TEXT NOT NULL,
    "payload_hash" TEXT NOT NULL,
    "canonical_object_key" TEXT NOT NULL,
    "partition_date" DATE,
    "status" "AnalyticsLoadBatchStatus" NOT NULL DEFAULT 'PENDING',
    "transaction_id" TEXT,
    "total_rows" INTEGER,
    "filtered_rows" INTEGER,
    "last_error_code" TEXT,
    "visible_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_load_batches_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "analytics_entity_heads" (
    "id" TEXT NOT NULL,
    "project_id" TEXT NOT NULL,
    "entity_type" "AnalyticsEntityType" NOT NULL,
    "entity_key" TEXT NOT NULL,
    "owning_trace_id" TEXT,
    "source_version" BIGINT NOT NULL,
    "canonical_payload_hash" TEXT NOT NULL,
    "partition_date" DATE NOT NULL,
    "canonicalizer_version" TEXT NOT NULL,
    "fence_generation" BIGINT NOT NULL,
    "trace_deletion_generation" BIGINT NOT NULL DEFAULT 0,
    "project_deletion_generation" BIGINT NOT NULL DEFAULT 0,
    "operation_id" TEXT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_entity_heads_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "analytics_deletion_tombstones" (
    "id" TEXT NOT NULL,
    "project_id" TEXT NOT NULL,
    "trace_id" TEXT NOT NULL,
    "generation" BIGINT NOT NULL,
    "status" "AnalyticsDeletionStatus" NOT NULL DEFAULT 'RETRYING',
    "barrier_label" TEXT,
    "barrier_visible_at" TIMESTAMP(3),
    "completed_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_deletion_tombstones_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "analytics_project_deletion_generations" (
    "project_id" TEXT NOT NULL,
    "generation" BIGINT NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_project_deletion_generations_pkey" PRIMARY KEY ("project_id")
);

-- CreateTable
CREATE TABLE "analytics_deletion_operations" (
    "id" TEXT NOT NULL,
    "scope" "AnalyticsDeletionScope" NOT NULL,
    "organization_id" TEXT NOT NULL,
    "project_id" TEXT NOT NULL,
    "trace_id" TEXT,
    "generation" BIGINT NOT NULL,
    "worker_fence" BIGINT NOT NULL DEFAULT 0,
    "lease_owner" TEXT,
    "lease_expires_at" TIMESTAMP(3),
    "requester_principal_type" TEXT NOT NULL,
    "requester_principal_id" TEXT NOT NULL,
    "status" "AnalyticsDeletionStatus" NOT NULL DEFAULT 'RETRYING',
    "phase" TEXT NOT NULL DEFAULT 'visibility_barrier',
    "logically_invisible" BOOLEAN NOT NULL DEFAULT false,
    "cancellation_reason_code" TEXT,
    "status_expires_at" TIMESTAMP(3) NOT NULL,
    "completed_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_deletion_operations_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "trace_control_states" (
    "id" TEXT NOT NULL,
    "project_id" TEXT NOT NULL,
    "trace_id" TEXT NOT NULL,
    "bookmarked" BOOLEAN NOT NULL DEFAULT false,
    "public" BOOLEAN NOT NULL DEFAULT false,
    "revision" BIGINT NOT NULL DEFAULT 0,
    "initialized_by_operation_id" TEXT,
    "last_mutation_source" TEXT NOT NULL DEFAULT 'ingestion',
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "trace_control_states_pkey" PRIMARY KEY ("id")
);

-- CreateTable
CREATE TABLE "analytics_checkpoint_generations" (
    "generation" BIGINT NOT NULL,
    "status" "AnalyticsCheckpointStatus" NOT NULL DEFAULT 'PREPARING',
    "lease_owner" TEXT NOT NULL,
    "lease_expires_at" TIMESTAMP(3) NOT NULL,
    "operation_high_watermark_accepted_at" TIMESTAMP(3) NOT NULL,
    "load_high_watermark_created_at" TIMESTAMP(3) NOT NULL,
    "postgres_snapshot_id" TEXT,
    "doris_snapshot_id" TEXT,
    "artifact_digests" JSONB,
    "key_id" TEXT,
    "predecessor_hash" TEXT,
    "manifest_hash" TEXT,
    "signature" TEXT,
    "sealed_at" TIMESTAMP(3),
    "aborted_at" TIMESTAMP(3),
    "abort_reason_code" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_checkpoint_generations_pkey" PRIMARY KEY ("generation")
);

-- CreateTable
CREATE TABLE "analytics_background_migration_retirement" (
    "id" TEXT NOT NULL,
    "fence_name" TEXT NOT NULL,
    "manager_instance_id" TEXT NOT NULL,
    "generation" BIGINT NOT NULL,
    "status" "AnalyticsBackgroundMigrationRetirementStatus" NOT NULL DEFAULT 'OBSERVING',
    "minimum_manager_build_id" TEXT NOT NULL,
    "manager_build_id" TEXT NOT NULL,
    "manager_heartbeat_at" TIMESTAMP(3) NOT NULL,
    "active_migration_name" TEXT,
    "active_lease_expires_at" TIMESTAMP(3),
    "drained_at" TIMESTAMP(3),
    "terminalized_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_background_migration_retirement_pkey" PRIMARY KEY ("id")
);

-- CreateIndex
CREATE INDEX "analytics_ingestion_operations_status_recoverable_until_idx" ON "analytics_ingestion_operations"("status", "recoverable_until");

-- CreateIndex
CREATE INDEX "analytics_ingestion_operations_manifest_state_status_update_idx" ON "analytics_ingestion_operations"("manifest_state", "status", "updated_at");

-- CreateIndex
CREATE INDEX "analytics_ingestion_operations_manifest_state_canonicalizat_idx" ON "analytics_ingestion_operations"("manifest_state", "canonicalization_lease_until");

-- CreateIndex
CREATE INDEX "analytics_ingestion_operations_project_id_status_accepted_a_idx" ON "analytics_ingestion_operations"("project_id", "status", "accepted_at");

-- CreateIndex
CREATE INDEX "analytics_ingestion_operations_canonicalizer_version_schema_idx" ON "analytics_ingestion_operations"("canonicalizer_version", "schema_version", "recoverable_until");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_ingestion_operations_project_id_source_operation__key" ON "analytics_ingestion_operations"("project_id", "source_operation_id");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_ingestion_operations_id_project_id_key" ON "analytics_ingestion_operations"("id", "project_id");

-- CreateIndex
CREATE INDEX "analytics_ingestion_candidates_operation_id_disposition_idx" ON "analytics_ingestion_candidates"("operation_id", "disposition");

-- CreateIndex
CREATE INDEX "analytics_ingestion_candidates_project_id_owning_trace_id_d_idx" ON "analytics_ingestion_candidates"("project_id", "owning_trace_id", "disposition");

-- CreateIndex
CREATE INDEX "analytics_ingestion_candidates_load_batch_id_idx" ON "analytics_ingestion_candidates"("load_batch_id");

-- CreateIndex
CREATE INDEX "analytics_ingestion_candidates_disposition_quarantine_expir_idx" ON "analytics_ingestion_candidates"("disposition", "quarantine_expires_at");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_ingestion_candidates_operation_id_candidate_key_key" ON "analytics_ingestion_candidates"("operation_id", "candidate_key");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_ingestion_outbox_operation_id_key" ON "analytics_ingestion_outbox"("operation_id");

-- CreateIndex
CREATE INDEX "analytics_ingestion_outbox_status_next_attempt_at_locked_un_idx" ON "analytics_ingestion_outbox"("status", "next_attempt_at", "locked_until");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_load_batches_label_key" ON "analytics_load_batches"("label");

-- CreateIndex
CREATE INDEX "analytics_load_batches_operation_id_status_idx" ON "analytics_load_batches"("operation_id", "status");

-- CreateIndex
CREATE INDEX "analytics_load_batches_project_id_status_updated_at_idx" ON "analytics_load_batches"("project_id", "status", "updated_at");

-- CreateIndex
CREATE INDEX "analytics_load_batches_status_updated_at_idx" ON "analytics_load_batches"("status", "updated_at");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_load_batches_project_id_operation_id_database_nam_key" ON "analytics_load_batches"("project_id", "operation_id", "database_name", "target_table", "logical_batch_id", "attempt");

-- CreateIndex
CREATE INDEX "analytics_entity_heads_project_id_owning_trace_id_entity_ty_idx" ON "analytics_entity_heads"("project_id", "owning_trace_id", "entity_type");

-- CreateIndex
CREATE INDEX "analytics_entity_heads_project_id_entity_type_partition_dat_idx" ON "analytics_entity_heads"("project_id", "entity_type", "partition_date");

-- CreateIndex
CREATE INDEX "analytics_entity_heads_operation_id_idx" ON "analytics_entity_heads"("operation_id");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_entity_heads_project_id_entity_type_entity_key_key" ON "analytics_entity_heads"("project_id", "entity_type", "entity_key");

-- CreateIndex
CREATE INDEX "analytics_deletion_tombstones_project_id_status_updated_at_idx" ON "analytics_deletion_tombstones"("project_id", "status", "updated_at");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_deletion_tombstones_project_id_trace_id_key" ON "analytics_deletion_tombstones"("project_id", "trace_id");

-- CreateIndex
CREATE INDEX "analytics_deletion_operations_organization_id_id_idx" ON "analytics_deletion_operations"("organization_id", "id");

-- CreateIndex
CREATE INDEX "analytics_deletion_operations_organization_id_project_id_st_idx" ON "analytics_deletion_operations"("organization_id", "project_id", "status");

-- CreateIndex
CREATE INDEX "analytics_deletion_operations_status_updated_at_idx" ON "analytics_deletion_operations"("status", "updated_at");

-- CreateIndex
CREATE INDEX "analytics_deletion_operations_project_id_scope_generation_idx" ON "analytics_deletion_operations"("project_id", "scope", "generation");

-- CreateIndex
CREATE INDEX "trace_control_states_project_id_initialized_by_operation_id_idx" ON "trace_control_states"("project_id", "initialized_by_operation_id");

-- CreateIndex
CREATE INDEX "trace_control_states_project_id_public_idx" ON "trace_control_states"("project_id", "public");

-- CreateIndex
CREATE UNIQUE INDEX "trace_control_states_project_id_trace_id_key" ON "trace_control_states"("project_id", "trace_id");

-- CreateIndex
CREATE INDEX "analytics_checkpoint_generations_status_lease_expires_at_idx" ON "analytics_checkpoint_generations"("status", "lease_expires_at");

-- CreateIndex
CREATE UNIQUE INDEX "analytics_background_migration_retirement_fence_name_manage_key" ON "analytics_background_migration_retirement"("fence_name", "manager_instance_id");

-- CreateIndex
CREATE INDEX "analytics_background_migration_retirement_fence_name_status_idx" ON "analytics_background_migration_retirement"("fence_name", "status", "manager_heartbeat_at");

-- AddForeignKey
ALTER TABLE "analytics_ingestion_outbox" ADD CONSTRAINT "analytics_ingestion_outbox_operation_id_fkey" FOREIGN KEY ("operation_id") REFERENCES "analytics_ingestion_operations"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "analytics_ingestion_operations" ADD CONSTRAINT "analytics_ingestion_operations_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "projects"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "analytics_ingestion_candidates" ADD CONSTRAINT "analytics_ingestion_candidates_operation_id_project_id_fkey" FOREIGN KEY ("operation_id", "project_id") REFERENCES "analytics_ingestion_operations"("id", "project_id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "analytics_load_batches" ADD CONSTRAINT "analytics_load_batches_operation_id_project_id_fkey" FOREIGN KEY ("operation_id", "project_id") REFERENCES "analytics_ingestion_operations"("id", "project_id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "analytics_entity_heads" ADD CONSTRAINT "analytics_entity_heads_operation_id_project_id_fkey" FOREIGN KEY ("operation_id", "project_id") REFERENCES "analytics_ingestion_operations"("id", "project_id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "analytics_entity_heads" ADD CONSTRAINT "analytics_entity_heads_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "projects"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "analytics_deletion_tombstones" ADD CONSTRAINT "analytics_deletion_tombstones_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "projects"("id") ON DELETE CASCADE ON UPDATE CASCADE;

-- AddForeignKey
ALTER TABLE "trace_control_states" ADD CONSTRAINT "trace_control_states_project_id_fkey" FOREIGN KEY ("project_id") REFERENCES "projects"("id") ON DELETE CASCADE ON UPDATE CASCADE;
