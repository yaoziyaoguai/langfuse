CREATE TYPE "BatchExportManifestState" AS ENUM ('PREPARING', 'SEALED');
CREATE TYPE "BatchExportExecutionState" AS ENUM (
    'PENDING',
    'EXPORTING',
    'COMPLETED',
    'FAILED',
    'CANCELLED',
    'QUARANTINED'
);
CREATE TYPE "BatchExportDispatchStatus" AS ENUM (
    'PENDING',
    'PUBLISHED',
    'CANCELLED',
    'FAILED'
);

ALTER TABLE "batch_exports"
    ADD COLUMN "analytics_backend" "AnalyticsBackendType",
    ADD COLUMN "deployment_generation" BIGINT,
    ADD COLUMN "workload_epoch_fingerprint" TEXT,
    ADD COLUMN "runtime_contract_version" INTEGER,
    ADD COLUMN "producer_runtime_lease_id" TEXT,
    ADD COLUMN "capability_activation_generation" BIGINT,
    ADD COLUMN "capability_contract_version" INTEGER,
    ADD COLUMN "manifest_state" "BatchExportManifestState",
    ADD COLUMN "manifest_generation" BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN "manifest_claim_id" TEXT,
    ADD COLUMN "manifest_lease_owner" TEXT,
    ADD COLUMN "manifest_lease_expires_at" TIMESTAMP(3),
    ADD COLUMN "manifest_object_key" TEXT,
    ADD COLUMN "manifest_checksum" TEXT,
    ADD COLUMN "manifest_row_count" INTEGER,
    ADD COLUMN "manifest_byte_count" BIGINT,
    ADD COLUMN "manifest_filter_hash" TEXT,
    ADD COLUMN "manifest_format_version" INTEGER,
    ADD COLUMN "manifest_sealed_at" TIMESTAMP(3),
    ADD COLUMN "execution_state" "BatchExportExecutionState",
    ADD COLUMN "execution_generation" BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN "execution_claim_id" TEXT,
    ADD COLUMN "execution_lease_owner" TEXT,
    ADD COLUMN "execution_lease_expires_at" TIMESTAMP(3),
    ADD COLUMN "failure_code" TEXT;

ALTER TABLE "batch_exports"
    ADD CONSTRAINT "batch_exports_producer_runtime_lease_id_fkey"
    FOREIGN KEY ("producer_runtime_lease_id") REFERENCES "analytics_runtime_leases"("id")
    ON DELETE RESTRICT ON UPDATE CASCADE,
    ADD CONSTRAINT "batch_exports_provenance_check" CHECK (
      ("analytics_backend" IS NULL
        AND "deployment_generation" IS NULL
        AND "workload_epoch_fingerprint" IS NULL
        AND "runtime_contract_version" IS NULL
        AND "producer_runtime_lease_id" IS NULL
        AND "capability_activation_generation" IS NULL
        AND "capability_contract_version" IS NULL)
      OR
      ("analytics_backend" = 'DORIS'
        AND "deployment_generation" > 0
        AND length("workload_epoch_fingerprint") = 64
        AND "runtime_contract_version" > 0
        AND "producer_runtime_lease_id" IS NOT NULL
        AND "capability_activation_generation" > 0
        AND "capability_contract_version" > 0)
    ),
    ADD CONSTRAINT "batch_exports_manifest_generation_check" CHECK ("manifest_generation" >= 0),
    ADD CONSTRAINT "batch_exports_execution_generation_check" CHECK ("execution_generation" >= 0),
    ADD CONSTRAINT "batch_exports_manifest_descriptor_check" CHECK (
      ("manifest_state" IS NULL
        AND "manifest_object_key" IS NULL
        AND "manifest_checksum" IS NULL
        AND "manifest_row_count" IS NULL
        AND "manifest_byte_count" IS NULL
        AND "manifest_filter_hash" IS NULL
        AND "manifest_format_version" IS NULL
        AND "manifest_sealed_at" IS NULL)
      OR
      ("manifest_state" = 'PREPARING'
        AND "manifest_object_key" IS NULL
        AND "manifest_checksum" IS NULL
        AND "manifest_row_count" IS NULL
        AND "manifest_byte_count" IS NULL
        AND "manifest_filter_hash" IS NOT NULL
        AND length("manifest_filter_hash") = 64
        AND "manifest_format_version" IS NULL
        AND "manifest_sealed_at" IS NULL)
      OR
      ("manifest_state" = 'SEALED'
        AND "manifest_object_key" IS NOT NULL
        AND length("manifest_checksum") = 64
        AND "manifest_row_count" >= 0
        AND "manifest_byte_count" > 0
        AND length("manifest_filter_hash") = 64
        AND "manifest_format_version" > 0
        AND "manifest_sealed_at" IS NOT NULL)
    );

CREATE UNIQUE INDEX "batch_exports_manifest_object_key_key"
ON "batch_exports"("manifest_object_key");
CREATE INDEX "batch_exports_backend_generation_status_idx"
ON "batch_exports"("analytics_backend", "deployment_generation", "status");
CREATE INDEX "batch_exports_manifest_state_lease_idx"
ON "batch_exports"("manifest_state", "manifest_lease_expires_at");
CREATE INDEX "batch_exports_execution_state_lease_idx"
ON "batch_exports"("execution_state", "execution_lease_expires_at");

CREATE TABLE "batch_export_dispatch_outbox" (
    "id" TEXT NOT NULL,
    "batch_export_id" TEXT NOT NULL,
    "status" "BatchExportDispatchStatus" NOT NULL DEFAULT 'PENDING',
    "generation" INTEGER NOT NULL DEFAULT 1,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "locked_by" TEXT,
    "locked_until" TIMESTAMP(3),
    "published_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "batch_export_dispatch_outbox_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "batch_export_dispatch_outbox_generation_check" CHECK ("generation" > 0),
    CONSTRAINT "batch_export_dispatch_outbox_attempts_check" CHECK ("attempts" >= 0)
);

CREATE UNIQUE INDEX "batch_export_dispatch_outbox_batch_export_id_key"
ON "batch_export_dispatch_outbox"("batch_export_id");
CREATE INDEX "batch_export_dispatch_outbox_pending_idx"
ON "batch_export_dispatch_outbox"("status", "next_attempt_at", "locked_until");
CREATE INDEX "batch_export_dispatch_outbox_recovery_idx"
ON "batch_export_dispatch_outbox"("status", "updated_at");

ALTER TABLE "batch_export_dispatch_outbox"
ADD CONSTRAINT "batch_export_dispatch_outbox_batch_export_id_fkey"
FOREIGN KEY ("batch_export_id") REFERENCES "batch_exports"("id")
ON DELETE CASCADE ON UPDATE CASCADE;
