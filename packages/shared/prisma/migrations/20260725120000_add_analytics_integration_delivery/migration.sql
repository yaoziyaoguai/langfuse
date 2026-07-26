CREATE TYPE "AnalyticsIntegrationType" AS ENUM (
  'POSTHOG',
  'MIXPANEL',
  'BLOB_STORAGE'
);

CREATE TYPE "AnalyticsIntegrationLifecycleStatus" AS ENUM (
  'BOOTSTRAPPING_DARK',
  'BOOTSTRAPPING_ACTIVE',
  'ACTIVE',
  'PAUSED_BACKLOG',
  'RESCANNING',
  'DRAINING',
  'DISABLED'
);

CREATE TYPE "AnalyticsIntegrationDeliveryKind" AS ENUM (
  'TRACE',
  'GENERATION',
  'OBSERVATION',
  'SCORE'
);

CREATE TYPE "AnalyticsIntegrationDeliveryStatus" AS ENUM (
  'SUSPENDED',
  'PENDING',
  'CLAIMED',
  'COMPLETED',
  'SOURCE_DELETED',
  'QUARANTINED'
);

CREATE TABLE "analytics_integration_states" (
  "id" TEXT NOT NULL,
  "project_id" TEXT NOT NULL,
  "integration_type" "AnalyticsIntegrationType" NOT NULL,
  "generation" BIGINT NOT NULL DEFAULT 1,
  "status" "AnalyticsIntegrationLifecycleStatus" NOT NULL DEFAULT 'DISABLED',
  "pending_rows" BIGINT NOT NULL DEFAULT 0,
  "pending_estimated_bytes" BIGINT NOT NULL DEFAULT 0,
  "rescan_required" BOOLEAN NOT NULL DEFAULT false,
  "cutoff_acceptance_sequence" BIGINT,
  "bootstrap_manifest_key" TEXT,
  "bootstrap_manifest_checksum" TEXT,
  "bootstrap_manifest_rows" BIGINT,
  "bootstrap_sealed_at" TIMESTAMP(3),
  "last_error_code" TEXT,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "analytics_integration_states_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "analytics_integration_states_generation_check"
    CHECK ("generation" > 0),
  CONSTRAINT "analytics_integration_states_pending_rows_check"
    CHECK ("pending_rows" >= 0),
  CONSTRAINT "analytics_integration_states_pending_bytes_check"
    CHECK ("pending_estimated_bytes" >= 0),
  CONSTRAINT "analytics_integration_states_bootstrap_manifest_check" CHECK (
    (
      "bootstrap_manifest_key" IS NULL
      AND "bootstrap_manifest_checksum" IS NULL
      AND "bootstrap_manifest_rows" IS NULL
      AND "bootstrap_sealed_at" IS NULL
    )
    OR
    (
      "bootstrap_manifest_key" IS NOT NULL
      AND length("bootstrap_manifest_checksum") = 64
      AND "bootstrap_manifest_rows" >= 0
      AND "bootstrap_sealed_at" IS NOT NULL
    )
  )
);

CREATE UNIQUE INDEX "analytics_integration_states_project_type_key"
  ON "analytics_integration_states"("project_id", "integration_type");
CREATE UNIQUE INDEX "analytics_integration_states_identity_generation_key"
  ON "analytics_integration_states"("id", "integration_type", "generation");
CREATE INDEX "analytics_integration_states_status_rescan_idx"
  ON "analytics_integration_states"("status", "rescan_required", "updated_at");

ALTER TABLE "analytics_integration_states"
  ADD CONSTRAINT "analytics_integration_states_project_id_fkey"
  FOREIGN KEY ("project_id")
  REFERENCES "projects"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE "analytics_integration_pending_deliveries" (
  "id" TEXT NOT NULL,
  "integration_state_id" TEXT NOT NULL,
  "integration_type" "AnalyticsIntegrationType" NOT NULL,
  "integration_generation" BIGINT NOT NULL,
  "operation_id" TEXT NOT NULL,
  "project_id" TEXT NOT NULL,
  "source_candidate_key" TEXT NOT NULL,
  "delivery_kind" "AnalyticsIntegrationDeliveryKind" NOT NULL,
  "entity_key" TEXT NOT NULL,
  "estimated_bytes" INTEGER NOT NULL,
  "analytics_backend" "AnalyticsBackendType" NOT NULL,
  "deployment_generation" BIGINT NOT NULL,
  "workload_epoch_fingerprint" TEXT NOT NULL,
  "runtime_contract_version" INTEGER NOT NULL,
  "capture_runtime_lease_id" TEXT NOT NULL,
  "capability_activation_generation" BIGINT NOT NULL,
  "capability_contract_version" INTEGER NOT NULL,
  "status" "AnalyticsIntegrationDeliveryStatus" NOT NULL,
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "claim_owner" TEXT,
  "claim_expires_at" TIMESTAMP(3),
  "completed_at" TIMESTAMP(3),
  "failure_code" TEXT,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "analytics_integration_pending_deliveries_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "analytics_integration_deliveries_generation_check"
    CHECK (
      "integration_generation" > 0
      AND "deployment_generation" > 0
      AND "capability_activation_generation" > 0
    ),
  CONSTRAINT "analytics_integration_deliveries_contract_check"
    CHECK (
      "runtime_contract_version" > 0
      AND "capability_contract_version" > 0
      AND length("workload_epoch_fingerprint") = 64
    ),
  CONSTRAINT "analytics_integration_deliveries_attempts_check"
    CHECK ("attempts" >= 0),
  CONSTRAINT "analytics_integration_deliveries_estimated_bytes_check"
    CHECK ("estimated_bytes" >= 0)
);

CREATE UNIQUE INDEX "analytics_integration_deliveries_stable_key"
  ON "analytics_integration_pending_deliveries"(
    "integration_state_id",
    "integration_generation",
    "operation_id",
    "delivery_kind",
    "entity_key"
  );
CREATE INDEX "analytics_integration_deliveries_claim_idx"
  ON "analytics_integration_pending_deliveries"(
    "integration_state_id",
    "integration_generation",
    "status",
    "next_attempt_at",
    "id"
  );
CREATE INDEX "analytics_integration_deliveries_backend_generation_idx"
  ON "analytics_integration_pending_deliveries"(
    "analytics_backend",
    "deployment_generation",
    "status"
  );
CREATE INDEX "analytics_integration_deliveries_operation_idx"
  ON "analytics_integration_pending_deliveries"(
    "operation_id",
    "project_id"
  );
CREATE INDEX "analytics_integration_deliveries_claim_expiry_idx"
  ON "analytics_integration_pending_deliveries"(
    "status",
    "claim_expires_at"
  );

ALTER TABLE "analytics_integration_pending_deliveries"
  ADD CONSTRAINT "analytics_integration_deliveries_state_fkey"
  FOREIGN KEY (
    "integration_state_id",
    "integration_type",
    "integration_generation"
  )
  REFERENCES "analytics_integration_states"(
    "id",
    "integration_type",
    "generation"
  )
  ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT "analytics_integration_deliveries_operation_fkey"
  FOREIGN KEY ("operation_id", "project_id")
  REFERENCES "analytics_ingestion_operations"("id", "project_id")
  ON DELETE CASCADE ON UPDATE CASCADE,
  ADD CONSTRAINT "analytics_integration_deliveries_runtime_fkey"
  FOREIGN KEY ("capture_runtime_lease_id")
  REFERENCES "analytics_runtime_leases"("id")
  ON DELETE RESTRICT ON UPDATE CASCADE;
