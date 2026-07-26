CREATE TYPE "AnalyticsIntegrationExecutionKind" AS ENUM (
  'BOOTSTRAP',
  'INCREMENTAL',
  'RESCAN'
);

CREATE TYPE "AnalyticsIntegrationExecutionStatus" AS ENUM (
  'SEALED',
  'PUBLISHED',
  'RUNNING',
  'RETRYING',
  'COMPLETED',
  'CANCELLED',
  'QUARANTINED'
);

CREATE TABLE "analytics_integration_executions" (
  "id" TEXT NOT NULL,
  "integration_state_id" TEXT NOT NULL,
  "integration_type" "AnalyticsIntegrationType" NOT NULL,
  "integration_generation" BIGINT NOT NULL,
  "project_id" TEXT NOT NULL,
  "kind" "AnalyticsIntegrationExecutionKind" NOT NULL,
  "analytics_backend" "AnalyticsBackendType" NOT NULL,
  "deployment_generation" BIGINT NOT NULL,
  "workload_epoch_fingerprint" TEXT NOT NULL,
  "runtime_contract_version" INTEGER NOT NULL,
  "capability_activation_generation" BIGINT NOT NULL,
  "capability_contract_version" INTEGER NOT NULL,
  "sealed_runtime_lease_id" TEXT NOT NULL,
  "manifest" JSONB NOT NULL,
  "manifest_checksum" TEXT NOT NULL,
  "delivery_count" INTEGER NOT NULL,
  "status" "AnalyticsIntegrationExecutionStatus" NOT NULL DEFAULT 'SEALED',
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "queue_job_id" TEXT,
  "published_at" TIMESTAMP(3),
  "claim_owner" TEXT,
  "claim_expires_at" TIMESTAMP(3),
  "last_error_code" TEXT,
  "completed_at" TIMESTAMP(3),
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "analytics_integration_executions_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "analytics_integration_executions_generation_check"
    CHECK (
      "integration_generation" > 0
      AND "deployment_generation" > 0
      AND "capability_activation_generation" > 0
    ),
  CONSTRAINT "analytics_integration_executions_contract_check"
    CHECK (
      "runtime_contract_version" > 0
      AND "capability_contract_version" > 0
      AND length("workload_epoch_fingerprint") = 64
    ),
  CONSTRAINT "analytics_integration_executions_manifest_check"
    CHECK (
      length("manifest_checksum") = 64
      AND "delivery_count" >= 0
      AND "delivery_count" <= 500
    ),
  CONSTRAINT "analytics_integration_executions_attempts_check"
    CHECK ("attempts" >= 0)
);

CREATE UNIQUE INDEX "analytics_integration_executions_manifest_key"
  ON "analytics_integration_executions"(
    "integration_state_id",
    "integration_generation",
    "manifest_checksum"
  );
CREATE INDEX "analytics_integration_executions_publish_idx"
  ON "analytics_integration_executions"(
    "status",
    "next_attempt_at",
    "created_at"
  );
CREATE INDEX "analytics_integration_executions_state_idx"
  ON "analytics_integration_executions"(
    "integration_state_id",
    "integration_generation",
    "status"
  );
CREATE INDEX "analytics_integration_executions_backend_idx"
  ON "analytics_integration_executions"(
    "analytics_backend",
    "deployment_generation",
    "status"
  );
CREATE INDEX "analytics_integration_executions_claim_idx"
  ON "analytics_integration_executions"("status", "claim_expires_at");

ALTER TABLE "analytics_integration_executions"
  ADD CONSTRAINT "analytics_integration_executions_state_fkey"
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
  ADD CONSTRAINT "analytics_integration_executions_runtime_fkey"
  FOREIGN KEY ("sealed_runtime_lease_id")
  REFERENCES "analytics_runtime_leases"("id")
  ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "analytics_integration_pending_deliveries"
  ADD COLUMN "execution_id" TEXT;

CREATE INDEX "analytics_integration_deliveries_execution_idx"
  ON "analytics_integration_pending_deliveries"("execution_id", "status");

ALTER TABLE "analytics_integration_pending_deliveries"
  ADD CONSTRAINT "analytics_integration_deliveries_execution_fkey"
  FOREIGN KEY ("execution_id")
  REFERENCES "analytics_integration_executions"("id")
  ON DELETE RESTRICT ON UPDATE CASCADE;
