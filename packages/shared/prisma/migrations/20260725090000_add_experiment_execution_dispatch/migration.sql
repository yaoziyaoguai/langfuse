CREATE TYPE "ExperimentExecutionState" AS ENUM (
  'PENDING',
  'PROCESSING',
  'COMPLETED',
  'FAILED',
  'CANCELLED',
  'QUARANTINED'
);

CREATE TYPE "ExperimentExecutionDispatchStatus" AS ENUM (
  'PENDING',
  'PUBLISHED',
  'CANCELLED'
);

ALTER TABLE "dataset_runs"
  ADD COLUMN "analytics_backend" "AnalyticsBackendType",
  ADD COLUMN "deployment_generation" BIGINT,
  ADD COLUMN "workload_epoch_fingerprint" TEXT,
  ADD COLUMN "runtime_contract_version" INTEGER,
  ADD COLUMN "producer_runtime_lease_id" TEXT,
  ADD COLUMN "capability_activation_generation" BIGINT,
  ADD COLUMN "capability_contract_version" INTEGER,
  ADD COLUMN "experiment_execution_state" "ExperimentExecutionState",
  ADD COLUMN "experiment_execution_generation" BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN "experiment_execution_claim_id" TEXT,
  ADD COLUMN "experiment_execution_lease_owner" TEXT,
  ADD COLUMN "experiment_execution_lease_expires_at" TIMESTAMP(3),
  ADD COLUMN "experiment_failure_code" TEXT,
  ADD COLUMN "experiment_completed_at" TIMESTAMP(3);

ALTER TABLE "dataset_runs"
  ADD CONSTRAINT "dataset_runs_experiment_producer_runtime_lease_id_fkey"
  FOREIGN KEY ("producer_runtime_lease_id")
  REFERENCES "analytics_runtime_leases"("id")
  ON DELETE RESTRICT ON UPDATE CASCADE,
  ADD CONSTRAINT "dataset_runs_experiment_provenance_check" CHECK (
    ("analytics_backend" IS NULL
      AND "deployment_generation" IS NULL
      AND "workload_epoch_fingerprint" IS NULL
      AND "runtime_contract_version" IS NULL
      AND "producer_runtime_lease_id" IS NULL
      AND "capability_activation_generation" IS NULL
      AND "capability_contract_version" IS NULL
      AND "experiment_execution_state" IS NULL)
    OR
    ("analytics_backend" = 'DORIS'
      AND "deployment_generation" > 0
      AND length("workload_epoch_fingerprint") = 64
      AND "runtime_contract_version" > 0
      AND "producer_runtime_lease_id" IS NOT NULL
      AND "capability_activation_generation" > 0
      AND "capability_contract_version" > 0
      AND "experiment_execution_state" IS NOT NULL)
  ),
  ADD CONSTRAINT "dataset_runs_experiment_execution_generation_check"
  CHECK ("experiment_execution_generation" >= 0);

CREATE INDEX "dataset_runs_experiment_backend_state_idx"
  ON "dataset_runs"(
    "analytics_backend",
    "deployment_generation",
    "experiment_execution_state"
  );
CREATE INDEX "dataset_runs_experiment_execution_lease_idx"
  ON "dataset_runs"(
    "experiment_execution_state",
    "experiment_execution_lease_expires_at"
  );

CREATE TABLE "experiment_execution_dispatch_outbox" (
  "id" TEXT NOT NULL,
  "dataset_run_id" TEXT NOT NULL,
  "project_id" TEXT NOT NULL,
  "status" "ExperimentExecutionDispatchStatus" NOT NULL DEFAULT 'PENDING',
  "generation" INTEGER NOT NULL DEFAULT 1,
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "published_at" TIMESTAMP(3),
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "experiment_execution_dispatch_outbox_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "experiment_execution_dispatch_generation_check"
    CHECK ("generation" > 0),
  CONSTRAINT "experiment_execution_dispatch_attempts_check"
    CHECK ("attempts" >= 0)
);

CREATE UNIQUE INDEX "experiment_execution_dispatch_run_project_key"
  ON "experiment_execution_dispatch_outbox"("dataset_run_id", "project_id");
CREATE INDEX "experiment_execution_dispatch_pending_idx"
  ON "experiment_execution_dispatch_outbox"(
    "status",
    "next_attempt_at",
    "created_at"
  );

ALTER TABLE "experiment_execution_dispatch_outbox"
  ADD CONSTRAINT "experiment_execution_dispatch_run_project_fkey"
  FOREIGN KEY ("dataset_run_id", "project_id")
  REFERENCES "dataset_runs"("id", "project_id")
  ON DELETE CASCADE ON UPDATE CASCADE;
