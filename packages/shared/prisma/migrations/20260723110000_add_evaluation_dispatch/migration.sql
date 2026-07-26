CREATE TYPE "AnalyticsEvaluationDispatchTargetType" AS ENUM (
  'TRACE_UPSERT',
  'OBSERVATION_UPSERT',
  'DATASET_RUN_ITEM_UPSERT',
  'HISTORICAL'
);

CREATE TYPE "AnalyticsEvaluationDispatchStatus" AS ENUM (
  'SUSPENDED',
  'PENDING',
  'PUBLISHED',
  'PROCESSING',
  'COMPLETED',
  'NOT_ACTIVATED',
  'QUARANTINED',
  'CANCELLED'
);

CREATE TABLE "analytics_evaluation_dispatches" (
  "id" TEXT NOT NULL,
  "operation_id" TEXT NOT NULL,
  "project_id" TEXT NOT NULL,
  "source_candidate_key" TEXT NOT NULL,
  "target_type" "AnalyticsEvaluationDispatchTargetType" NOT NULL,
  "target_id" TEXT NOT NULL,
  "trace_id" TEXT NOT NULL,
  "observation_id" TEXT,
  "dataset_item_id" TEXT,
  "dataset_item_valid_from" TIMESTAMP(3),
  "target_timestamp" TIMESTAMP(3) NOT NULL,
  "trace_environment" TEXT,
  "analytics_backend" "AnalyticsBackendType" NOT NULL,
  "deployment_generation" BIGINT NOT NULL,
  "workload_epoch_fingerprint" TEXT NOT NULL,
  "runtime_contract_version" INTEGER NOT NULL,
  "capture_runtime_lease_id" TEXT NOT NULL,
  "capability_activation_generation" BIGINT NOT NULL,
  "capability_contract_version" INTEGER NOT NULL,
  "status" "AnalyticsEvaluationDispatchStatus" NOT NULL,
  "dispatch_generation" INTEGER NOT NULL DEFAULT 1,
  "attempts" INTEGER NOT NULL DEFAULT 0,
  "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "published_at" TIMESTAMP(3),
  "completed_at" TIMESTAMP(3),
  "failure_code" TEXT,
  "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

  CONSTRAINT "analytics_evaluation_dispatches_pkey" PRIMARY KEY ("id"),
  CONSTRAINT "analytics_evaluation_dispatches_doris_provenance_check" CHECK (
    "analytics_backend" = 'DORIS'
    AND "deployment_generation" > 0
    AND length("workload_epoch_fingerprint") = 64
    AND "runtime_contract_version" > 0
    AND "capability_activation_generation" > 0
    AND "capability_contract_version" > 0
  ),
  CONSTRAINT "analytics_evaluation_dispatches_generation_check" CHECK (
    "dispatch_generation" > 0
  ),
  CONSTRAINT "analytics_evaluation_dispatches_attempts_check" CHECK (
    "attempts" >= 0
  ),
  CONSTRAINT "analytics_evaluation_dispatches_target_shape_check" CHECK (
    ("target_type" = 'TRACE_UPSERT'
      AND "target_id" = "trace_id"
      AND "observation_id" IS NULL
      AND "dataset_item_id" IS NULL)
    OR
    ("target_type" = 'OBSERVATION_UPSERT'
      AND "observation_id" IS NOT NULL
      AND "target_id" = "observation_id"
      AND "dataset_item_id" IS NULL)
    OR
    ("target_type" = 'DATASET_RUN_ITEM_UPSERT'
      AND "dataset_item_id" IS NOT NULL)
    OR
    "target_type" = 'HISTORICAL'
  )
);

CREATE UNIQUE INDEX "analytics_evaluation_dispatches_operation_target_key"
  ON "analytics_evaluation_dispatches"(
    "operation_id",
    "target_type",
    "target_id"
  );
CREATE INDEX "analytics_evaluation_dispatches_pending_idx"
  ON "analytics_evaluation_dispatches"(
    "status",
    "next_attempt_at",
    "created_at"
  );
CREATE INDEX "analytics_evaluation_dispatches_backend_generation_status_idx"
  ON "analytics_evaluation_dispatches"(
    "analytics_backend",
    "deployment_generation",
    "status"
  );
CREATE INDEX "analytics_eval_dispatch_activation_status_idx"
  ON "analytics_evaluation_dispatches"(
    "capability_activation_generation",
    "status"
  );
CREATE INDEX "analytics_evaluation_dispatches_project_trace_status_idx"
  ON "analytics_evaluation_dispatches"("project_id", "trace_id", "status");

ALTER TABLE "analytics_evaluation_dispatches"
  ADD CONSTRAINT "analytics_evaluation_dispatches_operation_id_project_id_fkey"
  FOREIGN KEY ("operation_id", "project_id")
  REFERENCES "analytics_ingestion_operations"("id", "project_id")
  ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "analytics_evaluation_dispatches"
  ADD CONSTRAINT "analytics_evaluation_dispatches_capture_runtime_lease_id_fkey"
  FOREIGN KEY ("capture_runtime_lease_id")
  REFERENCES "analytics_runtime_leases"("id")
  ON DELETE RESTRICT ON UPDATE CASCADE;
