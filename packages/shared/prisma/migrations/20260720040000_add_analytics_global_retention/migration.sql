CREATE TABLE "analytics_retention_runs" (
    "id" TEXT NOT NULL,
    "cutoff_date" DATE NOT NULL,
    "phase" TEXT NOT NULL DEFAULT 'DRAIN',
    "status" TEXT NOT NULL DEFAULT 'RUNNING',
    "last_error_code" TEXT,
    "started_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "completed_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_retention_runs_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "analytics_retention_state" (
    "id" TEXT NOT NULL,
    "purged_before" DATE,
    "active_cutoff" DATE,
    "active_run_id" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_retention_state_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "analytics_retention_runs_status_updated_at_idx"
    ON "analytics_retention_runs"("status", "updated_at");

CREATE UNIQUE INDEX "analytics_retention_state_active_run_id_key"
    ON "analytics_retention_state"("active_run_id");

CREATE INDEX "analytics_load_batches_status_partition_date_idx"
    ON "analytics_load_batches"("status", "partition_date");

CREATE INDEX "analytics_entity_heads_entity_type_partition_date_id_idx"
    ON "analytics_entity_heads"("entity_type", "partition_date", "id");

ALTER TABLE "analytics_retention_state"
    ADD CONSTRAINT "analytics_retention_state_active_run_id_fkey"
    FOREIGN KEY ("active_run_id") REFERENCES "analytics_retention_runs"("id")
    ON DELETE SET NULL ON UPDATE CASCADE;
