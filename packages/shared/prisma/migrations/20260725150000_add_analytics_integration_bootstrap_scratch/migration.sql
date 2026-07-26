ALTER TABLE "analytics_integration_states"
  ADD COLUMN "bootstrap_manifest" JSONB;

ALTER TABLE "analytics_integration_states"
  DROP CONSTRAINT "analytics_integration_states_bootstrap_manifest_check";

ALTER TABLE "analytics_integration_states"
  ADD CONSTRAINT "analytics_integration_states_bootstrap_manifest_check" CHECK (
    (
      "bootstrap_manifest" IS NULL
      AND "bootstrap_manifest_key" IS NULL
      AND "bootstrap_manifest_checksum" IS NULL
      AND "bootstrap_manifest_rows" IS NULL
      AND "bootstrap_sealed_at" IS NULL
    )
    OR
    (
      "bootstrap_manifest" IS NOT NULL
      AND "bootstrap_manifest_key" IS NOT NULL
      AND length("bootstrap_manifest_checksum") = 64
      AND "bootstrap_manifest_rows" >= 0
      AND "bootstrap_sealed_at" IS NOT NULL
    )
  );

ALTER TABLE "analytics_integration_executions"
  ADD COLUMN "scratch_host_id" TEXT,
  ADD COLUMN "scratch_relative_path" TEXT,
  ADD COLUMN "scratch_reserved_bytes" BIGINT,
  ADD COLUMN "scratch_lease_expires_at" TIMESTAMP(3),
  ADD CONSTRAINT "analytics_integration_executions_scratch_check" CHECK (
    (
      "scratch_host_id" IS NULL
      AND "scratch_relative_path" IS NULL
      AND "scratch_reserved_bytes" IS NULL
      AND "scratch_lease_expires_at" IS NULL
    )
    OR
    (
      "scratch_host_id" IS NOT NULL
      AND "scratch_relative_path" IS NOT NULL
      AND "scratch_reserved_bytes" > 0
      AND "scratch_lease_expires_at" IS NOT NULL
    )
  );

CREATE INDEX "analytics_integration_executions_scratch_lease_idx"
  ON "analytics_integration_executions"(
    "scratch_host_id",
    "scratch_lease_expires_at"
  );
