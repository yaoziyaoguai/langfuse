ALTER TABLE "analytics_ingestion_operations"
  ADD COLUMN "capability" "AnalyticsCapability",
  ADD COLUMN "capability_activation_generation" BIGINT,
  ADD COLUMN "capability_contract_version" INTEGER;

ALTER TABLE "analytics_ingestion_operations"
  ADD CONSTRAINT "analytics_ingestion_operations_capability_provenance_check"
  CHECK (
    (
      "capability" IS NULL
      AND "capability_activation_generation" IS NULL
      AND "capability_contract_version" IS NULL
    )
    OR
    (
      "capability" IS NOT NULL
      AND "capability_activation_generation" IS NOT NULL
      AND "capability_activation_generation" > 0
      AND "capability_contract_version" IS NOT NULL
      AND "capability_contract_version" > 0
    )
  );

CREATE INDEX "analytics_ingestion_operations_capability_gen_status_idx"
  ON "analytics_ingestion_operations"(
    "capability",
    "capability_activation_generation",
    "status"
  );
