ALTER TABLE "dataset_runs"
  ADD COLUMN "dataset_run_ingestion_activation_generation" BIGINT,
  ADD COLUMN "dataset_run_ingestion_contract_version" INTEGER;

ALTER TABLE "dataset_runs"
  ADD CONSTRAINT "dataset_runs_ingestion_capability_provenance_check"
  CHECK (
    (
      "dataset_run_ingestion_activation_generation" IS NULL
      AND "dataset_run_ingestion_contract_version" IS NULL
    )
    OR
    (
      "dataset_run_ingestion_activation_generation" IS NOT NULL
      AND "dataset_run_ingestion_activation_generation" > 0
      AND "dataset_run_ingestion_contract_version" IS NOT NULL
      AND "dataset_run_ingestion_contract_version" > 0
    )
  );

ALTER TABLE "batch_exports"
  ADD COLUMN "dataset_run_export_activation_generation" BIGINT,
  ADD COLUMN "dataset_run_export_contract_version" INTEGER;

ALTER TABLE "batch_exports"
  ADD CONSTRAINT "batch_exports_dataset_run_capability_provenance_check"
  CHECK (
    (
      "dataset_run_export_activation_generation" IS NULL
      AND "dataset_run_export_contract_version" IS NULL
    )
    OR
    (
      "dataset_run_export_activation_generation" IS NOT NULL
      AND "dataset_run_export_activation_generation" > 0
      AND "dataset_run_export_contract_version" IS NOT NULL
      AND "dataset_run_export_contract_version" > 0
    )
  );
