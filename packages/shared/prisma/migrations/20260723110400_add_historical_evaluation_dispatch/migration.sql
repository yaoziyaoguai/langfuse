ALTER TABLE "analytics_evaluation_dispatches"
  ADD COLUMN "request_id" TEXT,
  ADD COLUMN "job_configuration_id" TEXT;

UPDATE "analytics_evaluation_dispatches"
SET "request_id" = "operation_id"
WHERE "request_id" IS NULL;

ALTER TABLE "analytics_evaluation_dispatches"
  ALTER COLUMN "request_id" SET NOT NULL;

DROP INDEX "analytics_evaluation_dispatches_operation_target_key";
CREATE UNIQUE INDEX "analytics_evaluation_dispatches_request_target_key"
  ON "analytics_evaluation_dispatches"(
    "request_id",
    "target_type",
    "target_id"
  );

ALTER TABLE "analytics_evaluation_dispatches"
  DROP CONSTRAINT "analytics_evaluation_dispatches_target_shape_check";

ALTER TABLE "analytics_evaluation_dispatches"
  ADD CONSTRAINT "analytics_evaluation_dispatches_target_shape_check" CHECK (
    ("target_type" = 'TRACE_UPSERT'
      AND "target_id" = "trace_id"
      AND "observation_id" IS NULL
      AND "dataset_item_id" IS NULL
      AND "job_configuration_id" IS NULL)
    OR
    ("target_type" = 'OBSERVATION_UPSERT'
      AND "observation_id" IS NOT NULL
      AND "target_id" = "observation_id"
      AND "dataset_item_id" IS NULL
      AND "job_configuration_id" IS NULL)
    OR
    ("target_type" = 'DATASET_RUN_ITEM_UPSERT'
      AND "dataset_item_id" IS NOT NULL
      AND "job_configuration_id" IS NULL)
    OR
    ("target_type" = 'HISTORICAL'
      AND "job_configuration_id" IS NOT NULL)
  );

ALTER TABLE "analytics_evaluation_dispatches"
  ADD CONSTRAINT "analytics_evaluation_dispatches_request_id_check" CHECK (
    length("request_id") > 0
  );
