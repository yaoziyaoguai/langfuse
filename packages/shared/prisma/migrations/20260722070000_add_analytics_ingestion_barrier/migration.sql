CREATE SEQUENCE "analytics_ingestion_acceptance_sequence"
AS BIGINT
START WITH 4611686018427387904;

ALTER TABLE "analytics_ingestion_operations"
ADD COLUMN "acceptance_sequence" BIGINT;

ALTER TABLE "analytics_ingestion_operations"
ALTER COLUMN "acceptance_sequence"
SET DEFAULT nextval('analytics_ingestion_acceptance_sequence');

ALTER SEQUENCE "analytics_ingestion_acceptance_sequence"
OWNED BY "analytics_ingestion_operations"."acceptance_sequence";

ALTER TABLE "analytics_deletion_operations"
ADD COLUMN "ingestion_barrier_sequence" BIGINT;
