CREATE TABLE "analytics_ingestion_outbox_v2" (
    "id" TEXT NOT NULL,
    "operation_id" TEXT NOT NULL,
    "status" "AnalyticsOutboxStatus" NOT NULL DEFAULT 'PENDING',
    "generation" INTEGER NOT NULL DEFAULT 1,
    "attempts" INTEGER NOT NULL DEFAULT 0,
    "next_attempt_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "locked_by" TEXT,
    "locked_until" TIMESTAMP(3),
    "published_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_ingestion_outbox_v2_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "analytics_ingestion_outbox_v2_operation_id_key"
ON "analytics_ingestion_outbox_v2"("operation_id");

CREATE INDEX "analytics_ingestion_outbox_v2_status_next_attempt_at_locked_until_idx"
ON "analytics_ingestion_outbox_v2"("status", "next_attempt_at", "locked_until");

ALTER TABLE "analytics_ingestion_outbox_v2"
ADD CONSTRAINT "analytics_ingestion_outbox_v2_operation_id_fkey"
FOREIGN KEY ("operation_id") REFERENCES "analytics_ingestion_operations"("id")
ON DELETE CASCADE ON UPDATE CASCADE;
