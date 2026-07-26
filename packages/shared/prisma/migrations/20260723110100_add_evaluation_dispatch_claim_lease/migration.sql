ALTER TABLE "analytics_evaluation_dispatches"
  ADD COLUMN "processing_lease_owner" TEXT,
  ADD COLUMN "processing_lease_expires_at" TIMESTAMP(3);

CREATE INDEX "analytics_eval_dispatch_processing_lease_idx"
  ON "analytics_evaluation_dispatches"(
    "status",
    "processing_lease_expires_at"
  );
