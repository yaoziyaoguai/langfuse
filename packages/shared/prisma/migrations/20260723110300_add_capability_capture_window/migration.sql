ALTER TABLE "analytics_capability_activations"
  ADD COLUMN "capture_started_at" TIMESTAMP(3),
  ADD COLUMN "capture_expires_at" TIMESTAMP(3),
  ADD COLUMN "capture_row_budget" INTEGER,
  ADD COLUMN "capture_rows" BIGINT NOT NULL DEFAULT 0;

ALTER TABLE "analytics_capability_activations"
  ADD CONSTRAINT "analytics_capability_capture_window_check" CHECK (
    "capture_rows" >= 0
    AND ("capture_row_budget" IS NULL OR "capture_row_budget" > 0)
    AND (
      ("capture_started_at" IS NULL
        AND "capture_expires_at" IS NULL
        AND "capture_row_budget" IS NULL)
      OR
      ("capture_started_at" IS NOT NULL
        AND "capture_expires_at" IS NOT NULL
        AND "capture_expires_at" > "capture_started_at"
        AND "capture_row_budget" IS NOT NULL)
    )
  );
