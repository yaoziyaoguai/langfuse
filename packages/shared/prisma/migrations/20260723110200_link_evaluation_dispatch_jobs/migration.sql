ALTER TABLE "job_executions"
  ADD COLUMN "analytics_evaluation_dispatch_id" TEXT;

CREATE INDEX "job_executions_analytics_evaluation_dispatch_id_idx"
  ON "job_executions"("analytics_evaluation_dispatch_id");

ALTER TABLE "job_executions"
  ADD CONSTRAINT "job_executions_analytics_evaluation_dispatch_id_fkey"
  FOREIGN KEY ("analytics_evaluation_dispatch_id")
  REFERENCES "analytics_evaluation_dispatches"("id")
  ON DELETE SET NULL ON UPDATE CASCADE;
