ALTER TABLE "analytics_integration_pending_deliveries"
  DROP CONSTRAINT "analytics_integration_deliveries_state_fkey";

ALTER TABLE "analytics_integration_pending_deliveries"
  ADD CONSTRAINT "analytics_integration_deliveries_state_fkey"
  FOREIGN KEY ("integration_state_id")
  REFERENCES "analytics_integration_states"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "analytics_integration_executions"
  DROP CONSTRAINT "analytics_integration_executions_state_fkey";

ALTER TABLE "analytics_integration_executions"
  ADD CONSTRAINT "analytics_integration_executions_state_fkey"
  FOREIGN KEY ("integration_state_id")
  REFERENCES "analytics_integration_states"("id")
  ON DELETE CASCADE ON UPDATE CASCADE;
