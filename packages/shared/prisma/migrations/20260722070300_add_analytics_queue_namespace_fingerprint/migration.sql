ALTER TABLE "analytics_backend_deployment_state"
ADD COLUMN "queue_namespace_fingerprint" TEXT;

ALTER TABLE "analytics_runtime_leases"
ADD COLUMN "queue_namespace_fingerprint" TEXT;

ALTER TABLE "analytics_backend_deployment_state"
ADD CONSTRAINT "analytics_backend_deployment_state_queue_namespace_check"
CHECK (
  "queue_namespace_fingerprint" IS NULL
  OR length("queue_namespace_fingerprint") = 64
) NOT VALID;

ALTER TABLE "analytics_runtime_leases"
ADD CONSTRAINT "analytics_runtime_leases_queue_namespace_check"
CHECK (
  "queue_namespace_fingerprint" IS NULL
  OR length("queue_namespace_fingerprint") = 64
) NOT VALID;

ALTER TABLE "analytics_backend_deployment_state"
VALIDATE CONSTRAINT "analytics_backend_deployment_state_queue_namespace_check";

ALTER TABLE "analytics_runtime_leases"
VALIDATE CONSTRAINT "analytics_runtime_leases_queue_namespace_check";
