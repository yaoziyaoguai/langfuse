CREATE TYPE "AnalyticsBackendType" AS ENUM ('CLICKHOUSE', 'DORIS');
CREATE TYPE "AnalyticsRuntimeComponent" AS ENUM ('WEB', 'WORKER', 'CHECKPOINT');
CREATE TYPE "AnalyticsRuntimeLeaseState" AS ENUM ('STARTING', 'ACTIVE', 'QUIESCING', 'QUIESCED');
CREATE TYPE "AnalyticsCapability" AS ENUM (
    'coreBatchExports',
    'evaluations',
    'experiments',
    'datasetRunExports',
    'datasetRunIngestion',
    'analyticsIntegrations'
);
CREATE TYPE "AnalyticsCapabilityActivationStatus" AS ENUM ('DISABLED', 'DARK', 'ACTIVE', 'DRAINING');
CREATE TYPE "AnalyticsRuntimeCapabilityRole" AS ENUM ('CAPTURE', 'PRODUCER', 'CONSUMER', 'RECOVERY');
CREATE TYPE "AnalyticsDeploymentTransitionKind" AS ENUM ('INITIALIZE', 'ADOPT_EXISTING', 'SWITCH');

CREATE TABLE "analytics_backend_deployment_state" (
    "id" TEXT NOT NULL DEFAULT 'global',
    "backend" "AnalyticsBackendType" NOT NULL,
    "generation" BIGINT NOT NULL,
    "workload_epoch_fingerprint" TEXT NOT NULL,
    "foundation_contract_version" INTEGER NOT NULL,
    "attestation_digest" TEXT,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_backend_deployment_state_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "analytics_backend_deployment_state_singleton_check" CHECK ("id" = 'global'),
    CONSTRAINT "analytics_backend_deployment_state_generation_check" CHECK ("generation" > 0),
    CONSTRAINT "analytics_backend_deployment_state_epoch_check" CHECK (length("workload_epoch_fingerprint") = 64),
    CONSTRAINT "analytics_backend_deployment_state_contract_check" CHECK ("foundation_contract_version" > 0),
    CONSTRAINT "analytics_backend_deployment_state_attestation_check" CHECK ("attestation_digest" IS NULL OR length("attestation_digest") = 64)
);

CREATE TABLE "analytics_runtime_leases" (
    "id" TEXT NOT NULL,
    "component" "AnalyticsRuntimeComponent" NOT NULL,
    "instance_id" TEXT NOT NULL,
    "backend" "AnalyticsBackendType" NOT NULL,
    "deployment_generation" BIGINT NOT NULL,
    "workload_epoch_fingerprint" TEXT NOT NULL,
    "build_id" TEXT NOT NULL,
    "foundation_contract_version" INTEGER NOT NULL,
    "accepted_schema_version_min" INTEGER NOT NULL,
    "accepted_schema_version_max" INTEGER NOT NULL,
    "accepted_canonical_version_min" INTEGER NOT NULL,
    "accepted_canonical_version_max" INTEGER NOT NULL,
    "state" "AnalyticsRuntimeLeaseState" NOT NULL DEFAULT 'STARTING',
    "heartbeat_at" TIMESTAMP(3) NOT NULL,
    "lease_expires_at" TIMESTAMP(3) NOT NULL,
    "quiesced_at" TIMESTAMP(3),
    "superseded_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_runtime_leases_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "analytics_runtime_leases_generation_check" CHECK ("deployment_generation" >= 0),
    CONSTRAINT "analytics_runtime_leases_epoch_check" CHECK (length("workload_epoch_fingerprint") = 64),
    CONSTRAINT "analytics_runtime_leases_contract_check" CHECK ("foundation_contract_version" > 0),
    CONSTRAINT "analytics_runtime_leases_schema_range_check" CHECK ("accepted_schema_version_min" <= "accepted_schema_version_max"),
    CONSTRAINT "analytics_runtime_leases_canonical_range_check" CHECK ("accepted_canonical_version_min" <= "accepted_canonical_version_max"),
    CONSTRAINT "analytics_runtime_leases_expiry_check" CHECK ("lease_expires_at" > "heartbeat_at")
);

CREATE TABLE "analytics_runtime_capability_contracts" (
    "runtime_lease_id" TEXT NOT NULL,
    "capability" "AnalyticsCapability" NOT NULL,
    "supported_contract_version" INTEGER NOT NULL,
    "installed_roles" "AnalyticsRuntimeCapabilityRole"[] NOT NULL,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_runtime_capability_contracts_pkey" PRIMARY KEY ("runtime_lease_id", "capability"),
    CONSTRAINT "analytics_runtime_capability_contracts_version_check" CHECK ("supported_contract_version" > 0),
    CONSTRAINT "analytics_runtime_capability_contracts_roles_check" CHECK (cardinality("installed_roles") > 0)
);

CREATE TABLE "analytics_capability_activations" (
    "capability" "AnalyticsCapability" NOT NULL,
    "backend" "AnalyticsBackendType" NOT NULL,
    "deployment_generation" BIGINT NOT NULL,
    "generation" BIGINT NOT NULL,
    "contract_version" INTEGER NOT NULL,
    "minimum_runtime_contract" INTEGER NOT NULL,
    "status" "AnalyticsCapabilityActivationStatus" NOT NULL DEFAULT 'DISABLED',
    "capture_enabled" BOOLEAN NOT NULL DEFAULT false,
    "capture_required" BOOLEAN NOT NULL DEFAULT false,
    "rescan_required" BOOLEAN NOT NULL DEFAULT false,
    "cutoff_state" JSONB,
    "cutoff_activation_generation" BIGINT,
    "cutoff_digest" TEXT,
    "bootstrap_evidence_digest" TEXT,
    "bootstrap_completed_generation" BIGINT,
    "bootstrap_completed_at" TIMESTAMP(3),
    "activated_at" TIMESTAMP(3),
    "draining_at" TIMESTAMP(3),
    "disabled_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_capability_activations_pkey" PRIMARY KEY ("capability"),
    CONSTRAINT "analytics_capability_activations_backend_check" CHECK ("backend" = 'DORIS'),
    CONSTRAINT "analytics_capability_activations_deployment_generation_check" CHECK ("deployment_generation" >= 0),
    CONSTRAINT "analytics_capability_activations_generation_check" CHECK ("generation" > 0),
    CONSTRAINT "analytics_capability_activations_contract_check" CHECK ("contract_version" > 0 AND "minimum_runtime_contract" > 0),
    CONSTRAINT "analytics_capability_activations_capture_rescan_check" CHECK (NOT "capture_required" OR "rescan_required"),
    CONSTRAINT "analytics_capability_activations_cutoff_state_check" CHECK (
        (NOT "rescan_required" AND "cutoff_state" IS NULL AND "cutoff_activation_generation" IS NULL AND "cutoff_digest" IS NULL)
        OR
        ("rescan_required" AND "cutoff_state" IS NOT NULL AND "cutoff_activation_generation" IS NOT NULL AND "cutoff_digest" IS NOT NULL)
    ),
    CONSTRAINT "analytics_capability_activations_bootstrap_completion_check" CHECK (
        ("bootstrap_completed_generation" IS NULL AND "bootstrap_evidence_digest" IS NULL AND "bootstrap_completed_at" IS NULL)
        OR
        ("bootstrap_completed_generation" IS NOT NULL AND "bootstrap_evidence_digest" IS NOT NULL AND "bootstrap_completed_at" IS NOT NULL)
    ),
    CONSTRAINT "analytics_capability_activations_cutoff_digest_check" CHECK ("cutoff_digest" IS NULL OR length("cutoff_digest") = 64),
    CONSTRAINT "analytics_capability_activations_bootstrap_evidence_check" CHECK ("bootstrap_evidence_digest" IS NULL OR length("bootstrap_evidence_digest") = 64),
    CONSTRAINT "analytics_capability_activations_fence_generation_check" CHECK (
        ("cutoff_activation_generation" IS NULL OR "cutoff_activation_generation" > 0)
        AND ("bootstrap_completed_generation" IS NULL OR "bootstrap_completed_generation" > 0)
    )
);

CREATE TABLE "analytics_backend_claim_leases" (
    "id" TEXT NOT NULL,
    "runtime_lease_id" TEXT NOT NULL,
    "backend" "AnalyticsBackendType" NOT NULL,
    "deployment_generation" BIGINT NOT NULL,
    "workload_epoch_fingerprint" TEXT NOT NULL,
    "runtime_contract_version" INTEGER NOT NULL,
    "capability" "AnalyticsCapability",
    "capability_activation_generation" BIGINT,
    "capability_contract_version" INTEGER,
    "claim_kind" TEXT NOT NULL,
    "resource_identity" TEXT NOT NULL,
    "lease_expires_at" TIMESTAMP(3) NOT NULL,
    "released_at" TIMESTAMP(3),
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updated_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_backend_claim_leases_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "analytics_backend_claim_leases_generation_check" CHECK ("deployment_generation" > 0),
    CONSTRAINT "analytics_backend_claim_leases_epoch_check" CHECK (length("workload_epoch_fingerprint") = 64),
    CONSTRAINT "analytics_backend_claim_leases_contract_check" CHECK ("runtime_contract_version" > 0),
    CONSTRAINT "analytics_backend_claim_leases_capability_stamp_check" CHECK (
        ("capability" IS NULL AND "capability_activation_generation" IS NULL AND "capability_contract_version" IS NULL)
        OR
        ("backend" = 'CLICKHOUSE' AND "capability" IS NOT NULL AND "capability_activation_generation" IS NULL AND "capability_contract_version" IS NULL)
        OR
        ("backend" = 'DORIS' AND "capability" IS NOT NULL AND "capability_activation_generation" IS NOT NULL AND "capability_contract_version" IS NOT NULL)
    )
);

CREATE TABLE "analytics_backend_deployment_transitions" (
    "id" TEXT NOT NULL,
    "kind" "AnalyticsDeploymentTransitionKind" NOT NULL,
    "from_backend" "AnalyticsBackendType",
    "to_backend" "AnalyticsBackendType" NOT NULL,
    "from_generation" BIGINT,
    "to_generation" BIGINT NOT NULL,
    "target_workload_epoch_fingerprint" TEXT NOT NULL,
    "expected_inventory_digest" TEXT NOT NULL,
    "observed_inventory_digest" TEXT NOT NULL,
    "drain_evidence_digest" TEXT NOT NULL,
    "deny_probe_attestation_digest" TEXT NOT NULL,
    "source_empty_evidence_digest" TEXT,
    "target_empty_evidence_digest" TEXT,
    "completed_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "created_at" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT "analytics_backend_deployment_transitions_pkey" PRIMARY KEY ("id"),
    CONSTRAINT "analytics_backend_deployment_transitions_generation_check" CHECK ("to_generation" > 0 AND ("from_generation" IS NULL OR "from_generation" >= 0)),
    CONSTRAINT "analytics_backend_deployment_transitions_digest_check" CHECK (
        length("target_workload_epoch_fingerprint") = 64
        AND length("expected_inventory_digest") = 64
        AND length("observed_inventory_digest") = 64
        AND length("drain_evidence_digest") = 64
        AND length("deny_probe_attestation_digest") = 64
    ),
    CONSTRAINT "analytics_backend_deployment_transitions_empty_evidence_check" CHECK (
        (
            "kind" = 'SWITCH'
            AND "source_empty_evidence_digest" IS NOT NULL
            AND length("source_empty_evidence_digest") = 64
            AND "target_empty_evidence_digest" IS NOT NULL
            AND length("target_empty_evidence_digest") = 64
        )
        OR
        (
            "kind" <> 'SWITCH'
            AND "source_empty_evidence_digest" IS NULL
            AND "target_empty_evidence_digest" IS NULL
        )
    )
);

ALTER TABLE "analytics_ingestion_operations"
    ADD COLUMN "analytics_backend" "AnalyticsBackendType",
    ADD COLUMN "deployment_generation" BIGINT,
    ADD COLUMN "workload_epoch_fingerprint" TEXT,
    ADD COLUMN "runtime_contract_version" INTEGER,
    ADD COLUMN "producer_runtime_lease_id" TEXT;

ALTER TABLE "analytics_deletion_operations"
    ADD COLUMN "analytics_backend" "AnalyticsBackendType",
    ADD COLUMN "deployment_generation" BIGINT,
    ADD COLUMN "workload_epoch_fingerprint" TEXT,
    ADD COLUMN "runtime_contract_version" INTEGER,
    ADD COLUMN "producer_runtime_lease_id" TEXT;

ALTER TABLE "analytics_checkpoint_generations"
    ADD COLUMN "analytics_backend" "AnalyticsBackendType",
    ADD COLUMN "deployment_generation" BIGINT,
    ADD COLUMN "workload_epoch_fingerprint" TEXT,
    ADD COLUMN "runtime_contract_version" INTEGER,
    ADD COLUMN "producer_runtime_lease_id" TEXT;

ALTER TABLE "analytics_retention_runs"
    ADD COLUMN "analytics_backend" "AnalyticsBackendType",
    ADD COLUMN "deployment_generation" BIGINT,
    ADD COLUMN "workload_epoch_fingerprint" TEXT,
    ADD COLUMN "runtime_contract_version" INTEGER,
    ADD COLUMN "producer_runtime_lease_id" TEXT;

ALTER TABLE "analytics_ingestion_operations"
    ADD CONSTRAINT "analytics_ingestion_operations_provenance_check" CHECK (
        ("analytics_backend" IS NULL AND "deployment_generation" IS NULL AND "workload_epoch_fingerprint" IS NULL AND "runtime_contract_version" IS NULL AND "producer_runtime_lease_id" IS NULL)
        OR
        ("analytics_backend" IS NOT NULL AND "deployment_generation" IS NOT NULL AND "deployment_generation" > 0 AND "workload_epoch_fingerprint" IS NOT NULL AND length("workload_epoch_fingerprint") = 64 AND "runtime_contract_version" IS NOT NULL AND "runtime_contract_version" > 0 AND "producer_runtime_lease_id" IS NOT NULL AND length("producer_runtime_lease_id") > 0)
    ) NOT VALID;

ALTER TABLE "analytics_deletion_operations"
    ADD CONSTRAINT "analytics_deletion_operations_provenance_check" CHECK (
        ("analytics_backend" IS NULL AND "deployment_generation" IS NULL AND "workload_epoch_fingerprint" IS NULL AND "runtime_contract_version" IS NULL AND "producer_runtime_lease_id" IS NULL)
        OR
        ("analytics_backend" IS NOT NULL AND "deployment_generation" IS NOT NULL AND "deployment_generation" > 0 AND "workload_epoch_fingerprint" IS NOT NULL AND length("workload_epoch_fingerprint") = 64 AND "runtime_contract_version" IS NOT NULL AND "runtime_contract_version" > 0 AND "producer_runtime_lease_id" IS NOT NULL AND length("producer_runtime_lease_id") > 0)
    ) NOT VALID;

ALTER TABLE "analytics_checkpoint_generations"
    ADD CONSTRAINT "analytics_checkpoint_generations_provenance_check" CHECK (
        ("analytics_backend" IS NULL AND "deployment_generation" IS NULL AND "workload_epoch_fingerprint" IS NULL AND "runtime_contract_version" IS NULL AND "producer_runtime_lease_id" IS NULL)
        OR
        ("analytics_backend" IS NOT NULL AND "deployment_generation" IS NOT NULL AND "deployment_generation" > 0 AND "workload_epoch_fingerprint" IS NOT NULL AND length("workload_epoch_fingerprint") = 64 AND "runtime_contract_version" IS NOT NULL AND "runtime_contract_version" > 0 AND "producer_runtime_lease_id" IS NOT NULL AND length("producer_runtime_lease_id") > 0)
    ) NOT VALID;

ALTER TABLE "analytics_retention_runs"
    ADD CONSTRAINT "analytics_retention_runs_provenance_check" CHECK (
        ("analytics_backend" IS NULL AND "deployment_generation" IS NULL AND "workload_epoch_fingerprint" IS NULL AND "runtime_contract_version" IS NULL AND "producer_runtime_lease_id" IS NULL)
        OR
        ("analytics_backend" IS NOT NULL AND "deployment_generation" IS NOT NULL AND "deployment_generation" > 0 AND "workload_epoch_fingerprint" IS NOT NULL AND length("workload_epoch_fingerprint") = 64 AND "runtime_contract_version" IS NOT NULL AND "runtime_contract_version" > 0 AND "producer_runtime_lease_id" IS NOT NULL AND length("producer_runtime_lease_id") > 0)
    ) NOT VALID;

CREATE INDEX "analytics_runtime_leases_instance_id_idx" ON "analytics_runtime_leases"("instance_id");
CREATE UNIQUE INDEX "analytics_runtime_leases_current_instance_key" ON "analytics_runtime_leases"("instance_id") WHERE "superseded_at" IS NULL;
CREATE INDEX "analytics_runtime_leases_backend_generation_state_expiry_idx" ON "analytics_runtime_leases"("backend", "deployment_generation", "state", "lease_expires_at");
CREATE INDEX "analytics_runtime_leases_epoch_expiry_idx" ON "analytics_runtime_leases"("workload_epoch_fingerprint", "lease_expires_at");
CREATE INDEX "analytics_runtime_capability_contracts_capability_version_idx" ON "analytics_runtime_capability_contracts"("capability", "supported_contract_version");
CREATE INDEX "analytics_capability_activations_backend_generation_status_idx" ON "analytics_capability_activations"("backend", "deployment_generation", "status");
CREATE INDEX "analytics_backend_claim_leases_backend_gen_release_expiry_idx" ON "analytics_backend_claim_leases"("backend", "deployment_generation", "released_at", "lease_expires_at");
CREATE INDEX "analytics_backend_claim_leases_capability_gen_release_idx" ON "analytics_backend_claim_leases"("capability", "capability_activation_generation", "released_at");
CREATE INDEX "analytics_backend_claim_leases_runtime_release_idx" ON "analytics_backend_claim_leases"("runtime_lease_id", "released_at");
CREATE UNIQUE INDEX "analytics_backend_claim_leases_active_resource_key" ON "analytics_backend_claim_leases"("backend", "deployment_generation", "claim_kind", "resource_identity") WHERE "released_at" IS NULL;
CREATE UNIQUE INDEX "analytics_backend_deployment_transitions_backend_generation_key" ON "analytics_backend_deployment_transitions"("to_backend", "to_generation");
CREATE INDEX "analytics_backend_deployment_transitions_kind_completed_idx" ON "analytics_backend_deployment_transitions"("kind", "completed_at");
ALTER TABLE "analytics_runtime_capability_contracts"
    ADD CONSTRAINT "analytics_runtime_capability_contracts_runtime_lease_id_fkey"
    FOREIGN KEY ("runtime_lease_id") REFERENCES "analytics_runtime_leases"("id")
    ON DELETE CASCADE ON UPDATE CASCADE;

ALTER TABLE "analytics_backend_claim_leases"
    ADD CONSTRAINT "analytics_backend_claim_leases_runtime_lease_id_fkey"
    FOREIGN KEY ("runtime_lease_id") REFERENCES "analytics_runtime_leases"("id")
    ON DELETE RESTRICT ON UPDATE CASCADE;

ALTER TABLE "analytics_ingestion_operations"
    ADD CONSTRAINT "analytics_ingestion_operations_producer_runtime_lease_id_fkey"
    FOREIGN KEY ("producer_runtime_lease_id") REFERENCES "analytics_runtime_leases"("id")
    ON DELETE RESTRICT ON UPDATE CASCADE NOT VALID;

ALTER TABLE "analytics_deletion_operations"
    ADD CONSTRAINT "analytics_deletion_operations_producer_runtime_lease_id_fkey"
    FOREIGN KEY ("producer_runtime_lease_id") REFERENCES "analytics_runtime_leases"("id")
    ON DELETE RESTRICT ON UPDATE CASCADE NOT VALID;

ALTER TABLE "analytics_checkpoint_generations"
    ADD CONSTRAINT "analytics_checkpoint_generations_producer_runtime_lease_id_fkey"
    FOREIGN KEY ("producer_runtime_lease_id") REFERENCES "analytics_runtime_leases"("id")
    ON DELETE RESTRICT ON UPDATE CASCADE NOT VALID;

ALTER TABLE "analytics_retention_runs"
    ADD CONSTRAINT "analytics_retention_runs_producer_runtime_lease_id_fkey"
    FOREIGN KEY ("producer_runtime_lease_id") REFERENCES "analytics_runtime_leases"("id")
    ON DELETE RESTRICT ON UPDATE CASCADE NOT VALID;
