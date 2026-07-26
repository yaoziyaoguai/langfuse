INSERT INTO "analytics_capability_activations" (
    "capability",
    "backend",
    "deployment_generation",
    "generation",
    "contract_version",
    "minimum_runtime_contract",
    "status",
    "capture_enabled",
    "capture_required",
    "rescan_required"
)
VALUES
    ('coreBatchExports', 'DORIS', 0, 1, 1, 1, 'DISABLED', false, false, false),
    ('evaluations', 'DORIS', 0, 1, 1, 1, 'DISABLED', false, false, false),
    ('experiments', 'DORIS', 0, 1, 1, 1, 'DISABLED', false, false, false),
    ('datasetRunExports', 'DORIS', 0, 1, 1, 1, 'DISABLED', false, false, false),
    ('datasetRunIngestion', 'DORIS', 0, 1, 1, 1, 'DISABLED', false, false, false),
    ('analyticsIntegrations', 'DORIS', 0, 1, 1, 1, 'DISABLED', false, false, false);
