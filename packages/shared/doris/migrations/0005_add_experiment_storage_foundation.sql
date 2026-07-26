-- U4 additive experiment and dataset-run storage foundation.
--
-- Release A readers accept this suffix before any Release B writer is enabled.
-- The dataset-run ingestion and experiment capabilities remain closed until the
-- durable generation barriers and fleet compatibility gates have passed.

ALTER TABLE events_current ADD COLUMN experiment_id VARCHAR(64) NULL;
ALTER TABLE events_current ADD COLUMN experiment_name VARCHAR(512) NULL;
ALTER TABLE events_current ADD COLUMN experiment_metadata VARIANT NULL;
ALTER TABLE events_current ADD COLUMN experiment_metadata_json STRING NULL;
ALTER TABLE events_current ADD COLUMN experiment_description STRING NULL;
ALTER TABLE events_current ADD COLUMN experiment_dataset_id VARCHAR(64) NULL;
ALTER TABLE events_current ADD COLUMN experiment_item_id VARCHAR(64) NULL;
ALTER TABLE events_current ADD COLUMN experiment_item_version DATETIME(6) NULL;
ALTER TABLE events_current ADD COLUMN experiment_item_expected_output STRING NULL;
ALTER TABLE events_current ADD COLUMN experiment_item_metadata VARIANT NULL;
ALTER TABLE events_current ADD COLUMN experiment_item_metadata_json STRING NULL;
ALTER TABLE events_current ADD COLUMN experiment_item_root_span_id VARCHAR(128) NULL;

ALTER TABLE scores_current ADD COLUMN dataset_run_id VARCHAR(64) NULL;
ALTER TABLE scores_current ADD COLUMN execution_trace_id VARCHAR(64) NULL;

CREATE TABLE IF NOT EXISTS dataset_run_items_current (
    project_id                  VARCHAR(64)    NOT NULL,
    run_item_date               DATE           NOT NULL COMMENT 'Immutable UTC date of the first canonical run-item create.',
    run_item_id                 VARCHAR(64)    NOT NULL,
    version_token               BIGINT         NOT NULL,

    dataset_run_id              VARCHAR(64)    NOT NULL,
    dataset_item_id             VARCHAR(64)    NOT NULL,
    dataset_id                  VARCHAR(64)    NOT NULL,
    trace_id                    VARCHAR(64)    NOT NULL,
    observation_id              VARCHAR(128)   NULL,
    `error`                     STRING         NULL,
    created_at                  DATETIME(6)    NOT NULL,
    updated_at                  DATETIME(6)    NOT NULL,

    dataset_run_name            VARCHAR(512)   NOT NULL,
    dataset_run_description     STRING         NULL,
    dataset_run_metadata        VARIANT        NULL,
    dataset_run_metadata_json   STRING         NULL,
    dataset_run_created_at      DATETIME(6)    NOT NULL,

    dataset_item_version        DATETIME(6)    NULL,
    dataset_item_input          STRING         NULL,
    dataset_item_expected_output STRING        NULL,
    dataset_item_metadata       VARIANT        NULL,
    dataset_item_metadata_json  STRING         NULL,

    dataset_deletion_generation BIGINT         NOT NULL DEFAULT '0',
    run_deletion_generation     BIGINT         NOT NULL DEFAULT '0'
)
UNIQUE KEY (project_id, run_item_date, run_item_id)
AUTO PARTITION BY RANGE (date_trunc(`run_item_date`, 'day')) ()
DISTRIBUTED BY HASH(run_item_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "version_token"
);

CREATE TABLE IF NOT EXISTS dataset_tombstones (
    project_id            VARCHAR(64)  NOT NULL,
    dataset_id            VARCHAR(64)  NOT NULL,
    deletion_generation   BIGINT       NOT NULL,
    created_at            DATETIME(6)  NOT NULL
)
UNIQUE KEY (project_id, dataset_id)
DISTRIBUTED BY HASH(dataset_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "deletion_generation"
);

CREATE TABLE IF NOT EXISTS dataset_run_tombstones (
    project_id            VARCHAR(64)  NOT NULL,
    dataset_run_id        VARCHAR(64)  NOT NULL,
    dataset_id            VARCHAR(64)  NOT NULL,
    deletion_generation   BIGINT       NOT NULL,
    created_at            DATETIME(6)  NOT NULL
)
UNIQUE KEY (project_id, dataset_run_id)
DISTRIBUTED BY HASH(dataset_run_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "deletion_generation"
);
