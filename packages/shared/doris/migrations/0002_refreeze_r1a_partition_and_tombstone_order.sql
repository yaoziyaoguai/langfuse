-- U2 forward-only refreeze before any application traffic is routed to Doris.
--
-- 0001 is immutable and may already be recorded by a PoC/local database. Its
-- dynamic partition settings accidentally encoded 365-day global retention
-- and could not create arbitrary historical source-time partitions on demand.
-- These replacement tables use AUTO RANGE PARTITION without a retention
-- property. Tombstones also map deletion_generation as their sequence column.
--
-- REPLACE WITH TABLE is atomic. U2 readiness requires this migration ledger
-- entry before web/worker can become ready, so no application write can race
-- the replacement. swap=false discards the empty/pre-traffic 0001 table.

CREATE TABLE IF NOT EXISTS events_current_u2 (
    project_id            VARCHAR(64)    NOT NULL,
    partition_date        DATE           NOT NULL,
    trace_id              VARCHAR(64)    NOT NULL,
    span_id               VARCHAR(128)   NOT NULL,
    version_token         BIGINT         NOT NULL,
    parent_span_id        VARCHAR(128)   NULL,
    `type`                VARCHAR(32)    NOT NULL,
    `name`                VARCHAR(512)   NULL,
    environment           VARCHAR(64)    NOT NULL DEFAULT 'default',
    user_id               VARCHAR(128)   NULL,
    session_id            VARCHAR(128)   NULL,
    `level`               VARCHAR(32)    NULL,
    is_app_root           BOOLEAN        NOT NULL DEFAULT 'false',
    bookmarked            BOOLEAN        NOT NULL DEFAULT 'false',
    `public`              BOOLEAN        NOT NULL DEFAULT 'false',
    `release`             VARCHAR(128)   NULL,
    `version`             VARCHAR(128)   NULL,
    trace_name            VARCHAR(512)   NULL,
    start_time            DATETIME(6)    NOT NULL,
    end_time              DATETIME(6)    NULL,
    completion_start_time DATETIME(6)    NULL,
    created_at            DATETIME(6)    NOT NULL,
    updated_at            DATETIME(6)    NOT NULL,
    provided_model_name   VARCHAR(128)   NULL,
    internal_model_id     VARCHAR(128)   NULL,
    prompt_id             VARCHAR(128)   NULL,
    prompt_name           VARCHAR(256)   NULL,
    prompt_version        INT            NULL,
    total_input_tokens    BIGINT         NULL,
    total_output_tokens   BIGINT         NULL,
    total_cost            DECIMAL(18, 12) NULL,
    tags                  ARRAY<VARCHAR(256)> NULL,
    metadata              VARIANT        NULL,
    usage_details         VARIANT        NULL,
    cost_details          VARIANT        NULL,
    provided_usage_details VARIANT       NULL,
    provided_cost_details VARIANT        NULL,
    model_parameters      VARIANT        NULL,
    tool_definitions      VARIANT        NULL,
    tool_calls            ARRAY<VARCHAR> NULL,
    tool_call_names       ARRAY<VARCHAR> NULL,
    input                 STRING         NULL,
    output                STRING         NULL,
    input_preview         VARCHAR(200)   NULL,
    output_preview        VARCHAR(200)   NULL,
    `source`              VARCHAR(64)    NOT NULL,
    ingestion_sdk_name    VARCHAR(64)    NOT NULL DEFAULT 'unknown',
    ingestion_sdk_version VARCHAR(64)    NOT NULL DEFAULT 'unknown',
    service_name          VARCHAR(128)   NULL,
    telemetry_sdk_language VARCHAR(32)   NULL,
    blob_storage_file_path STRING        NULL,
    event_bytes           BIGINT         NULL,
    INDEX idx_inv_input  (input)  USING INVERTED  PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
    INDEX idx_inv_output (output) USING INVERTED  PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
    INDEX idx_inv_name   (`name`) USING INVERTED,
    INDEX idx_ng_input   (input)  USING NGRAM_BF  PROPERTIES("gram_size" = "3", "bf_size" = "64000"),
    INDEX idx_ng_output  (output) USING NGRAM_BF  PROPERTIES("gram_size" = "3", "bf_size" = "64000")
)
UNIQUE KEY (project_id, partition_date, trace_id, span_id)
AUTO PARTITION BY RANGE (date_trunc(`partition_date`, 'day')) ()
DISTRIBUTED BY HASH(trace_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "version_token"
);

CREATE TABLE IF NOT EXISTS scores_current_u2 (
    project_id            VARCHAR(64)    NOT NULL,
    score_date            DATE           NOT NULL,
    score_id              VARCHAR(64)    NOT NULL,
    version_token         BIGINT         NOT NULL,
    trace_id              VARCHAR(64)    NULL,
    observation_id        VARCHAR(128)   NULL,
    session_id            VARCHAR(128)   NULL,
    `name`                VARCHAR(256)   NOT NULL,
    `source`              VARCHAR(32)    NOT NULL,
    data_type             VARCHAR(16)    NOT NULL,
    `value`               DOUBLE         NULL,
    string_value          VARCHAR(512)   NULL,
    long_string_value     STRING         NULL,
    boolean_value         BOOLEAN        NULL,
    `comment`             STRING         NULL,
    author_user_id        VARCHAR(64)    NULL,
    config_id             VARCHAR(64)    NULL,
    queue_id              VARCHAR(64)    NULL,
    environment           VARCHAR(64)    NOT NULL DEFAULT 'default',
    metadata              VARIANT        NULL,
    `timestamp`           DATETIME(6)    NOT NULL,
    created_at            DATETIME(6)    NOT NULL,
    updated_at            DATETIME(6)    NOT NULL,
    INDEX idx_inv_comment (`comment`) USING INVERTED PROPERTIES("parser" = "unicode")
)
UNIQUE KEY (project_id, score_date, score_id)
AUTO PARTITION BY RANGE (date_trunc(`score_date`, 'day')) ()
DISTRIBUTED BY HASH(score_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "version_token"
);

CREATE TABLE IF NOT EXISTS blob_storage_file_log_u2 (
    project_id            VARCHAR(64)    NOT NULL,
    file_date             DATE           NOT NULL,
    entity_type           VARCHAR(32)    NOT NULL,
    entity_id             VARCHAR(128)   NOT NULL,
    file_id               VARCHAR(128)   NOT NULL,
    version_token         BIGINT         NOT NULL,
    event_id              VARCHAR(128)   NULL,
    bucket_name           VARCHAR(256)   NULL,
    bucket_path           STRING         NULL,
    created_at            DATETIME(6)    NOT NULL,
    updated_at            DATETIME(6)    NOT NULL
)
UNIQUE KEY (project_id, file_date, entity_type, entity_id, file_id)
AUTO PARTITION BY RANGE (date_trunc(`file_date`, 'day')) ()
DISTRIBUTED BY HASH(entity_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "version_token"
);

CREATE TABLE IF NOT EXISTS trace_tombstones_u2 (
    project_id            VARCHAR(64)    NOT NULL,
    trace_id              VARCHAR(64)    NOT NULL,
    deletion_generation   BIGINT         NOT NULL,
    created_at            DATETIME(6)    NOT NULL
)
UNIQUE KEY (project_id, trace_id)
DISTRIBUTED BY HASH(trace_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "deletion_generation"
);

CREATE TABLE IF NOT EXISTS project_tombstones_u2 (
    project_id            VARCHAR(64)    NOT NULL,
    deletion_generation   BIGINT         NOT NULL,
    created_at            DATETIME(6)    NOT NULL
)
UNIQUE KEY (project_id)
DISTRIBUTED BY HASH(project_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "deletion_generation"
);

ALTER TABLE events_current REPLACE WITH TABLE events_current_u2 PROPERTIES("swap" = "false");
ALTER TABLE scores_current REPLACE WITH TABLE scores_current_u2 PROPERTIES("swap" = "false");
ALTER TABLE blob_storage_file_log REPLACE WITH TABLE blob_storage_file_log_u2 PROPERTIES("swap" = "false");
ALTER TABLE trace_tombstones REPLACE WITH TABLE trace_tombstones_u2 PROPERTIES("swap" = "false");
ALTER TABLE project_tombstones REPLACE WITH TABLE project_tombstones_u2 PROPERTIES("swap" = "false");
