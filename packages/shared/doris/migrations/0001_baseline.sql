-- Langfuse Community Doris Analytics Storage — U1 candidate physical design.
--
-- This is the SINGLE U1-frozen R1A physical design. It is a test-only PoC asset
-- promoted (or amended) by U2 into packages/shared/doris/migrations/*.sql. It
-- must not fork into an unresolved menu: U1 chooses one key/bucket/index/
-- projection/search/batching decision and records why in
-- docs/operations/doris-poc.md.
--
-- Mapping from the current ClickHouse v4 design (characterization evidence only,
-- NOT a mechanical translation — see packages/shared/clickhouse/scripts/dev-tables.sh):
--   * ReplacingMergeTree(event_ts, is_deleted)  ->  Doris Unique Key MoW +
--                                                    sequence col (version_token) +
--                                                    built-in DELETE_SIGN.
--   * PARTITION BY toYYYYMM(start_time)         ->  DAILY RANGE(partition_date)
--                                                    (partition_date is the immutable
--                                                    UTC date of canonical start_time,
--                                                    fixed on first publish).
--   * ORDER BY (.., xxHash32(trace_id), span_id)->  UNIQUE KEY
--                                                       (project_id, partition_date,
--                                                        trace_id, span_id)
--                                                    + DISTRIBUTED BY HASH(trace_id).
--   * SAMPLE BY xxHash32(trace_id)              ->  NOT ported (Doris has no SAMPLE BY;
--                                                    sampling is not an R1A contract).
--   * events_full + events_core + MV            ->  ONE table events_current. There is
--                                                    no full/core dual write and no
--                                                    synthetic root (real-root fallback
--                                                    is a read-time decision in U5).
--   * text(tokenizer=...) / ngrambf_v1          ->  INVERTED index (parser=unicode,
--                                                    support_phrase=true) for full I/O.
--   * MATERIALIZED calculated_*_cost / ALIAS    ->  typed top-level billing columns
--                                                    (total_input_tokens /
--                                                    total_output_tokens / total_cost)
--                                                    populated by the writer; the
--                                                    VARIANT cost_details keeps the
--                                                    long-tail breakdown.
--
-- Stable identity/order columns are typed and explicit; long-tail metadata and
-- rapidly evolving model/tool parameters use VARIANT, per the plan Storage Model
-- and .agents/ARCHITECTURE_PRINCIPLES.md (wide events, columnar access). Doris
-- reserved words used as column names (type/name/level/public/release/version/
-- source/value/comment/timestamp) are backtick-quoted.

-- =============================================================================
-- events_current: the only application-written event fact table (R1A).
-- =============================================================================
CREATE TABLE IF NOT EXISTS events_current (
    -- --- Frozen identity (Unique Key prefix; contiguous, immutable) ---
    project_id            VARCHAR(64)    NOT NULL COMMENT 'Trusted project scope (session/API key/MCP context).',
    partition_date        DATE           NOT NULL COMMENT 'Immutable UTC date of canonical start_time; frozen on first publish, cross-day mutation quarantines.',
    trace_id              VARCHAR(64)    NOT NULL,
    span_id               VARCHAR(128)   NOT NULL COMMENT 'Stored separately from trace_id so the key pair is collision-free without ambiguous string concatenation.',

    -- --- Source Version Contract: application sequence for latest-wins ---
    -- UTC Unix epoch nanoseconds, checked signed BIGINT. Equivalent RFC3339 /
    -- protobuf timestamps must yield the same token. Ordinary sequence < INT64_MAX;
    -- a row-level terminal delete carries 9223372036854775807 so it always wins.
    version_token         BIGINT         NOT NULL COMMENT 'Sequence col; INT64_MAX (9223372036854775807) = terminal delete row.',

    -- --- Typed hot columns: identity / filter / group / order / preview ---
    parent_span_id        VARCHAR(128)   NULL,
    `type`                VARCHAR(32)    NOT NULL COMMENT 'Event/observation type (e.g. span, generation, event).',
    `name`                VARCHAR(512)   NULL,
    environment           VARCHAR(64)    NOT NULL DEFAULT 'default',
    user_id               VARCHAR(128)   NULL,
    session_id            VARCHAR(128)   NULL,
    `level`               VARCHAR(32)    NULL,
    is_app_root           BOOLEAN        NOT NULL DEFAULT 'false' COMMENT 'True for the real root span; never a synthetic row.',
    bookmarked            BOOLEAN        NOT NULL DEFAULT 'false',
    `public`              BOOLEAN        NOT NULL DEFAULT 'false',
    `release`             VARCHAR(128)   NULL,
    `version`             VARCHAR(128)   NULL,
    trace_name            VARCHAR(512)   NULL,

    -- --- Time columns. Microsecond precision matches the v4 events_full path. ---
    start_time            DATETIME(6)    NOT NULL,
    end_time              DATETIME(6)    NULL,
    completion_start_time DATETIME(6)    NULL,
    created_at            DATETIME(6)    NOT NULL,
    updated_at            DATETIME(6)    NOT NULL,

    -- --- Model / prompt / billing typed columns (exact totals) ---
    provided_model_name   VARCHAR(128)   NULL,
    internal_model_id     VARCHAR(128)   NULL,
    prompt_id             VARCHAR(128)   NULL,
    prompt_name           VARCHAR(256)   NULL,
    prompt_version        INT            NULL,
    total_input_tokens    BIGINT         NULL COMMENT 'Typed for exact count/token/cost parity; custom keys stay in usage_details.',
    total_output_tokens   BIGINT         NULL,
    total_cost            DECIMAL(18, 12) NULL,

    -- tags as ARRAY so any/none/all filters map to array_contains(_all/any).
    tags                  ARRAY<VARCHAR(256)> NULL,

    -- --- Long-tail metadata / model / tool / usage-cost detail (VARIANT) ---
    metadata              VARIANT        NULL COMMENT 'Long-tail metadata key/value; typed hot keys can be promoted later.',
    usage_details         VARIANT        NULL,
    cost_details          VARIANT        NULL,
    provided_usage_details VARIANT       NULL,
    provided_cost_details VARIANT        NULL,
    model_parameters      VARIANT        NULL,
    tool_definitions      VARIANT        NULL,
    tool_calls            ARRAY<VARCHAR> NULL,
    tool_call_names       ARRAY<VARCHAR> NULL,

    -- --- Full-fidelity I/O (single table; list/dashboard reads never SELECT these) ---
    input                 STRING         NULL,
    output                STRING         NULL,
    input_preview         VARCHAR(200)   NULL COMMENT 'Truncated preview for list/dashboard rows; mirrors the old events_core 200-char shape without a second table.',
    output_preview        VARCHAR(200)   NULL,

    -- --- Instrumentation source attribution ---
    `source`              VARCHAR(64)    NOT NULL,
    ingestion_sdk_name    VARCHAR(64)    NOT NULL DEFAULT 'unknown',
    ingestion_sdk_version VARCHAR(64)    NOT NULL DEFAULT 'unknown',
    service_name          VARCHAR(128)   NULL,
    telemetry_sdk_language VARCHAR(32)   NULL,

    -- --- Blob / media reference lifecycle ---
    blob_storage_file_path STRING        NULL,
    event_bytes           BIGINT         NULL,

    -- R1B experiment_* columns are intentionally ABSENT from the R1A launch DDL.

    -- --- Full-content search indexes (U1-frozen analyzer/index path) ---
    -- Two complementary indexes:
    --   * INVERTED (parser=unicode): word-tokenized MATCH search. Segments
    --     Chinese/English correctly. NOTE: the unicode/icu tokenizers do NOT
    --     strip Korean particles (비용에 is one token, not 비용+에), so pure
    --     MATCH is insufficient for Korean substring search.
    --   * NGRAM_BF: accelerates bounded LIKE '%substr%' for substring search in
    --     ANY language (incl. Korean particles, Arabic, astral-plane), within
    --     the R1A <=30-day full-content range. This mirrors Langfuse's existing
    --     position()-based substring full-text contract.
    INDEX idx_inv_input  (input)  USING INVERTED  PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
    INDEX idx_inv_output (output) USING INVERTED  PROPERTIES("parser" = "unicode", "support_phrase" = "true"),
    INDEX idx_inv_name   (`name`) USING INVERTED,
    INDEX idx_ng_input   (input)  USING NGRAM_BF  PROPERTIES("gram_size" = "3", "bf_size" = "64000"),
    INDEX idx_ng_output  (output) USING NGRAM_BF  PROPERTIES("gram_size" = "3", "bf_size" = "64000")
)
UNIQUE KEY (project_id, partition_date, trace_id, span_id)
PARTITION BY RANGE(partition_date) ()
DISTRIBUTED BY HASH(trace_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    -- version_token is the sequence column: on key conflict the larger token wins.
    "function_column.sequence_col" = "version_token",
    -- Daily partitions created/retained automatically around NOW; out-of-window
    -- PoC dates get explicit ADD PARTITION in test setup (see DorisPoC test).
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "365",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "30"
);

-- =============================================================================
-- scores_current: current numeric/boolean/categorical scores (R1A).
-- score_id is immutable; score_date is fixed from the first valid raw score
-- timestamp (UTC date) and cross-day mutation quarantines.
-- =============================================================================
CREATE TABLE IF NOT EXISTS scores_current (
    project_id            VARCHAR(64)    NOT NULL,
    score_date            DATE           NOT NULL COMMENT 'Immutable UTC date of first valid raw score timestamp.',
    score_id              VARCHAR(64)    NOT NULL,
    version_token         BIGINT         NOT NULL COMMENT 'UTC epoch ns; raw updated_at or raw score timestamp; INT64_MAX = terminal delete.',

    trace_id              VARCHAR(64)    NULL,
    observation_id        VARCHAR(128)   NULL,
    session_id            VARCHAR(128)   NULL,
    `name`                VARCHAR(256)   NOT NULL COMMENT 'Mutable data, not identity; name with delimiter must not miscount.',
    `source`              VARCHAR(32)    NOT NULL,
    data_type             VARCHAR(16)    NOT NULL COMMENT 'NUMERIC | BOOLEAN | CATEGORICAL.',
    `value`               DOUBLE         NULL COMMENT 'NUMERIC value; NULL for boolean/categorical.',
    string_value          VARCHAR(512)   NULL COMMENT 'CATEGORICAL value.',
    long_string_value     STRING         NULL,
    boolean_value         BOOLEAN        NULL,
    `comment`             STRING         NULL,
    author_user_id        VARCHAR(64)    NULL,
    config_id             VARCHAR(64)    NULL,
    queue_id              VARCHAR(64)    NULL,
    environment           VARCHAR(64)    NOT NULL DEFAULT 'default',
    metadata              VARIANT        NULL,
    `timestamp`           DATETIME(6)    NOT NULL COMMENT 'Raw score timestamp; ordering token is version_token, not this.',
    created_at            DATETIME(6)    NOT NULL,
    updated_at            DATETIME(6)    NOT NULL,

    INDEX idx_inv_comment (`comment`) USING INVERTED PROPERTIES("parser" = "unicode")
)
UNIQUE KEY (project_id, score_date, score_id)
PARTITION BY RANGE(score_date) ()
DISTRIBUTED BY HASH(score_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "version_token",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "365",
    "dynamic_partition.prefix" = "sp",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "30"
);

-- =============================================================================
-- blob_storage_file_log: current file/entity reference lifecycle for media
-- cleanup (R1A). Store path is OUTSIDE the key; raw multi-trace OTLP objects
-- rely on prefix lifecycle, not false single-trace ownership.
-- =============================================================================
CREATE TABLE IF NOT EXISTS blob_storage_file_log (
    project_id            VARCHAR(64)    NOT NULL,
    file_date             DATE           NOT NULL COMMENT 'UTC date of first reference; immutable.',
    entity_type           VARCHAR(32)    NOT NULL,
    entity_id             VARCHAR(128)   NOT NULL,
    file_id               VARCHAR(128)   NOT NULL COMMENT 'Stable file ID; parent identity is in the key.',
    version_token         BIGINT         NOT NULL,
    event_id              VARCHAR(128)   NULL,
    bucket_name           VARCHAR(256)   NULL,
    bucket_path           STRING         NULL COMMENT 'Store path outside the key so multi-trace objects are not falsely owned.',
    created_at            DATETIME(6)    NOT NULL,
    updated_at            DATETIME(6)    NOT NULL
)
UNIQUE KEY (project_id, file_date, entity_type, entity_id, file_id)
PARTITION BY RANGE(file_date) ()
DISTRIBUTED BY HASH(entity_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "function_column.sequence_col" = "version_token",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-365",
    "dynamic_partition.end" = "365",
    "dynamic_partition.prefix" = "bp",
    "dynamic_partition.buckets" = "4",
    "dynamic_partition.create_history_partition" = "true",
    "dynamic_partition.history_partition_num" = "30"
);

-- =============================================================================
-- trace_tombstones: query-visible trace-level deletion barrier (R1A).
-- Must be VISIBLE before delete completes; every event/score/metric query path
-- anti-joins against it so unseen in-flight keys cannot reappear.
-- =============================================================================
CREATE TABLE IF NOT EXISTS trace_tombstones (
    project_id            VARCHAR(64)    NOT NULL,
    trace_id              VARCHAR(64)    NOT NULL,
    deletion_generation   BIGINT         NOT NULL COMMENT 'Monotonic Postgres deletion generation; higher than any ordinary version_token.',
    created_at            DATETIME(6)    NOT NULL
)
UNIQUE KEY (project_id, trace_id)
DISTRIBUTED BY HASH(trace_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- =============================================================================
-- project_tombstones: query-visible project-level deletion barrier (R1A).
-- Retained independently of ordinary project rows and enforced by every
-- telemetry query/load/replay until the final sweep proves empty.
-- =============================================================================
CREATE TABLE IF NOT EXISTS project_tombstones (
    project_id            VARCHAR(64)    NOT NULL,
    deletion_generation   BIGINT         NOT NULL,
    created_at            DATETIME(6)    NOT NULL
)
UNIQUE KEY (project_id)
DISTRIBUTED BY HASH(project_id) BUCKETS 2
PROPERTIES (
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true"
);

-- R1B tables (dataset_run_items_current, analytics_retention_runs) are excluded
-- from this R1A launch DDL; U9 adds them only after adoption evidence.
