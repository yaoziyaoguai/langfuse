# Analytics Backend Selection

Langfuse selects exactly one analytics storage backend when each web or worker
process starts:

```text
LANGFUSE_ANALYTICS_BACKEND=clickhouse  # default
LANGFUSE_ANALYTICS_BACKEND=doris
```

All web and worker workloads in one deployment must use the same value. Changing
the value requires a restart. Runtime hot switching, per-project selection, and
dual-write are intentionally unsupported.

The selector changes the active adapter; it does not migrate data. A deployment
that already contains ClickHouse data must not change to Doris until that data
has been migrated and verified separately. Switching the variable against an
empty target produces an empty analytics view, not an implicit copy.

## ClickHouse

ClickHouse remains the default and retains its existing runtime, migrations,
ingestion workers, queries, deletion jobs, exports, evaluators, and integrations.
Configure `CLICKHOUSE_*` as before. The web image applies ClickHouse migrations
on startup unless `LANGFUSE_AUTO_CLICKHOUSE_MIGRATION_DISABLED=true`.

The restored ClickHouse schema keeps the existing MergeTree layout and query
builder. In particular, event queries continue to use the shared event query
builder and never add `FINAL` to the events table.

## Doris

Doris uses the canonical ingestion pipeline, Doris query adapters, fenced
deletion lifecycle, and schema readiness checks. Configure:

- web: `DORIS_QUERY_*` with a SELECT-only identity (the root Compose maps
  `DORIS_WEB_QUERY_USER/PASSWORD` into these runtime names);
- worker: its own `DORIS_QUERY_*` plus `DORIS_STREAM_LOAD_*` (the root Compose
  maps `DORIS_WORKER_QUERY_USER/PASSWORD` into its query identity);
- one-shot migrator: `DORIS_MIGRATION_*` only.

The Doris adapter exposes OTLP/v4 telemetry, legacy trace/observation ingestion,
scores, trace/observation/session reads, core metrics, analytics deletion, and
Community product monitors. Legacy ingestion is converted into canonical events;
it never restores or writes the ClickHouse v3 trace/observation tables. Partial
legacy updates merge against the current visible Doris snapshot and are
serialized per entity so concurrent updates cannot overwrite one another from a
stale read.

Evaluator execution, experiments/dataset-run analytics, batch exports,
third-party analytics integrations, and custom dashboard authoring remain
unavailable in Doris mode. Their UI/API/MCP entry points fail with a structured
`UnsupportedFeature` response before enqueuing work, and their ClickHouse-backed
workers remain unregistered. Dataset-run items sent through the legacy ingestion
endpoint receive child-level HTTP 501 while supported tracing and score children
continue normally. These capabilities are deferred, not impossible: each needs
its ClickHouse-specific repositories, queue producers, and recovery semantics
replaced at the analytics boundary before its gate can be removed.

### Optional global retention

Deployment-wide Doris analytics retention is disabled unless
`LANGFUSE_DORIS_GLOBAL_RETENTION_DAYS` is set. It is intentionally global, not
per-project, and accepts a minimum of three days. The worker publishes an
immutable cutoff in Postgres before deletion, waits for all older
`LOADING`/`UNKNOWN` batches to settle, then removes at most
`LANGFUSE_DORIS_GLOBAL_RETENTION_BATCH_SIZE` entity heads per interval through
the existing least-privilege Stream Load delete identity. Doris deletion must be
visible before the corresponding Postgres head is removed; stable labels make a
crash between those steps retryable. The active and completed cutoff remain an
anti-resurrection barrier for delayed or replayed ingestion.

```text
LANGFUSE_DORIS_GLOBAL_RETENTION_DAYS=30
LANGFUSE_DORIS_GLOBAL_RETENTION_INTERVAL_MS=60000
LANGFUSE_DORIS_GLOBAL_RETENTION_DRAIN_MS=120000
LANGFUSE_DORIS_GLOBAL_RETENTION_BATCH_SIZE=1000
```

This policy removes Doris analytics projections and their entity heads. Raw and
canonical ingestion objects, media, Postgres control/status records, and backups
retain their separate lifecycle; enabling this option must not be represented as
immediate raw-object erasure. Take and verify a backup before first enablement,
because advancing a completed cutoff is intentionally irreversible.

Apply Doris migrations before starting or promoting web/worker:

```bash
pnpm --filter @langfuse/shared run doris:migrate
```

The production image also contains
`/app/packages/shared/dist/doris/scripts/migrate.js`, so an orchestrator can run
the same migrator as a separate one-shot workload. Do not inject migration or
Stream Load credentials into the web container. Doris readiness fails closed if
the database version, migration checksums, physical schema, or recoverable
canonical operations are incompatible.

Production Doris connections require verified TLS and separate least-privilege
query, Stream Load, and migrator identities. The bundled `1 FE + 1 BE`
development profile is development-only:

```bash
docker compose -f docker-compose.dev.yml --profile doris up -d
pnpm --filter @langfuse/shared run doris:migrate
```

To exercise locally built web and worker images, combine that infrastructure
file with `docker-compose.build.yml`; the build file forwards the same selector
and keeps web query, worker query, and worker Stream Load identities separate.

The root `docker-compose.yml` keeps its bundled ClickHouse service for backward
compatible default startup. With `LANGFUSE_ANALYTICS_BACKEND=doris`, application
traffic does not use it and neither web nor worker waits for its health, but
Compose still starts that idle service; production
Doris deployments should point web/worker at an external Doris topology and may
omit the bundled ClickHouse service in their orchestrator manifests.

See [Doris runtime security](./doris-security.md) for credential and network
requirements.

## Verification before promotion

For either backend:

1. Apply only that backend's migrations.
2. Start every workload with the same selector value.
3. Require `/api/public/ready` and worker readiness to pass.
4. Ingest a trace and score, then verify list, detail, metrics, and deletion.
5. Confirm the unselected backend receives no new writes or queries.
