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

For the complete implementation rationale, ClickHouse impact analysis, Doris
data flows, capability timeline, and verification evidence, see
[ClickHouse / Doris Dual-Backend Implementation](./doris-clickhouse-dual-backend-implementation.md).

The selector changes the active adapter; it does not migrate data. The supported
switch procedure requires both the source and target analytics backends to be
empty. Historical migration between ClickHouse and Doris is outside this
contract; changing only the variable can expose an empty or unrelated analytics
view and is not a supported switch.

## Authoritative deployment marker

Managed F0+ deployments bind the selected backend to a single Postgres marker:
backend, generation, foundation contract, and a fingerprint of the workload
epoch. The raw workload epoch is treated as a secret. Web, worker, and the
one-shot operator commands read it only from an environment variable or mounted
file; it is never accepted as a command-line argument or printed in command
output.

A brownfield deployment without this marker remains in `ADOPTION_REQUIRED`.
Backend emptiness alone never proves that an installation is new. A provably
new deployment must explicitly set
`LANGFUSE_ANALYTICS_ALLOW_FRESH_INITIALIZATION=true` on its first managed boot;
the default is `false`. Remove the opt-in after generation 1 is created. Never
set it while upgrading an existing installation, even if its analytics backend
currently contains no rows.

Before adoption, operators must irreversibly stop the pre-F0 fleet, quiesce the
new F0 web and worker replicas, drain unstamped durable work, revoke the old
ingress/Postgres/Redis/backend/object-store credential epoch, and produce
reviewed drain and deny-probe attestation digests. The command verifies the
database-visible leases, inventory, claims, and pending registries. It consumes
the external attestation digests but cannot perform or infer those external
revocations itself.

The expected inventory is a strict JSON array containing every web and worker
replica, for example:

```json
[
  { "component": "web", "instanceId": "web-a" },
  { "component": "worker", "instanceId": "worker-a" }
]
```

Its SHA-256 digest is calculated over the JSON encoding of the sorted
`component:instanceId` strings. The operator must provide both the file and the
independently checked digest; a mismatch fails before the transition.

Run the one-shot adoption only after all external preconditions are complete:

```bash
export LANGFUSE_ANALYTICS_WORKLOAD_EPOCH_FILE=/run/secrets/analytics-workload-epoch

pnpm --filter @langfuse/shared run analytics-backend:adopt-existing -- \
  --expected-backend doris \
  --foundation-contract-version 1 \
  --expected-inventory-file /run/operator/analytics-inventory.json \
  --expected-inventory-digest <sha256> \
  --drain-attestation-digest <sha256> \
  --deny-probe-attestation-digest <sha256>
```

This creates generation 1 for the current backend. It does not move data or
switch backends, and a subsequent pre-F0 binary/credential rollback is not
supported.

An intentional backend switch is stricter. It requires the current generation's
exact quiesced inventory, no live claims, every durable-work registry drained,
all six durable capabilities disabled without replay state, no historical
analytics control state, and both the explicit source and target analytics
backends to be empty. The command probes both backends and compares their
evidence digests with the reviewed inputs before the marker CAS. It also requires
a new target workload epoch; reusing the source epoch is rejected.

```bash
export LANGFUSE_ANALYTICS_EXPECTED_WORKLOAD_EPOCH_FILE=/run/secrets/source-workload-epoch
export LANGFUSE_ANALYTICS_TARGET_WORKLOAD_EPOCH_FILE=/run/secrets/target-workload-epoch

pnpm --filter @langfuse/shared run analytics-backend:switch -- \
  --expected-backend clickhouse \
  --expected-generation 1 \
  --target-backend doris \
  --target-foundation-contract-version 1 \
  --expected-inventory-file /run/operator/analytics-inventory.json \
  --expected-inventory-digest <sha256> \
  --expected-source-emptiness-evidence-digest <sha256> \
  --expected-target-emptiness-evidence-digest <sha256> \
  --drain-attestation-digest <sha256> \
  --deny-probe-attestation-digest <sha256>
```

The direct-value alternatives are
`LANGFUSE_ANALYTICS_WORKLOAD_EPOCH`,
`LANGFUSE_ANALYTICS_EXPECTED_WORKLOAD_EPOCH`, and
`LANGFUSE_ANALYTICS_TARGET_WORKLOAD_EPOCH`; set exactly one direct value or file
for each required epoch. Operator failures exit non-zero and deliberately redact
the underlying error from the command wrapper so supplied epoch and attestation
values are not echoed there. Investigate readiness and the sanitized
control-plane diagnostics instead of retrying with secrets in argv.

Analytics schema changes use a two-release compatibility gate. Release A may
add only a checksum-allowlisted additive Doris migration and an explicit
current-plus-one-adjacent reader window; writers continue emitting the current
contract. Every live web and worker advertises its accepted schema and
canonicalizer ranges. Release B remains disabled until the exact live inventory
accepts the required versions and the designated rollback build has completed a
`QUIESCED` drill as both a web and a worker. Unknown migrations, expired or
incompatible leases, a one-component rollback drill, and unsupported durable
operations all fail closed.

The Postgres control-plane migration adds provenance checks as `NOT VALID`.
Postgres still enforces those checks for every new or updated row, while avoiding
synchronous full-table scans of existing ingestion, deletion, checkpoint, and
retention registries during rollout. Existing rows predate the nullable
provenance columns and therefore retain the all-null legacy shape. Operators may
validate the four named `*_provenance_check` constraints later in a separately
observed maintenance operation; promotion does not depend on an inline table
scan in the schema migration.

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

For a fresh deployment that intentionally runs only Doris and an external Doris
cluster, follow the end-to-end
[Doris-only deployment guide](./doris-only-deployment.md). It includes the
dedicated Compose manifest, environment template, migration order, first-boot
marker, readiness checks, smoke test, and capability activation order.

- web: `DORIS_QUERY_*` with a SELECT-only identity (the root Compose maps
  `DORIS_WEB_QUERY_USER/PASSWORD` into these runtime names);
- worker: its own `DORIS_QUERY_*` plus `DORIS_STREAM_LOAD_*` (the root Compose
  maps `DORIS_WORKER_QUERY_USER/PASSWORD` into its query identity);
- one-shot migrator: `DORIS_MIGRATION_*` only.

The Doris adapter exposes canonical OTLP/v4 telemetry, direct score ingestion,
trace/observation/session reads, the implemented observation/session/user
metrics, analytics deletion, Community product monitors, and custom dashboards
through the shared logical query interface. ClickHouse retains native progress events while the Doris
adapter emits rows after its bounded query completes. A query dimension or
filter that the Doris compiler does not implement fails explicitly instead of
falling back to ClickHouse.

The legacy ingestion route processes supported children independently on both
backends. In Doris mode, dataset-run children additionally require an active
`datasetRunIngestion` generation and fail at the child boundary before
mutation when unavailable; supported trace, observation, and score children in
the same batch continue normally. ClickHouse keeps its existing legacy
ingestion and native streaming export behavior.

Core Doris batch exports for traces, observations/events, scores, sessions,
dataset items, and audit logs are installed behind the `coreBatchExports`
durable activation. Creation locks and stamps the selected backend, deployment
generation, activation generation, and contract version in the same Postgres
transaction as a durable dispatch outbox. Missing, disabled, dark, draining, or
mismatched activation fails before the `BatchExport` row is created. The stable
queue job ID and recovery publisher make a process crash between commit and
queue publication retryable.

The worker claims a fenced manifest generation, runs one project-scoped Doris
identity statement, and streams a canonically sorted, compressed ID-only
manifest to object storage. It seals only the authoritative attempt after
verifying the claim and generation. Execution streams and verifies the manifest
checksum, encoded byte count, row count, metadata, base64/gzip shape, and
decompression limits; exact-ID batches revalidate their execution claim before
every Doris read. Expired manifest/execution leases can be recovered by a newer
generation, while stale workers cannot seal, fail, or complete that generation.
Unreferenced attempts and expired terminal manifests are removed by the orphan
cleaner.

This is an identity-set snapshot, not a cross-system payload snapshot. A row
committed after the Doris identity statement starts is not added even if its
business timestamp is old. A sealed identity that is updated before fetch uses
the current payload; one deleted before fetch is omitted; an already emitted
row is not retracted. Dataset-run-item exports are installed behind their
separate `datasetRunExports` generation and additionally require
`coreBatchExports` to be active.

Evaluator execution is installed behind the durable `evaluations` activation.
Its visible-operation capture, authoritative dispatch, publisher/recovery,
target adapters, all evaluator consumers, deterministic score persistence, and
disabled-window replay are backend-selected; ClickHouse behavior remains
unchanged. Doris UI/API/MCP and producers still fail before mutation unless the
current generation is `ACTIVE`. Activation, rollback, resource limits, and
diagnostics are documented in
[`doris-evaluations.md`](./doris-evaluations.md).
Brownfield adoption preserves existing `EvalTemplate` and `JobConfiguration`
rows; it does not replace, disable, or delete evaluator definitions. Pending
legacy executions must still drain before a backend transition, and Doris
evaluation remains closed until its own dark-deploy, fleet-census, bootstrap,
and activation gates complete.

Experiments and dataset-run analytics are installed behind the independent
`datasetRunIngestion`, `experiments`, and `datasetRunExports` durable
activations. Their UI/API/MCP entry points fail with a structured
`UnsupportedFeature` response before mutation while inactive. Activation order,
retry, partial failure, deletion, export, rollback, and diagnostics are
documented in
[`doris-experiments.md`](./doris-experiments.md).

Third-party analytics integrations are installed behind the durable
`analyticsIntegrations` activation. Their settings/API entry points fail before
mutation while inactive; DARK bootstrap cannot perform HTTP/S3 effects, and
ClickHouse-backed integration workers are never a fallback. Activation,
delivery/retry, rollback/replay, outbound security, Parquet scratch, privacy,
and diagnostics are documented in
[`doris-analytics-integrations.md`](./doris-analytics-integrations.md).

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

### Optional control-state compaction

Checkpoint-covered ingestion child ledgers can be compacted by an opt-in Worker
task. It is disabled by default and must remain disabled until checkpoint
capture, restore, signature verification, and the external latest-checkpoint
authority are operational:

```text
LANGFUSE_ANALYTICS_CONTROL_STATE_CLEANER_ENABLED=false
LANGFUSE_ANALYTICS_CONTROL_STATE_CLEANER_INTERVAL_MS=21600000
```

The cleaner only removes candidate/load child rows for successful terminal
operations that are below a signed, sealed checkpoint high-watermark and past
the post-replay safety delay. It preserves the operation status, frozen
compaction manifest, and entity heads. Missing or incomplete checkpoint evidence
turns the run into a no-op; disabling the scheduler does not affect ingestion.

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
4. Ingest a trace and score, then verify the currently enabled list, detail,
   observation/trace metrics, score analytics, and deletion paths. For each
   durable capability being promoted, also run its documented DARK census,
   activation, recovery, and user-flow checks.
5. Confirm the unselected backend receives no new writes or queries.
