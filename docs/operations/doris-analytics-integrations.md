# Doris Analytics Integration Operations

Doris-backed PostHog, Mixpanel, and Blob Storage integrations are installed
behind the durable `analyticsIntegrations` capability. ClickHouse keeps its
existing scheduler, source, clients, queues, and settings behavior; it does not
read this activation row. Doris never falls back to ClickHouse.

In Doris mode, the settings pages and mutation APIs remain fail-closed until
the deployment's current `analyticsIntegrations` generation is `ACTIVE`.
Configuration changes fail before mutation when the runtime lease, deployment
generation, capability generation, or connection-time outbound policy does not
match.

## Delivery contract

Postgres is the durable control plane. When canonical ingestion becomes
`VISIBLE`, the same transaction captures stable trace, generation, observation,
and score identities for every delivery-enabled integration generation. A
pending row records its project, integration/config generation, analytics
backend, deployment generation, workload epoch fingerprint, runtime contract,
capability activation generation, and capture lease.

The scheduler seals immutable execution manifests from those rows. Queue
payloads copy the durable envelope; they are not authoritative and cannot
replace its provenance. A Worker claim revalidates the envelope, current
integration state, activation, and runtime lease before source reads and renews
its lease during long remote or object-storage operations.

PostHog and Mixpanel receive the existing allowlisted semantic event objects,
not Doris rows, raw SQL, inputs, outputs, or arbitrary metadata. Blob exports
use a separate exact-read projection and preserve JSON, CSV, JSONL, Parquet,
checksum, and manifest contracts. A missing current identity terminates its
delivery as `SOURCE_DELETED`.

Delivery is at-least-once. Only complete remote/Blob success marks every row in
the sealed manifest terminal and advances `lastSyncAt`. A process crash,
partial remote failure, S3 failure, or lease loss preserves the manifest for
fenced retry; receivers should continue to deduplicate by their existing event
identity.

## Outbound security prerequisites

Self-hosted Doris activation requires connection-time Blob endpoint validation
to be enabled on every Web and Worker runtime. Without a dedicated allowlist,
the runtime does not advertise the `analyticsIntegrations` contract and the
fleet census cannot activate it.

Configure the smallest policy that covers the deployment's approved
destinations:

```text
LANGFUSE_BLOB_STORAGE_ENDPOINT_WHITELISTED_HOST=objects.internal.example
LANGFUSE_BLOB_STORAGE_ENDPOINT_WHITELISTED_IPS=10.20.0.12
LANGFUSE_BLOB_STORAGE_ENDPOINT_WHITELISTED_IP_SEGMENTS=10.20.0.0/24
```

PostHog validates every DNS result and redirect hop at connection time. It uses
manual redirects, rejects private/link-local/metadata destinations unless
explicitly allowed, rejects HTTPS downgrade, and strips sensitive headers on
cross-origin redirects. Mixpanel uses its fixed HTTPS endpoint. Blob transports
apply the same connection-time validation inside the storage SDK, so a safe
save-time preflight cannot be bypassed through later DNS rebinding.

Customer PostHog, Mixpanel, and object-storage credentials are decrypted only
when constructing the owning client. They must not appear in source records,
queue payloads, manifests, logs, spans, or operator command arguments.

## Activation

Deploy the pending-delivery hook, bootstrap scanner, dispatch recovery runner,
all three consumers, and the scratch reconciler before enabling capture. Every
expected Web and Worker must hold a live runtime lease advertising contract
version 1 and its required roles.

Create a reviewed absolute-path inventory:

```json
[{ "instanceId": "web-a" }, { "instanceId": "worker-a" }]
```

Inspect and begin a new generation:

```bash
pnpm --filter worker run analytics:integrations -- status

pnpm --filter worker run analytics:integrations -- begin-dark \
  --expected-generation <current-activation-generation>
```

After the exact fleet census is compatible, enable DARK capture:

```bash
pnpm --filter worker run analytics:integrations -- enable-capture \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <dark-generation> \
  --runtime-inventory-file /run/operator/analytics-inventory.json
```

DARK capture may create `SUSPENDED` pending rows and sealed full-history
identity manifests. It cannot make PostHog/Mixpanel requests, upload Blob
objects, advance `lastSyncAt`, or report a configuration active. Existing
enabled Doris configurations must each have a checksum-valid, recoverable
bootstrap manifest before activation.

Activate with the same exact inventory:

```bash
pnpm --filter worker run analytics:integrations -- activate \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <dark-generation> \
  --runtime-inventory-file /run/operator/analytics-inventory.json
```

`activate` verifies durable bootstrap evidence under the activation lock and
then performs the generation CAS. External bootstrap executions and incremental
delivery start only after that CAS. Command output contains only state,
generation, and evidence identifiers; failures deliberately redact details.

New Doris integrations also enter `BOOTSTRAPPING_ACTIVE`, seal a full-history
manifest, and become `ACTIVE` only after its external bootstrap succeeds.
Disabled ClickHouse configurations are not migrated or modified.

## Drain, disable, and replay

Stop new settings/API mutations and scheduler executions:

```bash
pnpm --filter worker run analytics:integrations -- begin-drain \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <active-generation>
```

`DRAINING` preserves same-generation `SUSPENDED` capture while already sealed
executions drain. After the durable drain proof succeeds:

```bash
pnpm --filter worker run analytics:integrations -- disable \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <active-generation>
```

`disable` seals an acceptance-sequence cutoff and marks enabled configurations
for rescan. A later DARK generation transfers the previous generation's
suspended rows, replays the retained cutoff provenance, seals full-history
manifests, and takes over new capture before it may become active. Do not remove
canonical ingestion artifacts or retention barriers required by an outstanding
cutoff.

An empty BullMQ queue is not a drain proof. Nonterminal execution rows,
claimed deliveries, bootstrap manifests, scratch leases, or a required replay
make disable/backend switch fail closed.

## Capacity and Parquet scratch

The hard pending-ledger ceilings are:

- per integration: 100,000 rows or 128 MiB estimated payload;
- per deployment: 1,000,000 rows or 1 GiB estimated payload.

The first exceeded ceiling moves the affected integration to
`PAUSED_BACKLOG`, sets `rescanRequired`, and stops candidate fan-out for that
integration without blocking unrelated projects. Recovery drains sealed work,
performs a fenced full-history rescan, and resumes incremental capture.

Doris Parquet is written by the Worker to a private local file before the
existing multipart uploader commits it. Configure:

```text
LANGFUSE_DORIS_PARQUET_SCRATCH_ROOT=/var/lib/langfuse/doris-parquet-scratch
LANGFUSE_DORIS_PARQUET_SCRATCH_MAX_BYTES=2147483648
LANGFUSE_DORIS_PARQUET_SCRATCH_CLEANUP_INTERVAL_MS=60000
```

Mount the root on an encrypted ephemeral volume. The Worker enforces an
absolute, canonical, non-symlink root with mode `0700`, per-execution
directories with mode `0700`, files/reservations with mode `0600`, and a
host-shared hard reservation limit. Each allocation has a durable host/path/
byte lease. Normal completion removes it; the periodic reconciler removes only
owned directories whose lease is no longer live and preserves unknown paths.

Alert before scratch reservation approaches the configured capacity, on
repeated `PAUSED_BACKLOG`, expired execution/scratch leases, and persistent
orphan cleanup. The application does not pass customer object-storage
credentials to Doris and does not use Doris `INTO OUTFILE`.

## State and diagnostics

| Integration state       | Meaning                                                       |
| ----------------------- | ------------------------------------------------------------- |
| `BOOTSTRAPPING_DARK`    | local manifest/capture only; no third-party effect            |
| `BOOTSTRAPPING_ACTIVE`  | external full-history bootstrap may execute                   |
| `ACTIVE`                | incremental sealing, delivery, and recovery enabled           |
| `PAUSED_BACKLOG`        | hard ledger budget reached; rescan required                   |
| `RESCANNING`            | fenced full-history recovery in progress                      |
| `DRAINING`              | new mutations stopped; same-generation suspended capture kept |
| `DISABLED`              | no delivery; cutoff/replay state may remain                   |

Use read-only aggregate queries. Do not select manifest bodies, entity keys, or
customer payloads into an incident ticket:

```sql
SELECT status, generation, deployment_generation, capture_enabled,
       capture_rows, capture_row_budget, capture_required, rescan_required,
       cutoff_digest, bootstrap_evidence_digest
 FROM analytics_capability_activations
 WHERE capability = 'analyticsIntegrations';

SELECT integration_type, status, rescan_required,
       count(*) AS configurations,
       sum(pending_rows) AS pending_rows,
       sum(pending_estimated_bytes) AS pending_estimated_bytes
  FROM analytics_integration_states
 GROUP BY integration_type, status, rescan_required
 ORDER BY integration_type, status;

SELECT integration_type, status, failure_code, count(*)
  FROM analytics_integration_pending_deliveries
 GROUP BY integration_type, status, failure_code
 ORDER BY integration_type, status, failure_code;

SELECT integration_type, kind, status, last_error_code, count(*)
  FROM analytics_integration_executions
 GROUP BY integration_type, kind, status, last_error_code
 ORDER BY integration_type, kind, status;
```

Investigate persistent `CLAIMED`, expired `RUNNING`, `RETRYING`,
`QUARANTINED`, `PAUSED_BACKLOG`, unexpected `SUSPENDED`, and non-null scratch
leases before promotion or backend switching. Confirm readiness shows the exact
current Web/Worker inventory, matching deployment/capability generations, and
the connection-time outbound policy on every member.

## Privacy and deletion

The exported semantic allowlist deliberately omits raw input/output and
arbitrary metadata. Blob exports contain only the explicitly selected field
groups and format contract.

Deleting or changing a local entity prevents later retries from exporting the
deleted projection and prevents local resurrection through the selected Doris
path. It cannot retract an event or object already received by PostHog,
Mixpanel, or customer storage. Third-party retention, deletion requests, data
processing agreements, bucket lifecycle, access policy, and regional controls
remain the deployment operator's responsibility.
