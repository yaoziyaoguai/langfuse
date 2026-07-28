# Analytics Backend Capability Baseline

This document freezes the Langfuse Community analytics product baseline for
the Doris parity work. It is a product-surface inventory, not a storage table
inventory. Internal tables, queues, and helpers are included only as evidence
for a reachable Community capability.

The baseline is fixed at commit `5a964434d1941b0fa879cb681f4b5b81ed8a0ccf`.
ClickHouse remains the default backend. A Doris deployment selects Doris for
the whole deployment; dual-write, per-project selection, runtime hot switching,
and implicit historical migration are not supported.

The executable copy of this matrix is
`web/src/features/capabilities/communityCapabilityManifest.test-fixture.ts`.
It is deliberately test-only: runtime routing remains owned by each feature and
by the six durable activation rows.

## Status vocabulary

- `available`: the current Doris foundation exposes the surface.
- `explicitly-unavailable`: the current Doris foundation fails before mutation;
  the owning unit must complete it before activation.
- `storage-neutral`: PostgreSQL or another non-analytics control plane owns it.
- `cloud-ee-excluded`: it is not a Community product surface.
- `static-synchronous`: ordinary rollout/readiness applies; no cross-process
  accepted-work handoff exists.
- `durable-activation`: producer, consumer, recovery, and fleet contracts must
  pass a PostgreSQL activation-generation CAS before the Doris surface opens.

## Frozen matrix

| Product capability                         | Scope    | Current Doris     | Target          | Activation              | Owner | Reachability evidence                                   |
| ------------------------------------------ | -------- | ----------------- | --------------- | ----------------------- | ----- | ------------------------------------------------------- |
| Backend selection, migrations, readiness   | Included | available         | available       | none                    | U0    | `LANGFUSE_ANALYTICS_BACKEND`, web/worker readiness      |
| OTLP/v4 canonical ingestion                | Included | available         | available       | static-synchronous      | U0    | public OTLP endpoints, analytics ingestion queue        |
| Legacy trace/observation/score ingestion   | Included | available         | available       | static-synchronous      | U6    | `/api/public/ingestion`; child-level dataset-run gate   |
| Trace/observation/score/session/user reads | Included | available         | available       | static-synchronous      | U3    | tRPC, Public API, MCP list/detail/search                |
| Trace metrics, bulk detail, export source  | Included | available         | available       | static-synchronous      | U1    | trace metrics tRPC, exact-ID reads, gated export source |
| Core analytics batch exports               | Included | available         | available       | `coreBatchExports`      | U2    | `batchExport.create`, durable dispatch/export workers   |
| Product monitors                           | Included | available         | available       | static-synchronous      | U3    | monitors page, tRPC, monitor worker                     |
| Custom dashboards and widgets              | Included | available         | available       | static-synchronous      | U3    | dashboard pages, tRPC, unstable API/MCP                 |
| Evaluator execution                        | Included | available         | available       | `evaluations`           | U5    | durable dispatch/replay, eval pages, tRPC, API, MCP     |
| Prompt/remote experiment execution         | Included | available         | available       | `experiments`           | U6    | experiment pages and tRPC                               |
| Dataset-run ingestion and projection       | Included | available         | available       | `datasetRunIngestion`   | U6    | dataset-run Public API and MCP                          |
| Dataset-run exports                        | Included | available         | available       | `datasetRunExports`     | U6    | dataset-run-items batch-export table                    |
| Dataset-run query, metrics, and comparison | Included | available         | available       | `datasetRunIngestion`   | U6    | dataset run pages, tRPC, MCP                            |
| PostHog, Mixpanel, and Blob integrations   | Included | available         | available       | `analyticsIntegrations` | U7    | integration pages, tRPC, public Blob API                |
| Projects, datasets, prompts, configs, auth | Included | storage-neutral   | storage-neutral | none                    | U0    | PostgreSQL control-plane models                         |
| Cloud core-data S3 operational export      | Excluded | cloud-ee-excluded | excluded        | none                    | U0    | `CoreDataS3ExportQueue`                                 |
| Enterprise and Cloud-only product surfaces | Excluded | cloud-ee-excluded | excluded        | none                    | U0    | Community corpus only; see Enterprise overlay below     |

## Self-hosted Enterprise overlay

The frozen matrix above deliberately measures Community parity. It must not be
read as saying that this fork removed or cannot run self-hosted Enterprise
features. With a valid `LANGFUSE_EE_LICENSE_KEY`, the following upstream
Self-Hosted Enterprise surfaces work in a Doris deployment:

| Capability                    | Owning data/control plane                | Doris status | Evidence                                                                  |
| ----------------------------- | ---------------------------------------- | ------------ | ------------------------------------------------------------------------- |
| RBAC and membership roles     | PostgreSQL                               | available    | existing auth, role, and membership services                              |
| SCIM / organization Admin API | PostgreSQL                               | available    | `web/src/pages/api/public/scim/**`                                        |
| Audit Log                     | PostgreSQL                               | available    | Prisma `AuditLog`, audit viewer/router, SCIM audit regression             |
| Ingestion Masking             | raw object → Worker → selected analytics | available    | durable OTLP masking context and production Doris canonicalizer wiring    |
| Per-project data retention    | PostgreSQL cutoff + selected analytics   | available    | existing `retentionDays`; Doris project cutoff and anti-resurrection path |

RBAC, SCIM, and Audit Log do not query ClickHouse and require no Doris query
adapter. Ingestion Masking is data-plane sensitive: for Doris, the accepted raw
OTLP envelope durably carries the organization and configured propagated
headers, and Worker invokes the existing licensed masking callback before
canonicalization and Stream Load. A fail-open callback preserves upstream
behavior. A fail-closed callback failure writes no unmasked canonical data and
leaves the durable ingestion operation retryable.

The raw object already contains the pre-masking customer payload and is a
sensitive store. Propagated header values share that raw-object lifecycle so a
retry can reproduce the original callback request. Operators must restrict the
raw bucket and configure only header names they intentionally allow to be
forwarded.

Cloud billing, Stripe workflows, Cloud usage metering, and Cloud operational
exports remain excluded because they are Cloud operations, not because Doris
cannot store their product objects. This Enterprise overlay does not add a
seventh durable Community activation row.

The six and only six durable Doris capability rows are:

1. `coreBatchExports`
2. `evaluations`
3. `experiments`
4. `datasetRunExports`
5. `datasetRunIngestion`
6. `analyticsIntegrations`

Monitors, custom dashboards, core queries, and core ingestion are intentionally
not activation rows. Adding another durable capability requires changing both
this baseline and the executable corpus before implementation.

## Activation and failure contract

For a durable capability, `communityAvailability.ts` expresses only product
eligibility. Doris mutation and producer paths additionally require a live
runtime lease and a matching durable activation generation. `DARK` can contain
only explicitly installed internal capture/bootstrap behavior; it cannot cause
third-party effects. `ACTIVE` admits external producers. `DRAINING` stops new
external work while preserving same-generation recovery. `DISABLED` rejects
new work and retains any required replay cutoff. The `DRAINING → DISABLED`
transaction must run the owning feature's durable drain proof while holding the
activation-row lock; an empty queue or a caller assertion is not evidence.

The one-shot checkpoint process registers as the auxiliary `CHECKPOINT`
runtime component. It remains subject to deployment generation, lease, and I/O
fencing, but is deliberately excluded from the `WEB`/`WORKER` serving-fleet
capability census and cannot advertise capability roles.

ClickHouse does not read Doris activation rows. An unavailable Doris surface
returns a structured error before it persists or enqueues work; it never falls
back to ClickHouse and never silently accepts work without a consumer.

`available` for a durable capability means that its producer, consumer,
recovery, and storage contract are installed. It does not bypass activation:
Doris core batch-export creation still fails before mutation unless the
deployment marker, runtime fleet census, and matching `coreBatchExports`
generation are `ACTIVE`. Dataset-run exports use their own
`datasetRunExports` generation and also require `coreBatchExports` to be
active.

Operate `coreBatchExports` with its dedicated fail-closed command. `begin-dark`
increments the activation generation; `activate` proves an empty
same-generation durable registry and checks the exact live Web/Worker census.
`disable` succeeds only after `begin-drain` and after every same-generation
export and dispatch outbox is terminal.

```bash
pnpm --filter worker run analytics:core-batch-exports -- status
pnpm --filter worker run analytics:core-batch-exports -- begin-dark \
  --expected-generation <current-activation-generation>
pnpm --filter worker run analytics:core-batch-exports -- activate \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <dark-generation> \
  --runtime-inventory-file /run/operator/analytics-inventory.json
```

Doris evaluator execution is likewise installed but remains fail-closed until
the current `evaluations` activation is `ACTIVE`. See
[`doris-evaluations.md`](./doris-evaluations.md) for dark activation,
cutoff/replay, rollback, limits, and diagnostics.

Doris experiment execution, dataset-run ingestion/projection, and
dataset-run-item exports are installed but remain fail-closed until their
independent durable activations are active. Their dependency order, retry,
partial failure, deletion, export, rollback, and diagnostics contracts are
documented in
[`doris-experiments.md`](./doris-experiments.md).

Doris PostHog, Mixpanel, and Blob integrations are installed but remain
fail-closed until `analyticsIntegrations` is active. Their pending-delivery
ledger, DARK no-egress bootstrap, cutoff replay, outbound validation, Parquet
scratch, limits, privacy, and diagnostics are documented in
[`doris-analytics-integrations.md`](./doris-analytics-integrations.md).

## Change control

The Community matrix may be corrected when reachability evidence proves that a
baseline surface was omitted, but code search alone must not silently expand
scope. Each new row needs an inclusion or exclusion rationale, an owning unit,
and an activation class. Self-hosted Enterprise capabilities remain in the
separate overlay above; Cloud-only code remains excluded even when it shares an
internal queue or repository with Community code.
