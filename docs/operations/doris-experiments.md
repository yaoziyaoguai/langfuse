# Doris Experiments and Dataset Runs

Doris experiment execution and dataset-run analytics are installed behind
three independent durable capabilities:

- `datasetRunIngestion` accepts and projects dataset-run items;
- `experiments` creates and executes prompt/remote experiments;
- `datasetRunExports` exports the dataset-run-item projection.

ClickHouse retains its existing paths and does not read these activation rows.
There is no runtime fallback or dual write. In Doris mode, an unavailable or
stale capability fails before the corresponding Postgres mutation, queue
publication, or analytics I/O.

## Storage and execution contract

Postgres remains the control plane for datasets, dataset items, runs,
configuration, dispatch intent, claims, and terminal execution state. Doris
owns the selected analytics projection:

- `dataset_run_items_current` stores the run/item/trace association and its
  dataset/run deletion generations;
- `events_current` stores experiment identity, name, dataset, item, version,
  metadata, input/output, and trace/observation telemetry;
- `scores_current.dataset_run_id` links evaluator and user scores to the run;
- tombstones and Postgres deletion generations prevent delayed ingestion or
  replay from resurrecting deleted data.

Public API, tRPC, MCP, evaluator scheduling, experiment reads, metrics,
comparison, and export all use the same backend-selected repositories. Doris
queries never fall back to ClickHouse or reconstruct the analytics projection
from Postgres control rows.

Creating a Doris experiment commits the `DatasetRuns` intent and deterministic
dispatch outbox in one transaction. The row is stamped with both the
`experiments` generation and its `datasetRunIngestion` dependency generation.
Publication can be recovered after a Web crash. A Worker claim is leased and
generation-fenced; an expired claim may be recovered, while an older Worker
cannot renew, complete, or fail the replacement generation.

Each completed item writes its internal telemetry and dataset-run projection
through canonical ingestion. Item/model failures remain visible on the run and
can be retried through the existing experiment workflow. Re-delivery reuses the
durable run and deterministic analytics identities instead of creating a
second run. A run is not reported complete merely because queue publication
succeeded.

Dataset-run scores preserve `datasetRunId`, `executionTraceId`, config,
environment, metadata, and the existing Community score fields. Evaluator
score receipt time is independent from the score's business timestamp, so
historical evaluation does not start outside the durable recovery window.

## Legacy ingestion

`POST /api/public/ingestion` continues to process supported trace,
observation, and score children independently. A dataset-run child additionally
requires the current `datasetRunIngestion` activation. An inactive, draining,
or mismatched generation returns the existing child-level unsupported error
before changing Postgres or accepting canonical analytics work; it does not
discard the whole mixed batch or silently accept an unconsumable child.

## Dataset-run exports

Dataset-run-item exports require both `coreBatchExports` and
`datasetRunExports` to be `ACTIVE`. Creation atomically stamps both activation
generations. The Worker seals a stable, sorted Doris identity manifest, stores
it in object storage, verifies its checksum and limits, and then exact-reads
the selected identities.

The manifest is an identity-set snapshot, not a cross-system payload snapshot.
An identity updated before exact read exports its current payload; one deleted
before exact read is omitted. Disabling and reactivating
`datasetRunExports` increments its generation: work accepted by an older
generation cannot resume under the new one.

## Activation order

Every Web and Worker must first hold a live analytics runtime lease advertising
the code-owned contract and installed roles. Use an absolute-path JSON file
containing the exact expected runtime instance IDs:

```json
[{ "instanceId": "web-a" }, { "instanceId": "worker-a" }]
```

Inspect a capability:

```bash
pnpm --filter worker run analytics:u6-capabilities -- status \
  --capability datasetRunIngestion
```

Begin a new DARK generation from the generation returned by `status`:

```bash
pnpm --filter worker run analytics:u6-capabilities -- begin-dark \
  --capability datasetRunIngestion \
  --expected-generation <current-activation-generation>
```

Activate only after the exact live inventory is healthy:

```bash
pnpm --filter worker run analytics:u6-capabilities -- activate \
  --capability datasetRunIngestion \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <dark-generation> \
  --runtime-inventory-file /run/operator/analytics-inventory.json
```

The command verifies that the DARK generation contains no unexpected durable
work, records deterministic bootstrap evidence, validates the fleet census,
and performs the activation CAS. Output contains only capability state,
generations, and the evidence digest; failures are deliberately redacted.

Activate in this order:

1. `datasetRunIngestion`
2. `experiments`
3. `datasetRunExports`, after `coreBatchExports` is already `ACTIVE`

Activation refuses a missing, non-active, or different-deployment dependency.
The three producer transactions read and persist their own generation stamps;
one activation row never substitutes for another.

## Drain and rollback

Begin drain with the exact active generation:

```bash
pnpm --filter worker run analytics:u6-capabilities -- begin-drain \
  --capability experiments \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <active-generation>
```

After all same-generation work is terminal, disable it:

```bash
pnpm --filter worker run analytics:u6-capabilities -- disable \
  --capability experiments \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <active-generation>
```

`disable` runs the owning durable drain proof while holding the activation
lock. Pending ingestion receipts, experiment intents/outboxes/claims, or
dataset-run exports make it fail closed. An empty BullMQ queue is not
sufficient evidence.

Rollback dependents before dependencies:

1. disable `experiments`, then `datasetRunIngestion`;
2. disable `datasetRunExports`, then `coreBatchExports`.

Beginning drain on a dependency while a dependent is still enabled is refused.
`DRAINING` stops new external producers but permits exact same-generation
recovery. `DISABLED` does not allow old work to cross a later activation
generation.

## Partial failure and deletion

- A prompt/model failure leaves a durable failed run instead of a successful
  empty result.
- Partial item failure preserves successful items and an observable error count;
  retry uses the existing run and deterministic item identities.
- A publication crash leaves a pending outbox for recovery.
- A Worker crash leaves an expiring claim; takeover increments the execution
  generation and fences the old Worker.
- Dataset, run, trace, and project deletion publish visibility barriers before
  physical cleanup. Delayed ingestion, replay, or an old claim cannot make the
  deleted association visible again.
- Local deletion cannot retract telemetry or files already received by an
  external system.

## Diagnostics

Use status commands and control-plane counts; do not inspect or log customer
payloads:

```sql
SELECT capability, status, generation, deployment_generation,
       bootstrap_evidence_digest
  FROM analytics_capability_activations
 WHERE capability IN (
   'DATASET_RUN_INGESTION',
   'EXPERIMENTS',
   'DATASET_RUN_EXPORTS'
 );

SELECT experiment_execution_state, count(*)
  FROM dataset_runs
 WHERE analytics_backend = 'DORIS'
 GROUP BY experiment_execution_state;

SELECT status, count(*)
  FROM analytics_ingestion_operations
 WHERE capability = 'DATASET_RUN_INGESTION'
 GROUP BY status;

SELECT execution_state, manifest_state, count(*)
  FROM batch_exports
 WHERE dataset_run_export_activation_generation IS NOT NULL
 GROUP BY execution_state, manifest_state;
```

Investigate pending outboxes, expired `PROCESSING` claims, quarantined work,
nonterminal ingestion operations, and unsealed export manifests before drain or
promotion. Readiness must show the exact current Web/Worker inventory and
matching deployment/capability generations.

For local control-plane and user-state fixtures:

```bash
pnpm run seed -- experiment-foundation
```

The scenario uses public/canonical surfaces and does not insert directly into
Doris or ClickHouse.
