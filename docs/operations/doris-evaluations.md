# Doris Evaluator Operations

Doris evaluator execution is installed behind the durable `evaluations`
capability. ClickHouse keeps its existing evaluator paths and does not read this
activation row. A Doris deployment exposes evaluator pages, tRPC, unstable
Public API, MCP, and producers only when the current deployment generation has
an `ACTIVE` evaluations activation.

## Runtime contract

The Doris path supports trace, observation, dataset-associated, and historical
evaluation targets; primary and secondary LLM-as-judge execution; code
evaluators; and historical batch actions. Experiment-targeted batch evaluation
remains closed until the separate `experiments` capability is completed.

When a canonical analytics operation becomes `VISIBLE`, its target identity and
provenance are captured in `analytics_evaluation_dispatches` in the same
Postgres state transition. The row records the analytics backend, deployment
generation, capability activation generation, contract version, runtime lease,
operation, project, and target. The publisher derives a deterministic BullMQ
job ID from that row. Consumers validate the durable row before reading a
target, and a stale or tampered envelope is quarantined before analytics I/O.

Evaluator scores re-enter the selected canonical ingestion path with a
deterministic score ID. Doris execution does not write the legacy ClickHouse
`IngestionQueue`; a `JobExecution` becomes `COMPLETED` only after its score is
visible.

## Activation

Deploy the capture hook, dispatch publisher/recovery runner, and every evaluator
consumer before opening the product surface. Each web and worker runtime must
hold a live analytics runtime lease advertising evaluations contract version 1
and its installed roles. Build a reviewed absolute-path JSON inventory that
contains every expected runtime instance:

```json
[{ "instanceId": "web-a" }, { "instanceId": "worker-a" }]
```

Inspect the current state:

```bash
pnpm --filter worker run analytics:evaluations -- status
```

Start a new dark generation from the generation returned by `status`:

```bash
pnpm --filter worker run analytics:evaluations -- begin-dark \
  --expected-generation <current-activation-generation> \
  --contract-version 1 \
  --minimum-runtime-contract 1
```

Activate only after the exact live inventory is healthy. Use the database ID of
a current worker analytics runtime lease for replay admission, not its
human-readable instance ID:

```bash
pnpm --filter worker run analytics:evaluations -- activate \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <dark-generation> \
  --runtime-inventory-file /run/operator/analytics-inventory.json \
  --worker-runtime-lease-id <worker-runtime-lease-id>
```

`activate` first enables bounded DARK capture under the activation-row lock. If
a previous disable sealed a cutoff, it transfers older suspended work, scans
only visible Doris operations in the sealed acceptance-sequence interval,
verifies canonical artifacts and authoritative dispatch coverage, and persists
a bootstrap evidence digest. The final CAS succeeds only if the exact runtime
census and evidence still match.

The command emits only capability, generation, state, and evidence identifiers.
Failure output is deliberately generic; inspect sanitized application logs and
the control-plane fields below instead of placing credentials or payloads in
command arguments.

## Drain, disable, and replay

Stop new UI/API/MCP mutations and producers while retaining same-generation
capture:

```bash
pnpm --filter worker run analytics:evaluations -- begin-drain \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <active-generation>
```

After published and processing work has drained, disable the generation:

```bash
pnpm --filter worker run analytics:evaluations -- disable \
  --expected-deployment-generation <deployment-generation> \
  --expected-activation-generation <active-generation>
```

`disable` verifies the durable drain while holding the activation-row lock and
seals the enabled evaluator-configuration digest plus the visible-operation
acceptance watermark. Mutations are fenced during `DARK` and `DRAINING`, so the
configuration snapshot cannot silently diverge. Telemetry accepted while the
capability is disabled remains in the canonical artifact store. The next
`begin-dark` and `activate` command replay the bounded disabled interval before
the new generation becomes active.

Do not delete canonical ingestion artifacts required by an outstanding
evaluations cutoff. If the artifact is missing or its checksum changes,
activation fails closed; restore the exact artifact before retrying.

## Limits and recovery

- DARK capture is limited to 24 hours and 250,000 dispatch rows per activation
  attempt. Expiry or budget exhaustion records a failure code on existing
  `SUSPENDED` rows, seals a replay cutoff, and returns the capability to
  `DISABLED`. The next generation transfers those captured rows and replays
  only the subsequent disabled interval.
- The publisher scans at most 50 pending rows every 10 seconds. Publication
  failures use exponential backoff from 10 seconds to 10 minutes.
- A consumer claim lasts at most 5 minutes. An expired `PROCESSING` claim is
  reclaimable; retrying execution increments the dispatch generation and uses
  the same authoritative dispatch identity.
- A missing/deleted target is terminal and visible. Provenance mismatches are
  quarantined. Transient execution failures return the row to `PENDING`.
- LLM, code, queue, and Doris failures expose only allowlisted messages/codes.
  Responses, `JobExecution.error`, BullMQ terminal errors, logs, and spans must
  not contain DSNs, Authorization values, SQL, prompts, inputs, outputs, or
  payload bodies.

The supported states are:

| State                    | Producer behavior                                            | Publisher/consumer behavior              |
| ------------------------ | ------------------------------------------------------------ | ---------------------------------------- |
| `DARK`, capture disabled | reject product mutations; no dispatch capture                | consumers installed but no new work      |
| `DARK`, capture enabled  | reject product mutations; capture `SUSPENDED`                | no execution before activation           |
| `ACTIVE`                 | admit fenced mutations and capture `PENDING`                 | publish, recover, and execute            |
| `DRAINING`               | reject new mutations; visible telemetry captures `SUSPENDED` | drain sealed same-generation work        |
| `DISABLED`               | reject before mutation/enqueue                               | retain cutoff for next-generation replay |

## Diagnostics

Use read-only Postgres queries and record counts, not payloads:

```sql
SELECT status, generation, deployment_generation, capture_enabled,
       capture_rows, capture_row_budget, capture_expires_at,
       rescan_required, cutoff_digest, bootstrap_evidence_digest
  FROM analytics_capability_activations
 WHERE capability = 'EVALUATIONS';

SELECT status, failure_code, count(*)
  FROM analytics_evaluation_dispatches
 GROUP BY status, failure_code
 ORDER BY status, failure_code;
```

Investigate persistent `PENDING`, expired `PROCESSING`, `QUARANTINED`, and
failure-coded `SUSPENDED` rows before promotion. Readiness must also show the
exact web and worker inventory with matching backend/deployment generation and
evaluation contracts.

For a local user-state fixture on either backend:

```bash
pnpm run seed -- evaluator-states
```

It writes trace and score analytics through public APIs and creates
deterministic inactive evaluator control-plane fixtures for empty, queued,
running, success, terminal error, delayed retry, and recovered views. It
requires no model provider credential and performs no raw Doris or ClickHouse
insert.
