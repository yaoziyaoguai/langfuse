# Doris PoC — U1 Frozen Physical Design and Measured Results

> Historical design record: this file freezes the initial physical-engine PoC,
> not the current product capability matrix. Current availability and activation
> contracts are maintained in
> [`analytics-backend-capabilities.md`](./analytics-backend-capabilities.md).

This document records the U1 engine/physical-design PoC outcome for the Langfuse
Community Doris Analytics Storage Refactor. The machine-readable companion is
`packages/shared/doris/poc/workload-manifest.yaml`.

U1 freezes compatibility, workload corpus, candidate DDL, key/bucket/index/
projection/search choices, and writer byte/backpressure limits. It is the
go/no-go gate: any unresolved physical branch or failed mandatory gate blocks
U2–U8. The local engine-layer correctness/durability/transport/query gates PASS
on real Doris 4.0.7. Production-scale capacity, producer readiness, object-store
conditional create, backup repository, and HA-failover gates are
`OPERATOR_REQUIRED` — they need operator-frozen inputs the plan names as "Gate
Inputs That Do Not Change Product Scope" and cannot be substituted by a local run.

## Pinned target (revalidated at implementation start)

- Doris **4.0.7** (official Stable). Build reported by FE: `doris-4.0.7-rc02`.
- Images: `apache/doris:fe-4.0.7`, `apache/doris:be-4.0.7`. 存算一体 cluster.
- Local topology: 1 FE + 1 BE on `docker-compose.doris-poc.yml`, ports bound to
  `127.0.0.1` (FE MySQL `9031`, FE HTTP `8031`, BE HTTP `8041`). This is a
  development target, **not** production evidence.
- 4.1.x canary/soak is explicitly post-cutover and out of R1A scope.

## Frozen physical design (single decision, no unresolved menu)

| Concern                | Decision                                                                                                                                         |
| ---------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| Event fact table       | ONE table `events_current`. No `events_full`/`events_core` dual write, no MV, no synthetic root (real-root fallback is a U5 read-time decision). |
| Engine                 | Doris Unique Key, Merge-on-Write (`enable_unique_key_merge_on_write=true`).                                                                      |
| Unique key             | `(project_id, partition_date, trace_id, span_id)` — collision-free pair identity.                                                                |
| Sequence (latest-wins) | `version_token` BIGINT, UTC epoch nanoseconds (`function_column.sequence_col`).                                                                  |
| Terminal delete        | `__DORIS_DELETE_SIGN__=1` with `version_token=INT64_MAX` (9223372036854775807).                                                                  |
| Partition              | `AUTO PARTITION BY RANGE(date_trunc(partition_date, 'day'))`; no global retention. `partition_date` = immutable UTC date of canonical `start_time`. |
| Distribution           | `HASH(trace_id)` BUCKETS 8 (PoC; production bucket count scales with BE).                                                                        |
| Hot columns            | Typed (identity/filter/group/order/billing/preview).                                                                                             |
| Long-tail              | VARIANT (metadata/usage/cost/model/tool).                                                                                                        |
| I/O                    | Single STRING `input`/`output` + `input_preview`/`output_preview` (VARCHAR 200) for list/dashboard.                                              |
| Full-text              | INVERTED (`parser=unicode`, `support_phrase=true`) on input/output/name **and** NGRAM_BF on input/output for substring.                          |
| Tombstone barriers     | `trace_tombstones`, `project_tombstones` (Unique Key, monotonic `deletion_generation`).                                                          |
| Sampling               | NOT ported (Doris has no SAMPLE BY; sampling is not an R1A contract).                                                                            |

## Measured PoC results (local 1 FE + 1 BE)

Engine correctness/durability — `pnpm --filter @langfuse/shared run test:doris`
(15 tests, all PASS against real Doris):

- latest-wins by `version_token`; a lower late write cannot overwrite the winner.
- same `version_token` ⇒ exactly ONE current row (the CAS winner is chosen
  upstream by the entity-head CAS in U4; the loser quarantines before any load).
- terminal delete via `INT64_MAX` + `DELETE_SIGN` makes the entity invisible.
- **anti-resurrection**: a late lower/equal write after delete cannot revive it.
- duplicate deterministic Stream Load label converges to one row.
- cross-trace equal `span_id` does not collide.
- VARIANT metadata/usage/cost round-trip + typed billing columns.
- `trace_tombstones` anti-join hides a trace from every query path.
- SQL injection payloads stay parameter-bound values (table not droppable).
- compaction invariance: out-of-order writes keep the same winner after compaction.
- query semantics: project isolation, date-bounded partition prune, stable cursor
  tie-break, array any/none/all, null/empty.

Transport (proven in the suite + benchmark):

- Doris FE Stream Load **requires** `Expect: 100-continue`; the PoC client uses
  `node:http` with the 100-continue handshake (undici/fetch refuse the literal
  header).
- Allowlisted FE→BE `307`: the client strips the embedded userinfo (`root:@`)
  from the Location, validates the BE origin against the allowlist, rewrites to
  the reachable origin, and re-PUTs preserving method/body/credential. A
  non-allowlisted origin is rejected **before** any auth/body is forwarded.
- `max_filter_ratio=0` rejects filtered rows; duplicate labels report
  `Label Already Exists` and reconcile to the single committed row.

Benchmark — `pnpm --filter @langfuse/shared run benchmark:doris` (11 gates:
6 PASS, 5 OPERATOR_REQUIRED, 0 FAIL):

- pinned version 4.0.7 ✓
- write throughput local: 302 rows/s, 0.1 MiB/s (sequential batches through the
  100-continue→307→100-continue flow on a 1FE+1BE Colima VM; production capacity
  is OPERATOR_REQUIRED).
- publish-visible latency: 10000/10000 visible after 0.4s.
- query latency: detail p95 27.4ms (p50 13.7ms); bounded-count p95 57.3ms.
- fault: duplicate-label converges; non-allowlisted redirect rejected.

## Frozen writer limits (derived from the PoC; re-derived for production)

`max_batch_bytes` 100 MiB, `max_batch_rows` 1,000,000 (soft; primary limit is
bytes/partitions/latency), `max_inflight_loads` 4, `global_buffered_byte_cap`
512 MiB, `worker_concurrency` 4, `shutdown_drain_deadline_ms` 30,000. These feed
the U4 durable writer backpressure contract.

## Findings (must be carried into U2/U5)

0. **U2 partition lifecycle correction.** The first frozen DDL used
   `dynamic_partition.start=-365`, which deletes partitions older than the
   configured window and therefore implemented an undeclared global retention
   policy. It also could not accept arbitrary historical source-time days
   without manual partition DDL. U2 corrected the candidate to Doris AUTO
   PARTITION and applied the same change through immutable forward migration
   `0002_refreeze_r1a_partition_and_tombstone_order.sql` before application
   traffic. R1A has no global-retention property; U9 owns any later adoption.
   See the Doris [dynamic partition](https://doris.apache.org/docs/dev/table-design/data-partitioning/dynamic-partitioning/)
   and [auto partition](https://doris.apache.org/docs/4.x/table-design/data-partitioning/auto-partitioning/)
   contracts.

1. **Korean particle tokenization gap.** The `unicode`/`icu` inverted-index
   tokenizers do **not** strip Korean particles (`비용에` is one token, not
   `비용`+`에`). Word-tokenized `MATCH` is therefore insufficient for Korean
   substring search. Frozen decision: INVERTED (MATCH) for word-boundary search
   in languages it segments well (Chinese, English) **plus** NGRAM_BF-accelerated
   bounded `LIKE` for substring search in any language (incl. Korean particles,
   Arabic, astral-plane). This mirrors Langfuse's existing `position()`-based
   substring full-text contract; the R1A ≤30-day full-content range bounds it.
2. **`event_ts` is no longer write-time.** The ClickHouse design stamped
   `event_ts = now()` at write, so replays reordered versions (a latent bug).
   The Doris design uses `version_token` = the deterministic Source Version
   Contract token (UTC epoch ns from raw/receipt time, never processing clock),
   which the PoC proves gives stable latest-wins independent of arrival order.
3. **`partition_date` is part of the unique key** and immutable; a cross-day
   `start_time` mutation is a distinct key and quarantines at the entity-head
   CAS (U4), never a second current row.
4. **FE Stream Load credential is embedded in the 307 Location** as userinfo;
   the client must strip it (do not carry raw credentials in the rewritten URL;
   the client supplies its own Authorization header).

## OPERATOR_REQUIRED gates (block production PASS, not local PASS)

The following need operator-frozen inputs (PRD §9–§10, plan "Gate Inputs That Do
Not Change Product Scope") before U2 production benchmarks. They are recorded
as OPERATOR_REQUIRED, not FAILed, because a local 1FE+1BE run cannot produce
them and the plan forbids substituting a mock/no-crash run for real evidence.

- Production topology (non-HA with accepted RPO/RTO risk, or 3 FE / ≥3 BE / 3
  replicas), resource budget, RPO, RTO.
- Capacity at production scale: real retained corpus (≥10M current events / 30d
  baseline), write duty cycle, payload distribution, query concurrency.
- Producer readiness census: service/config ownership source + authenticated
  principal/endpoint traffic over the declared window; per-producer Doris-only
  E2E (F6).
- Object-store provider proof of conditional-create collision + immediate
  `HEAD`/`GET` read-after-write visibility for the canonical publication protocol.
- Backup repository + manifest key manager/retention horizon + append-only
  latest-checkpoint authority outside that repository (U8 implements/drills it).
- FE failover / BE loss — applies only if the operator freezes an HA target; the
  non-HA local target instead proves loss detection + durable backlog + restart.

## How to reproduce

```bash
docker-compose -f docker-compose.doris-poc.yml up -d   # ~90s for FE+BE
pnpm --filter @langfuse/shared run test:doris           # 15 integration tests
pnpm --filter @langfuse/shared run benchmark:doris      # 11-gate report
docker-compose -f docker-compose.doris-poc.yml down -v
```

The local compose caps FE heap at 2 GiB (`-Xmx2048m`) and BE
`mem_limit_percentage=70` to fit an 8 GB development VM; these are PoC-only
tunings, not production config.
