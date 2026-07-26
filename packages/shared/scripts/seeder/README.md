# Langfuse Seed CLI

One command to put any local test state into Langfuse — for developers and
for coding agents.

```bash
pnpm run seed -- doctor                  # check the stack, get the exact fix per failure
pnpm run seed -- list                    # scenarios and flags (--json for machines)
pnpm run seed -- analytics-smoke         # public OTLP write + public API readback (CH or Doris)
pnpm run seed -- experiment-foundation   # public dataset/run/experiment/score round trip
pnpm run seed -- evaluator-states        # evaluator list/detail state matrix
pnpm run seed -- integration-states      # safe integration settings/error states
pnpm run seed -- trace-tree --observations 5000 --breadth 1000 --v4
pnpm run seed -- long-session --traces 300 --observations-per-trace 8
pnpm run seed -- many-traces --count 100000 --days 14
```

The CLI follows `LANGFUSE_ANALYTICS_BACKEND=clickhouse|doris`. `list`, help,
unsupported-scenario checks, and dry-runs load no analytics client and need no
database variables. Real runs preflight only the selected backend, verify their
writes, and print UI deep links plus a machine-readable JSON summary as the
last stdout line. This file is the design explainer; the command reference for
agents lives in [AGENTS.md](./AGENTS.md) and the `seed-test-data` skill.

`analytics-smoke` is backend-neutral: it writes one span through the public
OTLP endpoint, waits for Doris' durable operation when applicable, then reads
the trace and observation back through the public trace API. It never queries
ClickHouse or Doris directly. It intentionally accepts only the default seed
project because the public API request uses that project's fixed local seed
key; a different `--project` fails before ingestion. The older
shape/performance scenarios remain ClickHouse-only and fail before their
implementation is imported in Doris mode; `list` reports `target`,
`availability`, and `supportedBackends`.

`experiment-foundation` is the backend-neutral dataset-run contract fixture.
It creates one dataset and item through public APIs, writes an OTLP span with
experiment attributes, creates a public dataset-run item and associated score,
then verifies all projections through public reads. Doris runs require the
durable `datasetRunIngestion` capability to be active; before that U6 gate is
opened, the scenario fails visibly and never falls back to a direct database
write.

`evaluator-states` is the backend-neutral evaluator UI fixture. It reuses the
public OTLP smoke path, writes successful/recovered result scores through the
public score API, and verifies both through the selected analytics backend.
Only evaluator control-plane state is seeded directly in local Postgres: one
empty rule and one history rule with queued, running, success, terminal error,
delayed retry, and recovered executions. Both rules stay inactive so the
fixture never requires or calls a model provider.

`integration-states` is the backend-neutral integration-settings fixture. It
creates deterministic PostHog, Mixpanel, and Blob Storage configuration rows
for the default local project. Every integration is disabled and uses reserved
`.invalid` endpoints, so running the fixture cannot schedule or send
third-party data. The Blob Storage row selects the backend-valid export source
and includes a terminal error for status and diagnostic UI coverage.

## Why this exists

Two consumers need local seed data and both were underserved:

1. **Coding agents.** "Test the trace list with real data" used to end in
   ad-hoc ts-node scripts and Docker/ClickHouse debugging loops. Now
   `doctor` diagnoses the whole stack with a remediation command per
   failure, and the `seed-test-data` skill routes agents to a one-liner.
2. **Developers.** The default dx seed produces data the frontend shrugs
   at. These scenarios produce the shapes that actually break products:
   thousand-child fan-outs, 60-level chains, megabyte malformed payloads,
   unicode, monster sessions, 100k-trace lists.

The core design: every scenario is a plain function `(params) =>
SeedSummary` with two faces — the CLI for agents, and (future) direct
programmatic calls from the dx seed chain.

## Scenarios

| Scenario                | Backend                     | Covers                                                                                                                                                                                                                                                                                                                                          | Key flags                                                                                                             |
| ----------------------- | --------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `analytics-smoke`       | CH + Doris                  | one public OTLP trace/observation plus backend-neutral public API status/readback                                                                                                                                                                                                                                                               | common flags                                                                                                          |
| `experiment-foundation` | CH + Doris after activation | one public dataset/item, OTLP experiment trace, dataset-run item, and associated score with public readback                                                                                                                                                                                                                                     | common flags                                                                                                          |
| `evaluator-states`      | CH + activated Doris        | public trace/scores plus deterministic Postgres evaluator control state for empty, queued, running, success, terminal error, retrying, and recovered list/detail views; no model provider                                                                                                                                                       | common flags                                                                                                          |
| `integration-states`    | CH + Doris                  | deterministic disabled PostHog/Mixpanel/Blob Storage configuration pages plus a visible Blob terminal-error diagnostic; guaranteed zero third-party egress                                                                                                                                                                                      | common flags                                                                                                          |
| `trace-tree`            | CH                          | one trace with a large, branching observation tree: all ten observation kinds always present, guaranteed depth backbone, hub node with many children, errors/retries/missing end times                                                                                                                                                          | `--observations`, `--depth`, `--breadth`, `--payload-bytes`, `--payload-style json\|text\|malformed\|unicode`, `--v4` |
| `long-session`          | CH                          | one session with many traces for session-detail and virtualization work; creates the Postgres `trace_sessions` row the session page requires                                                                                                                                                                                                    | `--traces`, `--observations-per-trace`, `--payload-bytes`, `--minutes`, `--session-id`, `--v4`                        |
| `many-traces`           | CH                          | trace-list and filter performance via `numbers()` bulk SQL; parent/score/prompt/session links all resolve                                                                                                                                                                                                                                       | `--count`, `--days`, `--observations-per-trace`, `--scores-per-trace`, `--rich-payloads`                              |
| `annotation-queue`      | CH                          | two human-annotation queues for the annotate UI: a "core types" queue with one of every score-field render path (categorical toggle/combobox, boolean, ranged/decimal/unranged numeric, text) over fresh trace items, and an "edge cases" queue adding archived/stale/partial scores, comments, and observation/session/deleted/completed items | `--core-items`, `--v4` (default true)                                                                                 |

Common flags: `--project` (defaults to the seeded example project),
`--environment`, `--seed`, `--id-prefix`, `--dry-run` (instant, arithmetic
counts, writes nothing), `--json` (machine mode: pure-JSON stdout).

Scenarios compose: e.g. a session where one trace has zero observations is
two `long-session` runs sharing a `--session-id` with different
`--id-prefix` values.

## The contract (additive-only)

Scenario names, flag names, JSON summary keys, and exit-code semantics are a
public contract for agents and scripts — evolve them additively, never
rename or remove.

- The last stdout line is a JSON summary: `target`, `traceIds`, `sessionIds`,
  `counts`, `verified` (selected-backend readbacks; legacy ClickHouse
  scenarios retain exact `uniqExact` checks), `links`, `durationMs`.
- Every error prints `error:` and `fix:` lines, never a stack trace —
  including a missing selected-backend variable. The lightweight bootstrap
  and registry do not import `src/server`; preflight reports missing values
  before loading a scenario implementation.
- Determinism: same `--seed` and flags produce byte-identical data. Ids
  never contain dates; timestamps anchor to the current UTC day, so
  same-day re-runs overwrite in place and later-day re-runs re-anchor the
  same ids. Independent copies come only from `--id-prefix`.

## Data integrity guarantees

Seeded data behaves like production data:

- parents start before and end after their children (waterfall containment,
  in scenarios and in the bulk SQL)
- `completion_start_time` (TTFT) falls inside the generation's duration
- scores reference observations and sessions of their own trace; each score
  name maps to exactly one data_type; BOOLEAN string values are
  `True`/`False` (production casing)
- generations link to real Postgres prompts (the trace-detail prompt badge
  resolves) or carry NULLs — never fabricated ids
- session/user pools are `--id-prefix`-scoped, with their `trace_sessions`
  rows created

## ClickHouse determinism rules (the hard-won part)

ReplacingMergeTree dedups by the full ORDER BY tuple, so **any value that
lands in an ORDER BY key must not come from the sequential rng stream or
the wall clock** — otherwise re-runs silently duplicate rows and
`uniqExact` readbacks cannot see it. Concretely:

- time anchors come from `utcDayStartMs()` (UTC midnight, computed in TS —
  ClickHouse's `today()` is server-timezone)
- per-row variation comes from the stateless `jitter(seed, index, max)`
  (scenarios) or salted `xxHash32(number)` columns (bulk SQL); wrap hash
  inputs in `toUInt64` — xxHash32 hashes the binary representation, and a
  type-narrowing modulo silently changes the hash of the same value
- the sequential `Rng` stream is fine for anything NOT in an ORDER BY key
  (names, payload contents, usage numbers)

Relevant ORDER BY keys: v3 observations sort on `type`; all v3 tables sort
on `toDate(...)`-style time keys; `events_full` sorts on microsecond
`start_time`.

## v3 + v4

`--v4` mirrors every observation into `events_full` following the canonical
mapping in `clickhouse/scripts/dev-tables.sh`: one synthetic trace span per
trace (`span_id = 't-<traceId>'`, `parent_span_id = ''`) carries the
trace-level fields the v4 aggregations read, and root observations hang off
it. `events_core` fills via the materialized view. Facts that matter:

- `events_full` has no `id` column; `span_id` is the row identifier
- the v4 read path is the per-user "Fast (Preview)" sidebar toggle or
  `LANGFUSE_MIGRATION_V4_WRITE_MODE=events_only` server-side; the trace URL
  is identical in both modes
- `many-traces` is deliberately v3-only — its traces correctly show "not
  found" in events-only mode

## Relationship to `pnpm run dx`

The dx flow is unchanged: the CLI is additive and no presets are wired in.
The shared bulk builders that dx's `ch:seed` step uses received the same
integrity fixes (resolvable parents/prompts/scores, deterministic re-runs),
so dx-seeded bulk data is strictly better-shaped than before, at the same
cost.

## Layout

- `cli.ts` — env-independent bootstrap; `cli-main.ts` — the actual CLI
- `doctor.ts` — backend-aware checks. ClickHouse gets direct table/memory
  checks; both modes verify the running web's backend identity through an
  unauthenticated, empty, non-writing public API probe. Doris is otherwise
  checked through web readiness, so the CLI never constructs a Doris or
  ClickHouse client for the other backend
- `scenarios/` — one file per scenario plus `rng.ts` (Rng/jitter/anchor),
  `payload.ts`, `event-mirror.ts` (v3→v4 mapping), `verify.ts` (readbacks)
- `seed-postgres.ts`, `seed-clickhouse.ts`, `utils/` — the pre-existing dx
  seed path (the bulk builders in `utils/clickhouse-builder.ts` are shared)

## What's next (deliberately not built yet)

- **Canonical ports for legacy scenarios**: move the richer shape scenarios
  from ClickHouse-specific builders/readbacks to public/canonical ingestion
  and backend-neutral repositories before marking them Doris-available.
- **dx presets**: a `scenarios/presets.ts` invoked from the examples seed,
  selected via `LANGFUSE_SEED_PRESET`, to give default dx data more variety
  without flags or interactivity.
- More scenarios as needs surface: score zoo (blocked on the insert
  schema's non-nullable `value` for TEXT scores), annotation queue items,
  dataset experiment loops, media edge cases, deliberate orphan shapes
  behind explicit flags.

The original design discussion (registry/profiles/budgets alternatives,
bug-history research) lives in git history: `4b77c8ef7` (first RFC draft)
and this file's own history as `seeder-2-0-rfc.md`.
