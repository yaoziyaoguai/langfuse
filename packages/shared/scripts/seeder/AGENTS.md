# Seeder — Agent Guide

If you need local test data (traces, observation trees, sessions, bulk rows,
v4 events), use the seed CLI. Do not write ad-hoc ts-node scripts or raw
ClickHouse inserts — the CLI handles env loading, preflight, batching,
verification, and deep links.

```bash
pnpm run seed -- doctor        # check the stack; prints the fix per failure
pnpm run seed -- list          # scenarios and flags (--json for machines)
pnpm run seed -- analytics-smoke  # public OTLP write/readback on ClickHouse or Doris
pnpm run seed -- experiment-foundation  # public dataset/run/experiment/score round trip (CH or activated Doris)
pnpm run seed -- evaluator-states  # evaluator list/detail states (CH or activated Doris)
pnpm run seed -- trace-tree --observations 5000 --breadth 500 --v4
pnpm run seed -- trace-tree --observations 12 --plain --v4  # SPAN/GENERATION/EVENT only (collapsed-by-default graph panel)
pnpm run seed -- deep-chain --v4  # 1401 sequential generations in ONE parent chain (depth = count; LFE-10959 layout stress)
pnpm run seed -- agent-timeline --turns 6 --v4  # realistic agent flow-with-loop over a timeline (graph view)
pnpm run seed -- support-agent --v4 --id-prefix <hex>  # demo-grade handcrafted support-copilot run (videos/screenshots)
pnpm run seed -- long-session --traces 300 --observations-per-trace 8
pnpm run seed -- session-shapes --shape all        # chat / coding-agent / mixed v4 sessions
pnpm run seed -- many-traces --count 100000 --days 14
pnpm run seed -- scored-traces --traces 24 --v4   # scores w/ spaces in the name
```

The CLI follows `LANGFUSE_ANALYTICS_BACKEND=clickhouse|doris`. The last stdout
line of a run is a JSON summary with `target`, `traceIds`, `sessionIds`,
`counts`, `verified` (selected-backend readback), and `links` (UI deep links).
`--dry-run` predicts counts without writing; `--json` suppresses progress
output. `list` includes each scenario's selected `target`, `availability`, and
`supportedBackends`. `analytics-smoke` uses the default seed project's local
fixture key and rejects another `--project` before ingestion. Full usage
and the need→command table live in the `seed-test-data` skill
(`.agents/skills/seed-test-data/SKILL.md`).

`experiment-foundation` also uses only public/canonical surfaces. In Doris
mode it requires the durable `datasetRunIngestion` capability to be active;
before U6 activation, its expected result is a fail-visible 501 rather than a
direct database fallback.

`evaluator-states` writes its trace and result scores through public analytics
APIs on either backend, then adds deterministic evaluator templates, inactive
rules, and execution-history states in local Postgres. Keeping the rules
inactive prevents the fixture from calling a model provider; no raw
ClickHouse/Doris insert or model credential is used.

## Layout

- `cli.ts` — entry point (`pnpm run seed`, i.e. shared `seed:scenario`)
- `doctor.ts` — stack checks with remediation commands; scenarios run a fast
  backend-isolated preflight subset before writing
- `scenarios/` — one file per scenario plus shared `rng.ts`, `payload.ts`,
  `event-mirror.ts` (v3 observation → v4 `events_full` row), `verify.ts`
- `seed-postgres.ts`, `seed-clickhouse.ts`, `utils/` — the pre-existing
  `pnpm run dx` seed path (unchanged by the CLI)
- `README.md` — design rationale, contract, and roadmap

## Rules for changes

- Scenario names, flag names, and JSON summary keys are a public contract for
  agents and scripts: evolve additively, never rename or remove.
- Scenarios must be deterministic: take randomness from `Rng` (seeded via
  `--seed`), derive ids from `--id-prefix`, and never call `Math.random`.
- Any value that lands in a ClickHouse ORDER BY key (timestamps; observation
  `type` on v3; `start_time` on events) must NOT come from the sequential
  rng stream or wall clock: use `utcDayStartMs()` for time anchors and the
  stateless `jitter(seed, index, max)` for per-row variation. Stream-position
  randomness re-keys rows whenever an unrelated flag (e.g. payload size)
  changes how much rng earlier code consumed, silently duplicating rows on
  re-run; `uniqExact` readbacks cannot see it.
- Every available scenario verifies its writes through the selected backend
  and fails loudly on mismatch. Existing shape/performance scenarios are
  explicitly ClickHouse-only; Doris must fail before loading their modules.
- `list`, help, unsupported scenarios, and dry-runs must not construct an
  analytics client. Doctor/preflight may only probe the selected backend and
  must verify that the running web process uses that same backend.
- New scenarios: add `scenarios/<name>.ts`, register in `scenarios/index.ts`,
  declare `supportedBackends`, update the skill and this file, and run the
  seeder unit tests plus `pnpm exec eslint scripts/seeder --fix` and
  `pnpm run typecheck` in `packages/shared`.
- No customer data, no secrets, no fixtures that require model provider keys.
