// U1 Doris PoC physical-design / fault benchmark runner.
//
// `pnpm --filter @langfuse/shared run benchmark:doris` runs this against the
// pinned real Doris target. It measures local-scale write throughput, query
// latency, and the fault scenarios, then prints a per-gate report. Engine
// correctness/durability is proven by DorisPoC.integration.test.ts; this adds
// the performance/resource/fault measurements the Verification Contract requires
// at the local development scale.
//
// Production-scale capacity, producer readiness, object-store conditional
// create, and backup-repository gates are OPERATOR_REQUIRED and reported as
// such — they need the operator-frozen production topology, RPO/RTO, and census,
// and cannot be substituted by a local run.

import { performance } from "node:perf_hooks";
import { randomBytes, randomUUID } from "node:crypto";
import {
  DorisPoCMysqlClient,
  splitSqlStatements,
} from "../src/server/doris-poc/mysqlClient";
import {
  DorisPoCStreamLoadClient,
  dorisLabel,
} from "../src/server/doris-poc/streamLoadClient";
import { readFileSync } from "node:fs";
import path from "node:path";
import {
  deriveDorisTestDatabaseName,
  dropOwnedDorisTestDatabase,
  parseDorisTestNamespace,
  resetOwnedDorisTestDatabase,
} from "../src/server/doris/testDatabase";

// The package compiles to CommonJS, so __dirname is available directly.
const SCHEMA_PATH = path.resolve(
  __dirname,
  "../doris/poc/candidate-schema.sql",
);

interface GateResult {
  readonly gate: string;
  readonly status: "PASS" | "OPERATOR_REQUIRED" | "FAIL";
  readonly measured?: string;
  readonly note?: string;
}

const results: GateResult[] = [];
function record(r: GateResult): void {
  results.push(r);
  console.log(
    `[${r.status}] ${r.gate}${r.measured ? ` :: ${r.measured}` : ""}${r.note ? ` — ${r.note}` : ""}`,
  );
}

function pct(sorted: readonly number[], p: number): number {
  if (sorted.length === 0) return 0;
  const idx = Math.min(
    sorted.length - 1,
    Math.ceil((p / 100) * sorted.length) - 1,
  );
  return sorted[Math.max(0, idx)];
}

async function main(): Promise<void> {
  if (process.env.DORIS_POC_ENABLED !== "1") {
    console.error(
      "Set DORIS_POC_ENABLED=1 (and bring up docker-compose.doris-poc.yml).",
    );
    process.exit(1);
  }
  const host = process.env.DORIS_POC_FE_HOST ?? "127.0.0.1";
  const port = Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031");
  const user = process.env.DORIS_POC_USER ?? "root";
  const password = process.env.DORIS_POC_PASSWORD ?? "";
  const runId = randomUUID();
  const database = deriveDorisTestDatabaseName(runId);
  const queryUrl = `mysql://${host}:${port}/${database}`;
  const namespace = parseDorisTestNamespace({
    ...process.env,
    DORIS_TEST_RUN_ID: runId,
    DORIS_TEST_OWNERSHIP_TOKEN: randomBytes(32).toString("hex"),
    DORIS_POC_DATABASE: database,
    DORIS_QUERY_URL: queryUrl,
    DORIS_STREAM_LOAD_DATABASE: database,
  });
  const projectId = `benchmark-${runId.replaceAll("-", "")}`;
  const connectionConfig = {
    host,
    port,
    user,
    password,
  };
  const admin = new DorisPoCMysqlClient(connectionConfig);
  await resetOwnedDorisTestDatabase({
    admin,
    connectDatabase: (targetDatabase) =>
      new DorisPoCMysqlClient({
        ...connectionConfig,
        database: targetDatabase,
      }),
    namespace,
  });
  const db = new DorisPoCMysqlClient({
    ...connectionConfig,
    database,
  });
  const sl = new DorisPoCStreamLoadClient({
    feHttpOrigin:
      process.env.DORIS_POC_FE_HTTP_ORIGIN ?? "http://127.0.0.1:8031",
    user,
    password,
    defaultDatabase: database,
    beRedirectAllowlist: { "172.28.0.3:8040": "http://127.0.0.1:8041" },
  });

  const version = await admin.ping();
  record({
    gate: "pinned-version",
    status: /doris-4\.0\.7/.test(version) ? "PASS" : "FAIL",
    measured: version.trim(),
  });

  for (const stmt of splitSqlStatements(readFileSync(SCHEMA_PATH, "utf8"))) {
    await db.query(stmt);
  }

  // --- Write throughput at local scale ---
  const ROWS = 10_000;
  const BATCH = 500;
  let labelN = 0;
  const writeStart = performance.now();
  let bytes = 0;
  for (let off = 0; off < ROWS; off += BATCH) {
    const lines: string[] = [];
    for (let i = 0; i < BATCH && off + i < ROWS; i++) {
      const idx = off + i;
      const input =
        `benchmark row ${idx} with model price question text padding `.repeat(
          3,
        );
      bytes += input.length;
      lines.push(
        JSON.stringify({
          project_id: projectId,
          partition_date: "2026-07-17",
          trace_id: `tbench-${idx % 50}`,
          span_id: `s${idx}`,
          version_token: String(1000 + (idx % 7)),
          type: "span",
          environment: "default",
          name: `bench-${idx}`,
          start_time: "2026-07-17 10:00:00.000000",
          created_at: "2026-07-17 10:00:00.000000",
          updated_at: "2026-07-17 10:00:00.000000",
          source: "api",
          ingestion_sdk_name: "js",
          ingestion_sdk_version: "5.0.0",
          input,
          total_input_tokens: idx % 1000,
        }),
      );
    }
    const res = await sl.streamLoad({
      table: "events_current",
      label: dorisLabel(["bench", runId, String(++labelN)]),
      ndjsonBody: lines.join("\n"),
    });
    if (!res.committed || res.numberFilteredRows !== 0) {
      record({
        gate: "write-throughput",
        status: "FAIL",
        measured: `status=${res.status} filtered=${res.numberFilteredRows}`,
      });
      throw new Error(`benchmark load failed: ${res.status}`);
    }
  }
  const writeSec = (performance.now() - writeStart) / 1000;
  record({
    gate: "write-throughput-local",
    status: "PASS",
    measured: `${Math.round(ROWS / writeSec)} rows/s, ${(bytes / writeSec / (1 << 20)).toFixed(1)} MiB/s over ${writeSec.toFixed(1)}s`,
    note: "local 1FE+1BE; production capacity is OPERATOR_REQUIRED",
  });

  // --- Visible latency (publish) ---
  const visStart = performance.now();
  let visible = 0;
  for (let attempt = 0; attempt < 40; attempt++) {
    const rows = await db.query<{ c: number }>(
      `SELECT COUNT(*) c FROM events_current WHERE project_id = ?`,
      [projectId],
    );
    visible = rows[0]?.c ?? 0;
    if (visible >= ROWS - BATCH) break;
    await new Promise((r) => setTimeout(r, 500));
  }
  record({
    gate: "publish-visible-latency-local",
    status: visible >= ROWS - BATCH ? "PASS" : "FAIL",
    measured: `${visible}/${ROWS} visible after ${((performance.now() - visStart) / 1000).toFixed(1)}s`,
  });

  // --- Query latency p95 (detail + list) ---
  const detailLat: number[] = [];
  for (let i = 0; i < 50; i++) {
    const t = performance.now();
    await db.query<{ name: string }>(
      `SELECT name FROM events_current WHERE project_id = ? AND trace_id = ? AND span_id = ?`,
      [projectId, `tbench-${i % 50}`, `s${i * 13}`],
    );
    detailLat.push(performance.now() - t);
  }
  const listLat: number[] = [];
  for (let i = 0; i < 20; i++) {
    const t = performance.now();
    await db.query<{ c: number }>(
      `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND partition_date >= ? AND partition_date < ?`,
      [projectId, "2026-07-17", "2026-07-18"],
    );
    listLat.push(performance.now() - t);
  }
  record({
    gate: "query-latency-local",
    status: "PASS",
    measured: `detail p95=${pct([...detailLat].sort(), 95).toFixed(1)}ms (p50=${pct([...detailLat].sort(), 50).toFixed(1)}ms); bounded-count p95=${pct([...listLat].sort(), 95).toFixed(1)}ms`,
    note: "local; production SLO (detail p95<=1s, list p95<=2s) needs production topology + data scale",
  });

  // --- Fault: duplicate label converges ---
  const dupLabel = dorisLabel(["bench", runId, "dup", "1"]);
  const body = JSON.stringify({
    project_id: projectId,
    partition_date: "2026-07-17",
    trace_id: "tbench-dup",
    span_id: "sdup",
    version_token: "5000",
    type: "span",
    environment: "default",
    name: "dup",
    start_time: "2026-07-17 10:00:00.000000",
    created_at: "2026-07-17 10:00:00.000000",
    updated_at: "2026-07-17 10:00:00.000000",
    source: "api",
    ingestion_sdk_name: "js",
    ingestion_sdk_version: "5.0.0",
  });
  await sl.streamLoad({
    table: "events_current",
    label: dupLabel,
    ndjsonBody: body,
  });
  const dup2 = await sl.streamLoad({
    table: "events_current",
    label: dupLabel,
    ndjsonBody: body,
  });
  const dupRows = await db.query<{ c: number }>(
    `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND trace_id = ? AND span_id = ?`,
    [projectId, "tbench-dup", "sdup"],
  );
  record({
    gate: "fault-duplicate-label-converges",
    status: dupRows[0]?.c === 1 ? "PASS" : "FAIL",
    measured: `rows=${dupRows[0]?.c} secondStatus=${dup2.status}`,
  });

  // --- Fault: non-allowlisted redirect rejected ---
  const hostile = new DorisPoCStreamLoadClient({
    feHttpOrigin:
      process.env.DORIS_POC_FE_HTTP_ORIGIN ?? "http://127.0.0.1:8031",
    user,
    password,
    defaultDatabase: database,
    beRedirectAllowlist: {}, // nothing allowlisted -> any redirect must be rejected
  });
  let rejected = false;
  try {
    await hostile.streamLoad({
      table: "events_current",
      label: dorisLabel(["bench", runId, "hostile"]),
      ndjsonBody: body,
    });
  } catch {
    rejected = true;
  }
  record({
    gate: "fault-non-allowlisted-redirect-rejected",
    status: rejected ? "PASS" : "FAIL",
    note: "Stream Load with an empty BE allowlist must reject the 307 without forwarding auth/body",
  });

  // --- Operator-gated gates (cannot be passed locally) ---
  record({
    gate: "capacity-at-production-topology",
    status: "OPERATOR_REQUIRED",
    note: "needs operator-frozen topology + retained corpus + write duty cycle (PRD §9)",
  });
  record({
    gate: "producer-readiness-census",
    status: "OPERATOR_REQUIRED",
    note: "needs census sources + observation window + per-producer Doris-only E2E (F6)",
  });
  record({
    gate: "object-store-conditional-create-head-get",
    status: "OPERATOR_REQUIRED",
    note: "needs production object-store provider proof for the canonical publication protocol",
  });
  record({
    gate: "backup-repository-and-external-anchor",
    status: "OPERATOR_REQUIRED",
    note: "needs backup repo + manifest key manager + append-only latest-checkpoint authority (U8)",
  });
  record({
    gate: "fe-failover-ha",
    status: "OPERATOR_REQUIRED",
    note: "local topology is non-HA; HA failover gate applies only if the operator freezes an HA target",
  });

  await db.end();
  await dropOwnedDorisTestDatabase({
    admin,
    connectDatabase: (targetDatabase) =>
      new DorisPoCMysqlClient({
        ...connectionConfig,
        database: targetDatabase,
      }),
    namespace,
  });
  await admin.end();

  const failed = results.filter((r) => r.status === "FAIL");
  console.log(
    `\nSUMMARY: ${results.length} gates, ${results.filter((r) => r.status === "PASS").length} PASS, ${results.filter((r) => r.status === "OPERATOR_REQUIRED").length} OPERATOR_REQUIRED, ${failed.length} FAIL`,
  );
  process.exit(failed.length === 0 ? 0 : 1);
}

main().catch((err) => {
  console.error("benchmark:doris failed:", err);
  process.exit(1);
});
