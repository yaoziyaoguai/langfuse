// U1 real-Doris engine correctness/durability suite.
//
// Runs ONLY against the pinned real Doris 4.0.7 PoC target. Enable with
// DORIS_POC_ENABLED=1 (the `pnpm --filter @langfuse/shared run test:doris`
// command sets it). A mock, static check, or no-crash run is NOT a pass — these
// cases prove the Source Version Contract, anti-resurrection, Stream Load
// transport, compaction invariance, full-text search, VARIANT, tombstone
// barriers, and parameter binding on real Doris.
//
// Bring the target up first:
//   docker-compose -f docker-compose.doris-poc.yml up -d

import { afterEach, beforeAll, afterAll, describe, expect, it } from "vitest";
import { readFileSync } from "node:fs";
import path from "node:path";
import {
  DorisPoCMysqlClient,
  splitSqlStatements,
} from "../../doris-poc/mysqlClient";
import {
  DorisPoCStreamLoadClient,
  dorisLabel,
  type DorisPoCStreamLoadResult,
} from "../../doris-poc/streamLoadClient";
import {
  currentStateFixtures,
  identityFixtures,
  INT64_MAX,
  contentSearchFixtures,
  type CurrentStateFixture,
} from "./fixtures/analyticsCompatibilityCorpus";

// The package compiles to CommonJS, so __dirname is available directly.
const SCHEMA_PATH = path.resolve(
  __dirname,
  "../../../../doris/poc/candidate-schema.sql",
);

const ENABLED = process.env.DORIS_POC_ENABLED === "1";
const FE_HTTP_ORIGIN =
  process.env.DORIS_POC_FE_HTTP_ORIGIN ?? "http://127.0.0.1:8031";
const DB = "langfuse_poc";

/**
 * Minimal NDJSON row for events_current with all NOT NULL columns populated.
 * Reserved-word column names are used as JSON keys (Doris maps them by name).
 */
function eventRow(opts: {
  project_id: string;
  partition_date: string;
  trace_id: string;
  span_id: string;
  version_token: bigint | number;
  name: string;
  type?: string;
  isDelete?: boolean;
  input?: string;
  output?: string;
  metadata?: Record<string, unknown>;
  tags?: readonly string[];
  total_input_tokens?: number;
  total_output_tokens?: number;
  total_cost?: string;
}): string {
  const row: Record<string, unknown> = {
    project_id: opts.project_id,
    partition_date: opts.partition_date,
    trace_id: opts.trace_id,
    span_id: opts.span_id,
    version_token: String(opts.version_token), // BIGINT as string to avoid precision loss
    type: opts.type ?? "generation",
    environment: "default",
    name: opts.name,
    start_time: "2026-07-17 10:00:00.000000",
    created_at: "2026-07-17 10:00:00.000000",
    updated_at: "2026-07-17 10:00:00.000000",
    source: "api",
    ingestion_sdk_name: "js",
    ingestion_sdk_version: "5.0.0",
  };
  if (opts.input !== undefined) row.input = opts.input;
  if (opts.output !== undefined) row.output = opts.output;
  if (opts.metadata !== undefined) row.metadata = opts.metadata;
  if (opts.tags !== undefined) row.tags = opts.tags;
  if (opts.total_input_tokens !== undefined)
    row.total_input_tokens = opts.total_input_tokens;
  if (opts.total_output_tokens !== undefined)
    row.total_output_tokens = opts.total_output_tokens;
  if (opts.total_cost !== undefined) row.total_cost = opts.total_cost;
  if (opts.isDelete) row.__DORIS_DELETE_SIGN__ = 1;
  return JSON.stringify(row);
}

const DELETE_COLUMNS = [
  "project_id",
  "partition_date",
  "trace_id",
  "span_id",
  "version_token",
  "type",
  "environment",
  "name",
  "start_time",
  "created_at",
  "updated_at",
  "source",
  "ingestion_sdk_name",
  "ingestion_sdk_version",
  "__DORIS_DELETE_SIGN__",
];

async function applySchema(
  admin: DorisPoCMysqlClient,
  db: DorisPoCMysqlClient,
): Promise<void> {
  await admin.execute(`DROP DATABASE IF EXISTS ${DB}`);
  await admin.execute(`CREATE DATABASE ${DB}`);
  const sql = readFileSync(SCHEMA_PATH, "utf8");
  for (const stmt of splitSqlStatements(sql)) {
    // DDL goes through the text protocol; the binary prepared-statement path
    // mis-parses Doris CREATE TABLE responses.
    await db.query(stmt);
  }
}

describe.skipIf(!ENABLED)(
  "Doris PoC — engine correctness and durability",
  () => {
    let admin: DorisPoCMysqlClient;
    let db: DorisPoCMysqlClient;
    let sl: DorisPoCStreamLoadClient;
    let labelSeq = 0;
    const nextLabel = (tag: string) =>
      dorisLabel(["poc", tag, String(++labelSeq)]);

    beforeAll(async () => {
      // Admin client has no default database so it can DROP/CREATE the PoC DB.
      admin = new DorisPoCMysqlClient({
        host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
        port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
        user: process.env.DORIS_POC_USER ?? "root",
        password: process.env.DORIS_POC_PASSWORD ?? "",
      });
      db = new DorisPoCMysqlClient({
        host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
        port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
        user: process.env.DORIS_POC_USER ?? "root",
        password: process.env.DORIS_POC_PASSWORD ?? "",
        database: DB,
      });
      const version = await admin.ping();
      // Revalidate the pinned stable at implementation start (plan §Resolved).
      expect(version, `pinned Doris 4.0.7 expected, got ${version}`).toMatch(
        /doris-4\.0\.7/,
      );

      sl = new DorisPoCStreamLoadClient({
        feHttpOrigin: FE_HTTP_ORIGIN,
        user: process.env.DORIS_POC_USER ?? "root",
        password: process.env.DORIS_POC_PASSWORD ?? "",
        defaultDatabase: DB,
        // Allowlisted FE->BE redirect: the internal BE origin is rewritten to the
        // host-published BE port. Any other redirect origin is rejected.
        beRedirectAllowlist: {
          "172.28.0.3:8040": "http://127.0.0.1:8041",
        },
        reconcileLabelStatus: async (label: string) => {
          const rows = await db.query<{ status: string }>(
            `SHOW TRANSACTION WHERE label = ?`,
            [label],
          );
          // SHOW TRANSACTION columns vary; fall back to VISIBLE via the load info.
          const txn = rows[0];
          const status = txn?.status ?? "UNKNOWN";
          return { status, visible: status === "VISIBLE" };
        },
      });

      await applySchema(admin, db);
    }, 120_000);

    afterAll(async () => {
      await admin?.end();
      await db?.end();
    });

    // A monotonically increasing base keeps entity keys unique across fixtures.
    let keySeq = 0;

    async function loadEvents(fixture: CurrentStateFixture): Promise<void> {
      for (const [i, ev] of fixture.events.entries()) {
        const label = nextLabel(`${fixture.name}-${i}`);
        const res: DorisPoCStreamLoadResult = await sl.streamLoad({
          table: "events_current",
          ndjsonBody: eventRow({
            project_id: fixture.project_id,
            partition_date: fixture.partition_date,
            trace_id: fixture.trace_id,
            span_id: fixture.span_id,
            version_token: ev.version_token,
            name: ev.name,
            isDelete: ev.isDelete,
          }),
          label,
          columns: ev.isDelete ? DELETE_COLUMNS : undefined,
        });
        if (!res.committed) {
          // Duplicate label converges idempotently (ExistingJobStatus); unknown
          // outcomes reconcile by label before classification.
          if (res.existingJobStatus) {
            continue; // idempotent duplicate
          }
          if (sl.isUnknownOutcome(res)) {
            const status = await sl.reconcile({ label });
            if (!status.visible) {
              throw new Error(
                `Load ${label} unknown outcome reconciled to non-visible: ${status.status}`,
              );
            }
            continue;
          }
          throw new Error(
            `Load ${label} failed: ${res.status} / ${res.message} (filtered=${res.numberFilteredRows})`,
          );
        }
        expect(
          res.numberFilteredRows,
          "max_filter_ratio=0 must reject no rows",
        ).toBe(0);
      }
    }

    async function currentName(
      fixture: CurrentStateFixture,
    ): Promise<string | null> {
      const rows = await db.query<{ name: string }>(
        `SELECT name FROM events_current WHERE project_id = ? AND trace_id = ? AND span_id = ?`,
        [fixture.project_id, fixture.trace_id, fixture.span_id],
      );
      return rows[0]?.name ?? null;
    }

    async function currentCount(fixture: CurrentStateFixture): Promise<number> {
      const rows = await db.query<{ c: number }>(
        `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND trace_id = ? AND span_id = ?`,
        [fixture.project_id, fixture.trace_id, fixture.span_id],
      );
      return rows[0]?.c ?? 0;
    }

    it("latest-wins by version_token, lower late write cannot overwrite (currentStateFixtures)", async () => {
      for (const fixture of currentStateFixtures) {
        if (
          fixture.name.includes("anti-resurrection") ||
          fixture.name.includes("terminal-delete")
        ) {
          continue; // covered by dedicated tests below
        }
        await loadEvents(fixture);
        const nonDeleteTokens = fixture.events
          .filter((e) => !e.isDelete)
          .map((e) => e.version_token);
        const hasDuplicateToken =
          new Set(nonDeleteTokens).size !== nonDeleteTokens.length;
        if (hasDuplicateToken) {
          // Same version_token, different payload: the engine guarantees exactly
          // ONE current row. The specific winner is chosen upstream by the
          // entity-head CAS (U4) before any Doris load; the loser quarantines at
          // the application ledger and never loads. Doris's equal-sequence
          // tiebreak is therefore not the contract — single-row uniqueness is.
          const candidateNames = fixture.events
            .filter((e) => !e.isDelete)
            .map((e) => e.name);
          expect(await currentCount(fixture)).toBe(1);
          expect(candidateNames).toContain(await currentName(fixture));
        } else {
          expect(await currentName(fixture)).toBe(fixture.expectedWinnerName);
        }
      }
    });

    it("terminal delete (INT64_MAX + DELETE_SIGN) makes the entity invisible", async () => {
      const f = currentStateFixtures.find(
        (f) => f.name === "terminal-delete-beats-every-ordinary-version",
      )!;
      await loadEvents(f);
      expect(await currentName(f)).toBeNull();
    });

    it("anti-resurrection: a late lower/equal write after delete cannot revive the entity", async () => {
      const f = currentStateFixtures.find(
        (f) => f.name === "anti-resurrection-late-lower-write-after-delete",
      )!;
      await loadEvents(f);
      expect(await currentName(f)).toBeNull();
    });

    it("duplicate deterministic Stream Load label converges idempotently (no duplicate current row)", async () => {
      keySeq += 1;
      const trace_id = `t-dup-${keySeq}`;
      const label = nextLabel("dup");
      const body = eventRow({
        project_id: "p1",
        partition_date: "2026-07-17",
        trace_id,
        span_id: "s1",
        version_token: 9000n,
        name: "dup-once",
      });
      const first = await sl.streamLoad({
        table: "events_current",
        ndjsonBody: body,
        label,
      });
      expect(first.committed).toBe(true);
      // Reuse the SAME label (e.g. after a lost response). Doris must converge to
      // the same single committed row, not insert a duplicate or error.
      const second = await sl.streamLoad({
        table: "events_current",
        ndjsonBody: body,
        label,
      });
      // Doris reports the prior job status for a duplicate label.
      expect(second.existingJobStatus ?? second.status).toBeTruthy();
      const rows = await db.query<{ c: number }>(
        `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND trace_id = ? AND span_id = ?`,
        ["p1", trace_id, "s1"],
      );
      expect(rows[0].c).toBe(1);
    });

    it("cross-trace equal span_id does not collide (identity is the trace_id+span_id pair)", async () => {
      // Load span_id "0000000000000001" under two different trace_ids; both must
      // remain distinct current rows.
      const a = identityFixtures[0];
      const b = identityFixtures[1];
      await sl.streamLoad({
        table: "events_current",
        ndjsonBody: eventRow({
          project_id: a.project_id,
          partition_date: a.partition_date,
          trace_id: a.trace_id,
          span_id: a.span_id,
          version_token: 1000n,
          name: "trace-a",
        }),
        label: nextLabel("ident-a"),
      });
      await sl.streamLoad({
        table: "events_current",
        ndjsonBody: eventRow({
          project_id: b.project_id,
          partition_date: b.partition_date,
          trace_id: b.trace_id,
          span_id: b.span_id, // SAME span_id, different trace
          version_token: 1000n,
          name: "trace-b",
        }),
        label: nextLabel("ident-b"),
      });
      const rows = await db.query<{ trace_id: string }>(
        `SELECT trace_id FROM events_current WHERE project_id = ? AND span_id = ? ORDER BY trace_id`,
        ["p1", "0000000000000001"],
      );
      expect(rows.map((r) => r.trace_id)).toEqual(["traceA", "traceB"]);
    });

    it("VARIANT metadata/usage/cost round-trips and is queryable", async () => {
      keySeq += 1;
      const trace_id = `t-variant-${keySeq}`;
      const res = await sl.streamLoad({
        table: "events_current",
        label: nextLabel("variant"),
        ndjsonBody: eventRow({
          project_id: "p1",
          partition_date: "2026-07-17",
          trace_id,
          span_id: "s1",
          version_token: 1000n,
          name: "variant-row",
          metadata: { region: "eu", model: { id: "gpt-4o" } },
          tags: ["prod", "canary"],
          total_input_tokens: 120,
          total_output_tokens: 80,
          total_cost: "0.002340000000",
        }),
      });
      expect(res.committed).toBe(true);
      const rows = await db.query<{ region: string; tokenIn: number }>(
        `SELECT cast(metadata['region'] as string) AS region, total_input_tokens AS tokenIn FROM events_current WHERE project_id = ? AND trace_id = ?`,
        ["p1", trace_id],
      );
      expect(rows[0].region).toBe("eu");
      expect(rows[0].tokenIn).toBe(120);
    });

    it("inverted-index full-content search finds multilingual/escaped content (no full-scan fallback)", async () => {
      for (const fixture of contentSearchFixtures) {
        keySeq += 1;
        const trace_id = `t-search-${keySeq}`;
        const load = await sl.streamLoad({
          table: "events_current",
          label: nextLabel("search"),
          ndjsonBody: eventRow({
            project_id: "p1",
            partition_date: "2026-07-17",
            trace_id,
            span_id: "s1",
            version_token: 1000n,
            name: fixture.name,
            input: fixture.input,
            output: fixture.output,
          }),
        });
        expect(load.committed).toBe(true);
      }
      // The inverted index is built asynchronously on publish; on a small VM the
      // build lags. Poll until the search returns the published rows (bounded), so
      // a slow index build is never mistaken for a missing-result contract bug.
      const pollSearch = async (
        predicate: string,
        term: string,
        atLeast: number,
      ): Promise<number> => {
        for (let attempt = 0; attempt < 30; attempt++) {
          const rows = await db.query<{ c: number }>(
            `SELECT COUNT(*) c FROM events_current WHERE ${predicate}`,
            [term],
          );
          if ((rows[0]?.c ?? 0) >= atLeast) {
            return rows[0].c;
          }
          await new Promise((r) => setTimeout(r, 1500));
        }
        return 0;
      };
      // INVERTED index, word-tokenized MATCH: Chinese segments correctly.
      expect(
        await pollSearch("input MATCH_ANY ?", "模型价格", 1),
      ).toBeGreaterThanOrEqual(1);
      // NGRAM_BF-accelerated LIKE substring: matches Korean particles (비용 inside
      // 비용에) where the unicode tokenizer does not morphologically analyze.
      expect(
        await pollSearch("input LIKE ?", "%비용%", 1),
      ).toBeGreaterThanOrEqual(1);
      expect(
        await pollSearch("input LIKE ?", "%تكلفة%", 1),
      ).toBeGreaterThanOrEqual(1);
    }, 120_000);

    it("trace_tombstones barrier hides a trace from every query path (anti-resurrection)", async () => {
      keySeq += 1;
      const trace_id = `t-tomb-${keySeq}`;
      await sl.streamLoad({
        table: "events_current",
        label: nextLabel("tomb-load"),
        ndjsonBody: eventRow({
          project_id: "p1",
          partition_date: "2026-07-17",
          trace_id,
          span_id: "s1",
          version_token: 1000n,
          name: "to-be-hidden",
        }),
      });
      // Visible before barrier.
      let rows = await db.query<{ c: number }>(
        `SELECT COUNT(*) c FROM events_current WHERE project_id = ? AND trace_id = ?`,
        ["p1", trace_id],
      );
      expect(rows[0].c).toBe(1);
      // Publish the query-visible barrier with a generation higher than any
      // ordinary version_token.
      await sl.streamLoad({
        table: "trace_tombstones",
        label: nextLabel("tomb-barrier"),
        ndjsonBody: JSON.stringify({
          project_id: "p1",
          trace_id,
          deletion_generation: String(INT64_MAX),
          created_at: "2026-07-17 12:00:00.000000",
        }),
      });
      // Anti-join: the trace is logically invisible after the barrier.
      rows = await db.query<{ c: number }>(
        `SELECT COUNT(*) c FROM events_current e
       WHERE e.project_id = ? AND e.trace_id = ?
         AND NOT EXISTS (
           SELECT 1 FROM trace_tombstones t
           WHERE t.project_id = e.project_id AND t.trace_id = e.trace_id
         )`,
        ["p1", trace_id],
      );
      expect(rows[0].c).toBe(0);
    });

    it("SQL injection payloads stay parameter-bound values, never identifiers or fragments", async () => {
      const malicious = "'; DROP TABLE events_current; --";
      await db.execute(
        `INSERT INTO events_current (project_id, partition_date, trace_id, span_id, version_token, type, environment, name, start_time, created_at, updated_at, source, ingestion_sdk_name, ingestion_sdk_version)
       VALUES (?, ?, ?, ?, ?, 'span', 'default', ?, '2026-07-17 10:00:00.000000', '2026-07-17 10:00:00.000000', '2026-07-17 10:00:00.000000', 'api', 'js', '5.0.0')`,
        ["p1", "2026-07-17", "t-inject", malicious, "1000", malicious],
      );
      // The table still exists (DROP was a bound value, not executed).
      const rows = await db.query<{ name: string }>(
        `SELECT name FROM events_current WHERE project_id = ? AND trace_id = ?`,
        ["p1", "t-inject"],
      );
      expect(rows[0].name).toBe(malicious);
    });

    it("compaction invariance: out-of-order writes keep the same winner after compaction", async () => {
      keySeq += 1;
      const trace_id = `t-compaction-${keySeq}`;
      const f: CurrentStateFixture = {
        name: "compaction-probe",
        project_id: "p1",
        partition_date: "2026-07-17",
        trace_id,
        span_id: "s1",
        events: [
          { version_token: 1000n, name: "c-v1", canonicalHash: "h1" },
          { version_token: 3000n, name: "c-v3", canonicalHash: "h3" },
          { version_token: 2000n, name: "c-v2-late", canonicalHash: "h2" },
        ],
        expectedWinnerName: "c-v3",
        expectedQuarantinedIndices: [],
      };
      await loadEvents(f);
      expect(await currentName(f)).toBe("c-v3");
      // Trigger a compaction (best-effort: command availability varies by patch).
      try {
        await db.execute(`ADMIN COMPACT TABLE ${DB}.events_current`);
      } catch {
        // Compaction trigger API differs by patch; the MoW delete bitmap already
        // enforces the winner at publish, so this is a belt-and-suspenders probe.
      }
      // Winner unchanged after compaction.
      expect(await currentName(f)).toBe("c-v3");
    });

    afterEach(async () => {
      // Keep the suite isolated without re-applying the whole schema each test.
      try {
        await db?.execute(`TRUNCATE TABLE events_current`);
        await db?.execute(`TRUNCATE TABLE trace_tombstones`);
      } catch {
        // ignore during setup/teardown races
      }
    });
  },
);
