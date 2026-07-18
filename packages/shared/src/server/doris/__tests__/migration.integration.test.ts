// U2 Doris migration runner integration test (real Doris).
//
// Verifies the forward-only, checksummed, idempotent migration contract against
// the pinned real Doris target: fresh DB applies the baseline, re-running is a
// no-op, and checksum drift is rejected.

import { beforeAll, describe, expect, it } from "vitest";
import { createConnection } from "mysql2/promise";
import {
  runMigrations,
  splitSqlStatements,
} from "../../../../doris/scripts/migrate";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";

import { DorisClient, type DorisQueryExecutor } from "../client";
import { checkDorisReadiness, EXPECTED_DORIS_MIGRATIONS } from "../readiness";

const ENABLED = process.env.DORIS_POC_ENABLED === "1";
const MIGRATION_FILE = path.resolve(
  __dirname,
  "../../../../doris/migrations/0001_baseline.sql",
);

describe.skipIf(!ENABLED)("Doris migration runner", () => {
  const cfg = {
    host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
    port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
    user: process.env.DORIS_POC_USER ?? "root",
    password: process.env.DORIS_POC_PASSWORD ?? "",
    database: process.env.DORIS_POC_DATABASE ?? "langfuse_poc",
  };

  // Admin connection (no default DB) to drop/create the PoC database fresh.
  beforeAll(async () => {
    const admin = await createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
    });
    await admin.query(`DROP DATABASE IF EXISTS ${cfg.database}`);
    await admin.query(`CREATE DATABASE ${cfg.database}`);
    await admin.end();
  });

  it("applies the baseline migration to a fresh database", async () => {
    const result = await runMigrations(cfg);
    expect(result.applied).toEqual(
      EXPECTED_DORIS_MIGRATIONS.map(({ name }) => name),
    );
    expect(result.skipped).toEqual([]);

    const conn = await createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: cfg.database,
    });
    const [tables] = (await conn.query(
      `SHOW TABLES LIKE '%current%'`,
    )) as unknown as [unknown[], unknown];
    expect((tables as unknown[]).length).toBeGreaterThanOrEqual(2); // events_current, scores_current
    const [ver] = (await conn.query(
      `SELECT COUNT(*) AS c FROM _langfuse_schema_migrations WHERE name = ?`,
      ["0001_baseline.sql"],
    )) as unknown as [{ c: number }[], unknown];
    expect(ver[0].c).toBe(1);

    const executor: DorisQueryExecutor = {
      query: (async (sql: string, params: readonly unknown[] = []) => {
        const [rows] = await conn.query(sql, [...params]);
        return rows as readonly object[];
      }) as DorisQueryExecutor["query"],
    };
    await expect(checkDorisReadiness(executor)).resolves.toEqual({
      ready: true,
      code: "READY",
      schemaVersion: EXPECTED_DORIS_MIGRATIONS.length,
    });
    await conn.end();
  }, 120_000);

  it("repairs a missing expand-migration ledger row without repeating its DDL", async () => {
    const conn = await createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: cfg.database,
    });
    await conn.query(`DELETE FROM _langfuse_schema_migrations WHERE name = ?`, [
      "0003_expand_events_status_message.sql",
    ]);
    await conn.end();

    const result = await runMigrations(cfg);
    expect(result.applied).toEqual(["0003_expand_events_status_message.sql"]);

    const verify = await createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: cfg.database,
    });
    const [columns] = (await verify.query(
      `SELECT column_name FROM information_schema.columns
       WHERE table_schema = ? AND table_name = ? AND column_name = ?`,
      [cfg.database, "events_current", "status_message"],
    )) as unknown as [unknown[], unknown];
    expect(columns).toHaveLength(1);
    await verify.end();
  });

  it("is idempotent: re-running is a no-op", async () => {
    const result = await runMigrations(cfg);
    expect(result.applied).toEqual([]);
    expect(result.skipped).toEqual(
      EXPECTED_DORIS_MIGRATIONS.map(({ name }) => name),
    );
  });

  it("creates an arbitrary historical source-date partition on demand", async () => {
    const conn = await createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: cfg.database,
    });
    await conn.query(`
      INSERT INTO events_current (
        project_id, partition_date, trace_id, span_id, version_token,
        \`type\`, environment, start_time, created_at, updated_at,
        \`source\`, ingestion_sdk_name, ingestion_sdk_version
      ) VALUES (
        'historical-project', '2001-01-02', 'historical-trace', 'historical-span', 1,
        'span', 'default', '2001-01-02 03:04:05.000000',
        '2001-01-02 03:04:05.000000', '2001-01-02 03:04:05.000000',
        'api', 'integration-test', '1.0.0'
      )
    `);
    const [rows] = (await conn.query(
      "SELECT partition_date FROM events_current WHERE project_id = ?",
      ["historical-project"],
    )) as unknown as [{ partition_date: Date }[], unknown];
    expect(rows).toHaveLength(1);
    expect(rows[0]?.partition_date).toBeDefined();
    await conn.end();
  });

  it("binds values through the production query client", async () => {
    const client = new DorisClient({
      ...cfg,
      tls: false,
      maxConnections: 2,
      connectTimeoutMs: 10_000,
      queryTimeoutMs: 30_000,
    });

    await expect(
      client.query<{ boundValue: string }>("SELECT ? AS boundValue", [
        "value'; DROP TABLE events_current; --",
      ]),
    ).resolves.toEqual([
      { boundValue: "value'; DROP TABLE events_current; --" },
    ]);
    await client.close();
  });

  it("rejects checksum drift on an already-applied migration", async () => {
    const conn = await createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: cfg.database,
    });
    await conn.query(
      `UPDATE _langfuse_schema_migrations SET checksum = ? WHERE name = ?`,
      ["deadbeef".repeat(8), "0001_baseline.sql"],
    );
    await conn.end();
    await expect(runMigrations(cfg)).rejects.toThrow(/drift/i);

    // Restore the correct checksum so the suite leaves a clean state.
    const correct = createHash("sha256")
      .update(readFileSync(MIGRATION_FILE, "utf8"))
      .digest("hex");
    const conn2 = await createConnection({
      host: cfg.host,
      port: cfg.port,
      user: cfg.user,
      password: cfg.password,
      database: cfg.database,
    });
    await conn2.query(
      `UPDATE _langfuse_schema_migrations SET checksum = ? WHERE name = ?`,
      [correct, "0001_baseline.sql"],
    );
    await conn2.end();
  });

  it("splitSqlStatements respects single-quoted strings (no split inside COMMENT)", () => {
    const stmts = splitSqlStatements(readFileSync(MIGRATION_FILE, "utf8"));
    for (const s of stmts) {
      expect(s.startsWith("CREATE TABLE")).toBe(true);
    }
  });
});
