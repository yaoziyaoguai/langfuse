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
    expect(result.applied).toContain("0001_baseline.sql");
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
    await conn.end();
  }, 120_000);

  it("is idempotent: re-running is a no-op", async () => {
    const result = await runMigrations(cfg);
    expect(result.applied).toEqual([]);
    expect(result.skipped).toContain("0001_baseline.sql");
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
