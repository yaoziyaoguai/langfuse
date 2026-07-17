// U2 Doris production migration runner (one-shot migrator workload).
//
// Applies the forward-only, ordered, checksummed Doris migrations under
// packages/shared/doris/migrations/*.sql. Idempotent: re-running is a no-op
// once a migration is recorded. Rejects drift: if an already-applied migration's
// checksum no longer matches the file, it fails rather than silently diverging.
//
// Run: `pnpm --filter @langfuse/shared run doris:migrate` (CLI), or import
// `runMigrations` from tests. This is a one-shot workload; the migrator identity
// is distinct from web/worker query/load identities (see the plan Security
// Matrix) and must not be mounted into web/worker images.

import { createHash } from "node:crypto";
import {
  createConnection,
  type Connection,
  type ConnectionOptions,
} from "mysql2/promise";
import { readFileSync, readdirSync } from "node:fs";
import path from "node:path";

// The package compiles to CommonJS; __dirname/__filename are available directly
// and tsx polyfills them when run as a script.
const MIGRATIONS_DIR = path.resolve(__dirname, "../migrations");
const VERSION_TABLE = "_langfuse_schema_migrations";

export interface MigrationConfig {
  readonly host: string;
  readonly port: number;
  readonly user: string;
  readonly password?: string;
  readonly database: string;
}

export function migrationConfigFromEnv(): MigrationConfig {
  // A full mysql:// URL takes precedence; otherwise individual Doris vars.
  const url = process.env.DORIS_MYSQL_URL;
  if (url) {
    const m = /^mysql:\/\/([^:]+):([^@]*)@([^:]+):(\d+)\/(\S+)$/.exec(url);
    if (m) {
      return {
        host: m[3],
        port: Number(m[4]),
        user: m[1],
        password: decodeURIComponent(m[2]),
        database: m[5],
      };
    }
  }
  return {
    host: process.env.DORIS_FE_HOST ?? "127.0.0.1",
    port: Number(process.env.DORIS_FE_MYSQL_PORT ?? "9030"),
    user: process.env.DORIS_MIGRATOR_USER ?? process.env.DORIS_USER ?? "root",
    password:
      process.env.DORIS_MIGRATOR_PASSWORD ?? process.env.DORIS_PASSWORD ?? "",
    database: process.env.DORIS_DATABASE ?? "langfuse",
  };
}

interface MigrationFile {
  readonly name: string;
  readonly sql: string;
  readonly checksum: string;
}

function loadMigrations(): readonly MigrationFile[] {
  const files = readdirSync(MIGRATIONS_DIR)
    .filter((f) => f.endsWith(".sql"))
    .sort();
  return files.map((name) => {
    const sql = readFileSync(path.join(MIGRATIONS_DIR, name), "utf8");
    return {
      name,
      sql,
      checksum: createHash("sha256").update(sql).digest("hex"),
    };
  });
}

/** Split a SQL file into statements, respecting single-quoted string literals. */
export function splitSqlStatements(sql: string): readonly string[] {
  const withoutLineComments = sql
    .split("\n")
    .filter((line) => !line.trim().startsWith("--"))
    .join("\n");
  const statements: string[] = [];
  let current = "";
  let inSingleQuote = false;
  for (let i = 0; i < withoutLineComments.length; i++) {
    const ch = withoutLineComments[i];
    if (ch === "'") {
      const next = withoutLineComments[i + 1];
      if (inSingleQuote && next === "'") {
        current += "''";
        i++;
        continue;
      }
      inSingleQuote = !inSingleQuote;
      current += ch;
      continue;
    }
    if (ch === ";" && !inSingleQuote) {
      const trimmed = current.trim();
      if (trimmed.length > 0) statements.push(trimmed);
      current = "";
      continue;
    }
    current += ch;
  }
  const tail = current.trim();
  if (tail.length > 0) statements.push(tail);
  return statements;
}

async function ensureVersionTable(conn: Connection): Promise<void> {
  await conn.query(`
    CREATE TABLE IF NOT EXISTS ${VERSION_TABLE} (
      name VARCHAR(255) NOT NULL,
      checksum CHAR(64) NOT NULL,
      applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    UNIQUE KEY (name)
    DISTRIBUTED BY HASH(name) BUCKETS 1
    PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
  `);
}

export interface MigrationResult {
  readonly applied: readonly string[];
  readonly skipped: readonly string[];
}

/**
 * Apply all pending Doris migrations in order. Idempotent and drift-rejecting.
 */
export async function runMigrations(
  config: MigrationConfig,
): Promise<MigrationResult> {
  const conn = await createConnection({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password ?? "",
    database: config.database,
    multipleStatements: false,
  } as ConnectionOptions);
  try {
    await ensureVersionTable(conn);
    const migrations = loadMigrations();
    const applied: string[] = [];
    const skipped: string[] = [];
    for (const mig of migrations) {
      const [rows] = (await conn.query(
        `SELECT checksum FROM ${VERSION_TABLE} WHERE name = ?`,
        [mig.name],
      )) as unknown as [{ checksum?: string }[], unknown];
      const recorded = rows[0]?.checksum;
      if (recorded) {
        if (recorded !== mig.checksum) {
          throw new Error(
            `Doris migration drift: '${mig.name}' recorded checksum ${recorded} != file ${mig.checksum}`,
          );
        }
        skipped.push(mig.name);
        continue;
      }
      for (const stmt of splitSqlStatements(mig.sql)) {
        await conn.query(stmt);
      }
      await conn.query(
        `INSERT INTO ${VERSION_TABLE} (name, checksum) VALUES (?, ?)`,
        [mig.name, mig.checksum],
      );
      applied.push(mig.name);
    }
    return { applied, skipped };
  } finally {
    await conn.end();
  }
}

// CLI entrypoint.
async function main(): Promise<void> {
  const result = await runMigrations(migrationConfigFromEnv());
  console.log(
    `Doris migrations: applied ${result.applied.length} (${result.applied.join(", ") || "none"}), skipped ${result.skipped.length} already applied`,
  );
}

const isMain =
  typeof __filename !== "undefined" &&
  process.argv[1] !== undefined &&
  path.resolve(process.argv[1]) === __filename;
if (isMain) {
  main().catch((err) => {
    console.error("doris:migrate failed:", err);
    process.exit(1);
  });
}
