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

import {
  parseDorisQueryConfig,
  type DorisNodeEnv,
} from "../../src/server/doris/config";

// The package compiles to CommonJS; __dirname/__filename are available directly
// and tsx polyfills them when run as a script.
const MIGRATIONS_DIR = path.resolve(__dirname, "../migrations");
const VERSION_TABLE = "_langfuse_schema_migrations";

export interface MigrationConfig {
  readonly host: string;
  readonly port: number;
  readonly database: string;
  readonly user: string;
  readonly password: string;
  readonly tls?: boolean;
  readonly tlsCaPath?: string;
  readonly connectTimeoutMs?: number;
  readonly queryTimeoutMs?: number;
}

type MigrationEnv = Readonly<Record<string, string | undefined>>;

export function migrationConfigFromEnv(
  input: MigrationEnv = process.env,
  nodeEnv: DorisNodeEnv = input.NODE_ENV === "production"
    ? "production"
    : "development",
): MigrationConfig {
  if (nodeEnv === "production" && !input.DORIS_MIGRATION_URL) {
    throw new Error("Production Doris migrator configuration is required");
  }

  return parseDorisQueryConfig(
    {
      DORIS_QUERY_URL:
        input.DORIS_MIGRATION_URL ?? "mysql://127.0.0.1:9030/langfuse",
      DORIS_QUERY_USER: input.DORIS_MIGRATION_USER ?? "root",
      DORIS_QUERY_PASSWORD: input.DORIS_MIGRATION_PASSWORD ?? "",
      DORIS_QUERY_TLS_ENABLED: input.DORIS_MIGRATION_TLS_ENABLED ?? "false",
      DORIS_QUERY_TLS_CA_PATH: input.DORIS_MIGRATION_TLS_CA_PATH,
      DORIS_QUERY_CONNECT_TIMEOUT_MS: input.DORIS_MIGRATION_CONNECT_TIMEOUT_MS,
      DORIS_QUERY_TIMEOUT_MS:
        input.DORIS_MIGRATION_QUERY_TIMEOUT_MS ?? "120000",
      DORIS_QUERY_MAX_CONNECTIONS: "1",
    },
    nodeEnv,
  );
}

interface MigrationFile {
  readonly name: string;
  readonly sql: string;
  readonly checksum: string;
}

const ADD_COLUMN_STATEMENT =
  /^ALTER\s+TABLE\s+`?([A-Za-z0-9_]+)`?\s+ADD\s+COLUMN\s+`?([A-Za-z0-9_]+)`?/i;

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

async function ensureVersionTable(
  conn: Connection,
  timeout: number,
): Promise<void> {
  await conn.query({
    sql: `
    CREATE TABLE IF NOT EXISTS ${VERSION_TABLE} (
      name VARCHAR(255) NOT NULL,
      checksum CHAR(64) NOT NULL,
      applied_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    UNIQUE KEY (name)
    DISTRIBUTED BY HASH(name) BUCKETS 1
    PROPERTIES ("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
  `,
    timeout,
  });
}

async function schemaChangeAlreadyApplied(
  conn: Connection,
  database: string,
  statement: string,
  timeout: number,
): Promise<boolean> {
  const match = statement.match(ADD_COLUMN_STATEMENT);
  if (!match?.[1] || !match[2]) return false;

  const [rows] = (await conn.query({
    sql: `
      SELECT 1 AS present
      FROM information_schema.columns
      WHERE table_schema = ? AND table_name = ? AND column_name = ?
      LIMIT 1
    `,
    values: [database, match[1], match[2]],
    timeout,
  })) as unknown as [{ present?: number }[], unknown];
  return rows.length === 1;
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
  if (!/^[A-Za-z_][A-Za-z0-9_]*$/.test(config.database)) {
    throw new Error("Doris migration database must be a simple identifier");
  }
  const conn = await createConnection({
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    connectTimeout: config.connectTimeoutMs,
    multipleStatements: false,
    ssl: config.tls
      ? {
          rejectUnauthorized: true,
          verifyIdentity: true,
          ca: config.tlsCaPath
            ? readFileSync(config.tlsCaPath, "utf8")
            : undefined,
        }
      : undefined,
  } as ConnectionOptions);
  try {
    const queryTimeoutMs = config.queryTimeoutMs ?? 120_000;
    await conn.query({
      sql: `CREATE DATABASE IF NOT EXISTS \`${config.database}\``,
      timeout: queryTimeoutMs,
    });
    await conn.changeUser({ database: config.database });
    await ensureVersionTable(conn, queryTimeoutMs);
    const migrations = loadMigrations();
    const applied: string[] = [];
    const skipped: string[] = [];
    for (const mig of migrations) {
      const [rows] = (await conn.query({
        sql: `SELECT checksum FROM ${VERSION_TABLE} WHERE name = ?`,
        values: [mig.name],
        timeout: queryTimeoutMs,
      })) as unknown as [{ checksum?: string }[], unknown];
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
        if (
          await schemaChangeAlreadyApplied(
            conn,
            config.database,
            stmt,
            queryTimeoutMs,
          )
        ) {
          continue;
        }
        await conn.query({ sql: stmt, timeout: queryTimeoutMs });
      }
      await conn.query({
        sql: `INSERT INTO ${VERSION_TABLE} (name, checksum) VALUES (?, ?)`,
        values: [mig.name, mig.checksum],
        timeout: queryTimeoutMs,
      });
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
  main().catch(() => {
    console.error("doris:migrate failed");
    process.exit(1);
  });
}
