// Test-only Doris MySQL-protocol client for the U1 PoC.
//
// U2 promotes a production version to packages/shared/src/server/doris/client.ts.
// This file exists only to prove MySQL parameter binding, DDL application, label
// reconciliation, and query semantics against the pinned real Doris target in
// the U1 integration/benchmark suites. It must not be imported by web/worker
// runtime code.

import mysql, { type Pool, type PoolOptions } from "mysql2/promise";

export interface DorisPoCMysqlConfig {
  /** FE MySQL query endpoint host (e.g. 127.0.0.1 for the published PoC port). */
  readonly host: string;
  readonly port: number;
  readonly user: string;
  readonly password?: string;
  readonly database?: string;
}

export interface DorisPoCQueryRow {
  readonly [column: string]: unknown;
}

const DEFAULT_TEST_CONFIG: DorisPoCMysqlConfig = {
  host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
  port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
  user: process.env.DORIS_POC_USER ?? "root",
  password: process.env.DORIS_POC_PASSWORD ?? "",
  database: process.env.DORIS_POC_DATABASE ?? "langfuse_poc",
};

function toPoolOptions(config: DorisPoCMysqlConfig): PoolOptions {
  return {
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password ?? "",
    database: config.database,
    // Doris MySQL protocol does not support all mysql2 negotiation flags; keep
    // the handshake minimal and deterministic for the PoC.
    connectTimeout: 10_000,
    enableKeepAlive: true,
    multipleStatements: true,
    timezone: "Z",
    // Local PoC only: TLS is terminated by the frozen private path in
    // production (see docs/operations/doris-security.md). The PoC cluster binds
    // to 127.0.0.1.
    ssl: undefined,
  };
}

/**
 * Thin mysql2/promise wrapper for the U1 Doris PoC. Parameter binding is the
 * security contract proven here: every caller value is bound, never string-
 * interpolated into SQL. See DorisPoC integration tests for the value-vs-
 * identifier injection corpus.
 */
export class DorisPoCMysqlClient {
  private readonly pool: Pool;
  private readonly owned: boolean;

  constructor(config: DorisPoCMysqlConfig = DEFAULT_TEST_CONFIG) {
    this.pool = mysql.createPool(toPoolOptions(config));
    this.owned = true;
  }

  /** Run a parameterized query and return typed rows. */
  async query<T extends DorisPoCQueryRow = DorisPoCQueryRow>(
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<readonly T[]> {
    const [rows] = await this.pool.query(sql, params as unknown[]);
    return rows as unknown as readonly T[];
  }

  /**
   * Run a statement that returns no rows (DDL/DML) with bound parameters. Uses
   * the text protocol: mysql2 escapes caller values so they are always treated
   * as values (never identifiers/fragments), and the text path is robust to
   * Doris MySQL-protocol quirks that the binary prepared-statement path trips
   * on for DDL and multi-line statements.
   */
  async execute(
    sql: string,
    params: ReadonlyArray<unknown> = [],
  ): Promise<void> {
    await this.pool.query(sql, params as unknown[]);
  }

  /** Verify the server is reachable and reports the pinned build version. */
  async ping(): Promise<string> {
    // Doris FE emulates MySQL 5.7, so VERSION() returns "5.7.99"; the real
    // build is in @@version_comment (e.g. "doris version doris-4.0.7-...").
    const rows = await this.query<{ version_comment: string }>(
      "SELECT @@version_comment AS version_comment",
    );
    return rows[0]?.version_comment ?? "";
  }

  async end(): Promise<void> {
    if (this.owned) {
      await this.pool.end();
    }
  }
}

/**
 * Split a multi-statement SQL file into individual statements for ordered,
 * repeatable application. Splits on `;` ONLY outside single-quoted string
 * literals (so a semicolon inside a COMMENT '...;...' does not break a CREATE
 * TABLE), and strips full-line `--` comments first so a `;` inside a comment
 * never spawns a fragment.
 */
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
      // A doubled '' inside a string is an escaped quote, not a terminator.
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
      if (trimmed.length > 0) {
        statements.push(trimmed);
      }
      current = "";
      continue;
    }
    current += ch;
  }
  const tail = current.trim();
  if (tail.length > 0) {
    statements.push(tail);
  }
  return statements;
}
