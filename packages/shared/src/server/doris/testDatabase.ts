import { createHash } from "node:crypto";

const TEST_RUN_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const OWNERSHIP_TOKEN_PATTERN = /^[0-9a-f]{64}$/;
const SIMPLE_IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]*$/;
const OWNERSHIP_TABLE = "_langfuse_test_ownership";
const FORBIDDEN_DATABASES = new Set([
  "default",
  "information_schema",
  "langfuse",
  "langfuse_poc",
  "mysql",
]);

type DorisTestEnv = Readonly<Record<string, string | undefined>>;

export interface DorisTestNamespace {
  readonly runId: string;
  readonly database: string;
  readonly ownershipTokenHash: string;
  readonly queryUrl: string;
}

export interface DorisTestDatabaseExecutor {
  query<T extends object = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
  ): Promise<readonly T[]>;
  execute(sql: string, params?: readonly unknown[]): Promise<void>;
  close?(): Promise<void>;
  end?(): Promise<void>;
}

export function deriveDorisTestDatabaseName(runId: string): string {
  if (!TEST_RUN_ID_PATTERN.test(runId)) {
    throw new Error("DORIS_TEST_RUN_ID must be a lowercase UUID v4");
  }
  return `langfuse_test_${runId.replaceAll("-", "")}`;
}

export function parseDorisTestNamespace(
  env: DorisTestEnv = process.env,
): DorisTestNamespace {
  const runId = env.DORIS_TEST_RUN_ID;
  if (!runId) throw new Error("DORIS_TEST_RUN_ID is required");
  const database = env.DORIS_POC_DATABASE;
  if (!database) throw new Error("DORIS_POC_DATABASE is required");
  const ownershipToken = env.DORIS_TEST_OWNERSHIP_TOKEN;
  if (!ownershipToken) {
    throw new Error("DORIS_TEST_OWNERSHIP_TOKEN is required");
  }
  const queryUrl = env.DORIS_QUERY_URL;
  if (!queryUrl) throw new Error("DORIS_QUERY_URL is required");
  const streamLoadDatabase = env.DORIS_STREAM_LOAD_DATABASE;
  if (!streamLoadDatabase) {
    throw new Error("DORIS_STREAM_LOAD_DATABASE is required");
  }

  const expectedDatabase = deriveDorisTestDatabaseName(runId);
  if (
    FORBIDDEN_DATABASES.has(database.toLowerCase()) ||
    !SIMPLE_IDENTIFIER_PATTERN.test(database) ||
    database !== expectedDatabase
  ) {
    throw new Error(
      "Unsafe Doris test database; expected the derived run namespace",
    );
  }
  if (!OWNERSHIP_TOKEN_PATTERN.test(ownershipToken)) {
    throw new Error(
      "DORIS_TEST_OWNERSHIP_TOKEN must be a 32-byte lowercase hex token",
    );
  }

  let parsedQueryUrl: URL;
  try {
    parsedQueryUrl = new URL(queryUrl);
  } catch {
    throw new Error("DORIS_QUERY_URL must be a valid MySQL URL");
  }
  if (
    parsedQueryUrl.protocol !== "mysql:" ||
    parsedQueryUrl.pathname !== `/${database}` ||
    parsedQueryUrl.search.length > 0 ||
    parsedQueryUrl.hash.length > 0
  ) {
    throw new Error(
      "DORIS_QUERY_URL database path must match the test namespace",
    );
  }
  if (streamLoadDatabase !== database) {
    throw new Error("DORIS_STREAM_LOAD_DATABASE must match the test namespace");
  }

  return {
    runId,
    database,
    ownershipTokenHash: createHash("sha256")
      .update(ownershipToken)
      .digest("hex"),
    queryUrl,
  };
}

function assertNamespaceShape(namespace: DorisTestNamespace): void {
  const expectedDatabase = deriveDorisTestDatabaseName(namespace.runId);
  if (
    namespace.database !== expectedDatabase ||
    FORBIDDEN_DATABASES.has(namespace.database.toLowerCase()) ||
    !/^[0-9a-f]{64}$/.test(namespace.ownershipTokenHash)
  ) {
    throw new Error("Unsafe Doris test namespace");
  }
}

export async function assertOwnedDorisTestDatabase(
  executor: DorisTestDatabaseExecutor,
  namespace: DorisTestNamespace,
): Promise<void> {
  assertNamespaceShape(namespace);

  let rows: readonly { readonly run_id: string; readonly token_hash: string }[];
  try {
    rows = await executor.query<{ run_id: string; token_hash: string }>(
      `SELECT run_id, token_hash FROM \`${OWNERSHIP_TABLE}\` LIMIT 2`,
    );
  } catch {
    throw new Error(
      "Doris test database ownership marker is missing or unreadable",
    );
  }

  if (
    rows.length !== 1 ||
    rows[0]?.run_id !== namespace.runId ||
    rows[0]?.token_hash !== namespace.ownershipTokenHash
  ) {
    throw new Error(
      "Doris test database ownership marker does not match this run",
    );
  }
}

async function closeExecutor(
  executor: DorisTestDatabaseExecutor,
): Promise<void> {
  if (executor.close) {
    await executor.close();
  } else if (executor.end) {
    await executor.end();
  }
}

export async function dropOwnedDorisTestDatabase(input: {
  readonly admin: DorisTestDatabaseExecutor;
  readonly connectDatabase: (
    database: string,
  ) => DorisTestDatabaseExecutor | Promise<DorisTestDatabaseExecutor>;
  readonly namespace: DorisTestNamespace;
}): Promise<void> {
  const { admin, connectDatabase, namespace } = input;
  assertNamespaceShape(namespace);

  const target = await connectDatabase(namespace.database);
  try {
    await assertOwnedDorisTestDatabase(target, namespace);
  } finally {
    await closeExecutor(target);
  }
  // 测试库已通过不可伪造的 run marker 校验；直接物理删除，避免 recycle bin 持续占用 tablet 配额。
  await admin.execute(`DROP DATABASE \`${namespace.database}\` FORCE`);
}

export async function resetOwnedDorisTestDatabase(input: {
  readonly admin: DorisTestDatabaseExecutor;
  readonly connectDatabase: (
    database: string,
  ) => DorisTestDatabaseExecutor | Promise<DorisTestDatabaseExecutor>;
  readonly namespace: DorisTestNamespace;
}): Promise<void> {
  const { admin, connectDatabase, namespace } = input;
  assertNamespaceShape(namespace);

  const existing = await admin.query<{ schema_name: string }>(
    `SELECT SCHEMA_NAME AS schema_name
       FROM information_schema.SCHEMATA
      WHERE SCHEMA_NAME = ?`,
    [namespace.database],
  );

  if (existing.length > 0) {
    await dropOwnedDorisTestDatabase({ admin, connectDatabase, namespace });
  }

  try {
    await admin.execute(`CREATE DATABASE \`${namespace.database}\``);
    const target = await connectDatabase(namespace.database);
    try {
      await target.execute(`
        CREATE TABLE \`${OWNERSHIP_TABLE}\` (
          run_id VARCHAR(36) NOT NULL,
          token_hash CHAR(64) NOT NULL,
          created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
        UNIQUE KEY (run_id)
        DISTRIBUTED BY HASH(run_id) BUCKETS 1
        PROPERTIES (
          "replication_num" = "1",
          "enable_unique_key_merge_on_write" = "true"
        )
      `);
      await target.execute(
        `INSERT INTO \`${OWNERSHIP_TABLE}\` (run_id, token_hash) VALUES (?, ?)`,
        [namespace.runId, namespace.ownershipTokenHash],
      );
    } finally {
      await closeExecutor(target);
    }
  } catch (error) {
    try {
      // existing 已先通过 marker 校验并删除；此时该随机命名空间只可能来自本次 reset。
      await admin.execute(
        `DROP DATABASE IF EXISTS \`${namespace.database}\` FORCE`,
      );
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "Doris test database initialization and cleanup failed",
      );
    }
    throw error;
  }
}

export async function truncateOwnedDorisTestTables(input: {
  readonly executor: DorisTestDatabaseExecutor;
  readonly namespace: DorisTestNamespace;
  readonly tables: readonly string[];
}): Promise<void> {
  for (const table of input.tables) {
    if (!SIMPLE_IDENTIFIER_PATTERN.test(table)) {
      throw new Error("Doris test table must be a simple identifier");
    }
    if (table === OWNERSHIP_TABLE) {
      throw new Error("Doris test ownership marker cannot be truncated");
    }
  }

  await assertOwnedDorisTestDatabase(input.executor, input.namespace);
  for (const table of input.tables) {
    // 这些表仅属于当前隔离测试运行，FORCE 可避免每个 suite 的清理进入 recycle bin。
    await input.executor.execute(`TRUNCATE TABLE \`${table}\` FORCE`);
  }
}
