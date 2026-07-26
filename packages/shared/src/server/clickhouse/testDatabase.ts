import { createHash } from "node:crypto";

const TEST_RUN_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const LOWERCASE_32_BYTE_HEX_PATTERN = /^[0-9a-f]{64}$/;
const SIMPLE_IDENTIFIER_PATTERN = /^[a-z_][a-z0-9_]*$/;
const OWNERSHIP_TABLE = "_langfuse_test_ownership";
const FORBIDDEN_DATABASES = new Set([
  "default",
  "information_schema",
  "langfuse",
  "langfuse_poc",
  "system",
  "test",
]);

type ClickHouseTestEnv = Readonly<Record<string, string | undefined>>;
type ClickHouseQueryParams = Readonly<Record<string, unknown>>;

export interface ClickHouseTestNamespace {
  readonly runId: string;
  readonly database: string;
  readonly ownershipTokenHash: string;
  readonly clickhouseUrl: string;
}

export interface ClickHouseTestDatabaseExecutor {
  query<T extends object = Record<string, unknown>>(
    sql: string,
    params?: ClickHouseQueryParams,
  ): Promise<readonly T[]>;
  execute(sql: string, params?: ClickHouseQueryParams): Promise<void>;
  close?(): Promise<void>;
}

export function deriveClickHouseTestDatabaseName(runId: string): string {
  if (!TEST_RUN_ID_PATTERN.test(runId)) {
    throw new Error("CLICKHOUSE_TEST_RUN_ID must be a lowercase UUID v4");
  }
  return `langfuse_test_${runId.replaceAll("-", "")}`;
}

function assertClickHouseUrl(database: string, clickhouseUrl: string): void {
  let parsedUrl: URL;
  try {
    parsedUrl = new URL(clickhouseUrl);
  } catch {
    throw new Error("CLICKHOUSE_URL must be a valid HTTP URL");
  }

  if (
    (parsedUrl.protocol !== "http:" && parsedUrl.protocol !== "https:") ||
    parsedUrl.pathname !== `/${database}` ||
    parsedUrl.search.length > 0 ||
    parsedUrl.hash.length > 0
  ) {
    throw new Error(
      "CLICKHOUSE_URL database path must match the test namespace",
    );
  }
}

export function parseClickHouseTestNamespace(
  env: ClickHouseTestEnv = process.env,
): ClickHouseTestNamespace {
  const runId = env.CLICKHOUSE_TEST_RUN_ID;
  if (!runId) throw new Error("CLICKHOUSE_TEST_RUN_ID is required");
  const ownershipToken = env.CLICKHOUSE_TEST_OWNERSHIP_TOKEN;
  if (!ownershipToken) {
    throw new Error("CLICKHOUSE_TEST_OWNERSHIP_TOKEN is required");
  }
  const database = env.CLICKHOUSE_DB;
  if (!database) throw new Error("CLICKHOUSE_DB is required");
  const clickhouseUrl = env.CLICKHOUSE_URL;
  if (!clickhouseUrl) throw new Error("CLICKHOUSE_URL is required");

  const expectedDatabase = deriveClickHouseTestDatabaseName(runId);
  if (
    FORBIDDEN_DATABASES.has(database.toLowerCase()) ||
    !SIMPLE_IDENTIFIER_PATTERN.test(database) ||
    database !== expectedDatabase
  ) {
    throw new Error(
      "Unsafe ClickHouse test database; expected the derived run namespace",
    );
  }
  if (!LOWERCASE_32_BYTE_HEX_PATTERN.test(ownershipToken)) {
    throw new Error(
      "CLICKHOUSE_TEST_OWNERSHIP_TOKEN must be a 32-byte lowercase hex token",
    );
  }
  assertClickHouseUrl(database, clickhouseUrl);

  return {
    runId,
    database,
    ownershipTokenHash: createHash("sha256")
      .update(ownershipToken)
      .digest("hex"),
    clickhouseUrl,
  };
}

function assertNamespaceShape(namespace: ClickHouseTestNamespace): void {
  const expectedDatabase = deriveClickHouseTestDatabaseName(namespace.runId);
  if (
    namespace.database !== expectedDatabase ||
    FORBIDDEN_DATABASES.has(namespace.database.toLowerCase()) ||
    !SIMPLE_IDENTIFIER_PATTERN.test(namespace.database) ||
    !LOWERCASE_32_BYTE_HEX_PATTERN.test(namespace.ownershipTokenHash)
  ) {
    throw new Error("Unsafe ClickHouse test namespace");
  }
  assertClickHouseUrl(namespace.database, namespace.clickhouseUrl);
}

export async function assertOwnedClickHouseTestDatabase(
  executor: ClickHouseTestDatabaseExecutor,
  namespace: ClickHouseTestNamespace,
): Promise<void> {
  assertNamespaceShape(namespace);

  let rows: readonly { readonly run_id: string; readonly token_hash: string }[];
  try {
    rows = await executor.query<{ run_id: string; token_hash: string }>(`
      SELECT
        toString(run_id) AS run_id,
        toString(token_hash) AS token_hash
      FROM \`${OWNERSHIP_TABLE}\`
      LIMIT 2
    `);
  } catch {
    throw new Error(
      "ClickHouse test database ownership marker is missing or unreadable",
    );
  }

  if (
    rows.length !== 1 ||
    rows[0]?.run_id !== namespace.runId ||
    rows[0]?.token_hash !== namespace.ownershipTokenHash
  ) {
    throw new Error(
      "ClickHouse test database ownership marker does not match this run",
    );
  }
}

async function closeExecutor(
  executor: ClickHouseTestDatabaseExecutor,
): Promise<void> {
  await executor.close?.();
}

export async function dropOwnedClickHouseTestDatabase(input: {
  readonly admin: ClickHouseTestDatabaseExecutor;
  readonly connectDatabase: (
    database: string,
  ) => ClickHouseTestDatabaseExecutor | Promise<ClickHouseTestDatabaseExecutor>;
  readonly namespace: ClickHouseTestNamespace;
}): Promise<void> {
  const { admin, connectDatabase, namespace } = input;
  assertNamespaceShape(namespace);

  const target = await connectDatabase(namespace.database);
  try {
    await assertOwnedClickHouseTestDatabase(target, namespace);
  } finally {
    await closeExecutor(target);
  }

  // marker 必须从待删除目标连接中重验；SYNC 确保调用者不会把仍在异步删除的库误判为已清理。
  await admin.execute(`DROP DATABASE \`${namespace.database}\` SYNC`);
}

export async function resetOwnedClickHouseTestDatabase(input: {
  readonly admin: ClickHouseTestDatabaseExecutor;
  readonly connectDatabase: (
    database: string,
  ) => ClickHouseTestDatabaseExecutor | Promise<ClickHouseTestDatabaseExecutor>;
  readonly namespace: ClickHouseTestNamespace;
}): Promise<void> {
  const { admin, connectDatabase, namespace } = input;
  assertNamespaceShape(namespace);

  const existing = await admin.query<{ name: string }>(
    `SELECT name
       FROM system.databases
      WHERE name = {database:String}
      LIMIT 1`,
    { database: namespace.database },
  );
  if (
    existing.length > 1 ||
    (existing.length === 1 && existing[0]?.name !== namespace.database)
  ) {
    throw new Error("Unexpected ClickHouse database lookup result");
  }

  if (existing.length === 1) {
    await dropOwnedClickHouseTestDatabase({
      admin,
      connectDatabase,
      namespace,
    });
  }

  let createdByThisReset = false;
  try {
    await admin.execute(`CREATE DATABASE \`${namespace.database}\``);
    createdByThisReset = true;

    const target = await connectDatabase(namespace.database);
    try {
      await target.execute(`
        CREATE TABLE \`${OWNERSHIP_TABLE}\` (
          run_id UUID,
          token_hash FixedString(64)
        )
        ENGINE = MergeTree
        ORDER BY tuple()
      `);
      await target.execute(
        `INSERT INTO \`${OWNERSHIP_TABLE}\` (run_id, token_hash)
         VALUES ({runId:UUID}, {tokenHash:String})`,
        {
          runId: namespace.runId,
          tokenHash: namespace.ownershipTokenHash,
        },
      );
    } finally {
      await closeExecutor(target);
    }
  } catch (error) {
    if (!createdByThisReset) {
      // CREATE 结果不确定时没有可信 marker，宁可遗留随机库也不能误删并发创建的同名库。
      throw error;
    }

    try {
      // CREATE 已确认成功，且名称由本次 UUID v4 唯一派生；仅清理本次刚创建的命名空间。
      await admin.execute(
        `DROP DATABASE IF EXISTS \`${namespace.database}\` SYNC`,
      );
    } catch (cleanupError) {
      throw new AggregateError(
        [error, cleanupError],
        "ClickHouse test database initialization and cleanup failed",
      );
    }
    throw error;
  }
}

export async function truncateOwnedClickHouseTestTables(input: {
  readonly executor: ClickHouseTestDatabaseExecutor;
  readonly namespace: ClickHouseTestNamespace;
  readonly tables: readonly string[];
}): Promise<void> {
  for (const table of input.tables) {
    if (table === OWNERSHIP_TABLE) {
      throw new Error("ClickHouse ownership marker cannot be truncated");
    }
    if (!SIMPLE_IDENTIFIER_PATTERN.test(table)) {
      throw new Error("ClickHouse test table must be a simple identifier");
    }
  }

  await assertOwnedClickHouseTestDatabase(input.executor, input.namespace);
  for (const table of input.tables) {
    await input.executor.execute(`TRUNCATE TABLE \`${table}\` SYNC`);
  }
}
