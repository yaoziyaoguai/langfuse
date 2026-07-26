import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";

import {
  assertOwnedClickHouseTestDatabase,
  deriveClickHouseTestDatabaseName,
  dropOwnedClickHouseTestDatabase,
  parseClickHouseTestNamespace,
  resetOwnedClickHouseTestDatabase,
  truncateOwnedClickHouseTestTables,
  type ClickHouseTestDatabaseExecutor,
} from "./testDatabase";

const RUN_ID = "01234567-89ab-4cde-8fab-0123456789ab";
const OWNERSHIP_TOKEN = "a".repeat(64);
const DATABASE = "langfuse_test_0123456789ab4cde8fab0123456789ab";

const safeEnv = {
  CLICKHOUSE_TEST_RUN_ID: RUN_ID,
  CLICKHOUSE_TEST_OWNERSHIP_TOKEN: OWNERSHIP_TOKEN,
  CLICKHOUSE_DB: DATABASE,
  CLICKHOUSE_URL: `http://127.0.0.1:8123/${DATABASE}`,
};

class FakeExecutor implements ClickHouseTestDatabaseExecutor {
  readonly queries: Array<{
    readonly sql: string;
    readonly params: Readonly<Record<string, unknown>>;
  }> = [];
  readonly executions: Array<{
    readonly sql: string;
    readonly params: Readonly<Record<string, unknown>>;
  }> = [];
  readonly close = vi.fn(async () => undefined);

  constructor(private readonly queryResults: readonly unknown[][]) {}

  async query<T extends object>(
    sql: string,
    params: Readonly<Record<string, unknown>> = {},
  ): Promise<readonly T[]> {
    this.queries.push({ sql, params });
    return (this.queryResults[this.queries.length - 1] ?? []) as readonly T[];
  }

  async execute(
    sql: string,
    params: Readonly<Record<string, unknown>> = {},
  ): Promise<void> {
    this.executions.push({ sql, params });
  }
}

describe("ClickHouse test database ownership guard", () => {
  it("derives one strict database name and only exposes the hashed token", () => {
    const namespace = parseClickHouseTestNamespace(safeEnv);

    expect(deriveClickHouseTestDatabaseName(RUN_ID)).toBe(DATABASE);
    expect(namespace).toEqual({
      runId: RUN_ID,
      database: DATABASE,
      ownershipTokenHash: createHash("sha256")
        .update(OWNERSHIP_TOKEN)
        .digest("hex"),
      clickhouseUrl: safeEnv.CLICKHOUSE_URL,
    });
    expect(namespace).not.toHaveProperty("ownershipToken");
  });

  it.each([
    ["missing run id", { ...safeEnv, CLICKHOUSE_TEST_RUN_ID: undefined }],
    [
      "uppercase run id",
      { ...safeEnv, CLICKHOUSE_TEST_RUN_ID: RUN_ID.toUpperCase() },
    ],
    [
      "non-v4 run id",
      {
        ...safeEnv,
        CLICKHOUSE_TEST_RUN_ID: "01234567-89ab-3cde-8fab-0123456789ab",
      },
    ],
    [
      "pathological run id",
      { ...safeEnv, CLICKHOUSE_TEST_RUN_ID: "../langfuse" },
    ],
    [
      "missing ownership token",
      { ...safeEnv, CLICKHOUSE_TEST_OWNERSHIP_TOKEN: undefined },
    ],
    [
      "uppercase ownership token",
      {
        ...safeEnv,
        CLICKHOUSE_TEST_OWNERSHIP_TOKEN: OWNERSHIP_TOKEN.toUpperCase(),
      },
    ],
    [
      "short ownership token",
      { ...safeEnv, CLICKHOUSE_TEST_OWNERSHIP_TOKEN: "guessable" },
    ],
    ["shared database", { ...safeEnv, CLICKHOUSE_DB: "langfuse" }],
    ["default database", { ...safeEnv, CLICKHOUSE_DB: "default" }],
    ["system database", { ...safeEnv, CLICKHOUSE_DB: "system" }],
    [
      "information schema database",
      { ...safeEnv, CLICKHOUSE_DB: "information_schema" },
    ],
    [
      "database name mismatch",
      { ...safeEnv, CLICKHOUSE_DB: `${DATABASE}_other` },
    ],
    [
      "URL database mismatch",
      {
        ...safeEnv,
        CLICKHOUSE_URL: "http://127.0.0.1:8123/another_database",
      },
    ],
    [
      "URL without database path",
      { ...safeEnv, CLICKHOUSE_URL: "http://127.0.0.1:8123" },
    ],
    ["non-HTTP URL", { ...safeEnv, CLICKHOUSE_URL: `file:///tmp/${DATABASE}` }],
    [
      "URL with settings",
      {
        ...safeEnv,
        CLICKHOUSE_URL: `${safeEnv.CLICKHOUSE_URL}?database=default`,
      },
    ],
  ])("rejects %s before a connection can be created", (_name, env) => {
    expect(() => parseClickHouseTestNamespace(env)).toThrow();
  });

  it("accepts exactly one matching ownership marker", async () => {
    const namespace = parseClickHouseTestNamespace(safeEnv);
    const target = new FakeExecutor([
      [
        {
          run_id: RUN_ID,
          token_hash: namespace.ownershipTokenHash,
        },
      ],
    ]);

    await expect(
      assertOwnedClickHouseTestDatabase(target, namespace),
    ).resolves.toBeUndefined();
    expect(target.queries[0]?.sql).toContain("LIMIT 2");
  });

  it.each([
    ["missing", []],
    [
      "duplicate",
      [
        { run_id: RUN_ID, token_hash: "b".repeat(64) },
        { run_id: RUN_ID, token_hash: "b".repeat(64) },
      ],
    ],
    ["wrong run", [{ run_id: "other", token_hash: "b".repeat(64) }]],
    ["wrong token", [{ run_id: RUN_ID, token_hash: "b".repeat(64) }]],
  ])("rejects a %s ownership marker", async (_name, rows) => {
    const target = new FakeExecutor([rows]);

    await expect(
      assertOwnedClickHouseTestDatabase(
        target,
        parseClickHouseTestNamespace(safeEnv),
      ),
    ).rejects.toThrow(/ownership marker/i);
    expect(target.executions).toEqual([]);
  });

  it("maps an unreadable marker to a fail-closed ownership error", async () => {
    const target = new FakeExecutor([]);
    vi.spyOn(target, "query").mockRejectedValueOnce(new Error("unknown table"));

    await expect(
      assertOwnedClickHouseTestDatabase(
        target,
        parseClickHouseTestNamespace(safeEnv),
      ),
    ).rejects.toThrow(/missing or unreadable/i);
    expect(target.executions).toEqual([]);
  });

  it("does not drop an existing database when its marker is missing", async () => {
    const admin = new FakeExecutor([[{ name: DATABASE }]]);
    const target = new FakeExecutor([[]]);

    await expect(
      resetOwnedClickHouseTestDatabase({
        admin,
        connectDatabase: () => target,
        namespace: parseClickHouseTestNamespace(safeEnv),
      }),
    ).rejects.toThrow(/ownership marker/i);
    expect(admin.executions).toEqual([]);
    expect(target.close).toHaveBeenCalledOnce();
  });

  it("drops only the exact database synchronously after revalidating its marker", async () => {
    const namespace = parseClickHouseTestNamespace(safeEnv);
    const admin = new FakeExecutor([]);
    const target = new FakeExecutor([
      [
        {
          run_id: RUN_ID,
          token_hash: namespace.ownershipTokenHash,
        },
      ],
    ]);

    await dropOwnedClickHouseTestDatabase({
      admin,
      connectDatabase: () => target,
      namespace,
    });

    expect(target.queries).toHaveLength(1);
    expect(target.close).toHaveBeenCalledOnce();
    expect(admin.executions.map(({ sql }) => sql)).toEqual([
      `DROP DATABASE \`${DATABASE}\` SYNC`,
    ]);
  });

  it("recreates an owned database and writes a fresh hash marker", async () => {
    const namespace = parseClickHouseTestNamespace(safeEnv);
    const admin = new FakeExecutor([[{ name: DATABASE }]]);
    const existing = new FakeExecutor([
      [
        {
          run_id: RUN_ID,
          token_hash: namespace.ownershipTokenHash,
        },
      ],
    ]);
    const fresh = new FakeExecutor([]);
    let connection = 0;

    await resetOwnedClickHouseTestDatabase({
      admin,
      connectDatabase: () => (connection++ === 0 ? existing : fresh),
      namespace,
    });

    expect(admin.executions.map(({ sql }) => sql)).toEqual([
      `DROP DATABASE \`${DATABASE}\` SYNC`,
      `CREATE DATABASE \`${DATABASE}\``,
    ]);
    expect(fresh.executions).toHaveLength(2);
    expect(fresh.executions[1]?.params).toEqual({
      runId: RUN_ID,
      tokenHash: namespace.ownershipTokenHash,
    });
    expect(fresh.close).toHaveBeenCalledOnce();
  });

  it("synchronously removes only the newly-created namespace when marker initialization fails", async () => {
    const admin = new FakeExecutor([[]]);
    const fresh = new FakeExecutor([]);
    vi.spyOn(fresh, "execute").mockRejectedValueOnce(
      new Error("ownership table creation failed"),
    );

    await expect(
      resetOwnedClickHouseTestDatabase({
        admin,
        connectDatabase: () => fresh,
        namespace: parseClickHouseTestNamespace(safeEnv),
      }),
    ).rejects.toThrow("ownership table creation failed");

    expect(admin.executions.map(({ sql }) => sql)).toEqual([
      `CREATE DATABASE \`${DATABASE}\``,
      `DROP DATABASE IF EXISTS \`${DATABASE}\` SYNC`,
    ]);
  });

  it("does not clean up after an uncertain CREATE DATABASE failure", async () => {
    const admin = new FakeExecutor([[]]);
    vi.spyOn(admin, "execute").mockRejectedValueOnce(
      new Error("create response lost"),
    );

    await expect(
      resetOwnedClickHouseTestDatabase({
        admin,
        connectDatabase: () => new FakeExecutor([]),
        namespace: parseClickHouseTestNamespace(safeEnv),
      }),
    ).rejects.toThrow("create response lost");

    expect(admin.executions).toEqual([]);
  });

  it("checks the marker before truncating any table", async () => {
    const target = new FakeExecutor([[]]);

    await expect(
      truncateOwnedClickHouseTestTables({
        executor: target,
        namespace: parseClickHouseTestNamespace(safeEnv),
        tables: ["events"],
      }),
    ).rejects.toThrow(/ownership marker/i);
    expect(target.executions).toEqual([]);
  });

  it("rejects unsafe table identifiers before checking ownership", async () => {
    const target = new FakeExecutor([]);

    await expect(
      truncateOwnedClickHouseTestTables({
        executor: target,
        namespace: parseClickHouseTestNamespace(safeEnv),
        tables: ["events; DROP DATABASE langfuse"],
      }),
    ).rejects.toThrow(/simple identifier/i);
    expect(target.queries).toEqual([]);
    expect(target.executions).toEqual([]);
  });

  it("never truncates the ownership marker itself", async () => {
    const target = new FakeExecutor([]);

    await expect(
      truncateOwnedClickHouseTestTables({
        executor: target,
        namespace: parseClickHouseTestNamespace(safeEnv),
        tables: ["_langfuse_test_ownership"],
      }),
    ).rejects.toThrow(/ownership marker/i);
    expect(target.queries).toEqual([]);
    expect(target.executions).toEqual([]);
  });

  it("truncates only simple identifiers synchronously after ownership is proven", async () => {
    const namespace = parseClickHouseTestNamespace(safeEnv);
    const target = new FakeExecutor([
      [
        {
          run_id: RUN_ID,
          token_hash: namespace.ownershipTokenHash,
        },
      ],
    ]);

    await truncateOwnedClickHouseTestTables({
      executor: target,
      namespace,
      tables: ["events", "traces"],
    });

    expect(target.executions.map(({ sql }) => sql)).toEqual([
      "TRUNCATE TABLE `events` SYNC",
      "TRUNCATE TABLE `traces` SYNC",
    ]);
  });
});
