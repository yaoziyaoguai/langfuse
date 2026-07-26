import { createHash } from "node:crypto";
import { describe, expect, it, vi } from "vitest";

import {
  assertOwnedDorisTestDatabase,
  dropOwnedDorisTestDatabase,
  parseDorisTestNamespace,
  resetOwnedDorisTestDatabase,
  truncateOwnedDorisTestTables,
  type DorisTestDatabaseExecutor,
} from "../testDatabase";

const RUN_ID = "01234567-89ab-4cde-8fab-0123456789ab";
const OWNERSHIP_TOKEN = "a".repeat(64);
const DATABASE = "langfuse_test_0123456789ab4cde8fab0123456789ab";

const safeEnv = {
  DORIS_TEST_RUN_ID: RUN_ID,
  DORIS_TEST_OWNERSHIP_TOKEN: OWNERSHIP_TOKEN,
  DORIS_POC_DATABASE: DATABASE,
  DORIS_QUERY_URL: `mysql://127.0.0.1:9031/${DATABASE}`,
  DORIS_STREAM_LOAD_DATABASE: DATABASE,
};

class FakeExecutor implements DorisTestDatabaseExecutor {
  readonly queries: Array<{
    readonly sql: string;
    readonly params: readonly unknown[];
  }> = [];
  readonly executions: Array<{
    readonly sql: string;
    readonly params: readonly unknown[];
  }> = [];

  constructor(private readonly queryResults: readonly unknown[][]) {}

  async query<T extends object>(
    sql: string,
    params: readonly unknown[] = [],
  ): Promise<readonly T[]> {
    this.queries.push({ sql, params });
    return (this.queryResults[this.queries.length - 1] ?? []) as readonly T[];
  }

  async execute(sql: string, params: readonly unknown[] = []): Promise<void> {
    this.executions.push({ sql, params });
  }
}

describe("Doris test database ownership guard", () => {
  it("derives one strict database name and hashes the ownership token", () => {
    expect(parseDorisTestNamespace(safeEnv)).toEqual({
      runId: RUN_ID,
      database: DATABASE,
      ownershipTokenHash: createHash("sha256")
        .update(OWNERSHIP_TOKEN)
        .digest("hex"),
      queryUrl: safeEnv.DORIS_QUERY_URL,
    });
  });

  it.each([
    ["missing run id", { ...safeEnv, DORIS_TEST_RUN_ID: undefined }],
    ["pathological run id", { ...safeEnv, DORIS_TEST_RUN_ID: "../shared" }],
    [
      "missing ownership token",
      { ...safeEnv, DORIS_TEST_OWNERSHIP_TOKEN: undefined },
    ],
    [
      "short ownership token",
      { ...safeEnv, DORIS_TEST_OWNERSHIP_TOKEN: "guessable" },
    ],
    ["shared PoC database", { ...safeEnv, DORIS_POC_DATABASE: "langfuse_poc" }],
    ["default database", { ...safeEnv, DORIS_POC_DATABASE: "default" }],
    [
      "missing stream-load database",
      { ...safeEnv, DORIS_STREAM_LOAD_DATABASE: undefined },
    ],
    [
      "stream-load database mismatch",
      { ...safeEnv, DORIS_STREAM_LOAD_DATABASE: "another_database" },
    ],
    [
      "query URL mismatch",
      {
        ...safeEnv,
        DORIS_QUERY_URL: "mysql://127.0.0.1:9031/another_database",
      },
    ],
  ])("rejects %s before a connection can be created", (_name, env) => {
    expect(() => parseDorisTestNamespace(env)).toThrow();
  });

  it("accepts the exact ownership marker", async () => {
    const namespace = parseDorisTestNamespace(safeEnv);
    const target = new FakeExecutor([
      [
        {
          run_id: RUN_ID,
          token_hash: namespace.ownershipTokenHash,
        },
      ],
    ]);

    await expect(
      assertOwnedDorisTestDatabase(target, namespace),
    ).resolves.toBeUndefined();
  });

  it.each([
    ["missing", []],
    ["wrong run", [{ run_id: "other", token_hash: "b".repeat(64) }]],
    ["wrong token", [{ run_id: RUN_ID, token_hash: "b".repeat(64) }]],
  ])("rejects a %s ownership marker", async (_name, rows) => {
    const target = new FakeExecutor([rows]);

    await expect(
      assertOwnedDorisTestDatabase(target, parseDorisTestNamespace(safeEnv)),
    ).rejects.toThrow(/ownership marker/i);
    expect(target.executions).toEqual([]);
  });

  it("does not drop an existing database when its marker is missing", async () => {
    const admin = new FakeExecutor([[{ schema_name: DATABASE }]]);
    const target = new FakeExecutor([[]]);

    await expect(
      resetOwnedDorisTestDatabase({
        admin,
        connectDatabase: () => target,
        namespace: parseDorisTestNamespace(safeEnv),
      }),
    ).rejects.toThrow(/ownership marker/i);
    expect(admin.executions).toEqual([]);
  });

  it("drops only the exact database after revalidating its marker", async () => {
    const namespace = parseDorisTestNamespace(safeEnv);
    const admin = new FakeExecutor([]);
    const target = new FakeExecutor([
      [
        {
          run_id: RUN_ID,
          token_hash: namespace.ownershipTokenHash,
        },
      ],
    ]);

    await dropOwnedDorisTestDatabase({
      admin,
      connectDatabase: () => target,
      namespace,
    });

    expect(admin.executions.map(({ sql }) => sql)).toEqual([
      `DROP DATABASE \`${DATABASE}\` FORCE`,
    ]);
  });

  it("recreates an owned database and writes a fresh marker", async () => {
    const namespace = parseDorisTestNamespace(safeEnv);
    const admin = new FakeExecutor([[{ schema_name: DATABASE }]]);
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

    await resetOwnedDorisTestDatabase({
      admin,
      connectDatabase: () => (connection++ === 0 ? existing : fresh),
      namespace,
    });

    expect(admin.executions.map(({ sql }) => sql)).toEqual([
      `DROP DATABASE \`${DATABASE}\` FORCE`,
      `CREATE DATABASE \`${DATABASE}\``,
    ]);
    expect(fresh.executions).toHaveLength(2);
    expect(fresh.executions[1]?.params).toEqual([
      RUN_ID,
      namespace.ownershipTokenHash,
    ]);
  });

  it("force-removes a database when ownership marker initialization fails", async () => {
    const admin = new FakeExecutor([[]]);
    const fresh = new FakeExecutor([]);
    vi.spyOn(fresh, "execute").mockRejectedValueOnce(
      new Error("ownership table creation failed"),
    );

    await expect(
      resetOwnedDorisTestDatabase({
        admin,
        connectDatabase: () => fresh,
        namespace: parseDorisTestNamespace(safeEnv),
      }),
    ).rejects.toThrow("ownership table creation failed");

    expect(admin.executions.map(({ sql }) => sql)).toEqual([
      `CREATE DATABASE \`${DATABASE}\``,
      `DROP DATABASE IF EXISTS \`${DATABASE}\` FORCE`,
    ]);
  });

  it("checks the marker before truncating any table", async () => {
    const target = new FakeExecutor([[]]);

    await expect(
      truncateOwnedDorisTestTables({
        executor: target,
        namespace: parseDorisTestNamespace(safeEnv),
        tables: ["events_current"],
      }),
    ).rejects.toThrow(/ownership marker/i);
    expect(target.executions).toEqual([]);
  });

  it("rejects truncating the ownership marker before querying the database", async () => {
    const namespace = parseDorisTestNamespace(safeEnv);
    const target = new FakeExecutor([
      [
        {
          run_id: RUN_ID,
          token_hash: namespace.ownershipTokenHash,
        },
      ],
    ]);

    await expect(
      truncateOwnedDorisTestTables({
        executor: target,
        namespace,
        tables: ["_langfuse_test_ownership"],
      }),
    ).rejects.toThrow(/ownership marker/i);
    expect(target.queries).toEqual([]);
    expect(target.executions).toEqual([]);
  });

  it("truncates only simple identifiers after ownership is proven", async () => {
    const namespace = parseDorisTestNamespace(safeEnv);
    const target = new FakeExecutor([
      [
        {
          run_id: RUN_ID,
          token_hash: namespace.ownershipTokenHash,
        },
      ],
    ]);

    await truncateOwnedDorisTestTables({
      executor: target,
      namespace,
      tables: ["events_current", "trace_tombstones"],
    });

    expect(target.executions.map(({ sql }) => sql)).toEqual([
      "TRUNCATE TABLE `events_current` FORCE",
      "TRUNCATE TABLE `trace_tombstones` FORCE",
    ]);
  });
});
