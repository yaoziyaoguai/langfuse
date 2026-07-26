import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { DorisPoCMysqlClient } from "../../doris-poc/mysqlClient";
import { parseDorisStreamLoadConfig } from "../config";
import {
  buildDorisLeastPrivilegeGrantStatements,
  type DorisLeastPrivilegeIdentities,
} from "../leastPrivilege";
import { DorisStreamLoadClient } from "../streamLoadClient";
import {
  assertOwnedDorisTestDatabase,
  parseDorisTestNamespace,
} from "../testDatabase";

const ENABLED = process.env.DORIS_POC_ENABLED === "1";
const TEST_NAMESPACE = ENABLED ? parseDorisTestNamespace() : null;
const DATABASE = TEST_NAMESPACE?.database ?? "doris_test_disabled";
const USER_PREFIX = process.env.DORIS_SECURITY_TEST_USER_PREFIX ?? "";
const PASSWORD = process.env.DORIS_SECURITY_TEST_PASSWORD ?? "";

describe.skipIf(!ENABLED)("Doris least-privilege workload identities", () => {
  const users = {
    database: DATABASE,
    webQueryUser: `${USER_PREFIX}_web`,
    workerQueryUser: `${USER_PREFIX}_query`,
    workerLoadUser: `${USER_PREFIX}_load`,
    migratorUser: `${USER_PREFIX}_migrator`,
  } satisfies DorisLeastPrivilegeIdentities;
  const identities = [
    users.webQueryUser,
    users.workerQueryUser,
    users.workerLoadUser,
    users.migratorUser,
  ];
  const adminConfig = {
    host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
    port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
    user: process.env.DORIS_POC_USER ?? "root",
    password: process.env.DORIS_POC_PASSWORD ?? "",
  };
  const migrationProbe = `${USER_PREFIX}_probe`;
  let admin: DorisPoCMysqlClient;
  let webQuery: DorisPoCMysqlClient;
  let workerQuery: DorisPoCMysqlClient;
  let workerLoad: DorisPoCMysqlClient;
  let migrator: DorisPoCMysqlClient;

  const connect = (user: string) =>
    new DorisPoCMysqlClient({
      ...adminConfig,
      user,
      password: PASSWORD,
      database: DATABASE,
    });

  const streamLoad = (user: string) => {
    const config = parseDorisStreamLoadConfig(
      {
        DORIS_LOCAL_DEV_MODE: process.env.DORIS_LOCAL_DEV_MODE,
        DORIS_QUERY_USER: process.env.DORIS_QUERY_USER,
        DORIS_STREAM_LOAD_FE_URL: process.env.DORIS_STREAM_LOAD_FE_URL,
        DORIS_STREAM_LOAD_USER: user,
        DORIS_STREAM_LOAD_PASSWORD: PASSWORD,
        DORIS_STREAM_LOAD_DATABASE: process.env.DORIS_STREAM_LOAD_DATABASE,
        DORIS_STREAM_LOAD_FE_IP_ALLOWLIST:
          process.env.DORIS_STREAM_LOAD_FE_IP_ALLOWLIST,
        DORIS_STREAM_LOAD_BE_ALLOWLIST:
          process.env.DORIS_STREAM_LOAD_BE_ALLOWLIST,
        DORIS_STREAM_LOAD_BE_IP_ALLOWLIST:
          process.env.DORIS_STREAM_LOAD_BE_IP_ALLOWLIST,
        DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP:
          process.env.DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP,
        DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST:
          process.env.DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST,
      },
      "development",
    );
    return new DorisStreamLoadClient(config);
  };

  beforeAll(async () => {
    if (
      !TEST_NAMESPACE ||
      !/^lfsec_[a-f0-9]{12}$/.test(USER_PREFIX) ||
      PASSWORD.length < 32
    ) {
      throw new Error("Owned Doris security test identity is required");
    }
    admin = new DorisPoCMysqlClient(adminConfig);
    const ownedDatabase = new DorisPoCMysqlClient({
      ...adminConfig,
      database: DATABASE,
    });
    try {
      await assertOwnedDorisTestDatabase(ownedDatabase, TEST_NAMESPACE);
    } finally {
      await ownedDatabase.end();
    }

    for (const identity of identities) {
      await admin.execute(
        `CREATE USER IF NOT EXISTS '${identity}'@'%' IDENTIFIED BY '${PASSWORD}'`,
      );
    }
    for (const statement of buildDorisLeastPrivilegeGrantStatements(users)) {
      await admin.execute(statement);
    }

    webQuery = connect(users.webQueryUser);
    workerQuery = connect(users.workerQueryUser);
    workerLoad = connect(users.workerLoadUser);
    migrator = connect(users.migratorUser);
  }, 120_000);

  afterAll(async () => {
    await admin?.execute(
      `DROP TABLE IF EXISTS \`${DATABASE}\`.\`${migrationProbe}\``,
    );
    await Promise.allSettled([
      webQuery?.end(),
      workerQuery?.end(),
      workerLoad?.end(),
      migrator?.end(),
    ]);
    for (const identity of identities) {
      await admin?.execute(`DROP USER IF EXISTS '${identity}'@'%'`);
    }
    await admin?.end();
  });

  it.each([
    ["web query", () => webQuery, () => users.webQueryUser],
    ["worker query", () => workerQuery, () => users.workerQueryUser],
  ])(
    "%s can read but cannot load or migrate",
    async (_name, client, identity) => {
      await expect(
        client().query("SELECT COUNT(*) AS count FROM events_current"),
      ).resolves.toHaveLength(1);
      await expect(
        client().execute(
          `CREATE TABLE \`${migrationProbe}\` (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")`,
        ),
      ).rejects.toThrow();
      await expect(
        streamLoad(identity()).load({
          table: "events_current",
          label: `deny_${USER_PREFIX}_${_name.replaceAll(" ", "_")}`,
          ndjsonBody: eventRow(`deny-${_name}`),
        }),
      ).rejects.toThrow();
    },
  );

  it("worker load can Stream Load but cannot query or migrate", async () => {
    const result = await streamLoad(users.workerLoadUser).load({
      table: "events_current",
      label: `allow_${USER_PREFIX}_load`,
      ndjsonBody: eventRow("worker-load"),
    });
    expect(result).toMatchObject({
      committed: true,
      numberTotalRows: 1,
      numberFilteredRows: 0,
    });
    await expect(
      workerLoad.query("SELECT COUNT(*) AS count FROM events_current"),
    ).rejects.toThrow();
    await expect(
      workerLoad.execute(
        `CREATE TABLE \`${migrationProbe}\` (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")`,
      ),
    ).rejects.toThrow();
  });

  it("one-shot migrator can read and mutate schema", async () => {
    await migrator.execute(
      `CREATE TABLE \`${migrationProbe}\` (id INT) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ("replication_num" = "1")`,
    );
    await migrator.execute(
      `ALTER TABLE \`${migrationProbe}\` ADD COLUMN value VARCHAR(64) NULL`,
    );
    await migrator.execute(
      `INSERT INTO \`${migrationProbe}\` (id, value) VALUES (1, 'migration-probe')`,
    );
    await expect(
      migrator.query(`SELECT value FROM \`${migrationProbe}\` WHERE id = 1`),
    ).resolves.toEqual([{ value: "migration-probe" }]);
    await migrator.execute(`DROP TABLE \`${migrationProbe}\``);
  });
});

function eventRow(tag: string): string {
  return JSON.stringify({
    project_id: "security-project",
    partition_date: "2026-07-23",
    trace_id: `security-trace-${tag}`,
    span_id: `security-span-${tag}`,
    version_token: "1",
    type: "span",
    environment: "default",
    name: `security-${tag}`,
    start_time: "2026-07-23 10:00:00.000000",
    created_at: "2026-07-23 10:00:00.000000",
    updated_at: "2026-07-23 10:00:00.000000",
    source: "api",
    ingestion_sdk_name: "security-test",
    ingestion_sdk_version: "1",
  });
}
