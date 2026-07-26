import { describe, expect, it } from "vitest";

import {
  DORIS_APPLICATION_WRITTEN_TABLES,
  buildDorisLeastPrivilegeGrantStatements,
} from "./leastPrivilege";

describe("Doris least-privilege grants", () => {
  it("keeps query, load, and migration privileges on separate identities", () => {
    const statements = buildDorisLeastPrivilegeGrantStatements({
      database: "langfuse",
      webQueryUser: "langfuse_web_query",
      workerQueryUser: "langfuse_worker_query",
      workerLoadUser: "langfuse_worker_load",
      migratorUser: "langfuse_migrator",
    });

    expect(statements).toEqual([
      "GRANT SELECT_PRIV ON `langfuse`.* TO 'langfuse_web_query'@'%'",
      "GRANT SELECT_PRIV ON `langfuse`.* TO 'langfuse_worker_query'@'%'",
      ...DORIS_APPLICATION_WRITTEN_TABLES.map(
        (table) =>
          `GRANT LOAD_PRIV ON \`langfuse\`.\`${table}\` TO 'langfuse_worker_load'@'%'`,
      ),
      "GRANT SELECT_PRIV, LOAD_PRIV, CREATE_PRIV, ALTER_PRIV, DROP_PRIV ON `langfuse`.* TO 'langfuse_migrator'@'%'",
    ]);
    expect(
      statements.filter((sql) => sql.includes("worker_load")),
    ).toHaveLength(DORIS_APPLICATION_WRITTEN_TABLES.length);
    expect(
      statements.some(
        (sql) => sql.includes("worker_load") && sql.includes("SELECT_PRIV"),
      ),
    ).toBe(false);
  });

  it("rejects unsafe identifiers and identity reuse", () => {
    const base = {
      database: "langfuse",
      webQueryUser: "langfuse_web_query",
      workerQueryUser: "langfuse_worker_query",
      workerLoadUser: "langfuse_worker_load",
      migratorUser: "langfuse_migrator",
    };

    expect(() =>
      buildDorisLeastPrivilegeGrantStatements({
        ...base,
        database: "langfuse`; DROP DATABASE prod",
      }),
    ).toThrow("Invalid Doris least-privilege identifier");
    expect(() =>
      buildDorisLeastPrivilegeGrantStatements({
        ...base,
        workerQueryUser: base.webQueryUser,
      }),
    ).toThrow("Doris workload identities must be distinct");
    expect(() =>
      buildDorisLeastPrivilegeGrantStatements({
        ...base,
        workerLoadUser: "root",
      }),
    ).toThrow("Doris workload identities must not use root");
  });
});
