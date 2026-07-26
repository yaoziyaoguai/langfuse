const SAFE_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/;

export const DORIS_APPLICATION_WRITTEN_TABLES = [
  "events_current",
  "scores_current",
  "blob_storage_file_log",
  "trace_tombstones",
  "project_tombstones",
  "dataset_run_items_current",
  "dataset_tombstones",
  "dataset_run_tombstones",
] as const;

export type DorisLeastPrivilegeIdentities = {
  readonly database: string;
  readonly webQueryUser: string;
  readonly workerQueryUser: string;
  readonly workerLoadUser: string;
  readonly migratorUser: string;
};

function assertIdentifier(value: string): void {
  if (!SAFE_IDENTIFIER.test(value)) {
    throw new Error("Invalid Doris least-privilege identifier");
  }
}

function quoteIdentifier(value: string): string {
  assertIdentifier(value);
  return `\`${value}\``;
}

function quotePrincipal(value: string): string {
  assertIdentifier(value);
  return `'${value}'@'%'`;
}

/**
 * 生成生产权限边界使用的 grant。身份创建和 secret rotation 仍由部署平台负责，
 * 这样应用仓库不会接触或持久化 Doris 密码。
 */
export function buildDorisLeastPrivilegeGrantStatements(
  input: DorisLeastPrivilegeIdentities,
): readonly string[] {
  const identities = [
    input.webQueryUser,
    input.workerQueryUser,
    input.workerLoadUser,
    input.migratorUser,
  ];
  [input.database, ...identities].forEach(assertIdentifier);
  if (new Set(identities).size !== identities.length) {
    throw new Error("Doris workload identities must be distinct");
  }
  if (identities.some((identity) => identity.toLowerCase() === "root")) {
    throw new Error("Doris workload identities must not use root");
  }

  const database = quoteIdentifier(input.database);
  const workerLoad = quotePrincipal(input.workerLoadUser);
  return [
    `GRANT SELECT_PRIV ON ${database}.* TO ${quotePrincipal(input.webQueryUser)}`,
    `GRANT SELECT_PRIV ON ${database}.* TO ${quotePrincipal(input.workerQueryUser)}`,
    ...DORIS_APPLICATION_WRITTEN_TABLES.map(
      (table) =>
        `GRANT LOAD_PRIV ON ${database}.${quoteIdentifier(table)} TO ${workerLoad}`,
    ),
    `GRANT SELECT_PRIV, LOAD_PRIV, CREATE_PRIV, ALTER_PRIV, DROP_PRIV ON ${database}.* TO ${quotePrincipal(input.migratorUser)}`,
  ];
}
