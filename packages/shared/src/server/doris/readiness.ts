import type { DorisQueryExecutor } from "./client";
import { ANALYTICS_CONTRACT_COMPATIBILITY } from "../analytics-persistence/versions";

export const EXPECTED_DORIS_MIGRATIONS = [
  {
    name: "0001_baseline.sql",
    checksum:
      "a042426386c102d88bb10b7e26c1864180f2e91f7d8a07d7829e8cd704c02d8e",
  },
  {
    name: "0002_refreeze_r1a_partition_and_tombstone_order.sql",
    checksum:
      "26d3b1eeb51070d002f701929a376e9c43faf765ff712a70a6241637ad8cdfab",
  },
  {
    name: "0003_expand_events_status_message.sql",
    checksum:
      "0a46d6f871d0958b33f10494fbff31368a6b8f24c24be243076987b3a1774ab0",
  },
  {
    name: "0004_preserve_dynamic_json_keys.sql",
    checksum:
      "a7a811639eddad62e32cc296c449ca1855b2a6fbd06f2987cda3696985ab1ab7",
  },
] as const;

export const SUPPORTED_DORIS_CANONICALIZER_VERSIONS =
  ANALYTICS_CONTRACT_COMPATIBILITY.readableCanonicalizerVersions;
// This is the canonical artifact/receipt contract version, not the number of
// physical Doris migrations applied to the database.
export const SUPPORTED_DORIS_SCHEMA_VERSIONS =
  ANALYTICS_CONTRACT_COMPATIBILITY.readableSchemaVersions;

export type DorisMigrationLedgerEntry = {
  readonly name: string;
  readonly checksum: string;
};

export const APPROVED_DORIS_ADDITIVE_MIGRATION_SUFFIXES: readonly (readonly DorisMigrationLedgerEntry[])[] =
  [
    [
      {
        name: "0005_add_experiment_storage_foundation.sql",
        checksum:
          "f0f32a99fb10bde178f8e256dac7923eca08c44452ea573d04e644bbcbc9a44f",
      },
    ],
  ];

export const LATEST_APPROVED_DORIS_MIGRATIONS: readonly DorisMigrationLedgerEntry[] =
  [
    ...EXPECTED_DORIS_MIGRATIONS,
    ...(APPROVED_DORIS_ADDITIVE_MIGRATION_SUFFIXES.at(-1) ?? []),
  ];

export type DorisMigrationCompatibility =
  | { readonly status: "CURRENT"; readonly suffixLength: 0 }
  | {
      readonly status: "APPROVED_ADDITIVE_SUFFIX";
      readonly suffixLength: number;
    }
  | { readonly status: "INCOMPATIBLE"; readonly suffixLength: null };

const MIGRATION_NAME = /^[0-9]{4}_[a-z0-9_]+\.sql$/;
const SHA256_HEX = /^[a-f0-9]{64}$/;

function migrationLedgersMatch(
  left: readonly DorisMigrationLedgerEntry[],
  right: readonly DorisMigrationLedgerEntry[],
): boolean {
  return (
    left.length === right.length &&
    left.every(
      (migration, index) =>
        migration.name === right[index]?.name &&
        migration.checksum === right[index]?.checksum,
    )
  );
}

function isValidAdditiveSuffix(input: {
  readonly current: readonly DorisMigrationLedgerEntry[];
  readonly suffix: readonly DorisMigrationLedgerEntry[];
}): boolean {
  if (input.suffix.length === 0) return false;
  const names = [...input.current, ...input.suffix].map(({ name }) => name);
  return (
    new Set(names).size === names.length &&
    names.every((name) => MIGRATION_NAME.test(name)) &&
    [...input.current, ...input.suffix].every(({ checksum }) =>
      SHA256_HEX.test(checksum),
    ) &&
    names.every((name, index) => index === 0 || name > names[index - 1]!)
  );
}

export function classifyDorisMigrationLedger(input: {
  readonly actual: readonly DorisMigrationLedgerEntry[];
  readonly current: readonly DorisMigrationLedgerEntry[];
  readonly approvedAdditiveSuffixes: readonly (readonly DorisMigrationLedgerEntry[])[];
}): DorisMigrationCompatibility {
  if (migrationLedgersMatch(input.actual, input.current)) {
    return { status: "CURRENT", suffixLength: 0 };
  }
  for (const suffix of input.approvedAdditiveSuffixes) {
    if (
      isValidAdditiveSuffix({ current: input.current, suffix }) &&
      migrationLedgersMatch(input.actual, [...input.current, ...suffix])
    ) {
      return {
        status: "APPROVED_ADDITIVE_SUFFIX",
        suffixLength: suffix.length,
      };
    }
  }
  return { status: "INCOMPATIBLE", suffixLength: null };
}

export type DorisReadinessCode =
  | "READY"
  | "ANALYTICS_UNAVAILABLE"
  | "SCHEMA_MISMATCH";

export interface DorisReadinessResult {
  readonly ready: boolean;
  readonly code: DorisReadinessCode;
  readonly schemaVersion: number | null;
}

const PHYSICAL_SCHEMA_FINGERPRINTS: Readonly<
  Record<string, readonly string[]>
> = {
  events_current: [
    "UNIQUE KEY (project_id, partition_date, trace_id, span_id)",
    "AUTO PARTITION BY RANGE (date_trunc(partition_date, 'day'))",
    '"enable_unique_key_merge_on_write"="true"',
    '"function_column.sequence_col"="version_token"',
    "INDEX idx_inv_input",
    "INDEX idx_inv_output",
    "INDEX idx_inv_name",
    "INDEX idx_ng_input",
    "INDEX idx_ng_output",
    "status_message TEXT",
    // Doris canonicalizes STRING to TEXT in SHOW CREATE TABLE output.
    "metadata_json TEXT",
    "usage_details_json TEXT",
    "cost_details_json TEXT",
    "provided_usage_details_json TEXT",
    "provided_cost_details_json TEXT",
    "model_parameters_json TEXT",
    "tool_definitions_json TEXT",
  ],
  scores_current: [
    "UNIQUE KEY (project_id, score_date, score_id)",
    "AUTO PARTITION BY RANGE (date_trunc(score_date, 'day'))",
    '"enable_unique_key_merge_on_write"="true"',
    '"function_column.sequence_col"="version_token"',
    "metadata_json TEXT",
  ],
  blob_storage_file_log: [
    "UNIQUE KEY (project_id, file_date, entity_type, entity_id, file_id)",
    "AUTO PARTITION BY RANGE (date_trunc(file_date, 'day'))",
    '"enable_unique_key_merge_on_write"="true"',
    '"function_column.sequence_col"="version_token"',
  ],
  trace_tombstones: [
    "UNIQUE KEY (project_id, trace_id)",
    '"enable_unique_key_merge_on_write"="true"',
    '"function_column.sequence_col"="deletion_generation"',
  ],
  project_tombstones: [
    "UNIQUE KEY (project_id)",
    '"enable_unique_key_merge_on_write"="true"',
    '"function_column.sequence_col"="deletion_generation"',
  ],
};

const ADDITIVE_PHYSICAL_SCHEMA_FINGERPRINTS: Readonly<
  Record<string, Readonly<Record<string, readonly string[]>>>
> = {
  "0005_add_experiment_storage_foundation.sql": {
    events_current: [
      "experiment_id VARCHAR(64)",
      "experiment_name VARCHAR(512)",
      "experiment_metadata VARIANT",
      "experiment_metadata_json TEXT",
      "experiment_description TEXT",
      "experiment_dataset_id VARCHAR(64)",
      "experiment_item_id VARCHAR(64)",
      "experiment_item_version DATETIME(6)",
      "experiment_item_expected_output TEXT",
      "experiment_item_metadata VARIANT",
      "experiment_item_metadata_json TEXT",
      "experiment_item_root_span_id VARCHAR(128)",
    ],
    scores_current: [
      "dataset_run_id VARCHAR(64)",
      "execution_trace_id VARCHAR(64)",
    ],
    dataset_run_items_current: [
      "UNIQUE KEY (project_id, run_item_date, run_item_id)",
      "AUTO PARTITION BY RANGE (date_trunc(run_item_date, 'day'))",
      '"enable_unique_key_merge_on_write"="true"',
      '"function_column.sequence_col"="version_token"',
      "dataset_run_metadata_json TEXT",
      "dataset_item_metadata_json TEXT",
      "dataset_deletion_generation BIGINT",
      "run_deletion_generation BIGINT",
    ],
    dataset_tombstones: [
      "UNIQUE KEY (project_id, dataset_id)",
      '"enable_unique_key_merge_on_write"="true"',
      '"function_column.sequence_col"="deletion_generation"',
    ],
    dataset_run_tombstones: [
      "UNIQUE KEY (project_id, dataset_run_id)",
      '"enable_unique_key_merge_on_write"="true"',
      '"function_column.sequence_col"="deletion_generation"',
    ],
  },
};

function normalizeDdl(value: string): string {
  return value.toLowerCase().replaceAll(/[`"\s]/g, "");
}

async function physicalSchemaMatches(
  executor: DorisQueryExecutor,
  actualMigrations: readonly DorisMigrationLedgerEntry[],
): Promise<boolean> {
  const fingerprints = new Map<string, string[]>(
    Object.entries(PHYSICAL_SCHEMA_FINGERPRINTS).map(([table, fragments]) => [
      table,
      [...fragments],
    ]),
  );
  for (const { name } of actualMigrations) {
    for (const [table, fragments] of Object.entries(
      ADDITIVE_PHYSICAL_SCHEMA_FINGERPRINTS[name] ?? {},
    )) {
      fingerprints.set(table, [
        ...(fingerprints.get(table) ?? []),
        ...fragments,
      ]);
    }
  }

  for (const [table, fragments] of fingerprints) {
    const rows = await executor.query<Record<string, unknown>>(
      `SHOW CREATE TABLE \`${table}\``,
    );
    const ddl = Object.values(rows[0] ?? {}).find(
      (value): value is string =>
        typeof value === "string" && /create\s+table/i.test(value),
    );
    if (!ddl) return false;
    const normalizedDdl = normalizeDdl(ddl);
    if (
      fragments.some(
        (fragment) => !normalizedDdl.includes(normalizeDdl(fragment)),
      )
    ) {
      return false;
    }
  }
  return true;
}

export interface AnalyticsCompatibilityQuery {
  readonly supportedCanonicalizerVersions: readonly string[];
  readonly supportedSchemaVersions: readonly number[];
  readonly now: Date;
}

export interface AnalyticsCompatibilityControlState {
  countIncompatibleRecoverableOperations(
    query: AnalyticsCompatibilityQuery,
  ): Promise<number>;
}

export async function checkDorisReadiness(
  executor: DorisQueryExecutor,
  migrationWindow: {
    readonly currentMigrations?: readonly DorisMigrationLedgerEntry[];
    readonly approvedAdditiveSuffixes?: readonly (readonly DorisMigrationLedgerEntry[])[];
  } = {},
): Promise<DorisReadinessResult> {
  try {
    const versionRows = await executor.query<{ versionComment: string }>(
      "SELECT @@version_comment AS versionComment",
    );
    if (!/doris-4\.0\.7(?:\D|$)/i.test(versionRows[0]?.versionComment ?? "")) {
      return {
        ready: false,
        code: "SCHEMA_MISMATCH",
        schemaVersion: null,
      };
    }

    const rows = await executor.query<{ name: string; checksum: string }>(
      "SELECT name, checksum FROM _langfuse_schema_migrations ORDER BY name",
    );
    const migrationCompatibility = classifyDorisMigrationLedger({
      actual: rows,
      current: migrationWindow.currentMigrations ?? EXPECTED_DORIS_MIGRATIONS,
      approvedAdditiveSuffixes:
        migrationWindow.approvedAdditiveSuffixes ??
        APPROVED_DORIS_ADDITIVE_MIGRATION_SUFFIXES,
    });

    if (
      migrationCompatibility.status === "INCOMPATIBLE" ||
      !(await physicalSchemaMatches(executor, rows))
    ) {
      return {
        ready: false,
        code: "SCHEMA_MISMATCH",
        schemaVersion: rows.length,
      };
    }

    return { ready: true, code: "READY", schemaVersion: rows.length };
  } catch {
    return {
      ready: false,
      code: "ANALYTICS_UNAVAILABLE",
      schemaVersion: null,
    };
  }
}

export async function checkAnalyticsReadiness(input: {
  readonly executor: DorisQueryExecutor;
  readonly controlState: AnalyticsCompatibilityControlState;
  readonly supportedCanonicalizerVersions: readonly string[];
  readonly supportedSchemaVersions: readonly number[];
  readonly now?: Date;
}): Promise<DorisReadinessResult> {
  const doris = await checkDorisReadiness(input.executor);
  if (!doris.ready) return doris;

  try {
    const incompatible =
      await input.controlState.countIncompatibleRecoverableOperations({
        supportedCanonicalizerVersions: input.supportedCanonicalizerVersions,
        supportedSchemaVersions: input.supportedSchemaVersions,
        now: input.now ?? new Date(),
      });
    if (incompatible > 0) {
      return {
        ready: false,
        code: "SCHEMA_MISMATCH",
        schemaVersion: doris.schemaVersion,
      };
    }
    return doris;
  } catch {
    return {
      ready: false,
      code: "ANALYTICS_UNAVAILABLE",
      schemaVersion: doris.schemaVersion,
    };
  }
}
