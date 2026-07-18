import type { DorisQueryExecutor } from "./client";

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
] as const;

export const SUPPORTED_DORIS_CANONICALIZER_VERSIONS = ["1"] as const;
export const SUPPORTED_DORIS_SCHEMA_VERSIONS = [2] as const;

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
  ],
  scores_current: [
    "UNIQUE KEY (project_id, score_date, score_id)",
    "AUTO PARTITION BY RANGE (date_trunc(score_date, 'day'))",
    '"enable_unique_key_merge_on_write"="true"',
    '"function_column.sequence_col"="version_token"',
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

function normalizeDdl(value: string): string {
  return value.toLowerCase().replaceAll(/[`"\s]/g, "");
}

async function physicalSchemaMatches(
  executor: DorisQueryExecutor,
): Promise<boolean> {
  for (const [table, fragments] of Object.entries(
    PHYSICAL_SCHEMA_FINGERPRINTS,
  )) {
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
    const matches =
      rows.length === EXPECTED_DORIS_MIGRATIONS.length &&
      rows.every(
        (row, index) =>
          row.name === EXPECTED_DORIS_MIGRATIONS[index]?.name &&
          row.checksum === EXPECTED_DORIS_MIGRATIONS[index]?.checksum,
      );

    if (!matches || !(await physicalSchemaMatches(executor))) {
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
