import { describe, expect, it, vi } from "vitest";

import type { DorisQueryExecutor } from "../client";
import {
  APPROVED_DORIS_ADDITIVE_MIGRATION_SUFFIXES,
  checkAnalyticsReadiness,
  checkDorisReadiness,
  classifyDorisMigrationLedger,
  EXPECTED_DORIS_MIGRATIONS,
  SUPPORTED_DORIS_SCHEMA_VERSIONS,
} from "../readiness";
import {
  CURRENT_ANALYTICS_SCHEMA_VERSION,
  NEXT_ANALYTICS_SCHEMA_VERSION,
} from "../../analytics-persistence/versions";

const createTableByName: Record<string, string> = {
  events_current: `
    CREATE TABLE events_current (... status_message TEXT,
      metadata_json TEXT, usage_details_json TEXT, cost_details_json TEXT,
      provided_usage_details_json TEXT, provided_cost_details_json TEXT,
      model_parameters_json TEXT, tool_definitions_json TEXT,
      experiment_id VARCHAR(64), experiment_name VARCHAR(512),
      experiment_metadata VARIANT, experiment_metadata_json TEXT,
      experiment_description TEXT, experiment_dataset_id VARCHAR(64),
      experiment_item_id VARCHAR(64), experiment_item_version DATETIME(6),
      experiment_item_expected_output TEXT, experiment_item_metadata VARIANT,
      experiment_item_metadata_json TEXT,
      experiment_item_root_span_id VARCHAR(128),
      INDEX idx_inv_input (input),
      INDEX idx_inv_output (output), INDEX idx_inv_name (name),
      INDEX idx_ng_input (input), INDEX idx_ng_output (output))
    UNIQUE KEY (project_id, partition_date, trace_id, span_id)
    AUTO PARTITION BY RANGE (date_trunc(partition_date, 'day')) ()
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="version_token")`,
  scores_current: `
    CREATE TABLE scores_current (... metadata_json TEXT,
      dataset_run_id VARCHAR(64), execution_trace_id VARCHAR(64))
    UNIQUE KEY (project_id, score_date, score_id)
    AUTO PARTITION BY RANGE (date_trunc(score_date, 'day')) ()
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="version_token")`,
  blob_storage_file_log: `
    CREATE TABLE blob_storage_file_log (...)
    UNIQUE KEY (project_id, file_date, entity_type, entity_id, file_id)
    AUTO PARTITION BY RANGE (date_trunc(file_date, 'day')) ()
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="version_token")`,
  trace_tombstones: `
    CREATE TABLE trace_tombstones (...)
    UNIQUE KEY (project_id, trace_id)
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="deletion_generation")`,
  project_tombstones: `
    CREATE TABLE project_tombstones (...)
    UNIQUE KEY (project_id)
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="deletion_generation")`,
  dataset_run_items_current: `
    CREATE TABLE dataset_run_items_current (... dataset_run_metadata_json TEXT,
      dataset_item_metadata_json TEXT, dataset_deletion_generation BIGINT,
      run_deletion_generation BIGINT)
    UNIQUE KEY (project_id, run_item_date, run_item_id)
    AUTO PARTITION BY RANGE (date_trunc(run_item_date, 'day')) ()
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="version_token")`,
  dataset_tombstones: `
    CREATE TABLE dataset_tombstones (...)
    UNIQUE KEY (project_id, dataset_id)
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="deletion_generation")`,
  dataset_run_tombstones: `
    CREATE TABLE dataset_run_tombstones (...)
    UNIQUE KEY (project_id, dataset_run_id)
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="deletion_generation")`,
};

function executorWith(overrides?: {
  readonly version?: string;
  readonly migrationChecksum?: string;
  readonly migrations?: readonly { name: string; checksum: string }[];
  readonly tableDdl?: Readonly<Record<string, string>>;
}): DorisQueryExecutor {
  const query = vi.fn(async (sql: string) => {
    if (sql.includes("@@version_comment")) {
      return [
        { versionComment: overrides?.version ?? "doris version doris-4.0.7" },
      ];
    }
    if (sql.includes("_langfuse_schema_migrations")) {
      return (
        overrides?.migrations ??
        EXPECTED_DORIS_MIGRATIONS.map(({ name, checksum }) => ({
          name,
          checksum: overrides?.migrationChecksum ?? checksum,
        }))
      );
    }
    const table = Object.keys(createTableByName).find((name) =>
      sql.includes(`\`${name}\``),
    );
    if (!table) throw new Error("unexpected query");
    return [
      {
        "Create Table":
          overrides?.tableDdl?.[table] ?? createTableByName[table],
      },
    ];
  });
  return { query: query as unknown as DorisQueryExecutor["query"] };
}

describe("Doris schema readiness", () => {
  it("uses the current canonical receipt schema rather than the migration count", () => {
    expect(SUPPORTED_DORIS_SCHEMA_VERSIONS).toEqual([
      CURRENT_ANALYTICS_SCHEMA_VERSION,
      NEXT_ANALYTICS_SCHEMA_VERSION,
    ]);
  });

  it("is ready only when migration checksums and physical fingerprints match", async () => {
    await expect(checkDorisReadiness(executorWith())).resolves.toEqual({
      ready: true,
      code: "READY",
      schemaVersion: EXPECTED_DORIS_MIGRATIONS.length,
    });
  });

  it("accepts only an explicitly approved additive migration suffix", async () => {
    const approvedSuffix = [
      { name: "0004_release_a_expand.sql", checksum: "d".repeat(64) },
    ] as const;
    const migrations = [...EXPECTED_DORIS_MIGRATIONS, ...approvedSuffix];

    expect(
      classifyDorisMigrationLedger({
        actual: migrations,
        current: EXPECTED_DORIS_MIGRATIONS,
        approvedAdditiveSuffixes: [approvedSuffix],
      }),
    ).toEqual({ status: "APPROVED_ADDITIVE_SUFFIX", suffixLength: 1 });
    await expect(
      checkDorisReadiness(executorWith({ migrations }), {
        currentMigrations: EXPECTED_DORIS_MIGRATIONS,
        approvedAdditiveSuffixes: [approvedSuffix],
      }),
    ).resolves.toMatchObject({ ready: true, code: "READY" });

    expect(
      classifyDorisMigrationLedger({
        actual: [
          ...EXPECTED_DORIS_MIGRATIONS,
          { name: "0004_unknown.sql", checksum: "e".repeat(64) },
        ],
        current: EXPECTED_DORIS_MIGRATIONS,
        approvedAdditiveSuffixes: [approvedSuffix],
      }),
    ).toEqual({ status: "INCOMPATIBLE", suffixLength: null });
    await expect(
      checkDorisReadiness(
        executorWith({
          migrations: [
            ...EXPECTED_DORIS_MIGRATIONS,
            { name: "0004_unknown.sql", checksum: "e".repeat(64) },
          ],
        }),
      ),
    ).resolves.toMatchObject({ ready: false, code: "SCHEMA_MISMATCH" });
  });

  it("requires the experiment physical schema when its approved suffix is present", async () => {
    const foundationSuffix = APPROVED_DORIS_ADDITIVE_MIGRATION_SUFFIXES[0]!;
    const migrations = [...EXPECTED_DORIS_MIGRATIONS, ...foundationSuffix];

    await expect(
      checkDorisReadiness(executorWith({ migrations })),
    ).resolves.toMatchObject({ ready: true, code: "READY" });

    await expect(
      checkDorisReadiness(
        executorWith({
          migrations,
          tableDdl: {
            ...createTableByName,
            dataset_run_items_current:
              createTableByName.dataset_run_items_current.replace(
                '"version_token"',
                '"wrong_token"',
              ),
          },
        }),
      ),
    ).resolves.toMatchObject({ ready: false, code: "SCHEMA_MISMATCH" });
  });

  it("reports migration or physical schema drift as a safe mismatch", async () => {
    await expect(
      checkDorisReadiness(executorWith({ migrationChecksum: "wrong" })),
    ).resolves.toMatchObject({ ready: false, code: "SCHEMA_MISMATCH" });

    await expect(
      checkDorisReadiness(
        executorWith({
          tableDdl: {
            ...createTableByName,
            events_current: createTableByName.events_current.replace(
              '"version_token"',
              '"wrong_token"',
            ),
          },
        }),
      ),
    ).resolves.toMatchObject({ ready: false, code: "SCHEMA_MISMATCH" });
  });

  it("blocks when a recoverable receipt requires another contract version", async () => {
    const controlState = {
      countIncompatibleRecoverableOperations: vi.fn().mockResolvedValue(1),
    };

    await expect(
      checkAnalyticsReadiness({
        executor: executorWith(),
        controlState,
        supportedCanonicalizerVersions: ["1"],
        supportedSchemaVersions: [2],
        now: new Date("2026-07-18T12:00:00.000Z"),
      }),
    ).resolves.toMatchObject({ ready: false, code: "SCHEMA_MISMATCH" });
    expect(
      controlState.countIncompatibleRecoverableOperations,
    ).toHaveBeenCalledWith({
      supportedCanonicalizerVersions: ["1"],
      supportedSchemaVersions: [2],
      now: new Date("2026-07-18T12:00:00.000Z"),
    });
  });

  it("sanitizes transport failures", async () => {
    const executor = {
      query: vi
        .fn()
        .mockRejectedValue(
          new Error("connect ECONNREFUSED db.internal:9030 password=secret"),
        ),
    };

    await expect(checkDorisReadiness(executor)).resolves.toEqual({
      ready: false,
      code: "ANALYTICS_UNAVAILABLE",
      schemaVersion: null,
    });
  });
});
