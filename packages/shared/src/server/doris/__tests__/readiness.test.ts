import { describe, expect, it, vi } from "vitest";

import type { DorisQueryExecutor } from "../client";
import {
  checkAnalyticsReadiness,
  checkDorisReadiness,
  EXPECTED_DORIS_MIGRATIONS,
} from "../readiness";

const createTableByName: Record<string, string> = {
  events_current: `
    CREATE TABLE events_current (... status_message TEXT,
      INDEX idx_inv_input (input),
      INDEX idx_inv_output (output), INDEX idx_inv_name (name),
      INDEX idx_ng_input (input), INDEX idx_ng_output (output))
    UNIQUE KEY (project_id, partition_date, trace_id, span_id)
    AUTO PARTITION BY RANGE (date_trunc(partition_date, 'day')) ()
    PROPERTIES ("enable_unique_key_merge_on_write"="true",
      "function_column.sequence_col"="version_token")`,
  scores_current: `
    CREATE TABLE scores_current (...)
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
};

function executorWith(overrides?: {
  readonly version?: string;
  readonly migrationChecksum?: string;
  readonly tableDdl?: Readonly<Record<string, string>>;
}): DorisQueryExecutor {
  const query = vi.fn(async (sql: string) => {
    if (sql.includes("@@version_comment")) {
      return [
        { versionComment: overrides?.version ?? "doris version doris-4.0.7" },
      ];
    }
    if (sql.includes("_langfuse_schema_migrations")) {
      return EXPECTED_DORIS_MIGRATIONS.map(({ name, checksum }) => ({
        name,
        checksum: overrides?.migrationChecksum ?? checksum,
      }));
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
  it("is ready only when migration checksums and physical fingerprints match", async () => {
    await expect(checkDorisReadiness(executorWith())).resolves.toEqual({
      ready: true,
      code: "READY",
      schemaVersion: EXPECTED_DORIS_MIGRATIONS.length,
    });
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
