import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";

import { describe, expect, it } from "vitest";

import {
  APPROVED_DORIS_ADDITIVE_MIGRATION_SUFFIXES,
  EXPECTED_DORIS_MIGRATIONS,
} from "../readiness";

const baseline = readFileSync(
  path.resolve(__dirname, "../../../../doris/migrations/0001_baseline.sql"),
  "utf8",
);
const refreeze = readFileSync(
  path.resolve(
    __dirname,
    "../../../../doris/migrations/0002_refreeze_r1a_partition_and_tombstone_order.sql",
  ),
  "utf8",
);
const statusMessageExpansion = readFileSync(
  path.resolve(
    __dirname,
    "../../../../doris/migrations/0003_expand_events_status_message.sql",
  ),
  "utf8",
);
const dynamicJsonPreservation = readFileSync(
  path.resolve(
    __dirname,
    "../../../../doris/migrations/0004_preserve_dynamic_json_keys.sql",
  ),
  "utf8",
);
const experimentStorageFoundation = readFileSync(
  path.resolve(
    __dirname,
    "../../../../doris/migrations/0005_add_experiment_storage_foundation.sql",
  ),
  "utf8",
);

describe("Doris R1A schema contract", () => {
  it("requires the forward events status-message expansion", () => {
    expect(EXPECTED_DORIS_MIGRATIONS.map(({ name }) => name)).toContain(
      "0003_expand_events_status_message.sql",
    );
    expect(statusMessageExpansion).toContain(
      "ADD COLUMN status_message STRING NULL",
    );
  });

  it("preserves exact dynamic JSON beside path-query VARIANT columns", () => {
    expect(EXPECTED_DORIS_MIGRATIONS.map(({ name }) => name)).toContain(
      "0004_preserve_dynamic_json_keys.sql",
    );
    for (const column of [
      "metadata_json",
      "usage_details_json",
      "cost_details_json",
      "provided_usage_details_json",
      "provided_cost_details_json",
      "model_parameters_json",
      "tool_definitions_json",
    ]) {
      expect(dynamicJsonPreservation).toContain(
        `ALTER TABLE events_current ADD COLUMN ${column} STRING NULL`,
      );
    }
    expect(dynamicJsonPreservation).toContain(
      "ALTER TABLE scores_current ADD COLUMN metadata_json STRING NULL",
    );
  });

  it("does not enable global retention in dynamic partition settings", () => {
    expect(refreeze).not.toContain("dynamic_partition.");
    expect(refreeze).not.toContain("partition.retention_count");
  });

  it.each(["partition_date", "score_date", "file_date"])(
    "creates historical %s partitions on demand",
    (column) => {
      expect(refreeze).toContain(
        `AUTO PARTITION BY RANGE (date_trunc(\`${column}\`, 'day'))`,
      );
    },
  );
  it.each(["trace_tombstones", "project_tombstones"])(
    "%s uses deletion_generation as the sequence column",
    (table) => {
      const tableDdl = refreeze.match(
        new RegExp(
          `CREATE TABLE IF NOT EXISTS ${table}([\\s\\S]*?)(?=CREATE TABLE IF NOT EXISTS|$)`,
        ),
      )?.[1];
      expect(tableDdl).toContain(
        '"function_column.sequence_col" = "deletion_generation"',
      );
    },
  );

  it("adds experiment storage only as an approved additive suffix", () => {
    expect(
      APPROVED_DORIS_ADDITIVE_MIGRATION_SUFFIXES.flat().map(({ name }) => name),
    ).toContain("0005_add_experiment_storage_foundation.sql");
    expect(experimentStorageFoundation).toContain(
      "CREATE TABLE IF NOT EXISTS dataset_run_items_current",
    );
    expect(experimentStorageFoundation).toContain(
      "UNIQUE KEY (project_id, run_item_date, run_item_id)",
    );
    for (const column of [
      "experiment_id",
      "experiment_name",
      "experiment_metadata",
      "experiment_description",
      "experiment_dataset_id",
      "experiment_item_id",
      "experiment_item_version",
      "experiment_item_expected_output",
      "experiment_item_metadata",
      "experiment_item_root_span_id",
    ]) {
      expect(experimentStorageFoundation).toContain(
        `ALTER TABLE events_current ADD COLUMN ${column}`,
      );
    }
    expect(experimentStorageFoundation).toContain(
      "ALTER TABLE scores_current ADD COLUMN dataset_run_id",
    );
    expect(experimentStorageFoundation).toContain(
      "ALTER TABLE scores_current ADD COLUMN execution_trace_id",
    );
  });

  it("keeps readiness checksums pinned to every immutable migration", () => {
    expect(
      [baseline, refreeze, statusMessageExpansion, dynamicJsonPreservation].map(
        (sql) => createHash("sha256").update(sql).digest("hex"),
      ),
    ).toEqual(EXPECTED_DORIS_MIGRATIONS.map(({ checksum }) => checksum));
    expect(
      createHash("sha256").update(experimentStorageFoundation).digest("hex"),
    ).toBe(APPROVED_DORIS_ADDITIVE_MIGRATION_SUFFIXES[0]?.[0]?.checksum);
  });
});
