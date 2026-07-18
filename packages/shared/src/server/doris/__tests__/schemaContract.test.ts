import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";

import { describe, expect, it } from "vitest";

import { EXPECTED_DORIS_MIGRATIONS } from "../readiness";

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

describe("Doris R1A schema contract", () => {
  it("requires the forward events status-message expansion", () => {
    expect(EXPECTED_DORIS_MIGRATIONS.map(({ name }) => name)).toContain(
      "0003_expand_events_status_message.sql",
    );
    expect(statusMessageExpansion).toContain(
      "ADD COLUMN status_message STRING NULL",
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

  it("keeps R1B dataset-run analytics absent", () => {
    expect(refreeze).not.toMatch(
      /CREATE TABLE IF NOT EXISTS dataset_run_items_current/,
    );
  });

  it("keeps readiness checksums pinned to every immutable migration", () => {
    expect(
      [baseline, refreeze, statusMessageExpansion].map((sql) =>
        createHash("sha256").update(sql).digest("hex"),
      ),
    ).toEqual(EXPECTED_DORIS_MIGRATIONS.map(({ checksum }) => checksum));
  });
});
