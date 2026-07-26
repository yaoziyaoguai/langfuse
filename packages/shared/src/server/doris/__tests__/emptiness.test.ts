import { describe, expect, it, vi } from "vitest";

import type { DorisQueryExecutor } from "../client";
import {
  DORIS_ANALYTICS_PHYSICAL_TABLES,
  probeDorisAnalyticsBackendEmptiness,
} from "../emptiness";
import { EXPECTED_DORIS_MIGRATIONS } from "../readiness";

const createTableByName: Readonly<Record<string, string>> = {
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

type FakeDorisOptions = {
  readonly catalog?: readonly {
    readonly tableName: string;
    readonly tableType: string;
  }[];
  readonly occupiedTables?: readonly string[];
  readonly version?: string;
};

function fakeDorisExecutor(options: FakeDorisOptions = {}): {
  readonly executor: DorisQueryExecutor;
  readonly query: ReturnType<typeof vi.fn>;
} {
  const catalog = options.catalog ?? [
    ...DORIS_ANALYTICS_PHYSICAL_TABLES.map((tableName) => ({
      tableName,
      tableType: "BASE TABLE",
    })),
    {
      tableName: "_langfuse_schema_migrations",
      tableType: "BASE TABLE",
    },
  ];
  const occupiedTables = new Set(options.occupiedTables ?? []);
  const query = vi.fn(async (sql: string) => {
    if (sql.includes("@@version_comment")) {
      return [
        {
          versionComment:
            options.version ?? "Apache Doris version doris-4.0.7-release",
        },
      ];
    }
    if (
      sql.includes("_langfuse_schema_migrations") &&
      !sql.includes("information_schema")
    ) {
      return EXPECTED_DORIS_MIGRATIONS.map(({ name, checksum }) => ({
        name,
        checksum,
      }));
    }
    if (sql.includes("SHOW CREATE TABLE")) {
      const table = Object.keys(createTableByName).find((name) =>
        sql.includes(`\`${name}\``),
      );
      if (!table) throw new Error("unexpected SHOW CREATE TABLE");
      return [{ "Create Table": createTableByName[table] }];
    }
    if (sql.includes("information_schema.tables")) return catalog;
    if (sql.includes("SELECT 1 AS hasRows")) {
      const table = DORIS_ANALYTICS_PHYSICAL_TABLES.find((name) =>
        sql.includes(`\`${name}\``),
      );
      if (!table) throw new Error("unexpected data probe");
      return occupiedTables.has(table) ? [{ hasRows: 1 }] : [];
    }
    throw new Error(`unexpected query: ${sql}`);
  });

  return {
    executor: { query: query as unknown as DorisQueryExecutor["query"] },
    query,
  };
}

describe("Doris analytics backend emptiness", () => {
  it("proves emptiness only after readiness, exact catalog, and all five table probes", async () => {
    const { executor, query } = fakeDorisExecutor();

    await expect(
      probeDorisAnalyticsBackendEmptiness({ executor }),
    ).resolves.toMatchObject({
      empty: true,
      evidenceDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });

    expect(
      query.mock.calls.filter(([sql]) =>
        String(sql).includes("SELECT 1 AS hasRows"),
      ),
    ).toHaveLength(5);
  });

  it("reports non-empty when any frozen physical table has a row", async () => {
    const { executor } = fakeDorisExecutor({
      occupiedTables: ["trace_tombstones"],
    });

    await expect(
      probeDorisAnalyticsBackendEmptiness({ executor }),
    ).resolves.toMatchObject({ empty: false });
  });

  it("fails closed for missing, unknown, or non-base catalog objects", async () => {
    const exact = fakeDorisExecutor().executor;
    const missing = fakeDorisExecutor({
      catalog: [
        ...DORIS_ANALYTICS_PHYSICAL_TABLES.slice(1).map((tableName) => ({
          tableName,
          tableType: "BASE TABLE",
        })),
        {
          tableName: "_langfuse_schema_migrations",
          tableType: "BASE TABLE",
        },
      ],
    }).executor;
    const unknown = fakeDorisExecutor({
      catalog: [
        ...DORIS_ANALYTICS_PHYSICAL_TABLES.map((tableName) => ({
          tableName,
          tableType: "BASE TABLE",
        })),
        {
          tableName: "_langfuse_schema_migrations",
          tableType: "BASE TABLE",
        },
        { tableName: "operator_backup", tableType: "BASE TABLE" },
      ],
    }).executor;
    const viewReplacement = fakeDorisExecutor({
      catalog: [
        ...DORIS_ANALYTICS_PHYSICAL_TABLES.map((tableName) => ({
          tableName,
          tableType: tableName === "events_current" ? "VIEW" : "BASE TABLE",
        })),
        {
          tableName: "_langfuse_schema_migrations",
          tableType: "BASE TABLE",
        },
      ],
    }).executor;

    await expect(
      probeDorisAnalyticsBackendEmptiness({ executor: exact }),
    ).resolves.toMatchObject({ empty: true });
    await expect(
      probeDorisAnalyticsBackendEmptiness({ executor: missing }),
    ).resolves.toMatchObject({ empty: false });
    await expect(
      probeDorisAnalyticsBackendEmptiness({ executor: unknown }),
    ).resolves.toMatchObject({ empty: false });
    await expect(
      probeDorisAnalyticsBackendEmptiness({ executor: viewReplacement }),
    ).resolves.toMatchObject({ empty: false });
  });

  it("fails closed before catalog inspection when readiness is not proven", async () => {
    const { executor, query } = fakeDorisExecutor({ version: "Doris 4.0.6" });

    await expect(
      probeDorisAnalyticsBackendEmptiness({ executor }),
    ).resolves.toMatchObject({ empty: false });
    expect(
      query.mock.calls.some(([sql]) =>
        String(sql).includes("information_schema.tables"),
      ),
    ).toBe(false);
  });

  it("produces stable evidence independent of catalog row order", async () => {
    const catalog = [
      ...DORIS_ANALYTICS_PHYSICAL_TABLES.map((tableName) => ({
        tableName,
        tableType: "BASE TABLE",
      })),
      {
        tableName: "_langfuse_schema_migrations",
        tableType: "BASE TABLE",
      },
    ];
    const first = await probeDorisAnalyticsBackendEmptiness({
      executor: fakeDorisExecutor({ catalog }).executor,
    });
    const second = await probeDorisAnalyticsBackendEmptiness({
      executor: fakeDorisExecutor({ catalog: [...catalog].reverse() }).executor,
    });

    expect(first.evidenceDigest).toBe(second.evidenceDigest);
  });
});
