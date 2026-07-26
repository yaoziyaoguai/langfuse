import { describe, expect, it, vi } from "vitest";

import {
  CLICKHOUSE_OPTIONAL_ANALYTICS_PHYSICAL_TABLES,
  CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES,
  type ClickHouseEmptinessQueryExecutor,
  probeClickHouseAnalyticsBackendEmptiness,
} from "./emptiness";

type CatalogRow = {
  readonly name: string;
  readonly engine: string;
};

const requiredCatalog: readonly CatalogRow[] = [
  { name: "schema_migrations", engine: "MergeTree" },
  ...CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES.map((name) => ({
    name,
    engine: "ReplacingMergeTree",
  })),
  { name: "analytics_traces", engine: "View" },
  { name: "analytics_observations", engine: "View" },
  { name: "analytics_scores", engine: "View" },
];

type FakeClickHouseOptions = {
  readonly migrationRows?: readonly {
    readonly version: string | number;
    readonly dirty: string | number;
  }[];
  readonly catalog?: readonly CatalogRow[];
  readonly activeParts?: readonly {
    readonly table: string;
    readonly activePartCount: string | number;
  }[];
  readonly failure?: Error;
};

function fakeClickHouseExecutor(options: FakeClickHouseOptions = {}): {
  readonly executor: ClickHouseEmptinessQueryExecutor;
  readonly query: ReturnType<typeof vi.fn>;
} {
  const query = vi.fn(async (sql: string) => {
    if (options.failure) throw options.failure;
    if (sql.includes("FROM schema_migrations")) {
      return options.migrationRows ?? [{ version: "36", dirty: 0 }];
    }
    if (sql.includes("FROM system.tables")) {
      return options.catalog ?? requiredCatalog;
    }
    if (sql.includes("FROM system.parts")) {
      return options.activeParts ?? [];
    }
    throw new Error(`unexpected query: ${sql}`);
  });
  return {
    executor: {
      query: query as unknown as ClickHouseEmptinessQueryExecutor["query"],
    },
    query,
  };
}

describe("ClickHouse analytics backend emptiness", () => {
  it("proves an exact migration-v36 catalog with no active analytics parts empty", async () => {
    const { executor, query } = fakeClickHouseExecutor();

    await expect(
      probeClickHouseAnalyticsBackendEmptiness({ executor }),
    ).resolves.toMatchObject({
      empty: true,
      evidenceDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    expect(
      query.mock.calls.some(([sql]) => String(sql).includes("system.parts")),
    ).toBe(true);
  });

  it("treats any active part in a required or present optional table as non-empty", async () => {
    const optionalTable = CLICKHOUSE_OPTIONAL_ANALYTICS_PHYSICAL_TABLES[0];
    const catalog = [
      ...requiredCatalog,
      { name: optionalTable, engine: "ReplacingMergeTree" },
    ];

    const requiredResult = await probeClickHouseAnalyticsBackendEmptiness({
      executor: fakeClickHouseExecutor({
        activeParts: [{ table: "traces", activePartCount: "1" }],
      }).executor,
    });
    const optionalResult = await probeClickHouseAnalyticsBackendEmptiness({
      executor: fakeClickHouseExecutor({
        catalog,
        activeParts: [{ table: optionalTable, activePartCount: 1 }],
      }).executor,
    });

    expect(requiredResult.empty).toBe(false);
    expect(optionalResult.empty).toBe(false);
  });

  it("accepts known optional physical tables and ignores known logical views for data emptiness", async () => {
    const catalog = [
      ...requiredCatalog,
      ...CLICKHOUSE_OPTIONAL_ANALYTICS_PHYSICAL_TABLES.map((name) => ({
        name,
        engine: "ReplicatedReplacingMergeTree",
      })),
      { name: "events_core_mv", engine: "MaterializedView" },
      {
        name: "ingestion_size_stats_observations_mv",
        engine: "MaterializedView",
      },
      {
        name: "ingestion_size_stats_traces_mv",
        engine: "MaterializedView",
      },
      { name: "analytics_events_core", engine: "View" },
    ];

    await expect(
      probeClickHouseAnalyticsBackendEmptiness({
        executor: fakeClickHouseExecutor({ catalog }).executor,
      }),
    ).resolves.toMatchObject({ empty: true });
  });

  it("fails closed for a missing required table, an unknown object, or a view replacing a physical table", async () => {
    const missing = requiredCatalog.filter(({ name }) => name !== "traces");
    const unknown = [
      ...requiredCatalog,
      { name: "operator_backup", engine: "MergeTree" },
    ];
    const viewReplacement = requiredCatalog.map((row) =>
      row.name === "traces" ? { ...row, engine: "View" } : row,
    );

    for (const catalog of [missing, unknown, viewReplacement]) {
      await expect(
        probeClickHouseAnalyticsBackendEmptiness({
          executor: fakeClickHouseExecutor({ catalog }).executor,
        }),
      ).resolves.toMatchObject({ empty: false });
    }
  });

  it("fails closed unless schema_migrations is exactly version 36 and clean", async () => {
    for (const migrationRows of [
      [{ version: 35, dirty: 0 }],
      [{ version: 36, dirty: 1 }],
      [
        { version: 35, dirty: 0 },
        { version: 36, dirty: 0 },
      ],
    ]) {
      const { executor, query } = fakeClickHouseExecutor({ migrationRows });
      await expect(
        probeClickHouseAnalyticsBackendEmptiness({ executor }),
      ).resolves.toMatchObject({ empty: false });
      expect(
        query.mock.calls.some(([sql]) =>
          String(sql).includes("FROM system.tables"),
        ),
      ).toBe(false);
    }
  });

  it("produces stable evidence independent of catalog and active-part row order", async () => {
    const catalog = [
      ...requiredCatalog,
      {
        name: CLICKHOUSE_OPTIONAL_ANALYTICS_PHYSICAL_TABLES[0],
        engine: "MergeTree",
      },
    ];
    const activeParts = [
      { table: "traces", activePartCount: 1 },
      {
        table: CLICKHOUSE_OPTIONAL_ANALYTICS_PHYSICAL_TABLES[0],
        activePartCount: "2",
      },
    ];
    const first = await probeClickHouseAnalyticsBackendEmptiness({
      executor: fakeClickHouseExecutor({ catalog, activeParts }).executor,
    });
    const second = await probeClickHouseAnalyticsBackendEmptiness({
      executor: fakeClickHouseExecutor({
        catalog: [...catalog].reverse(),
        activeParts: [...activeParts].reverse(),
      }).executor,
    });

    expect(first.evidenceDigest).toBe(second.evidenceDigest);
  });

  it("sanitizes executor failures into stable fail-closed evidence", async () => {
    const first = await probeClickHouseAnalyticsBackendEmptiness({
      executor: fakeClickHouseExecutor({
        failure: new Error("https://user:secret@db.internal:8123"),
      }).executor,
    });
    const second = await probeClickHouseAnalyticsBackendEmptiness({
      executor: fakeClickHouseExecutor({
        failure: new Error("password=another-secret"),
      }).executor,
    });

    expect(first).toEqual(second);
    expect(first).toMatchObject({
      empty: false,
      evidenceDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
  });
});
