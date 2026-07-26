import { createClient } from "@clickhouse/client";

import { VERSION } from "../../constants/VERSION";
import { env } from "../../env";
import {
  createAnalyticsBackendEmptinessEvidence,
  type AnalyticsBackendEmptinessProbeResult,
} from "../analytics-persistence/analyticsBackendEmptinessDigest";
import { guardClickHouseClient } from "./runtimeIoGuard";

export const CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES = [
  "traces",
  "observations",
  "scores",
  "event_log",
  "project_environments",
  "blob_storage_file_log",
  "dataset_run_items",
  "dataset_run_items_rmt",
] as const;

export const CLICKHOUSE_OPTIONAL_ANALYTICS_PHYSICAL_TABLES = [
  "observations_batch_staging",
  "events_full",
  "events_core",
  "ingestion_size_stats",
] as const;

const CLICKHOUSE_KNOWN_LOGICAL_OBJECT_ENGINES = {
  analytics_traces: "View",
  analytics_observations: "View",
  analytics_scores: "View",
  events_core_mv: "MaterializedView",
  ingestion_size_stats_observations_mv: "MaterializedView",
  ingestion_size_stats_traces_mv: "MaterializedView",
  analytics_events_core: "View",
} as const;

const CLICKHOUSE_SCHEMA_MIGRATIONS_TABLE = "schema_migrations";
const CLICKHOUSE_EXPECTED_MIGRATION_VERSION = 36;
const CLICKHOUSE_PHYSICAL_TABLES = [
  ...CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES,
  ...CLICKHOUSE_OPTIONAL_ANALYTICS_PHYSICAL_TABLES,
] as const;

export interface ClickHouseEmptinessQueryExecutor {
  query<T extends object = Record<string, unknown>>(
    sql: string,
  ): Promise<readonly T[]>;
}

type OwnedClickHouseEmptinessQueryExecutor =
  ClickHouseEmptinessQueryExecutor & {
    close(): Promise<void>;
  };

type ClickHouseMigrationRow = {
  readonly version: number | string;
  readonly dirty: number | string;
};

type ClickHouseCatalogRow = {
  readonly name: string;
  readonly engine: string;
};

type ClickHouseActivePartsRow = {
  readonly table: string;
  readonly activePartCount: number | string;
};

function createDefaultClickHouseExecutor(): OwnedClickHouseEmptinessQueryExecutor {
  const client = guardClickHouseClient(
    createClient({
      url: env.CLICKHOUSE_READ_ONLY_URL ?? env.CLICKHOUSE_URL,
      username: env.CLICKHOUSE_USER,
      password: env.CLICKHOUSE_PASSWORD,
      database: env.CLICKHOUSE_DB,
      application: `langfuse/${VERSION.replace("v", "")}/emptiness-probe`,
      request_timeout: 10_000,
      clickhouse_settings: { readonly: "1" },
    }),
    10_000,
  );

  return {
    async query<T extends object>(sql: string): Promise<readonly T[]> {
      const result = await client.query({
        query: sql,
        format: "JSONEachRow",
      });
      return result.json<T>();
    },
    async close(): Promise<void> {
      await client.close();
    },
  };
}

function parseNonNegativeInteger(value: number | string): number | null {
  const text = String(value);
  if (!/^\d+$/.test(text)) return null;
  const parsed = Number(text);
  return Number.isSafeInteger(parsed) ? parsed : null;
}

function migrationMatches(rows: readonly ClickHouseMigrationRow[]): boolean {
  if (rows.length !== 1) return false;
  const version = parseNonNegativeInteger(rows[0]?.version ?? "");
  const dirty = parseNonNegativeInteger(rows[0]?.dirty ?? "");
  return version === CLICKHOUSE_EXPECTED_MIGRATION_VERSION && dirty === 0;
}

function isPhysicalEngine(engine: string): boolean {
  return engine.endsWith("MergeTree");
}

function normalizeCatalog(
  rows: readonly ClickHouseCatalogRow[],
): readonly string[] {
  return rows.map(({ name, engine }) => `${name}:${engine}`).sort();
}

function catalogMatches(rows: readonly ClickHouseCatalogRow[]): boolean {
  const names = new Set<string>();
  const physicalNames = new Set<string>(CLICKHOUSE_PHYSICAL_TABLES);
  const logicalEngines: Readonly<Record<string, string>> =
    CLICKHOUSE_KNOWN_LOGICAL_OBJECT_ENGINES;

  for (const { name, engine } of rows) {
    if (names.has(name)) return false;
    names.add(name);

    if (name === CLICKHOUSE_SCHEMA_MIGRATIONS_TABLE) {
      if (!isPhysicalEngine(engine)) return false;
    } else if (physicalNames.has(name)) {
      if (!isPhysicalEngine(engine)) return false;
    } else if (name in logicalEngines) {
      if (logicalEngines[name] !== engine) return false;
    } else {
      return false;
    }
  }

  return (
    names.has(CLICKHOUSE_SCHEMA_MIGRATIONS_TABLE) &&
    CLICKHOUSE_REQUIRED_ANALYTICS_PHYSICAL_TABLES.every((name) =>
      names.has(name),
    )
  );
}

function normalizeActiveParts(
  rows: readonly ClickHouseActivePartsRow[],
):
  | readonly { readonly table: string; readonly activePartCount: number }[]
  | null {
  const physicalNames = new Set<string>(CLICKHOUSE_PHYSICAL_TABLES);
  const seen = new Set<string>();
  const normalized: { table: string; activePartCount: number }[] = [];
  for (const { table, activePartCount: rawCount } of rows) {
    const activePartCount = parseNonNegativeInteger(rawCount);
    if (
      !physicalNames.has(table) ||
      seen.has(table) ||
      activePartCount === null
    ) {
      return null;
    }
    seen.add(table);
    normalized.push({ table, activePartCount });
  }
  return normalized.sort((left, right) =>
    left.table.localeCompare(right.table),
  );
}

async function runClickHouseEmptinessProbe(
  executor: ClickHouseEmptinessQueryExecutor,
): Promise<AnalyticsBackendEmptinessProbeResult> {
  const migrations = await executor.query<ClickHouseMigrationRow>(`
    SELECT version, dirty
    FROM schema_migrations
    ORDER BY version
  `);
  if (!migrationMatches(migrations)) {
    return createAnalyticsBackendEmptinessEvidence("clickhouse", false, {
      status: "SCHEMA_MISMATCH",
      expectedMigrationVersion: CLICKHOUSE_EXPECTED_MIGRATION_VERSION,
    });
  }

  const catalog = await executor.query<ClickHouseCatalogRow>(`
    SELECT name, engine
    FROM system.tables
    WHERE database = currentDatabase() AND is_temporary = 0
    ORDER BY name
  `);
  const normalizedCatalog = normalizeCatalog(catalog);
  if (!catalogMatches(catalog)) {
    return createAnalyticsBackendEmptinessEvidence("clickhouse", false, {
      status: "CATALOG_MISMATCH",
      migrationVersion: CLICKHOUSE_EXPECTED_MIGRATION_VERSION,
      catalog: normalizedCatalog,
    });
  }

  const quotedPhysicalTables = CLICKHOUSE_PHYSICAL_TABLES.map(
    (table) => `'${table}'`,
  ).join(", ");
  const rawActiveParts = await executor.query<ClickHouseActivePartsRow>(`
    SELECT table, count() AS activePartCount
    FROM system.parts
    WHERE database = currentDatabase()
      AND active
      AND table IN (${quotedPhysicalTables})
    GROUP BY table
    ORDER BY table
  `);
  const activeParts = normalizeActiveParts(rawActiveParts);
  if (!activeParts) {
    return createAnalyticsBackendEmptinessEvidence("clickhouse", false, {
      status: "PARTS_MISMATCH",
      migrationVersion: CLICKHOUSE_EXPECTED_MIGRATION_VERSION,
      catalog: normalizedCatalog,
    });
  }
  const empty = activeParts.every(
    ({ activePartCount }) => activePartCount === 0,
  );

  return createAnalyticsBackendEmptinessEvidence("clickhouse", empty, {
    status: "PROBED",
    migrationVersion: CLICKHOUSE_EXPECTED_MIGRATION_VERSION,
    catalog: normalizedCatalog,
    activeParts,
  });
}

export async function probeClickHouseAnalyticsBackendEmptiness(
  input: {
    readonly executor?: ClickHouseEmptinessQueryExecutor;
  } = {},
): Promise<AnalyticsBackendEmptinessProbeResult> {
  let ownedExecutor: OwnedClickHouseEmptinessQueryExecutor | undefined;
  try {
    const executor =
      input.executor ?? (ownedExecutor = createDefaultClickHouseExecutor());
    return await runClickHouseEmptinessProbe(executor);
  } catch {
    return createAnalyticsBackendEmptinessEvidence("clickhouse", false, {
      status: "UNAVAILABLE",
    });
  } finally {
    if (ownedExecutor) {
      try {
        await ownedExecutor.close();
      } catch {
        // The read-only proof has already completed; never expose transport data.
      }
    }
  }
}
