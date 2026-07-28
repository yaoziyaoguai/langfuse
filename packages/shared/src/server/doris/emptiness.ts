import { env } from "../../env";
import {
  createAnalyticsBackendEmptinessEvidence,
  type AnalyticsBackendEmptinessProbeResult,
} from "../analytics-persistence/analyticsBackendEmptinessDigest";
import { DorisClientManager, type DorisQueryExecutor } from "./client";
import { parseDorisQueryConfig, resolveDorisNodeEnv } from "./config";
import { checkDorisReadiness } from "./readiness";

export const DORIS_ANALYTICS_PHYSICAL_TABLES = [
  "events_current",
  "scores_current",
  "blob_storage_file_log",
  "trace_tombstones",
  "project_tombstones",
  "dataset_run_items_current",
  "dataset_tombstones",
  "dataset_run_tombstones",
] as const;

const DORIS_SCHEMA_MIGRATIONS_TABLE = "_langfuse_schema_migrations";

type DorisCatalogRow = {
  readonly tableName: string;
  readonly tableType: string;
};

type DorisOccupancyRow = {
  readonly hasRows: number | string;
};

function getDefaultDorisExecutor(): DorisQueryExecutor {
  return DorisClientManager.getInstance().getClient(
    parseDorisQueryConfig(
      {
        DORIS_QUERY_URL: env.DORIS_QUERY_URL,
        DORIS_QUERY_USER: env.DORIS_QUERY_USER,
        DORIS_QUERY_PASSWORD: env.DORIS_QUERY_PASSWORD,
        DORIS_QUERY_TLS_ENABLED: env.DORIS_QUERY_TLS_ENABLED,
        DORIS_QUERY_TLS_CA_PATH: env.DORIS_QUERY_TLS_CA_PATH,
        DORIS_QUERY_MAX_CONNECTIONS: String(env.DORIS_QUERY_MAX_CONNECTIONS),
        DORIS_QUERY_CONNECT_TIMEOUT_MS: String(
          env.DORIS_QUERY_CONNECT_TIMEOUT_MS,
        ),
        DORIS_QUERY_TIMEOUT_MS: String(env.DORIS_QUERY_TIMEOUT_MS),
      },
      resolveDorisNodeEnv(env.NODE_ENV, env.DORIS_LOCAL_DEV_MODE),
    ),
  );
}

function normalizeCatalog(rows: readonly DorisCatalogRow[]): readonly string[] {
  return rows
    .map(
      ({ tableName, tableType }) => `${tableName}:${tableType.toUpperCase()}`,
    )
    .sort();
}

function catalogMatches(rows: readonly DorisCatalogRow[]): boolean {
  const observed = normalizeCatalog(rows);
  const expected = [
    DORIS_SCHEMA_MIGRATIONS_TABLE,
    ...DORIS_ANALYTICS_PHYSICAL_TABLES,
  ]
    .map((tableName) => `${tableName}:BASE TABLE`)
    .sort();

  return (
    observed.length === expected.length &&
    observed.every((value, index) => value === expected[index])
  );
}

export async function probeDorisAnalyticsBackendEmptiness(
  input: {
    readonly executor?: DorisQueryExecutor;
  } = {},
): Promise<AnalyticsBackendEmptinessProbeResult> {
  try {
    const executor = input.executor ?? getDefaultDorisExecutor();
    const readiness = await checkDorisReadiness(executor);
    if (!readiness.ready) {
      return createAnalyticsBackendEmptinessEvidence("doris", false, {
        status: "SCHEMA_NOT_READY",
        readinessCode: readiness.code,
        schemaVersion: readiness.schemaVersion,
      });
    }

    const catalog = await executor.query<DorisCatalogRow>(`
      SELECT TABLE_NAME AS tableName, TABLE_TYPE AS tableType
      FROM information_schema.tables
      WHERE TABLE_SCHEMA = DATABASE()
      ORDER BY TABLE_NAME
    `);
    const normalizedCatalog = normalizeCatalog(catalog);
    if (!catalogMatches(catalog)) {
      return createAnalyticsBackendEmptinessEvidence("doris", false, {
        status: "CATALOG_MISMATCH",
        schemaVersion: readiness.schemaVersion,
        catalog: normalizedCatalog,
      });
    }

    const occupancy: { readonly table: string; readonly hasRows: boolean }[] =
      [];
    for (const table of DORIS_ANALYTICS_PHYSICAL_TABLES) {
      const rows = await executor.query<DorisOccupancyRow>(
        `SELECT 1 AS hasRows FROM \`${table}\` LIMIT 1`,
      );
      occupancy.push({ table, hasRows: rows.length > 0 });
    }
    const empty = occupancy.every(({ hasRows }) => !hasRows);

    return createAnalyticsBackendEmptinessEvidence("doris", empty, {
      status: "PROBED",
      schemaVersion: readiness.schemaVersion,
      catalog: normalizedCatalog,
      occupancy,
    });
  } catch {
    return createAnalyticsBackendEmptinessEvidence("doris", false, {
      status: "UNAVAILABLE",
    });
  }
}
