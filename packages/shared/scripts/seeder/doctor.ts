import type { PrismaClient } from "@prisma/client";

import { getSeederRequiredEnvVars } from "./backend";
import { SeedError, type AnalyticsBackend } from "./scenarios/types";

export type CheckStatus = "pass" | "warn" | "fail";

export type CheckResult = {
  name: string;
  status: CheckStatus;
  detail: string;
  fix?: string;
};

export type BackendCheckBundle = {
  checks: CheckResult[];
  v4Tables?: CheckResult;
};

export type DoctorDependencies = {
  checkEnv: (backend: AnalyticsBackend) => CheckResult;
  checkPostgres: () => Promise<CheckResult>;
  checkMigrations: () => Promise<CheckResult>;
  checkProject: (projectId: string) => Promise<CheckResult>;
  backendProbes: Record<
    AnalyticsBackend,
    (baseUrl: string) => Promise<BackendCheckBundle>
  >;
  checkRedis: () => Promise<CheckResult>;
  checkBlobStorage: () => Promise<CheckResult>;
  checkWebApp: (baseUrl: string) => Promise<CheckResult>;
  checkWebReadiness: (
    baseUrl: string,
    backend: AnalyticsBackend,
  ) => Promise<CheckResult>;
  close: () => Promise<void>;
};

const FIX = {
  envFile: "cp .env.dev.example .env  (then review required values)",
  infraUp: "pnpm run infra:dev:up",
  chMigrate: "pnpm --filter=shared run ch:up",
  chDevTables: "pnpm --filter=shared run ch:dev-tables",
  dbMigrate: "pnpm --filter=shared run db:migrate",
  dbSeed:
    "pnpm --filter=shared run db:seed  (creates the default seed projects)",
  devWeb: "pnpm run dev:web",
};

const withTimeout = async <T>(promise: Promise<T>, ms: number): Promise<T> => {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      promise,
      new Promise<never>((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`timed out after ${ms}ms`)),
          ms,
        );
      }),
    ]);
  } finally {
    if (timer) clearTimeout(timer);
  }
};

export const checkSeederEnv = (
  backend: AnalyticsBackend,
  values: Readonly<Record<string, string | undefined>> = process.env,
): CheckResult => {
  const missing = getSeederRequiredEnvVars(backend).filter(
    (name) => values[name] === undefined,
  );
  return missing.length === 0
    ? {
        name: "env",
        status: "pass",
        detail: `analytics backend ${backend}; required env vars present`,
      }
    : {
        name: "env",
        status: "fail",
        detail: `analytics backend ${backend}; missing env vars: ${missing.join(", ")} — is the repo-root .env present?`,
        fix: FIX.envFile,
      };
};

const checkHttp = async (input: {
  name: string;
  url: string;
  fix: string;
  detailOnPass: string;
  failureStatus?: "warn" | "fail";
}): Promise<CheckResult> => {
  const failureStatus = input.failureStatus ?? "warn";
  try {
    const response = await fetch(input.url, {
      signal: AbortSignal.timeout(3000),
    });
    if (!response.ok) {
      return {
        name: input.name,
        status: failureStatus,
        detail: `${input.url} returned HTTP ${response.status}`,
        fix: input.fix,
      };
    }
    return {
      name: input.name,
      status: "pass",
      detail: input.detailOnPass,
    };
  } catch {
    return {
      name: input.name,
      status: failureStatus,
      detail: `no response from ${input.url}`,
      fix: input.fix,
    };
  }
};

export const createDefaultDoctorDependencies = (): DoctorDependencies => {
  let prismaPromise: Promise<PrismaClient> | undefined;
  let redis:
    | { ping: () => Promise<string>; disconnect: () => unknown }
    | null
    | undefined;

  const getPrisma = async (): Promise<PrismaClient> => {
    prismaPromise ??= import("@prisma/client").then(
      ({ PrismaClient: Client }) => new Client(),
    );
    return prismaPromise;
  };

  const checkPostgres = async (): Promise<CheckResult> => {
    try {
      const client = await getPrisma();
      await withTimeout(client.$queryRawUnsafe("SELECT 1"), 4000);
      return { name: "postgres", status: "pass", detail: "reachable" };
    } catch (error) {
      return {
        name: "postgres",
        status: "fail",
        detail: `cannot reach Postgres: ${(error as Error).message}`,
        fix: FIX.infraUp,
      };
    }
  };

  const checkMigrations = async (): Promise<CheckResult> => {
    try {
      const client = await getPrisma();
      const rows = await withTimeout(
        client.$queryRawUnsafe<{ count: bigint }[]>(
          "SELECT count(*)::bigint AS count FROM _prisma_migrations WHERE finished_at IS NOT NULL",
        ),
        4000,
      );
      const applied = Number(rows[0]?.count ?? 0);
      return applied > 0
        ? {
            name: "postgres-migrations",
            status: "pass",
            detail: `${applied} migrations applied`,
          }
        : {
            name: "postgres-migrations",
            status: "fail",
            detail: "no applied migrations found",
            fix: FIX.dbMigrate,
          };
    } catch (error) {
      return {
        name: "postgres-migrations",
        status: "fail",
        detail: `cannot read _prisma_migrations: ${(error as Error).message}`,
        fix: FIX.dbMigrate,
      };
    }
  };

  const checkProject = async (projectId: string): Promise<CheckResult> => {
    try {
      const client = await getPrisma();
      const project = await withTimeout(
        client.project.findUnique({ where: { id: projectId } }),
        4000,
      );
      return project
        ? {
            name: "project",
            status: "pass",
            detail: `project ${projectId} exists`,
          }
        : {
            name: "project",
            status: "fail",
            detail: `project ${projectId} not found in Postgres`,
            fix: `${FIX.dbSeed} — or pass an existing project via --project <id>`,
          };
    } catch (error) {
      const prismaCode = (error as { code?: string }).code ?? "";
      return {
        name: "project",
        status: "fail",
        detail: (error as Error).message,
        fix: prismaCode.startsWith("P1") ? FIX.infraUp : FIX.dbMigrate,
      };
    }
  };

  const probeClickhouse = async (): Promise<BackendCheckBundle> => {
    let client: Awaited<
      ReturnType<
        (typeof import("../../src/server/clickhouse/client.js"))["clickhouseClient"]
      >
    >;
    try {
      const { clickhouseClient } =
        await import("../../src/server/clickhouse/client.js");
      client = clickhouseClient();
      const result = await withTimeout(
        client.query({
          query:
            "SELECT name FROM system.tables WHERE database = currentDatabase() AND name IN ('traces', 'observations', 'scores', 'events_full', 'events_core')",
          format: "JSONEachRow",
        }),
        4000,
      );
      const rows = await result.json<{ name: string }>();
      const tables = new Set(rows.map((row) => row.name));
      const legacyMissing = ["traces", "observations", "scores"].filter(
        (table) => !tables.has(table),
      );
      const v4Missing = ["events_full", "events_core"].filter(
        (table) => !tables.has(table),
      );
      const v4Tables: CheckResult =
        v4Missing.length === 0
          ? {
              name: "clickhouse-v4-tables",
              status: "pass",
              detail: "events_full/events_core present",
            }
          : {
              name: "clickhouse-v4-tables",
              status: "warn",
              detail: `missing v4 dev tables: ${v4Missing.join(", ")} — --v4 scenarios unavailable`,
              fix: FIX.chDevTables,
            };
      const checks: CheckResult[] = [
        { name: "clickhouse", status: "pass", detail: "reachable" },
        legacyMissing.length === 0
          ? {
              name: "clickhouse-tables",
              status: "pass",
              detail: "traces/observations/scores present",
            }
          : {
              name: "clickhouse-tables",
              status: "fail",
              detail: `missing tables: ${legacyMissing.join(", ")} — migrations not applied`,
              fix: FIX.chMigrate,
            },
        v4Tables,
      ];

      try {
        const memoryResult = await withTimeout(
          client.query({
            query: `SELECT
              anyIf(value, metric = 'MemoryResident') AS resident,
              greatest(anyIf(value, metric = 'CGroupMemoryTotal'), anyIf(value, metric = 'OSMemoryTotal')) AS total
            FROM system.asynchronous_metrics
            WHERE metric IN ('MemoryResident', 'CGroupMemoryTotal', 'OSMemoryTotal')`,
            format: "JSONEachRow",
          }),
          4000,
        );
        const memoryRows = await memoryResult.json<{
          resident: number;
          total: number;
        }>();
        const resident = Number(memoryRows[0]?.resident ?? 0);
        const total = Number(memoryRows[0]?.total ?? 0);
        const summary = `${(resident / 1024 ** 3).toFixed(1)} GiB of ${(total / 1024 ** 3).toFixed(1)} GiB`;
        checks.push(
          total <= 0 || resident / total < 0.7
            ? {
                name: "clickhouse-memory",
                status: "pass",
                detail: total <= 0 ? "memory metrics unavailable" : summary,
              }
            : {
                name: "clickhouse-memory",
                status: "warn",
                detail: `${summary} — large seeds may hit MEMORY_LIMIT_EXCEEDED`,
                fix: "docker restart langfuse-clickhouse  (frees memory; data persists on the volume)",
              },
        );
      } catch {
        checks.push({
          name: "clickhouse-memory",
          status: "pass",
          detail: "memory metrics unavailable (skipped)",
        });
      }
      return { checks, v4Tables };
    } catch (error) {
      return {
        checks: [
          {
            name: "clickhouse",
            status: "fail",
            detail: `cannot reach ClickHouse: ${(error as Error).message}`,
            fix: FIX.infraUp,
          },
          {
            name: "clickhouse-tables",
            status: "fail",
            detail: "skipped (no connection)",
            fix: FIX.infraUp,
          },
        ],
        v4Tables: {
          name: "clickhouse-v4-tables",
          status: "warn",
          detail: "skipped (no connection)",
          fix: FIX.infraUp,
        },
      };
    }
  };

  const checkWebReadiness = async (
    baseUrl: string,
    backend: AnalyticsBackend,
  ): Promise<CheckResult> => {
    const readiness = await checkHttp({
      name: `${backend}-readiness`,
      url: `${baseUrl}/api/public/ready?analyticsBackend=${backend}`,
      fix: `${FIX.devWeb}  (with LANGFUSE_ANALYTICS_BACKEND=${backend})`,
      detailOnPass: `${backend} analytics runtime ready via web app (identity verified)`,
      failureStatus: "fail",
    });
    return readiness;
  };

  return {
    checkEnv: checkSeederEnv,
    checkPostgres,
    checkMigrations,
    checkProject,
    backendProbes: {
      clickhouse: probeClickhouse,
      // Doris is intentionally not constructed here. Its backend-aware web
      // readiness endpoint validates schema/runtime health and identity.
      doris: async () => ({ checks: [] }),
    },
    checkRedis: async () => {
      try {
        if (redis === undefined) {
          const redisModule = await import("../../src/server/redis/redis.js");
          redis = redisModule.redis;
        }
        if (!redis) {
          return {
            name: "redis",
            status: "warn",
            detail:
              "redis client not configured (only needed for ingestion/worker paths)",
            fix: FIX.infraUp,
          };
        }
        await withTimeout(redis.ping(), 2500);
        return { name: "redis", status: "pass", detail: "reachable" };
      } catch (error) {
        return {
          name: "redis",
          status: "warn",
          detail: `cannot reach Redis: ${(error as Error).message}`,
          fix: FIX.infraUp,
        };
      }
    },
    checkBlobStorage: async () => {
      const endpoint = process.env.LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT;
      if (!endpoint) {
        return {
          name: "blob-storage",
          status: "warn",
          detail:
            "LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT not set (needed by public ingestion)",
          fix: "set LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT in .env if ingestion tests are needed",
        };
      }
      return checkHttp({
        name: "blob-storage",
        url: `${endpoint.replace(/\/$/, "")}/minio/health/live`,
        fix: FIX.infraUp,
        detailOnPass: `reachable at ${endpoint}`,
      });
    },
    checkWebApp: (baseUrl) =>
      checkHttp({
        name: "web-app",
        url: `${baseUrl}/api/public/health`,
        fix: FIX.devWeb,
        detailOnPass: `responding at ${baseUrl} (deep links will work)`,
      }),
    checkWebReadiness,
    close: async () => {
      const client = await prismaPromise?.catch(() => undefined);
      await client?.$disconnect().catch(() => undefined);
      redis?.disconnect();
    },
  };
};

export const runDoctor = async (
  baseUrl: string,
  projectId: string,
  backend: AnalyticsBackend,
  dependencies: DoctorDependencies = createDefaultDoctorDependencies(),
): Promise<{ ok: boolean; checks: CheckResult[] }> => {
  try {
    const env = dependencies.checkEnv(backend);
    if (env.status === "fail") return { ok: false, checks: [env] };

    const [
      postgres,
      migrations,
      project,
      selected,
      backendReadiness,
      redis,
      blob,
      web,
    ] = await Promise.all([
      dependencies.checkPostgres(),
      dependencies.checkMigrations(),
      dependencies.checkProject(projectId),
      dependencies.backendProbes[backend](baseUrl),
      dependencies.checkWebReadiness(baseUrl, backend),
      dependencies.checkRedis(),
      dependencies.checkBlobStorage(),
      dependencies.checkWebApp(baseUrl),
    ]);
    const checks = [
      env,
      postgres,
      migrations,
      project,
      ...selected.checks,
      backendReadiness,
      redis,
      blob,
      web,
    ];
    return { ok: checks.every((check) => check.status !== "fail"), checks };
  } finally {
    await dependencies.close();
  }
};

/** Fast subset of doctor executed before a scenario loads its implementation. */
export const preflight = async (
  opts: {
    projectId: string;
    backend: AnalyticsBackend;
    baseUrl: string;
    needV4: boolean;
    needWeb: boolean;
    log: (message: string) => void;
  },
  dependencies: DoctorDependencies = createDefaultDoctorDependencies(),
): Promise<void> => {
  try {
    const env = dependencies.checkEnv(opts.backend);
    if (env.status === "fail") throw new SeedError(env.detail, env.fix);

    const [postgres, project, selected, webReadiness] = await Promise.all([
      dependencies.checkPostgres(),
      dependencies.checkProject(opts.projectId),
      dependencies.backendProbes[opts.backend](opts.baseUrl),
      opts.needWeb
        ? dependencies.checkWebReadiness(opts.baseUrl, opts.backend)
        : Promise.resolve<CheckResult | null>(null),
    ]);
    const required = [postgres, project, ...selected.checks];
    if (webReadiness) required.push(webReadiness);
    if (opts.needV4) {
      required.push(
        selected.v4Tables
          ? {
              ...selected.v4Tables,
              status:
                selected.v4Tables.status === "warn"
                  ? ("fail" as const)
                  : selected.v4Tables.status,
            }
          : {
              name: `${opts.backend}-v4-tables`,
              status: "fail",
              detail: `v4 table verification is unavailable for ${opts.backend}`,
            },
      );
    }

    const failed = required.find((check) => check.status === "fail");
    if (failed) {
      throw new SeedError(
        `preflight failed [${failed.name}]: ${failed.detail}`,
        failed.fix,
      );
    }
    opts.log(`preflight ok (postgres, project, ${opts.backend})`);
  } finally {
    await dependencies.close();
  }
};
