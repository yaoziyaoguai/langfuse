import { prisma } from "../../src/db";
import {
  checkDorisReadiness,
  getDorisQueryExecutor,
  redis,
} from "../../src/server";
import { SeedError } from "./scenarios/types";

export type CheckStatus = "pass" | "warn" | "fail";

export type CheckResult = {
  name: string;
  status: CheckStatus;
  detail: string;
  fix?: string;
};

const FIX = {
  envFile: "cp .env.dev.example .env  (then review required values)",
  infraUp: "pnpm run infra:dev:up",
  dorisMigrate: "pnpm --filter @langfuse/shared run doris:migrate",
  dbMigrate: "pnpm --filter @langfuse/shared run db:migrate",
  dbSeed:
    "pnpm --filter @langfuse/shared run db:seed  (creates the default seed projects)",
  devWeb: "pnpm run dev:web",
  devWorker: "pnpm run dev:worker",
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

const checkEnvFile = (): CheckResult => {
  const missing = [
    "DATABASE_URL",
    "DORIS_QUERY_URL",
    "DORIS_QUERY_USER",
  ].filter((name) => process.env[name] === undefined);
  return missing.length === 0
    ? { name: "env", status: "pass", detail: "required env vars present" }
    : {
        name: "env",
        status: "fail",
        detail: `missing env vars: ${missing.join(", ")} — is the repo-root .env present?`,
        fix: FIX.envFile,
      };
};

const checkPostgres = async (): Promise<CheckResult> => {
  try {
    await withTimeout(prisma.$queryRaw`SELECT 1`, 4_000);
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
    const rows = await withTimeout(
      prisma.$queryRaw<
        { count: bigint }[]
      >`SELECT count(*)::bigint AS count FROM _prisma_migrations WHERE finished_at IS NOT NULL`,
      4_000,
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
    const project = await withTimeout(
      prisma.project.findUnique({ where: { id: projectId } }),
      4_000,
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
    return {
      name: "project",
      status: "fail",
      detail: (error as Error).message,
      fix: FIX.dbMigrate,
    };
  }
};

const checkDoris = async (): Promise<CheckResult> => {
  try {
    const readiness = await withTimeout(
      checkDorisReadiness(getDorisQueryExecutor()),
      6_000,
    );
    return readiness.ready
      ? {
          name: "doris",
          status: "pass",
          detail: `reachable; schema version ${readiness.schemaVersion}`,
        }
      : {
          name: "doris",
          status: "fail",
          detail: `readiness failed: ${readiness.code}`,
          fix:
            readiness.code === "SCHEMA_MISMATCH"
              ? FIX.dorisMigrate
              : FIX.infraUp,
        };
  } catch (error) {
    return {
      name: "doris",
      status: "fail",
      detail: `cannot reach Doris: ${(error as Error).message}`,
      fix: FIX.infraUp,
    };
  }
};

const checkRedis = async (): Promise<CheckResult> => {
  if (!redis) {
    return {
      name: "redis",
      status: "fail",
      detail: "Redis is required for durable analytics ingestion",
      fix: FIX.infraUp,
    };
  }
  try {
    await withTimeout(redis.ping(), 2_500);
    return { name: "redis", status: "pass", detail: "reachable" };
  } catch (error) {
    return {
      name: "redis",
      status: "fail",
      detail: `cannot reach Redis: ${(error as Error).message}`,
      fix: FIX.infraUp,
    };
  }
};

const checkHttp = async (
  name: string,
  url: string,
  fix: string,
  detailOnPass: string,
): Promise<CheckResult> => {
  try {
    const response = await fetch(url, { signal: AbortSignal.timeout(3_000) });
    if (!response.ok) {
      return {
        name,
        status: "warn",
        detail: `${url} returned HTTP ${response.status}`,
        fix,
      };
    }
    return { name, status: "pass", detail: detailOnPass };
  } catch {
    return { name, status: "warn", detail: `no response from ${url}`, fix };
  }
};

const checkObjectStorage = async (): Promise<CheckResult> => {
  const endpoint = process.env.LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT;
  if (!endpoint) {
    return {
      name: "blob-storage",
      status: "fail",
      detail: "LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT is required by ingestion",
      fix: FIX.infraUp,
    };
  }
  return checkHttp(
    "blob-storage",
    `${endpoint.replace(/\/$/, "")}/minio/health/live`,
    FIX.infraUp,
    `reachable at ${endpoint}`,
  );
};

export const runDoctor = async (
  baseUrl: string,
  projectId: string,
): Promise<{ ok: boolean; checks: CheckResult[] }> => {
  const environment = checkEnvFile();
  if (environment.status === "fail") {
    return { ok: false, checks: [environment] };
  }
  const checks = [
    environment,
    ...(await Promise.all([
      checkPostgres(),
      checkMigrations(),
      checkProject(projectId),
      checkDoris(),
      checkRedis(),
      checkObjectStorage(),
      checkHttp(
        "web-app",
        `${baseUrl}/api/public/health`,
        FIX.devWeb,
        `responding at ${baseUrl} (deep links will work)`,
      ),
    ])),
  ];
  return { ok: checks.every((check) => check.status !== "fail"), checks };
};

export const preflight = async (opts: {
  projectId: string;
  needV4: boolean;
  log: (message: string) => void;
}): Promise<void> => {
  const environment = checkEnvFile();
  if (environment.status === "fail") {
    throw new SeedError(environment.detail, environment.fix);
  }
  const required = await Promise.all([
    checkPostgres(),
    checkProject(opts.projectId),
    checkDoris(),
    checkRedis(),
    checkObjectStorage(),
  ]);
  const failed = required.find((check) => check.status === "fail");
  if (failed) {
    throw new SeedError(
      `preflight failed [${failed.name}]: ${failed.detail}`,
      failed.fix,
    );
  }
  opts.log("preflight ok (postgres, project, redis, object storage, doris)");
  if (!opts.needV4) {
    opts.log("Doris R1A always writes the events-only model");
  }
};
