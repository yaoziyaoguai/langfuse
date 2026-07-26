import { randomBytes, randomUUID } from "node:crypto";
import { spawn } from "node:child_process";
import { isIP } from "node:net";

import {
  deriveDorisTestDatabaseName,
  dropOwnedDorisTestDatabase,
  parseDorisTestNamespace,
  resetOwnedDorisTestDatabase,
  truncateOwnedDorisTestTables,
} from "../src/server/doris/testDatabase";
import { DorisPoCMysqlClient } from "../src/server/doris-poc/mysqlClient";
import { runMigrations } from "../doris/scripts/migrate";

const TEST_FILES = [
  "src/server/repositories/analyticsBackendControl.integration.test.ts",
  "src/server/repositories/analyticsBackendControlSafety.integration.test.ts",
  "src/server/repositories/analyticsIngestionProvenance.integration.test.ts",
  "src/server/repositories/analyticsIngestionOperations.integration.test.ts",
  "src/server/repositories/analyticsDeletionProvenance.integration.test.ts",
  "src/server/repositories/analyticsRetentionProvenance.integration.test.ts",
  "src/server/repositories/analyticsCheckpoints.integration.test.ts",
  "src/server/repositories/analyticsRuntimeCompatibility.integration.test.ts",
  "src/server/repositories/batchExportManifests.integration.test.ts",
  "src/server/repositories/experimentExecutions.integration.test.ts",
  "src/server/repositories/analyticsU6Capabilities.integration.test.ts",
  "src/server/doris/__tests__/DorisPoC.integration.test.ts",
  "src/server/doris/__tests__/migration.integration.test.ts",
  "src/server/doris/__tests__/leastPrivilege.integration.test.ts",
  "src/server/queries/doris-sql/__tests__/querySemantics.integration.test.ts",
  "src/server/doris/__tests__/DorisTelemetryRepositories.integration.test.ts",
  "src/server/doris/__tests__/DorisScoresRepository.integration.test.ts",
  "src/features/query/server/adapters/doris/DorisAnalyticsQueryEngine.integration.test.ts",
] as const;
const WORKER_TEST_FILES = [
  "src/features/traces/processAnalyticsTraceDelete.integration.test.ts",
  "src/services/AnalyticsWriter/AnalyticsWriter.realDoris.integration.test.ts",
] as const;
const OBJECT_STORAGE_TEST_FILES = new Set<string>([
  "src/services/AnalyticsWriter/AnalyticsWriter.realDoris.integration.test.ts",
]);
export type DorisTestFile =
  | { readonly workspace: "shared"; readonly file: (typeof TEST_FILES)[number] }
  | {
      readonly workspace: "worker";
      readonly file: (typeof WORKER_TEST_FILES)[number];
    };

export const ALL_DORIS_TEST_FILES: readonly DorisTestFile[] = [
  ...TEST_FILES.map((file) => ({ workspace: "shared" as const, file })),
  ...WORKER_TEST_FILES.map((file) => ({
    workspace: "worker" as const,
    file,
  })),
];

export const SHARED_DORIS_TEST_TABLES = [
  "events_current",
  "scores_current",
  "blob_storage_file_log",
  "trace_tombstones",
  "project_tombstones",
  "dataset_run_items_current",
  "dataset_tombstones",
  "dataset_run_tombstones",
] as const;

export type DorisObjectStorageHarnessConfig = {
  readonly accessKeyId: string;
  readonly secretAccessKey: string;
  readonly bucket: string;
  readonly childEnvironment: Readonly<Record<string, string>>;
};

export function createDorisObjectStorageHarnessConfig(input: {
  readonly runId: string;
  readonly endpoint: string;
}): DorisObjectStorageHarnessConfig {
  if (
    !/^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/.test(
      input.runId,
    )
  ) {
    throw new Error("Invalid Doris test run ID");
  }

  const endpoint = new URL(input.endpoint);
  if (
    endpoint.protocol !== "http:" ||
    !["127.0.0.1", "localhost", "[::1]"].includes(endpoint.hostname) ||
    !endpoint.port ||
    endpoint.username ||
    endpoint.password ||
    endpoint.pathname !== "/" ||
    endpoint.search ||
    endpoint.hash
  ) {
    throw new Error("MinIO test endpoint must be a loopback HTTP origin");
  }

  const port = Number(endpoint.port);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error("MinIO test endpoint must use a valid TCP port");
  }

  const accessKeyId = `dt${randomBytes(9).toString("hex")}`;
  const secretAccessKey = randomBytes(32).toString("hex");
  const bucket = `langfuse-doris-test-${input.runId.replaceAll("-", "")}`;
  const normalizedEndpoint = endpoint.origin;
  return {
    accessKeyId,
    secretAccessKey,
    bucket,
    childEnvironment: {
      LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID: accessKeyId,
      LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY: secretAccessKey,
      LANGFUSE_S3_EVENT_UPLOAD_BUCKET: bucket,
      LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT: normalizedEndpoint,
      LANGFUSE_S3_EVENT_UPLOAD_REGION: "us-east-1",
      LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE: "true",
    },
  };
}

const POSTGRES_ONLY_TEST_FILES = new Set<string>([
  "src/server/repositories/analyticsBackendControl.integration.test.ts",
  "src/server/repositories/analyticsBackendControlSafety.integration.test.ts",
  "src/server/repositories/analyticsIngestionProvenance.integration.test.ts",
  "src/server/repositories/analyticsIngestionOperations.integration.test.ts",
  "src/server/repositories/analyticsDeletionProvenance.integration.test.ts",
  "src/server/repositories/analyticsRetentionProvenance.integration.test.ts",
  "src/server/repositories/analyticsCheckpoints.integration.test.ts",
  "src/server/repositories/analyticsRuntimeCompatibility.integration.test.ts",
  "src/server/repositories/batchExportManifests.integration.test.ts",
  "src/server/repositories/experimentExecutions.integration.test.ts",
  "src/server/repositories/analyticsU6Capabilities.integration.test.ts",
  "src/features/traces/processAnalyticsTraceDelete.integration.test.ts",
]);
const CANDIDATE_SCHEMA_TEST_FILE =
  "src/server/doris/__tests__/DorisPoC.integration.test.ts";
const MIGRATION_BOOTSTRAP_TEST_FILE =
  "src/server/doris/__tests__/migration.integration.test.ts";
const SHARED_PRODUCTION_SCHEMA_TEST_FILES = new Set<string>([
  "src/server/queries/doris-sql/__tests__/querySemantics.integration.test.ts",
  "src/server/doris/__tests__/DorisTelemetryRepositories.integration.test.ts",
  "src/server/doris/__tests__/DorisScoresRepository.integration.test.ts",
  "src/server/doris/__tests__/leastPrivilege.integration.test.ts",
  "src/features/query/server/adapters/doris/DorisAnalyticsQueryEngine.integration.test.ts",
  "src/services/AnalyticsWriter/AnalyticsWriter.realDoris.integration.test.ts",
]);

export type DorisTestSuiteKind =
  | "postgresOnly"
  | "candidateSchema"
  | "migrationBootstrap"
  | "sharedProductionSchema";
export type DorisBeforeTestAction =
  | "clone-postgres-database"
  | "bootstrap-production-schema"
  | "truncate-shared-data";
export type DorisTestExecutionStep = DorisTestFile & {
  readonly kind: DorisTestSuiteKind;
  readonly before: readonly DorisBeforeTestAction[];
};
export type DorisTestExecutionPlan = {
  readonly requiresDorisDatabase: boolean;
  readonly steps: readonly DorisTestExecutionStep[];
};

const COMPOSE_FILE = "../../docker-compose.dev.yml";

export function selectDorisTestFiles(
  requested: string | undefined,
): readonly DorisTestFile[] {
  if (!requested) return ALL_DORIS_TEST_FILES;
  const matches = ALL_DORIS_TEST_FILES.filter(
    (testFile) => testFile.file === requested,
  );
  if (matches.length !== 1) {
    throw new Error("DORIS_TEST_FILE must name an allowlisted Doris test file");
  }
  return matches;
}

function classifyDorisTestFile(testFile: DorisTestFile): DorisTestSuiteKind {
  if (POSTGRES_ONLY_TEST_FILES.has(testFile.file)) return "postgresOnly";
  if (testFile.file === CANDIDATE_SCHEMA_TEST_FILE) return "candidateSchema";
  if (testFile.file === MIGRATION_BOOTSTRAP_TEST_FILE) {
    return "migrationBootstrap";
  }
  if (SHARED_PRODUCTION_SCHEMA_TEST_FILES.has(testFile.file)) {
    return "sharedProductionSchema";
  }
  throw new Error(`Doris test file has no execution class: ${testFile.file}`);
}

export function planDorisTestExecution(
  testFiles: readonly DorisTestFile[],
): DorisTestExecutionPlan {
  let productionSchemaReady = false;
  let productionBootstrapCount = 0;

  const steps = testFiles.map<DorisTestExecutionStep>((testFile) => {
    const kind = classifyDorisTestFile(testFile);
    const before: DorisBeforeTestAction[] = [];

    if (kind === "postgresOnly") {
      before.push("clone-postgres-database");
    } else if (kind === "candidateSchema") {
      productionSchemaReady = false;
    } else if (kind === "migrationBootstrap") {
      productionBootstrapCount += 1;
      productionSchemaReady = true;
    } else if (kind === "sharedProductionSchema") {
      if (!productionSchemaReady) {
        productionBootstrapCount += 1;
        before.push("bootstrap-production-schema");
        productionSchemaReady = true;
      }
      before.push("truncate-shared-data");
    }

    if (productionBootstrapCount > 1) {
      throw new Error(
        "Doris execution plan would bootstrap the production schema more than once",
      );
    }

    return { ...testFile, kind, before };
  });

  return {
    requiresDorisDatabase: steps.some((step) => step.kind !== "postgresOnly"),
    steps,
  };
}

async function waitForExit(child: ReturnType<typeof spawn>): Promise<number> {
  return new Promise<number>((resolve, reject) => {
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (signal) {
        reject(new Error(`Test subprocess terminated by ${signal}`));
        return;
      }
      resolve(code ?? 1);
    });
  });
}

async function runChecked(
  command: string,
  args: readonly string[],
  env: NodeJS.ProcessEnv = process.env,
): Promise<void> {
  const exitCode = await waitForExit(
    spawn(command, [...args], { env, stdio: "inherit" }),
  );
  if (exitCode !== 0) {
    throw new Error(`${command} subprocess failed with exit code ${exitCode}`);
  }
}

async function captureChecked(
  command: string,
  args: readonly string[],
  env: NodeJS.ProcessEnv = process.env,
): Promise<string> {
  const child = spawn(command, [...args], {
    env,
    stdio: ["ignore", "pipe", "inherit"],
  });
  let stdout = "";
  child.stdout?.setEncoding("utf8");
  child.stdout?.on("data", (chunk: string) => {
    stdout += chunk;
  });
  const exitCode = await waitForExit(child);
  if (exitCode !== 0) {
    throw new Error(`${command} subprocess failed with exit code ${exitCode}`);
  }
  return stdout.trim();
}

async function runPostgresAdminSql(sql: string): Promise<void> {
  await runChecked("docker-compose", [
    "-f",
    COMPOSE_FILE,
    "exec",
    "-T",
    "postgres",
    "sh",
    "-ceu",
    'exec psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1 -c "$1"',
    "psql",
    sql,
  ]);
}

function postgresUrlForDatabase(url: string, database: string): string {
  const parsed = new URL(url);
  parsed.pathname = `/${database}`;
  return parsed.toString();
}

function postgresSuiteDatabaseName(database: string, index: number): string {
  if (!Number.isSafeInteger(index) || index < 0 || index > 99) {
    throw new TypeError("Invalid Postgres suite index");
  }
  const candidate = `${database}_p${String(index).padStart(2, "0")}`;
  if (!/^[a-z_][a-z0-9_]{0,62}$/.test(candidate)) {
    throw new Error("Unsafe Postgres suite database name");
  }
  return candidate;
}

export function postgresTemplateSessionTerminationSql(
  database: string,
): string {
  if (!/^langfuse_test_[a-f0-9]{32}$/.test(database)) {
    throw new Error("Unsafe Postgres database name");
  }
  return `SELECT pg_terminate_backend(pid)
            FROM pg_stat_activity
           WHERE datname = '${database}'
             AND pid <> pg_backend_pid()`;
}

async function captureCleanupError(
  errors: Error[],
  label: string,
  cleanup: () => Promise<void>,
): Promise<void> {
  try {
    await cleanup();
  } catch (error) {
    errors.push(
      new Error(`${label} cleanup failed`, {
        cause: error,
      }),
    );
  }
}

function minioComposeEnvironment(): NodeJS.ProcessEnv {
  const projectName = process.env.DORIS_TEST_MINIO_COMPOSE_PROJECT_NAME;
  return projectName
    ? { ...process.env, COMPOSE_PROJECT_NAME: projectName }
    : process.env;
}

async function resolveMinioTestEndpoint(): Promise<string> {
  const published = await captureChecked(
    "docker-compose",
    ["-f", COMPOSE_FILE, "port", "minio", "9000"],
    minioComposeEnvironment(),
  );
  const match = published.match(/^(?:127\.0\.0\.1|localhost):([0-9]+)$/);
  if (!match?.[1]) {
    throw new Error("MinIO test service must publish a loopback TCP port");
  }
  const port = Number(match[1]);
  if (!Number.isSafeInteger(port) || port < 1 || port > 65_535) {
    throw new Error("MinIO test service published an invalid TCP port");
  }
  return `http://127.0.0.1:${port}`;
}

async function resolvePostgresTestUrl(database: string): Promise<string> {
  const published = await captureChecked("docker-compose", [
    "-f",
    COMPOSE_FILE,
    "port",
    "postgres",
    "5432",
  ]);
  const match = published.match(/^(?:127\.0\.0\.1|localhost):([0-9]+)$/);
  if (!match?.[1]) {
    throw new Error("Postgres test service must publish a loopback TCP port");
  }
  const port = Number(match[1]);
  if (!Number.isInteger(port) || port < 1 || port > 65_535) {
    throw new Error("Postgres test service published an invalid TCP port");
  }
  return `postgresql://postgres:postgres@127.0.0.1:${port}/${database}`;
}

async function resolveDorisRedirectEnvironment(): Promise<
  Readonly<Record<string, string>>
> {
  const containerId = await captureChecked("docker-compose", [
    "-f",
    COMPOSE_FILE,
    "ps",
    "-q",
    "doris-be",
  ]);
  if (!/^[a-f0-9]{12,64}$/.test(containerId)) {
    throw new Error("Doris BE compose service must resolve to one container");
  }
  const networksJson = await captureChecked("docker", [
    "inspect",
    "--format",
    "{{json .NetworkSettings.Networks}}",
    containerId,
  ]);
  let networks: Record<string, { readonly IPAddress?: unknown }>;
  try {
    networks = JSON.parse(networksJson) as typeof networks;
  } catch {
    throw new Error("Doris BE container networks are not valid JSON");
  }
  const addresses = [
    ...new Set(
      Object.values(networks)
        .map((network) => network.IPAddress)
        .filter(
          (address): address is string =>
            typeof address === "string" && isIP(address) === 4,
        ),
    ),
  ];
  if (addresses.length !== 1) {
    throw new Error("Doris BE must have one unambiguous IPv4 address");
  }
  const published = await captureChecked("docker-compose", [
    "-f",
    COMPOSE_FILE,
    "port",
    "doris-be",
    "8040",
  ]);
  const publishedMatch = published.match(
    /^(?:127\.0\.0\.1|localhost):([0-9]+)$/,
  );
  if (!publishedMatch?.[1]) {
    throw new Error("Doris BE must publish its HTTP port on loopback");
  }
  const publishedPort = Number(publishedMatch[1]);
  if (
    !Number.isSafeInteger(publishedPort) ||
    publishedPort < 1 ||
    publishedPort > 65_535
  ) {
    throw new Error("Doris BE published an invalid HTTP port");
  }
  const publishedFe = await captureChecked("docker-compose", [
    "-f",
    COMPOSE_FILE,
    "port",
    "doris-fe",
    "8030",
  ]);
  const publishedFeMatch = publishedFe.match(
    /^(?:127\.0\.0\.1|localhost):([0-9]+)$/,
  );
  if (!publishedFeMatch?.[1]) {
    throw new Error("Doris FE must publish its HTTP port on loopback");
  }
  const publishedFePort = Number(publishedFeMatch[1]);
  if (
    !Number.isSafeInteger(publishedFePort) ||
    publishedFePort < 1 ||
    publishedFePort > 65_535
  ) {
    throw new Error("Doris FE published an invalid HTTP port");
  }

  const originalRedirectOrigin = `http://${addresses[0]}:8040`;
  const reachableRedirectOrigin = `http://127.0.0.1:${publishedPort}`;
  return {
    DORIS_POC_BE_REDIRECT_AUTHORITY: `${addresses[0]}:8040`,
    DORIS_POC_BE_REDIRECT_ORIGIN: reachableRedirectOrigin,
    DORIS_POC_FE_HTTP_ORIGIN: `http://127.0.0.1:${publishedFePort}`,
    DORIS_STREAM_LOAD_FE_URL: `http://127.0.0.1:${publishedFePort}`,
    DORIS_STREAM_LOAD_FE_IP_ALLOWLIST: "127.0.0.1",
    DORIS_STREAM_LOAD_USER: process.env.DORIS_POC_USER ?? "root",
    DORIS_STREAM_LOAD_PASSWORD: process.env.DORIS_POC_PASSWORD ?? "",
    DORIS_STREAM_LOAD_BE_ALLOWLIST: originalRedirectOrigin,
    DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: addresses[0]!,
    DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP: JSON.stringify({
      [originalRedirectOrigin]: reachableRedirectOrigin,
    }),
    DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST: "127.0.0.1",
  };
}

async function main(): Promise<void> {
  const testFiles = selectDorisTestFiles(process.env.DORIS_TEST_FILE);
  const executionPlan = planDorisTestExecution(testFiles);
  const runId = randomUUID();
  const ownershipToken = randomBytes(32).toString("hex");
  const securityUserPrefix = `lfsec_${runId.replaceAll("-", "").slice(0, 12)}`;
  const securityPassword = randomBytes(32).toString("base64url");
  const securityUsers = [
    `${securityUserPrefix}_web`,
    `${securityUserPrefix}_query`,
    `${securityUserPrefix}_load`,
    `${securityUserPrefix}_migrator`,
  ] as const;
  const database = deriveDorisTestDatabaseName(runId);
  const shadowDatabase = `${database}_shadow`;
  const needsObjectStorage = testFiles.some(({ file }) =>
    OBJECT_STORAGE_TEST_FILES.has(file),
  );
  const objectStorage = needsObjectStorage
    ? createDorisObjectStorageHarnessConfig({
        runId,
        endpoint: await resolveMinioTestEndpoint(),
      })
    : undefined;
  const host = process.env.DORIS_POC_FE_HOST ?? "127.0.0.1";
  const port = process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031";
  const parsedPort = Number(port);

  if (!Number.isInteger(parsedPort) || parsedPort < 1 || parsedPort > 65_535) {
    throw new Error("DORIS_POC_FE_MYSQL_PORT must be a valid TCP port");
  }
  if (!["127.0.0.1", "localhost", "::1"].includes(host)) {
    throw new Error("Real Doris tests require a loopback host");
  }

  const queryUrl = new URL("mysql://127.0.0.1");
  queryUrl.hostname = host;
  queryUrl.port = port;
  queryUrl.pathname = `/${database}`;

  const postgresUrl = await resolvePostgresTestUrl(database);
  const dorisRedirectEnvironment = executionPlan.requiresDorisDatabase
    ? await resolveDorisRedirectEnvironment()
    : {};
  const shadowPostgresUrl = new URL(postgresUrl);
  shadowPostgresUrl.pathname = `/${shadowDatabase}`;
  const unusedEventUploadBucket = `langfuse-doris-test-unused-${runId.replaceAll("-", "")}`;
  const childEnv: NodeJS.ProcessEnv = {
    ...process.env,
    ...dorisRedirectEnvironment,
    LANGFUSE_ANALYTICS_BACKEND: "doris",
    DORIS_LOCAL_DEV_MODE: "true",
    DORIS_POC_ENABLED: "1",
    DORIS_POC_FE_HOST: host,
    DORIS_POC_FE_MYSQL_PORT: port,
    DORIS_POC_DATABASE: database,
    DORIS_QUERY_URL: queryUrl.toString(),
    DORIS_STREAM_LOAD_DATABASE: database,
    DORIS_QUERY_USER: process.env.DORIS_POC_USER ?? "root",
    DORIS_QUERY_PASSWORD: process.env.DORIS_POC_PASSWORD ?? "",
    DORIS_QUERY_TLS_ENABLED: "false",
    DORIS_TEST_RUN_ID: runId,
    DORIS_TEST_OWNERSHIP_TOKEN: ownershipToken,
    DORIS_SECURITY_TEST_USER_PREFIX: securityUserPrefix,
    DORIS_SECURITY_TEST_PASSWORD: securityPassword,
    LANGFUSE_S3_EVENT_UPLOAD_BUCKET: unusedEventUploadBucket,
    ...objectStorage?.childEnvironment,
    DATABASE_URL: postgresUrl,
    DIRECT_URL: postgresUrl,
    SHADOW_DATABASE_URL: shadowPostgresUrl.toString(),
    DORIS_CONTROL_TEST_DATABASE_URL: postgresUrl,
  };
  delete childEnv.MINIO_ROOT_USER;
  delete childEnv.MINIO_ROOT_PASSWORD;

  const namespace = parseDorisTestNamespace(childEnv);
  const connectionConfig = {
    host,
    port: parsedPort,
    user: process.env.DORIS_POC_USER ?? "root",
    password: process.env.DORIS_POC_PASSWORD ?? "",
  };
  const admin = executionPlan.requiresDorisDatabase
    ? new DorisPoCMysqlClient(connectionConfig)
    : null;
  let postgresCreated = false;
  let shadowPostgresCreated = false;
  let dorisMayExist = false;
  let eventStorageUserCreated = false;
  let eventBucketCreated = false;
  let exitCode = 1;
  let executionError: unknown;
  const cleanupErrors: Error[] = [];
  const postgresSuiteDatabases = new Set<string>();

  process.stdout.write(
    `Preparing isolated Doris/Postgres databases ${database} (run ${runId})\n`,
  );

  const command = process.platform === "win32" ? "pnpm.cmd" : "pnpm";
  try {
    await runPostgresAdminSql(`CREATE DATABASE "${database}"`);
    postgresCreated = true;
    await runPostgresAdminSql(`CREATE DATABASE "${shadowDatabase}"`);
    shadowPostgresCreated = true;
    await runChecked(
      command,
      ["exec", "prisma", "migrate", "deploy"],
      childEnv,
    );
    if (objectStorage) {
      await runChecked(
        "docker-compose",
        [
          "-f",
          COMPOSE_FILE,
          "exec",
          "-T",
          "minio",
          "sh",
          "-ceu",
          'mc alias set doris-test http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && mc admin user add doris-test "$1" "$2" >/dev/null 2>&1',
          "minio-test",
          objectStorage.accessKeyId,
          objectStorage.secretAccessKey,
        ],
        minioComposeEnvironment(),
      );
      eventStorageUserCreated = true;
      await runChecked(
        "docker-compose",
        [
          "-f",
          COMPOSE_FILE,
          "exec",
          "-T",
          "minio",
          "sh",
          "-ceu",
          'mc alias set doris-test http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && mc admin policy attach doris-test readwrite --user "$1" >/dev/null 2>&1',
          "minio-test",
          objectStorage.accessKeyId,
        ],
        minioComposeEnvironment(),
      );
      await runChecked(
        "docker-compose",
        [
          "-f",
          COMPOSE_FILE,
          "exec",
          "-T",
          "minio",
          "sh",
          "-ceu",
          'mc alias set doris-test http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && mc mb "doris-test/$1" >/dev/null 2>&1',
          "minio-test",
          objectStorage.bucket,
        ],
        minioComposeEnvironment(),
      );
      eventBucketCreated = true;
    }

    exitCode = 0;
    for (const [stepIndex, step] of executionPlan.steps.entries()) {
      let suiteDatabase: string | undefined;
      let suiteEnv = childEnv;
      try {
        for (const action of step.before) {
          if (action === "clone-postgres-database") {
            const candidateDatabase = postgresSuiteDatabaseName(
              database,
              stepIndex,
            );
            // Vitest processes can leave TCP sessions visible for a short
            // window after exit. The source is a random harness-owned database
            // and no test process is running here, so close only those stale
            // sessions before PostgreSQL takes its TEMPLATE snapshot.
            await runPostgresAdminSql(
              postgresTemplateSessionTerminationSql(database),
            );
            await runPostgresAdminSql(
              `CREATE DATABASE "${candidateDatabase}" TEMPLATE "${database}"`,
            );
            suiteDatabase = candidateDatabase;
            postgresSuiteDatabases.add(suiteDatabase);
            const suitePostgresUrl = postgresUrlForDatabase(
              postgresUrl,
              suiteDatabase,
            );
            suiteEnv = {
              ...childEnv,
              DATABASE_URL: suitePostgresUrl,
              DIRECT_URL: suitePostgresUrl,
              DORIS_CONTROL_TEST_DATABASE_URL: suitePostgresUrl,
            };
            continue;
          }

          if (action === "bootstrap-production-schema") {
            if (!admin) {
              throw new Error("Doris bootstrap requires an admin client");
            }
            process.stdout.write(
              `Bootstrapping production Doris schema in ${database}\n`,
            );
            dorisMayExist = true;
            await resetOwnedDorisTestDatabase({
              admin,
              connectDatabase: (targetDatabase) =>
                new DorisPoCMysqlClient({
                  ...connectionConfig,
                  database: targetDatabase,
                }),
              namespace,
            });
            await runMigrations({
              ...connectionConfig,
              database,
              queryTimeoutMs: 120_000,
            });
            continue;
          }

          const target = new DorisPoCMysqlClient({
            ...connectionConfig,
            database,
          });
          try {
            await truncateOwnedDorisTestTables({
              executor: target,
              namespace,
              tables: SHARED_DORIS_TEST_TABLES,
            });
          } finally {
            await target.end();
          }
        }

        if (
          step.kind === "candidateSchema" ||
          step.kind === "migrationBootstrap"
        ) {
          dorisMayExist = true;
        }

        process.stdout.write(
          `Running ${step.workspace}/${step.file} in isolated database ${suiteDatabase ?? database}\n`,
        );
        const packageArgs =
          step.workspace === "worker" ? ["--filter", "worker"] : [];
        exitCode = await waitForExit(
          spawn(
            command,
            [
              ...packageArgs,
              "exec",
              "vitest",
              "run",
              "--no-file-parallelism",
              "--testTimeout=180000",
              "--hookTimeout=120000",
              step.file,
            ],
            {
              env: suiteEnv,
              stdio: "inherit",
            },
          ),
        );
      } finally {
        if (suiteDatabase) {
          await runPostgresAdminSql(
            `DROP DATABASE "${suiteDatabase}" WITH (FORCE)`,
          );
          postgresSuiteDatabases.delete(suiteDatabase);
          process.stdout.write(
            `Removed isolated Postgres suite database ${suiteDatabase}\n`,
          );
        }
      }
      if (exitCode !== 0) break;
    }
  } catch (error) {
    executionError = error;
  } finally {
    if (admin) {
      await captureCleanupError(cleanupErrors, "Doris database", async () => {
        const existingDorisDatabases = dorisMayExist
          ? await admin.query<{ schema_name: string }>(
              `SELECT SCHEMA_NAME AS schema_name
               FROM information_schema.SCHEMATA
              WHERE SCHEMA_NAME = ?`,
              [database],
            )
          : [];
        if (existingDorisDatabases.length > 0) {
          await dropOwnedDorisTestDatabase({
            admin,
            connectDatabase: (targetDatabase) =>
              new DorisPoCMysqlClient({
                ...connectionConfig,
                database: targetDatabase,
              }),
            namespace,
          });
          process.stdout.write(`Removed isolated Doris database ${database}\n`);
        }
      });
      await captureCleanupError(
        cleanupErrors,
        "Doris least-privilege test users",
        async () => {
          for (const user of securityUsers) {
            await admin.execute(`DROP USER IF EXISTS '${user}'@'%'`);
          }
        },
      );
      await captureCleanupError(cleanupErrors, "Doris admin client", () =>
        admin.end(),
      );
    }
    for (const suiteDatabase of postgresSuiteDatabases) {
      await captureCleanupError(
        cleanupErrors,
        `Postgres suite database ${suiteDatabase}`,
        async () => {
          await runPostgresAdminSql(
            `DROP DATABASE "${suiteDatabase}" WITH (FORCE)`,
          );
          process.stdout.write(
            `Removed isolated Postgres suite database ${suiteDatabase}\n`,
          );
        },
      );
    }
    if (postgresCreated) {
      await captureCleanupError(
        cleanupErrors,
        "Postgres database",
        async () => {
          await runPostgresAdminSql(`DROP DATABASE "${database}" WITH (FORCE)`);
          process.stdout.write(
            `Removed isolated Postgres database ${database}\n`,
          );
        },
      );
    }
    if (shadowPostgresCreated) {
      await captureCleanupError(
        cleanupErrors,
        "Postgres shadow database",
        async () => {
          await runPostgresAdminSql(
            `DROP DATABASE "${shadowDatabase}" WITH (FORCE)`,
          );
          process.stdout.write(
            `Removed isolated Postgres database ${shadowDatabase}\n`,
          );
        },
      );
    }
    if (eventBucketCreated && objectStorage) {
      await captureCleanupError(cleanupErrors, "MinIO bucket", async () => {
        await runChecked(
          "docker-compose",
          [
            "-f",
            COMPOSE_FILE,
            "exec",
            "-T",
            "minio",
            "sh",
            "-ceu",
            'mc alias set doris-test http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && mc rb --force "doris-test/$1" >/dev/null 2>&1',
            "minio-test",
            objectStorage.bucket,
          ],
          minioComposeEnvironment(),
        );
        process.stdout.write(
          `Removed isolated MinIO bucket ${objectStorage.bucket}\n`,
        );
      });
    }
    if (eventStorageUserCreated && objectStorage) {
      await captureCleanupError(cleanupErrors, "MinIO test user", async () => {
        await runChecked(
          "docker-compose",
          [
            "-f",
            COMPOSE_FILE,
            "exec",
            "-T",
            "minio",
            "sh",
            "-ceu",
            'mc alias set doris-test http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1 && mc admin user remove doris-test "$1" >/dev/null 2>&1',
            "minio-test",
            objectStorage.accessKeyId,
          ],
          minioComposeEnvironment(),
        );
        process.stdout.write("Removed isolated MinIO test user\n");
      });
    }
  }

  if (cleanupErrors.length > 0) {
    const errors =
      executionError === undefined
        ? cleanupErrors
        : [executionError, ...cleanupErrors];
    throw new AggregateError(
      errors,
      executionError === undefined
        ? "Doris test harness cleanup failed"
        : "Doris test harness execution and cleanup failed",
    );
  }

  if (executionError !== undefined) throw executionError;

  process.exitCode = exitCode;
}

if (require.main === module) {
  main().catch((error: unknown) => {
    const message = error instanceof Error ? error.message : "unknown error";
    process.stderr.write(`Doris test harness failed: ${message}\n`);
    process.exitCode = 1;
  });
}
