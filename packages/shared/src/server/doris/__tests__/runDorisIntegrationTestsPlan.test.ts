import { describe, expect, it } from "vitest";

import {
  ALL_DORIS_TEST_FILES,
  SHARED_DORIS_TEST_TABLES,
  createDorisObjectStorageHarnessConfig,
  planDorisTestExecution,
  postgresTemplateSessionTerminationSql,
  selectDorisTestFiles,
} from "../../../../scripts/run-doris-integration-tests";

function testFile(file: string) {
  const match = ALL_DORIS_TEST_FILES.find(
    (candidate) => candidate.file === file,
  );
  if (!match) throw new Error(`Unknown Doris test file: ${file}`);
  return match;
}

describe("Doris integration harness execution plan", () => {
  it("creates isolated MinIO credentials and a complete fail-closed writer environment", () => {
    const first = createDorisObjectStorageHarnessConfig({
      runId: "01234567-89ab-4def-8123-456789abcdef",
      endpoint: "http://127.0.0.1:9090",
    });
    const second = createDorisObjectStorageHarnessConfig({
      runId: "01234567-89ab-4def-8123-456789abcdef",
      endpoint: "http://127.0.0.1:9090",
    });

    expect(first.bucket).toBe(
      "langfuse-doris-test-0123456789ab4def8123456789abcdef",
    );
    expect(first.accessKeyId).not.toBe(second.accessKeyId);
    expect(first.secretAccessKey).not.toBe(second.secretAccessKey);
    expect(first.childEnvironment).toEqual({
      LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID: first.accessKeyId,
      LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY: first.secretAccessKey,
      LANGFUSE_S3_EVENT_UPLOAD_BUCKET: first.bucket,
      LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT: "http://127.0.0.1:9090",
      LANGFUSE_S3_EVENT_UPLOAD_REGION: "us-east-1",
      LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE: "true",
    });
  });

  it("rejects a non-loopback MinIO endpoint", () => {
    expect(() =>
      createDorisObjectStorageHarnessConfig({
        runId: "01234567-89ab-4def-8123-456789abcdef",
        endpoint: "http://minio.example.com:9000",
      }),
    ).toThrow(/loopback/i);
  });

  it("runs one production bootstrap and cleans shared suites without rebuilding per file", () => {
    const plan = planDorisTestExecution(selectDorisTestFiles(undefined));

    expect(plan.requiresDorisDatabase).toBe(true);
    expect(plan.steps).toHaveLength(20);
    expect(
      plan.steps.reduce<Record<string, number>>((counts, step) => {
        counts[step.kind] = (counts[step.kind] ?? 0) + 1;
        return counts;
      }, {}),
    ).toEqual({
      postgresOnly: 12,
      candidateSchema: 1,
      migrationBootstrap: 1,
      sharedProductionSchema: 6,
    });

    expect(
      plan.steps.filter((step) =>
        step.before.includes("bootstrap-production-schema"),
      ),
    ).toHaveLength(0);
    expect(
      plan.steps.filter((step) => step.kind === "migrationBootstrap"),
    ).toHaveLength(1);
    expect(
      plan.steps.filter((step) => step.kind === "sharedProductionSchema"),
    ).toSatisfy((steps: typeof plan.steps) =>
      steps.every(
        (step) =>
          step.before.length === 1 && step.before[0] === "truncate-shared-data",
      ),
    );
    expect(plan.steps.filter((step) => step.kind === "postgresOnly")).toSatisfy(
      (steps: typeof plan.steps) =>
        steps.every(
          (step) =>
            step.before.length === 1 &&
            step.before[0] === "clone-postgres-database",
        ),
    );
  });

  it("never truncates ownership or migration metadata", () => {
    expect(SHARED_DORIS_TEST_TABLES).toEqual([
      "events_current",
      "scores_current",
      "blob_storage_file_log",
      "trace_tombstones",
      "project_tombstones",
      "dataset_run_items_current",
      "dataset_tombstones",
      "dataset_run_tombstones",
    ]);
    expect(SHARED_DORIS_TEST_TABLES).not.toContain("_langfuse_test_ownership");
    expect(SHARED_DORIS_TEST_TABLES).not.toContain(
      "_langfuse_schema_migrations",
    );
  });

  it("does not initialize Doris for a selected Postgres-only suite", () => {
    const [selected] = selectDorisTestFiles(
      "src/server/repositories/analyticsBackendControl.integration.test.ts",
    );
    const plan = planDorisTestExecution([selected!]);

    expect(plan.requiresDorisDatabase).toBe(false);
    expect(plan.steps).toEqual([
      expect.objectContaining({
        kind: "postgresOnly",
        before: ["clone-postgres-database"],
      }),
    ]);
  });

  it.each([
    "src/server/doris/__tests__/DorisPoC.integration.test.ts",
    "src/server/doris/__tests__/migration.integration.test.ts",
  ])("lets %s manage its own schema", (file) => {
    const plan = planDorisTestExecution([testFile(file)]);

    expect(plan.requiresDorisDatabase).toBe(true);
    expect(plan.steps[0]?.before).toEqual([]);
  });

  it.each([
    "src/server/queries/doris-sql/__tests__/querySemantics.integration.test.ts",
    "src/server/doris/__tests__/DorisTelemetryRepositories.integration.test.ts",
    "src/server/doris/__tests__/DorisScoresRepository.integration.test.ts",
    "src/features/query/server/adapters/doris/DorisAnalyticsQueryEngine.integration.test.ts",
    "src/services/AnalyticsWriter/AnalyticsWriter.realDoris.integration.test.ts",
  ])("bootstraps once and cleans before selected shared suite %s", (file) => {
    const plan = planDorisTestExecution([testFile(file)]);

    expect(plan.requiresDorisDatabase).toBe(true);
    expect(plan.steps[0]).toEqual(
      expect.objectContaining({
        kind: "sharedProductionSchema",
        before: ["bootstrap-production-schema", "truncate-shared-data"],
      }),
    );
  });

  it("rejects a non-allowlisted DORIS_TEST_FILE", () => {
    expect(() => selectDorisTestFiles("src/not-allowlisted.test.ts")).toThrow(
      "DORIS_TEST_FILE must name an allowlisted Doris test file",
    );
  });

  it("terminates only sessions on a validated harness-owned template database", () => {
    expect(
      postgresTemplateSessionTerminationSql(
        "langfuse_test_0123456789abcdef0123456789abcdef",
      ),
    ).toContain("datname = 'langfuse_test_0123456789abcdef0123456789abcdef'");
    expect(() =>
      postgresTemplateSessionTerminationSql("postgres'; DROP DATABASE x; --"),
    ).toThrow(/unsafe postgres database name/i);
  });
});
