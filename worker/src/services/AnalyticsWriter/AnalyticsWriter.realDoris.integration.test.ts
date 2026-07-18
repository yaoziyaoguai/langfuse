import { PrismaClient } from "@prisma/client";
import type { Job } from "bullmq";
import {
  acceptAnalyticsIngestion,
  encodeRawAnalyticsIngestionEnvelope,
  normalizeVersionToken,
} from "@langfuse/shared/analytics-persistence";
import {
  StorageServiceFactory,
  type StorageService,
} from "@langfuse/shared/src/server";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { runMigrations } from "../../../../packages/shared/doris/scripts/migrate";
import { DorisPoCMysqlClient } from "../../../../packages/shared/src/server/doris-poc/mysqlClient";
import { DorisPoCStreamLoadClient } from "../../../../packages/shared/src/server/doris-poc/streamLoadClient";
import { createDorisAnalyticsPersistence } from "../dorisAnalyticsPersistence";
import { EventCanonicalizer } from "../EventCanonicalizer";
import { publishAnalyticsIngestionOutboxBatch } from "../../queues/analyticsIngestionQueue";
import { QueueName, type TQueueJobTypes } from "@langfuse/shared/src/server";

const ENABLED =
  process.env.DORIS_POC_ENABLED === "1" &&
  Boolean(process.env.DORIS_CONTROL_TEST_DATABASE_URL) &&
  Boolean(process.env.LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT);
const DB = "langfuse_poc";
const acceptedAt = normalizeVersionToken("2026-07-18T12:30:00.123456789Z");
const sourceVersion = normalizeVersionToken("2026-07-17T10:02:00.987654321Z");

describe.skipIf(!ENABLED)("AnalyticsWriter real storage path", () => {
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const organizationId = `writer-e2e-org-${suffix}`;
  const projectId = `writer-e2e-project-${suffix}`;
  const operationId = `writer-e2e-operation-${suffix}`;
  const scoreOperationId = `writer-e2e-score-operation-${suffix}`;
  const traceId = `writer-e2e-trace-${suffix}`;
  const spanId = `writer-e2e-span-${suffix}`;
  const scoreId = `writer-e2e-score-${suffix}`;
  const fileId = scoreOperationId;
  const canonicalPrefix = `writer-e2e/${suffix}/`;
  const rawObjectKey = `${canonicalPrefix}analytics-ingestion/raw/${projectId}/${operationId}.json`;
  const scoreRawObjectKey = `${canonicalPrefix}analytics-ingestion/raw/${projectId}/${scoreOperationId}.json`;
  const prisma = new PrismaClient({
    datasourceUrl: process.env.DORIS_CONTROL_TEST_DATABASE_URL,
  });
  const dorisConfig = {
    host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
    port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
    user: process.env.DORIS_POC_USER ?? "root",
    password: process.env.DORIS_POC_PASSWORD ?? "",
  };
  let admin: DorisPoCMysqlClient;
  let doris: DorisPoCMysqlClient;
  let storage: StorageService;

  beforeAll(async () => {
    storage = StorageServiceFactory.getInstance({
      accessKeyId: process.env.LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID,
      secretAccessKey: process.env.LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY,
      bucketName: process.env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET!,
      endpoint: process.env.LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT,
      region: process.env.LANGFUSE_S3_EVENT_UPLOAD_REGION,
      forcePathStyle:
        process.env.LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE === "true",
    });
    admin = new DorisPoCMysqlClient(dorisConfig);
    await admin.execute(`CREATE DATABASE IF NOT EXISTS ${DB}`);
    await runMigrations({ ...dorisConfig, database: DB });
    doris = new DorisPoCMysqlClient({ ...dorisConfig, database: DB });
    await prisma.organization.create({
      data: { id: organizationId, name: "Doris writer real-path test" },
    });
    await prisma.project.create({
      data: {
        id: projectId,
        name: "Doris writer real-path test",
        orgId: organizationId,
      },
    });
  }, 120_000);

  afterAll(async () => {
    const files = await storage.listFiles(canonicalPrefix);
    if (files.length > 0) {
      await storage.deleteFiles(files.map(({ file }) => file));
    }
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.$disconnect();
    await doris?.end();
    await admin?.end();
  }, 30_000);

  it("persists event, score, file reference, artifact, and durable ledgers", async () => {
    const rawEnvelope = {
      formatVersion: 1 as const,
      source: "internal-event" as const,
      payload: [
        {
          envelopeTimestamp: "2026-07-17T10:02:00.987654321Z",
          eventData: {
            projectId,
            traceId,
            spanId,
            startTimeISO: "2026-07-17T10:00:00.123456789Z",
            endTimeISO: "2026-07-17T10:00:02.123456789Z",
            completionStartTime: "2026-07-17T10:00:00.623456789Z",
            name: "real Doris generation",
            type: "GENERATION",
            environment: "production",
            version: "v1",
            release: "writer-e2e",
            traceName: "real storage path",
            userId: "writer-user",
            sessionId: "writer-session",
            level: "WARNING",
            statusMessage: "real Doris status",
            isAppRoot: true,
            bookmarked: true,
            public: false,
            tags: ["real", "doris"],
            input: { question: "2+2" },
            output: { answer: 4 },
            metadata: { nested: { stable: true } },
            modelName: "test-model",
            modelParameters: { temperature: 0 },
            providedUsageDetails: { input: 2, output: 1 },
            providedCostDetails: { input: 0.01 },
            toolDefinitions: { calculator: "{}" },
            toolCalls: ["calculator"],
            toolCallNames: ["calculator"],
            source: "sdk",
            ingestionSdkName: "langfuse-js",
            ingestionSdkVersion: "4.0.0",
            serviceName: "writer-e2e",
            telemetrySdkLanguage: "javascript",
            eventBytes: 321,
          },
        },
      ],
      attribution: {
        ingestionApiKey: "pk-writer-e2e",
        ingestionSdkName: "langfuse-js",
        ingestionSdkVersion: "4.0.0",
      },
    };
    expect(encodeRawAnalyticsIngestionEnvelope(rawEnvelope)).toContain(
      "real Doris generation",
    );
    await acceptAnalyticsIngestion({
      client: prisma,
      operationId,
      projectId,
      sourceOperationId: `source-${operationId}`,
      envelope: rawEnvelope,
      storageService: storage,
      rawPrefix: canonicalPrefix,
      acceptedAt: new Date("2026-07-18T12:30:00.123Z"),
      acceptedAtNanos: acceptedAt,
      canonicalizerVersion: "1",
      schemaVersion: 3,
    });
    const scoreEnvelope = {
      formatVersion: 1 as const,
      source: "score" as const,
      payload: [
        {
          id: `score-event-${suffix}`,
          type: "score-create" as const,
          timestamp: "2026-07-17T10:02:00.987654322Z",
          body: {
            id: scoreId,
            traceId,
            observationId: spanId,
            name: "quality",
            value: 0.75,
            dataType: "NUMERIC" as const,
            environment: "production",
            metadata: { evaluator: "writer-e2e" },
          },
        },
      ],
      attribution: {
        ingestionApiKey: "pk-writer-e2e",
        ingestionSdkName: "langfuse-js",
        ingestionSdkVersion: "5.0.0",
      },
    };
    await acceptAnalyticsIngestion({
      client: prisma,
      operationId: scoreOperationId,
      projectId,
      sourceOperationId: `source-${scoreOperationId}`,
      envelope: scoreEnvelope,
      storageService: storage,
      rawPrefix: canonicalPrefix,
      acceptedAt: new Date("2026-07-18T12:30:00.123Z"),
      acceptedAtNanos: acceptedAt,
      canonicalizerVersion: "1",
      schemaVersion: 3,
    });
    const streamLoad = new DorisPoCStreamLoadClient({
      feHttpOrigin:
        process.env.DORIS_POC_FE_HTTP_ORIGIN ?? "http://127.0.0.1:8031",
      user: dorisConfig.user,
      password: dorisConfig.password,
      defaultDatabase: DB,
      beRedirectAllowlist: {
        "172.28.0.3:8040": "http://127.0.0.1:8041",
      },
      reconcileLabelStatus: async (label) => {
        const rows = await doris.query<{ status: string }>(
          "SHOW TRANSACTION WHERE label = ?",
          [label],
        );
        const status = rows[0]?.status ?? "UNKNOWN";
        return { status, visible: status === "VISIBLE" };
      },
    });
    const { processor } = createDorisAnalyticsPersistence({
      runtimeEnv: {
        LANGFUSE_S3_EVENT_UPLOAD_BUCKET:
          process.env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET!,
        LANGFUSE_S3_EVENT_UPLOAD_PREFIX: canonicalPrefix,
      },
      prismaClient: prisma,
      storageService: storage,
      streamLoadTransport: {
        load: async (input) => {
          const result = await streamLoad.streamLoad({
            table: input.table,
            database: input.database,
            label: input.label,
            ndjsonBody: input.ndjsonBody.toString(),
          });
          return {
            status: result.status,
            label: result.label,
            numberTotalRows: result.numberTotalRows,
            numberFilteredRows: result.numberFilteredRows,
            committed: result.committed,
            requiresReconciliation: streamLoad.isUnknownOutcome(result),
          };
        },
        reconcile: (input) => streamLoad.reconcile({ label: input.label }),
      },
      databaseName: DB,
      workerId: "writer-e2e-worker",
      eventCanonicalizer: new EventCanonicalizer({
        warnOnUsageTotalMismatch: () => undefined,
        resolvePrompt: async () => null,
        resolveGenerationUsage: async () => ({
          usageDetails: { input: 2, output: 1, total: 3 },
          costDetails: { input: 0.01, output: 0.02, total: 0.03 },
          totalCost: 0.03,
        }),
      }),
    });
    const queuedJobs: TQueueJobTypes[QueueName.AnalyticsIngestionQueue][] = [];
    await expect(
      publishAnalyticsIngestionOutboxBatch({
        client: prisma,
        workerId: "writer-e2e-publisher",
        now: new Date("2026-07-18T12:30:01.000Z"),
        queue: {
          add: async (_name, data) => {
            queuedJobs.push(data);
          },
        },
      }),
    ).resolves.toBe(2);
    expect(queuedJobs).toHaveLength(2);
    for (const data of queuedJobs) {
      const job = { data } as Job<
        TQueueJobTypes[QueueName.AnalyticsIngestionQueue]
      >;
      await expect(processor(job, "token")).resolves.toBeUndefined();
      await expect(processor(job, "token")).resolves.toBeUndefined();
    }

    const [
      events,
      scores,
      files,
      artifactFiles,
      eventOperation,
      scoreOperation,
    ] = await Promise.all([
      doris.query<{
        version_token: string | number;
        status_message: string;
        input: string;
        total_input_tokens: string | number;
      }>(
        "SELECT CAST(version_token AS VARCHAR(32)) AS version_token, status_message, input, CAST(total_input_tokens AS VARCHAR(32)) AS total_input_tokens FROM events_current WHERE project_id = ? AND trace_id = ? AND span_id = ?",
        [projectId, traceId, spanId],
      ),
      doris.query<{ version_token: string | number; value: number }>(
        "SELECT CAST(version_token AS VARCHAR(32)) AS version_token, `value` FROM scores_current WHERE project_id = ? AND score_id = ?",
        [projectId, scoreId],
      ),
      doris.query<{ version_token: string | number; bucket_path: string }>(
        "SELECT CAST(version_token AS VARCHAR(32)) AS version_token, bucket_path FROM blob_storage_file_log WHERE project_id = ? AND file_id = ?",
        [projectId, fileId],
      ),
      storage
        .listFiles(canonicalPrefix)
        .then((items) =>
          items.filter(({ file }) => file.includes("/canonical-ingestion/")),
        ),
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: operationId },
        include: { candidates: true, loadBatches: true, outbox: true },
      }),
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: scoreOperationId },
        include: { candidates: true, loadBatches: true, outbox: true },
      }),
    ]);

    expect(events).toHaveLength(1);
    expect(String(events[0]!.version_token)).toBe(sourceVersion.toString());
    expect(events[0]).toMatchObject({
      status_message: "real Doris status",
      input: '{"question":"2+2"}',
    });
    expect(String(events[0]!.total_input_tokens)).toBe("2");
    expect(scores).toHaveLength(1);
    expect(String(scores[0]!.version_token)).toBe(
      (sourceVersion + 1n).toString(),
    );
    expect(Number(scores[0]!.value)).toBe(0.75);
    expect(files).toHaveLength(1);
    expect(String(files[0]!.version_token)).toBe(
      (sourceVersion + 1n).toString(),
    );
    expect(files[0]!.bucket_path).toBe(scoreRawObjectKey);
    expect(artifactFiles).toHaveLength(2);
    expect(eventOperation).toMatchObject({
      status: "VISIBLE",
      manifestState: "FROZEN",
      terminalAt: expect.any(Date),
      rawObjectKey,
      outbox: { status: "PUBLISHED" },
    });
    expect(eventOperation.candidates).toHaveLength(1);
    expect(eventOperation.loadBatches).toHaveLength(1);
    expect(
      eventOperation.loadBatches.every(({ status }) => status === "VISIBLE"),
    ).toBe(true);
    expect(scoreOperation).toMatchObject({
      status: "VISIBLE",
      manifestState: "FROZEN",
      terminalAt: expect.any(Date),
      rawObjectKey: scoreRawObjectKey,
      outbox: { status: "PUBLISHED" },
    });
    expect(scoreOperation.candidates).toHaveLength(2);
    expect(scoreOperation.loadBatches).toHaveLength(2);
    expect(
      scoreOperation.loadBatches.every(({ status }) => status === "VISIBLE"),
    ).toBe(true);
  }, 120_000);
});
