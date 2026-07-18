import { createHash } from "node:crypto";

import { PrismaClient } from "@prisma/client";
import type { Job } from "bullmq";
import {
  canonicalPayloadHash,
  normalizeVersionToken,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsEvent,
  type CanonicalAnalyticsFileReference,
  type CanonicalAnalyticsScore,
} from "@langfuse/shared/analytics-persistence";
import {
  createAnalyticsIngestionReceipt,
  StorageServiceFactory,
  type StorageService,
} from "@langfuse/shared/src/server";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { runMigrations } from "../../../../packages/shared/doris/scripts/migrate";
import { DorisPoCMysqlClient } from "../../../../packages/shared/src/server/doris-poc/mysqlClient";
import { DorisPoCStreamLoadClient } from "../../../../packages/shared/src/server/doris-poc/streamLoadClient";
import { createDorisAnalyticsPersistence } from "../dorisAnalyticsPersistence";
import {
  analyticsIngestionQueueProcessorBuilder,
  publishAnalyticsIngestionOutboxBatch,
} from "../../queues/analyticsIngestionQueue";
import { QueueName, type TQueueJobTypes } from "@langfuse/shared/src/server";

const ENABLED =
  process.env.DORIS_POC_ENABLED === "1" &&
  Boolean(process.env.DORIS_CONTROL_TEST_DATABASE_URL) &&
  Boolean(process.env.LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT);
const DB = "langfuse_poc";
const acceptedAt = normalizeVersionToken("2026-07-18T12:30:00.123456789Z");
const sourceVersion = normalizeVersionToken("2026-07-17T10:02:00.987654321Z");
const eventTime = normalizeVersionToken("2026-07-17T10:00:00.123456789Z");

describe.skipIf(!ENABLED)("AnalyticsWriter real storage path", () => {
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
  const organizationId = `writer-e2e-org-${suffix}`;
  const projectId = `writer-e2e-project-${suffix}`;
  const operationId = `writer-e2e-operation-${suffix}`;
  const traceId = `writer-e2e-trace-${suffix}`;
  const spanId = `writer-e2e-span-${suffix}`;
  const scoreId = `writer-e2e-score-${suffix}`;
  const fileId = `writer-e2e-file-${suffix}`;
  const canonicalPrefix = `writer-e2e/${suffix}/`;
  const rawObjectKey = `${canonicalPrefix}raw/${operationId}.json`;
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

  function canonicalBatch(): CanonicalAnalyticsBatch {
    const base = {
      projectId,
      partitionDate: "2026-07-17",
      canonicalizerVersion: "1",
      schemaVersion: 3,
      systemTimestamp: acceptedAt,
      rawObjectKey,
      resolvedEnrichmentIds: {},
    } as const;
    const eventContent = { traceId, spanId, name: "real Doris generation" };
    const event: CanonicalAnalyticsEvent = {
      ...base,
      kind: "event",
      sourceContract: "v4",
      sourceVersion,
      canonicalPayloadHash: canonicalPayloadHash(eventContent),
      traceId,
      spanId,
      parentSpanId: null,
      type: "GENERATION",
      name: eventContent.name,
      environment: "production",
      version: "v1",
      release: "writer-e2e",
      traceName: "real storage path",
      startTime: eventTime,
      endTime: eventTime + 2_000_000_000n,
      completionStartTime: eventTime + 500_000_000n,
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
      providedModelName: "test-model",
      internalModelId: null,
      promptId: null,
      promptName: null,
      promptVersion: null,
      modelParameters: { temperature: 0 },
      providedUsageDetails: { input: 2, output: 1 },
      usageDetails: { input: 2, output: 1 },
      providedCostDetails: { input: 0.01 },
      costDetails: { total: 0.03 },
      totalCost: 0.03,
      toolDefinitions: { calculator: "{}" },
      toolCalls: ["calculator"],
      toolCallNames: ["calculator"],
      source: "sdk",
      ingestionSdkName: "langfuse-js",
      ingestionSdkVersion: "4.0.0",
      serviceName: "writer-e2e",
      telemetrySdkLanguage: "javascript",
      eventBytes: 321,
    };
    const score: CanonicalAnalyticsScore = {
      ...base,
      kind: "score",
      sourceContract: "score",
      sourceVersion: sourceVersion + 1n,
      canonicalPayloadHash: canonicalPayloadHash({ scoreId, value: 0.75 }),
      scoreId,
      traceId,
      observationId: spanId,
      sessionId: "writer-session",
      timestamp: eventTime + 3_000_000_000n,
      name: "quality",
      source: "API",
      dataType: "NUMERIC",
      numericValue: 0.75,
      stringValue: null,
      longStringValue: null,
      booleanValue: null,
      comment: "real Doris score",
      authorUserId: null,
      configId: null,
      queueId: null,
      environment: "production",
      metadata: { evaluator: "writer-e2e" },
    };
    const fileReference: CanonicalAnalyticsFileReference = {
      ...base,
      kind: "fileReference",
      sourceContract: "file-reference",
      sourceVersion: sourceVersion + 2n,
      canonicalPayloadHash: canonicalPayloadHash({ fileId, entityId: spanId }),
      entityType: "EVENT",
      entityId: spanId,
      fileId,
      eventId: spanId,
      bucketName: "langfuse",
      bucketPath: `media/${projectId}/${fileId}`,
    };
    return {
      projectId,
      operationId,
      canonicalizerVersion: "1",
      schemaVersion: 3,
      acceptedAt,
      rawObjectKey,
      children: [event, score, fileReference].map((entity) => ({
        entity,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration: 0n,
        projectDeletionGeneration: 0n,
      })),
    };
  }

  it("persists event, score, file reference, artifact, and durable ledgers", async () => {
    const rawBody = JSON.stringify({ operationId, projectId, format: "v4" });
    await storage.uploadFile({
      fileName: rawObjectKey,
      fileType: "application/json",
      data: rawBody,
    });
    await createAnalyticsIngestionReceipt({
      client: prisma,
      operationId,
      projectId,
      sourceOperationId: `source-${operationId}`,
      sourceChecksum: createHash("sha256").update(rawBody).digest("hex"),
      rawObjectKey,
      acceptedAt: new Date("2026-07-18T12:30:00.123Z"),
      acceptedAtNanos: acceptedAt,
      canonicalizerVersion: "1",
      schemaVersion: 3,
      recoverableUntil: new Date("2026-07-25T12:30:00.000Z"),
      statusExpiresAt: new Date("2026-08-24T12:30:00.000Z"),
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
    const { writer } = createDorisAnalyticsPersistence({
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
    });
    const batch = canonicalBatch();
    let queuedJob:
      | TQueueJobTypes[QueueName.AnalyticsIngestionQueue]
      | undefined;
    await expect(
      publishAnalyticsIngestionOutboxBatch({
        client: prisma,
        workerId: "writer-e2e-publisher",
        now: new Date("2026-07-18T12:30:01.000Z"),
        queue: {
          add: async (_name, data) => {
            queuedJob = data;
          },
        },
      }),
    ).resolves.toBe(1);
    expect(queuedJob).toBeDefined();
    const canonicalize = async (operation: {
      rawObjectKey: string;
      acceptedAtNanos: bigint;
      canonicalizerVersion: string;
      schemaVersion: number;
    }) => {
      expect(await storage.download(operation.rawObjectKey)).toBe(rawBody);
      expect(operation).toMatchObject({
        acceptedAtNanos: acceptedAt,
        canonicalizerVersion: "1",
        schemaVersion: 3,
      });
      return batch;
    };
    const processor = analyticsIngestionQueueProcessorBuilder({
      client: prisma,
      sink: writer,
      canonicalize,
    });
    const job = { data: queuedJob! } as Job<
      TQueueJobTypes[QueueName.AnalyticsIngestionQueue]
    >;

    await expect(processor(job, "token")).resolves.toBeUndefined();
    await expect(processor(job, "token")).resolves.toBeUndefined();

    const [events, scores, files, artifactFiles, operation] = await Promise.all(
      [
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
      ],
    );

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
      (sourceVersion + 2n).toString(),
    );
    expect(files[0]!.bucket_path).toBe(`media/${projectId}/${fileId}`);
    expect(artifactFiles).toHaveLength(1);
    expect(operation).toMatchObject({
      status: "VISIBLE",
      manifestState: "FROZEN",
      terminalAt: expect.any(Date),
      outbox: { status: "PUBLISHED" },
    });
    expect(operation.candidates).toHaveLength(3);
    expect(operation.loadBatches).toHaveLength(3);
    expect(
      operation.loadBatches.every(({ status }) => status === "VISIBLE"),
    ).toBe(true);
  }, 120_000);
});
