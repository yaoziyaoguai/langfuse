import { JobExecutionStatus, Prisma, PrismaClient } from "@prisma/client";
import {
  BatchExportFileFormat,
  BatchExportTableName,
  ScoreDataTypeEnum,
} from "@langfuse/shared";
import type { Job } from "bullmq";
import {
  acceptAnalyticsIngestion,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
  NEXT_ANALYTICS_SCHEMA_VERSION,
  encodeRawAnalyticsIngestionEnvelope,
  normalizeVersionToken,
} from "@langfuse/shared/analytics-persistence";
import {
  DorisAnalyticsLifecycleStore,
  DorisStreamLoadClient,
  DorisTracesRepository,
  QueueName,
  StorageServiceFactory,
  activateAnalyticsCapability,
  analyticsDatasetDeletionQueueReference,
  analyticsDurableProvenanceFromRecord,
  beginAnalyticsCapabilityDark,
  beginAnalyticsCapabilityDrain,
  completeAnalyticsCapabilityBootstrap,
  disableAnalyticsCapability,
  enableAnalyticsCapabilityDarkCapture,
  fingerprintAnalyticsWorkloadEpoch,
  getAnalyticsIngestionStatusForProject,
  getDeletionProgressForProject,
  parseDorisStreamLoadConfig,
  registerAnalyticsRuntimeLease,
  scheduleProjectDeletionOperation,
  scheduleTraceDeletionOperations,
  sealAnalyticsEvaluationReplayCutoff,
  serializeAnalyticsDurableProvenance,
  streamTransformations,
  verifyDurableAnalyticsEvaluationBootstrap,
  verifyDurableAnalyticsEvaluationDrain,
  type StorageService,
  type TQueueJobTypes,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { runMigrations } from "../../../../packages/shared/doris/scripts/migrate";
import { DorisPoCMysqlClient } from "../../../../packages/shared/src/server/doris-poc/mysqlClient";
import {
  assertOwnedDorisTestDatabase,
  parseDorisTestNamespace,
} from "../../../../packages/shared/src/server/doris/testDatabase";
import { createDorisAnalyticsPersistence } from "../dorisAnalyticsPersistence";
import { EventCanonicalizer } from "../EventCanonicalizer";
import { publishAnalyticsIngestionOutboxBatch } from "../../queues/analyticsIngestionQueue";
import {
  DorisMaterializedDeletionWriter,
  type DorisAnalyticsLifecycleRuntime,
} from "../dorisAnalyticsLifecycle";
import { processAnalyticsTraceDelete } from "../../features/traces/processAnalyticsTraceDelete";
import { processAnalyticsProjectDelete } from "../../features/projects/processAnalyticsProjectDelete";
import { createDorisAnalyticsExportSource } from "../../features/batchExport/analyticsExportSourceFactories";
import {
  openVerifiedBatchExportIdentityManifest,
  writeBatchExportIdentityManifest,
} from "../../features/batchExport/BatchExportIdentityManifest";
import { processDatasetDelete } from "../../features/datasets/processDatasetDelete";
import { completeEvalExecution } from "../../features/evaluation/evalCompletion";
import { persistDorisEvalScoreBatch } from "../../features/evaluation/dorisEvalScorePersistence";
import { replayAnalyticsEvaluationCutoff } from "../../features/evaluation/analyticsEvaluationReplay";
import {
  CanonicalIngestionArtifactStore,
  StorageServiceCanonicalObjectStore,
} from "../CanonicalIngestionArtifactStore";

const DORIS_TEST_REQUESTED = process.env.DORIS_POC_ENABLED === "1";
const missingDorisTestEnvironment = [
  "DORIS_CONTROL_TEST_DATABASE_URL",
  "LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID",
  "LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY",
  "LANGFUSE_S3_EVENT_UPLOAD_BUCKET",
  "LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT",
].filter((name) => !process.env[name]);
if (DORIS_TEST_REQUESTED && missingDorisTestEnvironment.length > 0) {
  throw new Error(
    `Doris AnalyticsWriter integration prerequisites are missing: ${missingDorisTestEnvironment.join(", ")}`,
  );
}
const ENABLED = DORIS_TEST_REQUESTED;
const TEST_NAMESPACE = ENABLED ? parseDorisTestNamespace() : null;
const DB = TEST_NAMESPACE?.database ?? "doris_test_disabled";
const acceptedAt = normalizeVersionToken("2026-07-18T12:30:00.123456789Z");
const sourceVersion = normalizeVersionToken("2026-07-17T10:02:00.987654321Z");

describe.skipIf(!ENABLED)("AnalyticsWriter real storage path", () => {
  const suffix = `${(TEST_NAMESPACE?.runId ?? "disabled").replaceAll("-", "").slice(0, 12)}-${Math.random().toString(16).slice(2, 8)}`;
  const organizationId = `writer-e2e-org-${suffix}`;
  const projectId = `writer-e2e-project-${suffix}`;
  const operationId = `writer-e2e-operation-${suffix}`;
  const scoreOperationId = `writer-e2e-score-operation-${suffix}`;
  const legacyOperationId = `writer-e2e-legacy-operation-${suffix}`;
  const datasetOperationId = `writer-e2e-dataset-operation-${suffix}`;
  const traceId = `writer-e2e-trace-${suffix}`;
  const spanId = `writer-e2e-span-${suffix}`;
  const scoreId = `writer-e2e-score-${suffix}`;
  const datasetId = `ds-${suffix}`;
  const datasetRunId = `run-${suffix}`;
  const datasetItemId = `item-${suffix}`;
  const datasetRunItemId = `ri-${suffix}`;
  const replayOperationId = `writer-e2e-replay-operation-${suffix}`;
  const replayTraceId = `writer-e2e-replay-trace-${suffix}`;
  const fileId = scoreOperationId;
  const canonicalPrefix = `writer-e2e/${suffix}/`;
  const rawObjectKey = `${canonicalPrefix}analytics-ingestion/raw/${projectId}/${operationId}.json`;
  const scoreRawObjectKey = `${canonicalPrefix}analytics-ingestion/raw/${projectId}/${scoreOperationId}.json`;
  const legacyRawObjectKey = `${canonicalPrefix}analytics-ingestion/raw/${projectId}/${legacyOperationId}.json`;
  if (projectId.length > 64 || traceId.length > 64) {
    throw new Error("Doris writer test IDs exceed the storage contract");
  }
  const prisma = new PrismaClient({
    datasourceUrl: process.env.DORIS_CONTROL_TEST_DATABASE_URL,
  });
  const workloadEpochFingerprint = fingerprintAnalyticsWorkloadEpoch(
    `writer-e2e-${suffix}`,
  );
  const queueNamespaceFingerprint = "f".repeat(64);
  const deploymentGeneration = 1n;
  const dorisConfig = {
    host: process.env.DORIS_POC_FE_HOST ?? "127.0.0.1",
    port: Number(process.env.DORIS_POC_FE_MYSQL_PORT ?? "9031"),
    user: process.env.DORIS_POC_USER ?? "root",
    password: process.env.DORIS_POC_PASSWORD ?? "",
  };
  let doris: DorisPoCMysqlClient;
  let storage: StorageService;
  let streamLoad: DorisStreamLoadClient;
  let lifecycle: DorisAnalyticsLifecycleRuntime;
  let producerAdmissionContext: AnalyticsRuntimeAdmissionContext;
  let workerAdmissionContext: AnalyticsRuntimeAdmissionContext;
  let ingestionProcessor: ReturnType<
    typeof createDorisAnalyticsPersistence
  >["processor"];

  beforeAll(async () => {
    if (!TEST_NAMESPACE) throw new Error("Doris test namespace is required");
    storage = StorageServiceFactory.getInstance({
      accessKeyId: process.env.LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID,
      secretAccessKey: process.env.LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY,
      bucketName: process.env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET!,
      endpoint: process.env.LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT,
      region: process.env.LANGFUSE_S3_EVENT_UPLOAD_REGION,
      forcePathStyle:
        process.env.LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE === "true",
    });
    doris = new DorisPoCMysqlClient({ ...dorisConfig, database: DB });
    await assertOwnedDorisTestDatabase(doris, TEST_NAMESPACE);
    await runMigrations({ ...dorisConfig, database: DB });
    streamLoad = new DorisStreamLoadClient({
      ...parseDorisStreamLoadConfig(
        {
          DORIS_LOCAL_DEV_MODE: process.env.DORIS_LOCAL_DEV_MODE,
          DORIS_QUERY_USER: process.env.DORIS_QUERY_USER,
          DORIS_STREAM_LOAD_FE_URL: process.env.DORIS_STREAM_LOAD_FE_URL,
          DORIS_STREAM_LOAD_USER: process.env.DORIS_STREAM_LOAD_USER,
          DORIS_STREAM_LOAD_PASSWORD: process.env.DORIS_STREAM_LOAD_PASSWORD,
          DORIS_STREAM_LOAD_DATABASE: process.env.DORIS_STREAM_LOAD_DATABASE,
          DORIS_STREAM_LOAD_FE_IP_ALLOWLIST:
            process.env.DORIS_STREAM_LOAD_FE_IP_ALLOWLIST,
          DORIS_STREAM_LOAD_BE_ALLOWLIST:
            process.env.DORIS_STREAM_LOAD_BE_ALLOWLIST,
          DORIS_STREAM_LOAD_BE_IP_ALLOWLIST:
            process.env.DORIS_STREAM_LOAD_BE_IP_ALLOWLIST,
          DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP:
            process.env.DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP,
          DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST:
            process.env.DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST,
        },
        "development",
      ),
      reconcileLabelStatus: async (label) => {
        const rows = await doris.query<{ status: string }>(
          "SHOW TRANSACTION WHERE label = ?",
          [label],
        );
        const status = rows[0]?.status ?? "UNKNOWN";
        return { status, visible: status === "VISIBLE" };
      },
    });
    const lifecycleTransport = {
      load: (input: {
        table: string;
        database?: string;
        label: string;
        ndjsonBody: string | Buffer;
        columns?: readonly string[];
        mergeType?: "APPEND" | "DELETE";
      }) =>
        streamLoad.load({
          table: input.table,
          database: input.database,
          label: input.label,
          ndjsonBody: input.ndjsonBody,
          columns: input.columns,
          mergeType: input.mergeType,
        }),
      reconcile: (input: { label: string }) =>
        streamLoad.reconcile({ label: input.label }),
    };
    lifecycle = {
      store: new DorisAnalyticsLifecycleStore({
        streamLoad: lifecycleTransport,
        query: doris.query.bind(doris),
        getDeletionProgress: (input) =>
          getDeletionProgressForProject({ ...input, client: prisma }),
      }),
      materializedDeletion: new DorisMaterializedDeletionWriter(
        lifecycleTransport,
      ),
    };
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
    await prisma.analyticsBackendDeploymentState.create({
      data: {
        id: "global",
        backend: "DORIS",
        generation: deploymentGeneration,
        workloadEpochFingerprint,
        queueNamespaceFingerprint,
        foundationContractVersion: 1,
      },
    });
    await prisma.analyticsCapabilityActivation.update({
      where: { capability: "EVALUATIONS" },
      data: {
        backend: "DORIS",
        deploymentGeneration,
        generation: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        status: "ACTIVE",
        captureEnabled: true,
        captureStartedAt: null,
        captureExpiresAt: null,
        captureRowBudget: null,
        captureRows: 0n,
        captureRequired: false,
        rescanRequired: false,
        cutoffState: Prisma.DbNull,
        cutoffActivationGeneration: null,
        cutoffDigest: null,
        bootstrapCompletedGeneration: 1n,
        bootstrapEvidenceDigest: "e".repeat(64),
        bootstrapCompletedAt: new Date(),
        activatedAt: new Date(),
        drainingAt: null,
        disabledAt: null,
      },
    });
    const register = (
      instanceId: string,
      component: "web" | "worker",
      capabilityContracts: Parameters<
        typeof registerAnalyticsRuntimeLease
      >[0]["capabilityContracts"],
    ) =>
      registerAnalyticsRuntimeLease({
        client: prisma,
        component,
        instanceId,
        backend: "doris",
        deploymentGeneration,
        workloadEpochFingerprint,
        queueNamespaceFingerprint,
        buildId: "writer-e2e",
        foundationContractVersion: 1,
        acceptedSchemaVersion: {
          min: 1,
          max: NEXT_ANALYTICS_SCHEMA_VERSION,
        },
        acceptedCanonicalVersion: { min: 1, max: 1 },
        capabilityContracts,
        leaseMs: 600_000,
      });
    const producer = await register(`writer-e2e-web-${suffix}`, "web", [
      {
        capability: "evaluations",
        supportedContractVersion: 1,
        installedRoles: ["producer"],
      },
    ]);
    const worker = await register(`writer-e2e-worker-${suffix}`, "worker", [
      {
        capability: "evaluations",
        supportedContractVersion: 1,
        installedRoles: ["capture", "consumer", "recovery"],
      },
    ]);
    producerAdmissionContext = {
      runtimeLeaseId: producer.lease.id,
      backend: "doris",
      deploymentGeneration,
    };
    workerAdmissionContext = {
      runtimeLeaseId: worker.lease.id,
      backend: "doris",
      deploymentGeneration,
    };
    await prisma.dataset.create({
      data: {
        id: datasetId,
        projectId,
        name: "real dataset",
      },
    });
    await prisma.datasetItem.create({
      data: {
        id: datasetItemId,
        projectId,
        datasetId,
        input: { prompt: "hello" },
        expectedOutput: "world",
        metadata: { split: "test" },
        validFrom: new Date("2026-07-17T09:59:00.000Z"),
      },
    });
    await prisma.datasetRuns.create({
      data: {
        id: datasetRunId,
        projectId,
        datasetId,
        name: "real experiment",
        metadata: { owner: "test" },
        createdAt: new Date("2026-07-17T10:00:00.000Z"),
      },
    });
  }, 120_000);

  afterAll(async () => {
    const files = await storage.listFiles(canonicalPrefix);
    if (files.length > 0) {
      await storage.deleteFiles(files.map(({ file }) => file));
    }
    await prisma.analyticsDeletionOperation.deleteMany({
      where: { projectId },
    });
    await prisma.analyticsDatasetDeletionOperation.deleteMany({
      where: { projectId },
    });
    await prisma.analyticsDatasetRunDeletionGeneration.deleteMany({
      where: { projectId },
    });
    await prisma.analyticsDatasetDeletionGeneration.deleteMany({
      where: { projectId },
    });
    await prisma.analyticsProjectDeletionGeneration.deleteMany({
      where: { projectId },
    });
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.analyticsBackendClaimLease.deleteMany();
    await prisma.analyticsRuntimeCapabilityContract.deleteMany();
    await prisma.analyticsRuntimeLease.deleteMany();
    await prisma.analyticsBackendDeploymentTransition.deleteMany();
    await prisma.analyticsBackendDeploymentState.deleteMany();
    await prisma.analyticsCapabilityActivation.update({
      where: { capability: "EVALUATIONS" },
      data: {
        deploymentGeneration: 0n,
        status: "DISABLED",
        captureEnabled: false,
        captureStartedAt: null,
        captureExpiresAt: null,
        captureRowBudget: null,
        captureRows: 0n,
        captureRequired: false,
        rescanRequired: false,
        cutoffState: Prisma.DbNull,
        cutoffActivationGeneration: null,
        cutoffDigest: null,
        bootstrapCompletedGeneration: null,
        bootstrapEvidenceDigest: null,
        bootstrapCompletedAt: null,
        activatedAt: null,
        drainingAt: null,
        disabledAt: null,
      },
    });
    await prisma.$disconnect();
    await doris?.end();
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
      schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
      admissionContext: producerAdmissionContext,
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
      schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
      admissionContext: producerAdmissionContext,
    });
    await acceptAnalyticsIngestion({
      client: prisma,
      operationId: legacyOperationId,
      projectId,
      sourceOperationId: `source-${legacyOperationId}`,
      envelope: {
        formatVersion: 1,
        source: "legacy-event",
        payload: [
          {
            id: `legacy-trace-event-${suffix}`,
            type: "trace-create",
            timestamp: "2026-07-17T10:02:00.987654323Z",
            body: {
              id: traceId,
              timestamp: "2026-07-17T09:59:59.123456789Z",
              name: "legacy Doris trace",
              input: { legacy: true },
              metadata: { source: "legacy-sdk" },
              environment: "production",
            },
          },
        ],
        attribution: {
          ingestionApiKey: "pk-writer-legacy-e2e",
          ingestionSdkName: "langfuse-js",
          ingestionSdkVersion: "2.9.0",
        },
      },
      storageService: storage,
      rawPrefix: canonicalPrefix,
      acceptedAt: new Date("2026-07-18T12:30:00.123Z"),
      acceptedAtNanos: acceptedAt,
      canonicalizerVersion: "1",
      schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
      admissionContext: producerAdmissionContext,
    });
    ingestionProcessor = createDorisAnalyticsPersistence({
      runtimeEnv: {
        LANGFUSE_S3_EVENT_UPLOAD_BUCKET:
          process.env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET!,
        LANGFUSE_S3_EVENT_UPLOAD_PREFIX: canonicalPrefix,
      },
      prismaClient: prisma,
      storageService: storage,
      streamLoadTransport: {
        load: (input) =>
          streamLoad.load({
            table: input.table,
            database: input.database,
            label: input.label,
            ndjsonBody: input.ndjsonBody,
          }),
        reconcile: (input) => streamLoad.reconcile({ label: input.label }),
      },
      databaseName: DB,
      workerId: "writer-e2e-worker",
      getAdmissionContext: () => workerAdmissionContext,
      eventCanonicalizer: new EventCanonicalizer({
        warnOnUsageTotalMismatch: () => undefined,
        resolvePrompt: async () => null,
        resolveGenerationUsage: async () => ({
          usageDetails: { input: 2, output: 1, total: 3 },
          costDetails: { input: 0.01, output: 0.02, total: 0.03 },
          totalCost: 0.03,
        }),
      }),
    }).processor;
    const processor = ingestionProcessor;
    const queuedJobs: TQueueJobTypes[QueueName.AnalyticsIngestionQueue][] = [];
    await expect(
      publishAnalyticsIngestionOutboxBatch({
        client: prisma,
        workerId: "writer-e2e-publisher",
        now: new Date("2026-07-18T12:30:01.000Z"),
        getAdmissionContext: () => workerAdmissionContext,
        queue: {
          add: async (_name, data) => {
            queuedJobs.push(data);
            return {
              getState: async () => "waiting",
              retry: async () => undefined,
            };
          },
        },
      }),
    ).resolves.toBe(3);
    expect(queuedJobs).toHaveLength(3);
    for (const data of queuedJobs) {
      const job = { data } as Job<
        TQueueJobTypes[QueueName.AnalyticsIngestionQueue]
      >;
      await expect(processor(job, "token")).resolves.toBeUndefined();
      await expect(processor(job, "token")).resolves.toBeUndefined();
    }

    await expect(
      prisma.analyticsEvaluationDispatch.findMany({
        where: { operationId },
        select: {
          targetType: true,
          targetId: true,
          status: true,
          deploymentGeneration: true,
          capabilityActivationGeneration: true,
        },
        orderBy: { targetType: "asc" },
      }),
    ).resolves.toEqual([
      {
        targetType: "TRACE_UPSERT",
        targetId: traceId,
        status: "PENDING",
        deploymentGeneration,
        capabilityActivationGeneration: 1n,
      },
      {
        targetType: "OBSERVATION_UPSERT",
        targetId: spanId,
        status: "PENDING",
        deploymentGeneration,
        capabilityActivationGeneration: 1n,
      },
    ]);

    const evalTemplate = await prisma.evalTemplate.create({
      data: {
        id: `writer-e2e-eval-template-${suffix}`,
        projectId,
        name: `Writer E2E evaluator ${suffix}`,
        version: 1,
        type: "CODE",
        sourceCode: "return 0.9;",
        sourceCodeLanguage: "TYPESCRIPT",
      },
    });
    const evalConfiguration = await prisma.jobConfiguration.create({
      data: {
        id: `writer-e2e-eval-config-${suffix}`,
        projectId,
        jobType: "EVAL",
        evalTemplateId: evalTemplate.id,
        scoreName: "managed-quality",
        filter: [],
        targetObject: "trace",
        variableMapping: [],
        sampling: new Prisma.Decimal(1),
        delay: 0,
        timeScope: ["NEW"],
      },
    });
    const evalJobExecutionId = `writer-e2e-eval-job-${suffix}`;
    await prisma.jobExecution.create({
      data: {
        id: evalJobExecutionId,
        projectId,
        jobConfigurationId: evalConfiguration.id,
        status: JobExecutionStatus.PENDING,
        jobInputTraceId: traceId,
        jobInputTraceTimestamp: new Date("2026-07-17T10:00:00.123Z"),
      },
    });
    let legacyScoreWrites = 0;
    let scoreOperationProcessed = false;
    await completeEvalExecution({
      projectId,
      jobExecutionId: evalJobExecutionId,
      result: {
        scores: [
          {
            name: "managed-quality",
            value: 0.9,
            dataType: ScoreDataTypeEnum.NUMERIC,
          },
        ],
        executionTraceId: `writer-e2e-eval-trace-${suffix}`,
        metadata: { evaluator: "real-doris-e2e" },
      },
      traceId,
      observationId: spanId,
      environment: "production",
      scoreTimestamp: new Date("2026-07-17T10:03:00.000Z"),
      deps: {
        persistScoreBatch: async (params) => {
          await persistDorisEvalScoreBatch(
            {
              ...params,
              maxWaitMs: 30_000,
              pollIntervalMs: 1,
            },
            {
              accept: (acceptInput) =>
                acceptAnalyticsIngestion({
                  ...acceptInput,
                  client: prisma,
                  storageService: storage,
                  rawPrefix: canonicalPrefix,
                }),
              getStatus: async (statusInput) => {
                const currentStatus =
                  await getAnalyticsIngestionStatusForProject({
                    ...statusInput,
                    client: prisma,
                  });
                if (currentStatus && !scoreOperationProcessed) {
                  scoreOperationProcessed = true;
                  const scoreJobs: TQueueJobTypes[QueueName.AnalyticsIngestionQueue][] =
                    [];
                  await expect(
                    publishAnalyticsIngestionOutboxBatch({
                      client: prisma,
                      workerId: "writer-e2e-eval-score-publisher",
                      now: new Date(),
                      getAdmissionContext: () => workerAdmissionContext,
                      queue: {
                        add: async (_name, data) => {
                          scoreJobs.push(data);
                          return {
                            getState: async () => "waiting",
                            retry: async () => undefined,
                          };
                        },
                      },
                    }),
                  ).resolves.toBe(1);
                  expect(scoreJobs).toHaveLength(1);
                  await processor({ data: scoreJobs[0]! } as Job, "token");
                }
                return currentStatus?.status === "VISIBLE"
                  ? currentStatus
                  : getAnalyticsIngestionStatusForProject({
                      ...statusInput,
                      client: prisma,
                    });
              },
              getAdmissionContext: () => workerAdmissionContext,
              getStorageService: () => storage,
              wait: async () => undefined,
            },
          );
        },
        updateJobExecution: async ({ id, projectId, data }) => {
          await prisma.jobExecution.update({
            where: { id, projectId },
            data,
          });
        },
        uploadScore: async () => {
          legacyScoreWrites += 1;
          throw new Error("Legacy evaluator score upload must not run");
        },
        enqueueScoreIngestion: async () => {
          legacyScoreWrites += 1;
          throw new Error("Legacy evaluator ingestion queue must not run");
        },
        callLLM: async () => {
          throw new Error("LLM execution is outside this persistence test");
        },
        fetchModelConfig: async () => ({
          valid: false,
          error: "Model lookup is outside this persistence test",
        }),
      },
    });
    expect(legacyScoreWrites).toBe(0);
    const completedEvalJob = await prisma.jobExecution.findUniqueOrThrow({
      where: { id: evalJobExecutionId },
    });
    expect(completedEvalJob).toMatchObject({
      status: JobExecutionStatus.COMPLETED,
      jobOutputScoreId: expect.any(String),
      executionTraceId: `writer-e2e-eval-trace-${suffix}`,
    });
    await expect(
      doris.query<{ score_id: string; value: number; source: string }>(
        "SELECT score_id, `value`, source FROM scores_current WHERE project_id = ? AND score_id = ?",
        [projectId, completedEvalJob.jobOutputScoreId],
      ),
    ).resolves.toEqual([
      {
        score_id: completedEvalJob.jobOutputScoreId,
        value: 0.9,
        source: "EVAL",
      },
    ]);

    const [
      events,
      scores,
      files,
      artifactFiles,
      eventOperation,
      scoreOperation,
      legacyEvents,
      legacyOperation,
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
        include: { candidates: true, loadBatches: true, outboxV2: true },
      }),
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: scoreOperationId },
        include: { candidates: true, loadBatches: true, outboxV2: true },
      }),
      doris.query<{
        name: string;
        input: string;
        metadata: Record<string, unknown>;
      }>(
        "SELECT name, input, metadata FROM events_current WHERE project_id = ? AND trace_id = ? AND span_id = ?",
        [projectId, traceId, `t-${traceId}`],
      ),
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: legacyOperationId },
        include: { candidates: true, loadBatches: true, outboxV2: true },
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
    expect(artifactFiles).toHaveLength(4);
    expect(eventOperation).toMatchObject({
      status: "VISIBLE",
      manifestState: "FROZEN",
      terminalAt: expect.any(Date),
      rawObjectKey,
      outboxV2: { status: "PUBLISHED" },
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
      outboxV2: { status: "PUBLISHED" },
    });
    expect(scoreOperation.candidates).toHaveLength(2);
    expect(scoreOperation.loadBatches).toHaveLength(2);
    expect(
      scoreOperation.loadBatches.every(({ status }) => status === "VISIBLE"),
    ).toBe(true);
    expect(legacyEvents).toEqual([
      {
        name: "legacy Doris trace",
        input: '{"legacy":true}',
        metadata: { source: "legacy-sdk" },
      },
    ]);
    expect(legacyOperation).toMatchObject({
      status: "VISIBLE",
      manifestState: "FROZEN",
      terminalAt: expect.any(Date),
      rawObjectKey: legacyRawObjectKey,
      outboxV2: { status: "PUBLISHED" },
    });
    expect(legacyOperation.candidates).toHaveLength(1);
    expect(legacyOperation.loadBatches).toHaveLength(1);
  }, 120_000);

  it("replays a disabled-window canonical artifact before the next evaluation activation", async () => {
    await prisma.analyticsEvaluationDispatch.updateMany({
      where: {
        capabilityActivationGeneration: 1n,
        status: { in: ["PENDING", "PUBLISHED", "PROCESSING"] },
      },
      data: {
        status: "COMPLETED",
        completedAt: new Date("2026-07-18T12:32:00.000Z"),
      },
    });
    await beginAnalyticsCapabilityDrain({
      client: prisma,
      capability: "evaluations",
      expectedDeploymentGeneration: deploymentGeneration,
      expectedActivationGeneration: 1n,
      now: new Date("2026-07-18T12:33:00.000Z"),
    });
    const disabled = await disableAnalyticsCapability({
      client: prisma,
      capability: "evaluations",
      expectedDeploymentGeneration: deploymentGeneration,
      expectedActivationGeneration: 1n,
      captureRequired: true,
      rescanRequired: true,
      verifyDurableDrain: (transaction, provenance) =>
        verifyDurableAnalyticsEvaluationDrain(transaction, provenance),
      sealReplayCutoff: (transaction) =>
        sealAnalyticsEvaluationReplayCutoff({
          transaction,
          deploymentGeneration,
          activationGeneration: 1n,
          now: new Date("2026-07-18T12:34:00.000Z"),
        }),
      now: new Date("2026-07-18T12:34:00.000Z"),
    });
    expect(disabled.cutoffDigest).toMatch(/^[a-f0-9]{64}$/);

    const replayRawObjectKey = `${canonicalPrefix}analytics-ingestion/raw/${projectId}/${replayOperationId}.json`;
    await acceptAnalyticsIngestion({
      client: prisma,
      operationId: replayOperationId,
      projectId,
      sourceOperationId: `source-${replayOperationId}`,
      envelope: {
        formatVersion: 1,
        source: "legacy-event",
        payload: [
          {
            id: `replay-trace-event-${suffix}`,
            type: "trace-create",
            timestamp: "2026-07-18T12:34:01.000000000Z",
            body: {
              id: replayTraceId,
              timestamp: "2026-07-18T12:34:01.000000000Z",
              name: "disabled-window replay trace",
              environment: "production",
            },
          },
        ],
        attribution: {
          ingestionApiKey: "pk-writer-replay-e2e",
          ingestionSdkName: "langfuse-js",
          ingestionSdkVersion: "5.0.0",
        },
      },
      storageService: storage,
      rawPrefix: canonicalPrefix,
      acceptedAt: new Date("2026-07-18T12:34:01.000Z"),
      acceptedAtNanos: normalizeVersionToken("2026-07-18T12:34:01.000000000Z"),
      canonicalizerVersion: "1",
      schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
      admissionContext: producerAdmissionContext,
    });
    const queuedJobs: TQueueJobTypes[QueueName.AnalyticsIngestionQueue][] = [];
    await expect(
      publishAnalyticsIngestionOutboxBatch({
        client: prisma,
        workerId: "writer-e2e-replay-publisher",
        now: new Date("2026-07-18T12:34:02.000Z"),
        getAdmissionContext: () => workerAdmissionContext,
        queue: {
          add: async (_name, data) => {
            queuedJobs.push(data);
            return {
              getState: async () => "waiting",
              retry: async () => undefined,
            };
          },
        },
      }),
    ).resolves.toBe(1);
    expect(queuedJobs).toHaveLength(1);
    await ingestionProcessor({ data: queuedJobs[0]! } as Job, "token");
    await expect(
      prisma.analyticsEvaluationDispatch.count({
        where: { operationId: replayOperationId },
      }),
    ).resolves.toBe(0);
    await expect(
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: replayOperationId },
      }),
    ).resolves.toMatchObject({
      status: "VISIBLE",
      rawObjectKey: replayRawObjectKey,
      canonicalObjectKey: expect.any(String),
      canonicalArtifactChecksum: expect.stringMatching(/^[a-f0-9]{64}$/),
    });

    const nextDark = await beginAnalyticsCapabilityDark({
      client: prisma,
      capability: "evaluations",
      expectedGeneration: 1n,
      contractVersion: 1,
      minimumRuntimeContract: 1,
      now: new Date("2026-07-18T12:35:00.000Z"),
    });
    const runtimeInstanceIds = (
      await prisma.analyticsRuntimeLease.findMany({
        where: {
          id: {
            in: [
              producerAdmissionContext.runtimeLeaseId,
              workerAdmissionContext.runtimeLeaseId,
            ],
          },
        },
        select: { instanceId: true },
        orderBy: { instanceId: "asc" },
      })
    ).map(({ instanceId }) => instanceId);
    const capture = await enableAnalyticsCapabilityDarkCapture({
      client: prisma,
      capability: "evaluations",
      expectedDeploymentGeneration: deploymentGeneration,
      expectedActivationGeneration: nextDark.generation,
      expectedRuntimeInstanceIds: runtimeInstanceIds,
      now: new Date("2026-07-18T12:36:00.000Z"),
    });
    if (!capture.cutoffDigest) {
      throw new Error("Evaluation replay cutoff digest is missing");
    }
    const replay = await replayAnalyticsEvaluationCutoff({
      client: prisma,
      artifactStore: new CanonicalIngestionArtifactStore(
        new StorageServiceCanonicalObjectStore(storage),
      ),
      admissionContext: workerAdmissionContext,
      expectedCutoffDigest: capture.cutoffDigest,
      now: () => new Date("2026-07-18T12:37:00.000Z"),
    });
    expect(replay).toMatchObject({
      operationsScanned: 1,
      targetsVerified: 2,
      bootstrapEvidenceDigest: expect.stringMatching(/^[a-f0-9]{64}$/),
    });
    await expect(
      prisma.analyticsEvaluationDispatch.count({
        where: { operationId: replayOperationId },
      }),
    ).resolves.toBe(2);
    await expect(
      prisma.analyticsEvaluationDispatch.findFirstOrThrow({
        where: {
          operationId: replayOperationId,
          targetId: replayTraceId,
        },
      }),
    ).resolves.toMatchObject({
      status: "SUSPENDED",
      capabilityActivationGeneration: nextDark.generation,
      deploymentGeneration,
      captureRuntimeLeaseId: workerAdmissionContext.runtimeLeaseId,
    });

    const completed = await completeAnalyticsCapabilityBootstrap({
      client: prisma,
      capability: "evaluations",
      expectedDeploymentGeneration: deploymentGeneration,
      expectedActivationGeneration: nextDark.generation,
      expectedCutoffDigest: capture.cutoffDigest,
      verifyDurableBootstrap: (transaction) =>
        verifyDurableAnalyticsEvaluationBootstrap(transaction, {
          deploymentGeneration,
          activationGeneration: nextDark.generation,
          expectedCutoffDigest: capture.cutoffDigest!,
        }),
      now: new Date("2026-07-18T12:38:00.000Z"),
    });
    const activated = await activateAnalyticsCapability({
      client: prisma,
      capability: "evaluations",
      expectedDeploymentGeneration: deploymentGeneration,
      expectedActivationGeneration: nextDark.generation,
      expectedRuntimeInstanceIds: runtimeInstanceIds,
      expectedBootstrapEvidenceDigest: completed.bootstrapEvidenceDigest!,
      now: new Date("2026-07-18T12:39:00.000Z"),
    });
    expect(activated.status).toBe("ACTIVE");
    await expect(
      prisma.analyticsEvaluationDispatch.findFirstOrThrow({
        where: {
          operationId: replayOperationId,
          targetId: replayTraceId,
        },
      }),
    ).resolves.toMatchObject({
      status: "PENDING",
      capabilityActivationGeneration: nextDark.generation,
    });
  }, 120_000);

  it("makes a dataset-run tombstone visible before physically removing its row", async () => {
    await acceptAnalyticsIngestion({
      client: prisma,
      operationId: datasetOperationId,
      projectId,
      sourceOperationId: `source-${datasetOperationId}`,
      envelope: {
        formatVersion: 1,
        source: "dataset-run-item",
        payload: [
          {
            id: `dataset-event-${suffix}`,
            type: "dataset-run-item-create",
            timestamp: "2026-07-17T10:02:00.987654321Z",
            body: {
              id: datasetRunItemId,
              traceId,
              createdAt: "2026-07-17T10:02:00.000Z",
              datasetId,
              runId: datasetRunId,
              datasetItemId,
              datasetVersion: "2026-07-17T09:59:00.000Z",
            },
          },
        ],
        isLangfuseInternal: true,
        attribution: {
          ingestionApiKey: "pk-writer-dataset-e2e",
          ingestionSdkName: "langfuse-js",
          ingestionSdkVersion: "5.0.0",
        },
      },
      storageService: storage,
      rawPrefix: canonicalPrefix,
      acceptedAt: new Date("2026-07-18T12:30:00.123Z"),
      acceptedAtNanos: acceptedAt,
      canonicalizerVersion: "1",
      schemaVersion: NEXT_ANALYTICS_SCHEMA_VERSION,
      admissionContext: producerAdmissionContext,
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
        load: (input) =>
          streamLoad.load({
            table: input.table,
            database: input.database,
            label: input.label,
            ndjsonBody: input.ndjsonBody,
          }),
        reconcile: (input) => streamLoad.reconcile({ label: input.label }),
      },
      databaseName: DB,
      workerId: "writer-dataset-e2e-worker",
      getAdmissionContext: () => workerAdmissionContext,
      eventCanonicalizer: new EventCanonicalizer({
        warnOnUsageTotalMismatch: () => undefined,
        resolvePrompt: async () => null,
        resolveGenerationUsage: async () => ({
          usageDetails: { input: 0, output: 0, total: 0 },
          costDetails: { input: 0, output: 0, total: 0 },
          totalCost: 0,
        }),
      }),
    });
    const queuedJobs: TQueueJobTypes[QueueName.AnalyticsIngestionQueue][] = [];
    await expect(
      publishAnalyticsIngestionOutboxBatch({
        client: prisma,
        workerId: "writer-dataset-e2e-publisher",
        now: new Date("2026-07-18T12:30:01.000Z"),
        getAdmissionContext: () => workerAdmissionContext,
        queue: {
          add: async (_name, data) => {
            queuedJobs.push(data);
            return {
              getState: async () => "waiting",
              retry: async () => undefined,
            };
          },
        },
      }),
    ).resolves.toBe(1);
    expect(queuedJobs).toHaveLength(1);
    const job = {
      data: queuedJobs[0]!,
    } as Job<TQueueJobTypes[QueueName.AnalyticsIngestionQueue]>;
    await expect(processor(job, "token")).resolves.toBeUndefined();
    await expect(processor(job, "token")).resolves.toBeUndefined();

    await expect(
      doris.query<{
        run_item_id: string;
        dataset_run_name: string;
        dataset_run_metadata_json: string;
        dataset_item_input: string;
        dataset_item_expected_output: string;
        dataset_item_metadata_json: string;
      }>(
        "SELECT run_item_id, dataset_run_name, dataset_run_metadata_json, dataset_item_input, dataset_item_expected_output, dataset_item_metadata_json FROM dataset_run_items_current WHERE project_id = ? AND run_item_id = ?",
        [projectId, datasetRunItemId],
      ),
    ).resolves.toEqual([
      {
        run_item_id: datasetRunItemId,
        dataset_run_name: "real experiment",
        dataset_run_metadata_json: '{"owner":"test"}',
        dataset_item_input: '{"prompt":"hello"}',
        dataset_item_expected_output: '"world"',
        dataset_item_metadata_json: '{"split":"test"}',
      },
    ]);

    const exportSource = createDorisAnalyticsExportSource();
    const exportRequest = {
      projectId,
      tableName: BatchExportTableName.DatasetRunItems,
      filter: [],
      orderBy: null,
      cutoffCreatedAt: new Date("2026-07-18T00:00:00.000Z"),
      fileFormat: BatchExportFileFormat.JSONL,
      useEventsTable: true,
    };
    const identities = [];
    for await (const identity of exportSource.scanIdentities!(
      exportRequest,
      10,
    )) {
      identities.push(identity);
    }
    expect(identities).toEqual([{ id: datasetRunItemId }]);
    const exportedRows = [];
    const exportStream = await exportSource.open(exportRequest, {
      identities: (async function* () {
        yield* identities;
      })(),
      revalidate: async () => undefined,
    });
    for await (const row of exportStream) exportedRows.push(row);
    expect(exportedRows).toEqual([
      expect.objectContaining({
        id: datasetRunItemId,
        projectId,
        datasetItemId,
        traceId,
      }),
    ]);

    const generation =
      await prisma.analyticsDatasetRunDeletionGeneration.create({
        data: {
          projectId,
          datasetId,
          datasetRunId,
          generation: 1n,
        },
      });
    const operation = await prisma.analyticsDatasetDeletionOperation.create({
      data: {
        scope: "DATASET_RUNS",
        projectId,
        datasetId,
        datasetRunIds: [datasetRunId],
        runGenerations: { [datasetRunId]: generation.generation.toString() },
        outbox: { create: { status: "PUBLISHED" } },
      },
    });
    const scheduled = {
      operation,
      datasetGeneration: null,
      runGenerations: { [datasetRunId]: generation.generation },
    };
    await processDatasetDelete({
      deletionType: "dataset-runs",
      projectId,
      datasetId,
      datasetRunIds: [datasetRunId],
      analyticsDeletion: analyticsDatasetDeletionQueueReference(scheduled),
    });

    await expect(
      prisma.analyticsDatasetDeletionOperation.findUniqueOrThrow({
        where: { id: operation.id },
      }),
    ).resolves.toMatchObject({
      status: "COMPLETED",
      phase: "completed",
      logicallyInvisible: true,
    });
    expect(
      await doris.query<{ count: number }>(
        "SELECT count(*) AS count FROM dataset_run_tombstones WHERE project_id = ? AND dataset_run_id = ?",
        [projectId, datasetRunId],
      ),
    ).toEqual([{ count: 1 }]);
    expect(
      await doris.query<{ count: number }>(
        "SELECT count(*) AS count FROM dataset_run_items_current WHERE project_id = ? AND run_item_id = ?",
        [projectId, datasetRunItemId],
      ),
    ).toEqual([{ count: 0 }]);
  });

  it("seals a real Doris identity stream and inspects the exported MinIO file", async () => {
    const source = createDorisAnalyticsExportSource();
    const request = {
      projectId,
      tableName: BatchExportTableName.Traces,
      filter: [],
      orderBy: null,
      cutoffCreatedAt: new Date("2026-07-18T00:00:00.000Z"),
      fileFormat: BatchExportFileFormat.JSONL,
      useEventsTable: true,
    };
    const manifestObjectKey = `${canonicalPrefix}batch-export/manifest.txt`;
    const exportObjectKey = `${canonicalPrefix}batch-export/traces.jsonl`;
    const metadata = {
      batchExportId: `writer-e2e-export-${suffix}`,
      projectId,
      tableName: request.tableName,
      generation: 1n,
      claimId: `writer-e2e-claim-${suffix}`,
      filterHash: "a".repeat(64),
    };
    const descriptor = await writeBatchExportIdentityManifest({
      storage,
      objectKey: manifestObjectKey,
      metadata,
      identities: source.scanIdentities!(request, 10),
      maxRows: 10,
    });
    expect(descriptor.rowCount).toBe(1);

    const encodedManifest =
      await storage.downloadStreamIfExists(manifestObjectKey);
    expect(encodedManifest).not.toBeNull();
    const identities = await openVerifiedBatchExportIdentityManifest({
      encodedBody: encodedManifest!,
      descriptor,
      expected: metadata,
      maxRows: 10,
    });
    let revalidations = 0;
    const rows = await source.open(request, {
      identities,
      revalidate: async () => {
        revalidations += 1;
      },
    });
    const file = rows.pipe(
      streamTransformations[BatchExportFileFormat.JSONL](),
    );
    await storage.uploadFile({
      fileName: exportObjectKey,
      fileType: "application/x-ndjson; charset=utf-8",
      data: file,
    });

    const exported = await storage.download(exportObjectKey);
    const parsed = exported
      .trim()
      .split("\n")
      .map((line) => JSON.parse(line) as { id: string });
    expect(parsed).toEqual([expect.objectContaining({ id: traceId })]);
    expect(revalidations).toBe(1);
  }, 60_000);

  it("deletes trace and project data behind visible barriers without deleting lifecycle-owned raw objects", async () => {
    const rawObjectsBefore = await storage.listFiles(canonicalPrefix);
    expect(rawObjectsBefore.some(({ file }) => file === rawObjectKey)).toBe(
      true,
    );
    const [traceDeletion] = await scheduleTraceDeletionOperations({
      client: prisma,
      projectId,
      organizationId,
      traceIds: [traceId],
      requester: { principalType: "user", principalId: "writer-e2e-owner" },
      analyticsAdmissionContext: producerAdmissionContext,
    });
    const traceProvenance = analyticsDurableProvenanceFromRecord(
      traceDeletion!.operation,
    );
    if (!traceProvenance) {
      throw new Error("Expected managed trace deletion provenance");
    }
    await processAnalyticsTraceDelete(
      projectId,
      {
        operationId: traceDeletion!.operation.id,
        traceId,
        generation: traceDeletion!.generation,
        analyticsProvenance:
          serializeAnalyticsDurableProvenance(traceProvenance),
      },
      lifecycle,
      workerAdmissionContext,
    );

    const traces = new DorisTracesRepository({
      query: doris.query.bind(doris),
    });
    await expect(traces.get({ projectId, traceId })).resolves.toBeNull();
    const [traceOperation, tombstone, eventRows, scoreRows, fileRows] =
      await Promise.all([
        prisma.analyticsDeletionOperation.findUniqueOrThrow({
          where: { id: traceDeletion!.operation.id },
        }),
        doris.query<{ deletion_generation: string | number }>(
          "SELECT deletion_generation FROM trace_tombstones WHERE project_id = ? AND trace_id = ?",
          [projectId, traceId],
        ),
        doris.query(
          "SELECT span_id FROM events_current WHERE project_id = ? AND trace_id = ?",
          [projectId, traceId],
        ),
        doris.query(
          "SELECT score_id FROM scores_current WHERE project_id = ? AND trace_id = ?",
          [projectId, traceId],
        ),
        doris.query(
          "SELECT file_id FROM blob_storage_file_log WHERE project_id = ?",
          [projectId],
        ),
      ]);
    expect(traceOperation).toMatchObject({
      status: "COMPLETED",
      phase: "completed",
      logicallyInvisible: true,
    });
    expect(String(tombstone[0]!.deletion_generation)).toBe("1");
    expect(eventRows).toHaveLength(0);
    expect(scoreRows).toHaveLength(0);
    expect(fileRows).toHaveLength(0);
    const rawObjectsAfterTraceDelete = await storage.listFiles(canonicalPrefix);
    expect(
      rawObjectsAfterTraceDelete.some(({ file }) => file === rawObjectKey),
    ).toBe(true);

    const [supersededTraceDeletion] = await scheduleTraceDeletionOperations({
      client: prisma,
      projectId,
      organizationId,
      traceIds: [`${traceId}-superseded-by-project`],
      requester: { principalType: "user", principalId: "writer-e2e-owner" },
      analyticsAdmissionContext: producerAdmissionContext,
    });

    const projectDeletion = await scheduleProjectDeletionOperation({
      client: prisma,
      projectId,
      organizationId,
      requester: { principalType: "user", principalId: "writer-e2e-owner" },
      analyticsAdmissionContext: producerAdmissionContext,
    });
    const projectProvenance =
      analyticsDurableProvenanceFromRecord(projectDeletion);
    if (!projectProvenance) {
      throw new Error("Expected managed project deletion provenance");
    }
    await processAnalyticsProjectDelete(
      {
        projectId,
        organizationId,
        reference: {
          operationId: projectDeletion.id,
          generation: projectDeletion.generation,
          analyticsProvenance:
            serializeAnalyticsDurableProvenance(projectProvenance),
        },
      },
      lifecycle,
      workerAdmissionContext,
    );
    await expect(
      prisma.project.findUnique({ where: { id: projectId } }),
    ).resolves.toBeNull();
    await expect(
      prisma.analyticsDeletionOperation.findUniqueOrThrow({
        where: { id: projectDeletion.id },
      }),
    ).resolves.toMatchObject({
      organizationId,
      projectId,
      status: "COMPLETED",
      logicallyInvisible: true,
    });
    await expect(
      prisma.analyticsDeletionOperation.findUniqueOrThrow({
        where: { id: supersededTraceDeletion!.operation.id },
      }),
    ).resolves.toMatchObject({
      status: "COMPLETED",
      phase: "completed_by_project_deletion",
      cancellationReasonCode: "SUPERSEDED_BY_PROJECT_DELETION",
    });
    await expect(
      doris.query(
        "SELECT project_id FROM project_tombstones WHERE project_id = ?",
        [projectId],
      ),
    ).resolves.toHaveLength(1);
  }, 120_000);
});
