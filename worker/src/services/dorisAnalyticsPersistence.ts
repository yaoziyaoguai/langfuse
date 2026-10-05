import { hostname } from "node:os";
import { randomUUID } from "node:crypto";

import type { PrismaClient } from "@prisma/client";
import { LangfuseConflictError } from "@langfuse/shared";
import {
  AnalyticsPersistenceError,
  encodeEventIdentity,
} from "@langfuse/shared/analytics-persistence";
import { prisma } from "@langfuse/shared/src/db";
import {
  getDorisTelemetryRepositories,
  DorisStreamLoadClient,
  getS3EventStorageClient,
  PromptService,
  redis,
  parseDorisStreamLoadConfig,
  reconcileRawAnalyticsIngestionReceipts,
  expireUnreadyAnalyticsIngestionReceipts,
  resolveDorisNodeEnv,
  type ResourceSpan,
  type StorageService,
  type DorisObservation,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";
import { applyConfiguredIngestionMasking } from "@langfuse/shared/src/server/ingestion-masking";

import { env } from "../env";
import { analyticsIngestionQueueProcessorBuilder } from "../queues/analyticsIngestionQueue";
import {
  CanonicalIngestionArtifactStore,
  StorageServiceCanonicalObjectStore,
} from "./CanonicalIngestionArtifactStore";
import { AnalyticsWriter } from "./AnalyticsWriter";
import {
  DorisBatchSink,
  type DorisStreamLoadTransport,
} from "./AnalyticsWriter/DorisBatchSink";
import type { EventCanonicalizer } from "./EventCanonicalizer";
import { EventCanonicalizer as ProductionEventCanonicalizer } from "./EventCanonicalizer";
import {
  AnalyticsGenerationUsageResolver,
  warnOnUsageTotalMismatch,
} from "./AnalyticsGenerationUsageResolver";
import { RawAnalyticsIngestionCanonicalizer } from "./RawAnalyticsIngestionCanonicalizer";
import { assertDorisAnalyticsReady } from "./dorisAnalyticsReadiness";
import {
  type LegacyEventData,
  type LoadCurrentLegacyEvent,
} from "./LegacyEventCanonicalizer";
import { RedisLock } from "../utils/RedisLock";
import { getWorkerAnalyticsAdmissionContext } from "../analyticsRuntime";

type RuntimeEnvironment = {
  readonly NODE_ENV?: "development" | "test" | "production";
  readonly DORIS_LOCAL_DEV_MODE?: "true" | "false";
  readonly LANGFUSE_S3_EVENT_UPLOAD_BUCKET: string;
  readonly LANGFUSE_S3_EVENT_UPLOAD_PREFIX?: string;
  readonly DORIS_QUERY_USER?: string;
  readonly DORIS_STREAM_LOAD_FE_URL?: string;
  readonly DORIS_STREAM_LOAD_USER?: string;
  readonly DORIS_STREAM_LOAD_PASSWORD?: string;
  readonly DORIS_STREAM_LOAD_DATABASE?: string;
  readonly DORIS_STREAM_LOAD_FE_IP_ALLOWLIST?: string;
  readonly DORIS_STREAM_LOAD_BE_ALLOWLIST?: string;
  readonly DORIS_STREAM_LOAD_BE_IP_ALLOWLIST?: string;
  readonly DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP?: string;
  readonly DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST?: string;
  readonly DORIS_STREAM_LOAD_TLS_CA_PATH?: string;
  readonly DORIS_STREAM_LOAD_REQUEST_TIMEOUT_MS?: number | string;
  readonly DORIS_STREAM_LOAD_MAX_BODY_BYTES?: number | string;
};

function optionalString(value: number | string | undefined) {
  return value === undefined ? undefined : String(value);
}

export interface DorisAnalyticsPersistence {
  readonly writer: AnalyticsWriter;
  readonly canonicalizer: RawAnalyticsIngestionCanonicalizer;
  readonly processor: ReturnType<
    typeof analyticsIngestionQueueProcessorBuilder
  >;
  readonly reconcileRaw: (
    limit: number,
    cursor?: string,
  ) => ReturnType<typeof reconcileRawAnalyticsIngestionReceipts>;
  readonly workerId: string;
}

function stringRecord(
  value: Readonly<Record<string, unknown>> | undefined,
): Record<string, string> | undefined {
  if (!value) return undefined;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, String(item)]),
  );
}

function toLegacyEventData(observation: DorisObservation): LegacyEventData {
  return {
    projectId: observation.projectId,
    traceId: observation.traceId,
    spanId: observation.id,
    ...(observation.parentObservationId !== null && {
      parentSpanId: observation.parentObservationId,
    }),
    type: observation.type,
    name: observation.name ?? undefined,
    environment: observation.environment,
    version: observation.version ?? undefined,
    release: observation.release ?? undefined,
    traceName: observation.traceName ?? undefined,
    userId: observation.userId ?? undefined,
    sessionId: observation.sessionId ?? undefined,
    level: observation.level ?? undefined,
    statusMessage: observation.statusMessage ?? undefined,
    isAppRoot: observation.isAppRoot,
    bookmarked: observation.bookmarked,
    public: observation.public,
    tags: observation.tags.concat(),
    startTimeISO: observation.startTime.toISOString(),
    ...(observation.endTime && {
      endTimeISO: observation.endTime.toISOString(),
    }),
    ...(observation.completionStartTime && {
      completionStartTime: observation.completionStartTime.toISOString(),
    }),
    promptId: observation.promptId ?? undefined,
    promptName: observation.promptName ?? undefined,
    promptVersion:
      observation.promptVersion === null
        ? undefined
        : String(observation.promptVersion),
    modelId: observation.internalModelId ?? undefined,
    modelName: observation.providedModelName ?? undefined,
    modelParameters: observation.modelParameters
      ? { ...observation.modelParameters }
      : undefined,
    providedUsageDetails: { ...observation.providedUsageDetails },
    usageDetails: { ...observation.usageDetails },
    providedCostDetails: { ...observation.providedCostDetails },
    costDetails: { ...observation.costDetails },
    toolDefinitions: stringRecord(observation.toolDefinitions),
    toolCalls: observation.toolCalls
      ? observation.toolCalls.concat()
      : undefined,
    toolCallNames: observation.toolCallNames
      ? observation.toolCallNames.concat()
      : undefined,
    input: observation.input,
    output: observation.output,
    metadata: { ...(observation.metadata ?? {}) },
    source: "ingestion-api-legacy",
  };
}

export function createLegacyCurrentEventLoader(input: {
  readonly client: PrismaClient;
  readonly getObservation?: (query: {
    readonly projectId: string;
    readonly traceId?: string;
    readonly observationId: string;
  }) => Promise<DorisObservation | null>;
}): LoadCurrentLegacyEvent {
  const getObservation =
    input.getObservation ??
    ((query) => getDorisTelemetryRepositories().observations.get(query));
  return async ({ projectId, traceId, spanId }) => {
    const loadHead = (resolvedTraceId: string) =>
      input.client.analyticsEntityHead.findUnique({
        where: {
          projectId_entityType_entityKey: {
            projectId,
            entityType: "EVENT",
            entityKey: encodeEventIdentity({
              projectId,
              traceId: resolvedTraceId,
              spanId,
            }),
          },
        },
        select: { sourceVersion: true },
      });
    const knownHead = traceId ? await loadHead(traceId) : null;
    if (traceId && !knownHead) return null;
    let observation: DorisObservation | null;
    try {
      observation = await getObservation({
        projectId,
        ...(traceId ? { traceId } : {}),
        observationId: spanId,
      });
    } catch (error) {
      if (error instanceof LangfuseConflictError) {
        throw new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false, {
          tags: { phase: "legacy_current_state" },
        });
      }
      throw error;
    }
    if (!observation) {
      if (!knownHead) return null;
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: { phase: "legacy_current_state" },
      });
    }
    const head = knownHead ?? (await loadHead(observation.traceId));
    if (!head) {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: { phase: "legacy_current_state" },
      });
    }
    return {
      sourceVersion: head.sourceVersion,
      eventData: toLegacyEventData(observation),
    };
  };
}

const LEGACY_SOURCE_OPERATION = /^legacy:([a-f0-9]{64}):[a-f0-9]{64}$/;

async function withLegacyOperationLock(
  operation: { readonly sourceOperationId: string },
  run: () => Promise<void>,
): Promise<void> {
  const match = LEGACY_SOURCE_OPERATION.exec(operation.sourceOperationId);
  if (!match) {
    await run();
    return;
  }
  const lock = new RedisLock(`analytics:legacy-event:${match[1]}`, {
    ttlSeconds: 30 * 60,
    name: "Doris legacy event canonicalization",
    onUnavailable: "fail",
  });
  const result = await lock.withLock(run);
  if (result === null) {
    throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
      tags: {
        phase: "legacy_entity_lock",
        reasonCode: "LEGACY_ENTITY_LOCK_HELD",
      },
    });
  }
}

/**
 * U4 的显式 Doris composition root。发布路径在 U8 前不会调用它；集成测试可注入
 * 本地传输映射，但生产调用始终从经过校验的 workload env 构造凭据与 allowlist。
 */
export function createDorisAnalyticsPersistence(input: {
  readonly runtimeEnv?: RuntimeEnvironment;
  readonly prismaClient?: PrismaClient;
  readonly storageService?: StorageService;
  readonly streamLoadTransport?: DorisStreamLoadTransport;
  readonly databaseName?: string;
  readonly workerId?: string;
  readonly eventCanonicalizer?: EventCanonicalizer;
  readonly getAdmissionContext?: () => AnalyticsRuntimeAdmissionContext | null;
  readonly maskOtlp?: (input: {
    readonly projectId: string;
    readonly resourceSpans: ResourceSpan[];
    readonly orgId?: string;
    readonly propagatedHeaders?: Readonly<Record<string, string>>;
  }) => Promise<ResourceSpan[]>;
}): DorisAnalyticsPersistence {
  const runtimeEnv = input.runtimeEnv ?? env;
  const nodeEnv = runtimeEnv.NODE_ENV ?? "development";
  const streamConfig =
    input.streamLoadTransport && input.databaseName
      ? null
      : parseDorisStreamLoadConfig(
          {
            DORIS_LOCAL_DEV_MODE: runtimeEnv.DORIS_LOCAL_DEV_MODE,
            DORIS_QUERY_USER: runtimeEnv.DORIS_QUERY_USER,
            DORIS_STREAM_LOAD_FE_URL: runtimeEnv.DORIS_STREAM_LOAD_FE_URL,
            DORIS_STREAM_LOAD_USER: runtimeEnv.DORIS_STREAM_LOAD_USER,
            DORIS_STREAM_LOAD_PASSWORD: runtimeEnv.DORIS_STREAM_LOAD_PASSWORD,
            DORIS_STREAM_LOAD_DATABASE: runtimeEnv.DORIS_STREAM_LOAD_DATABASE,
            DORIS_STREAM_LOAD_FE_IP_ALLOWLIST:
              runtimeEnv.DORIS_STREAM_LOAD_FE_IP_ALLOWLIST,
            DORIS_STREAM_LOAD_BE_ALLOWLIST:
              runtimeEnv.DORIS_STREAM_LOAD_BE_ALLOWLIST,
            DORIS_STREAM_LOAD_BE_IP_ALLOWLIST:
              runtimeEnv.DORIS_STREAM_LOAD_BE_IP_ALLOWLIST,
            DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP:
              runtimeEnv.DORIS_STREAM_LOAD_REDIRECT_ORIGIN_REWRITE_MAP,
            DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST:
              runtimeEnv.DORIS_STREAM_LOAD_REDIRECT_REWRITE_IP_ALLOWLIST,
            DORIS_STREAM_LOAD_TLS_CA_PATH:
              runtimeEnv.DORIS_STREAM_LOAD_TLS_CA_PATH,
            DORIS_STREAM_LOAD_REQUEST_TIMEOUT_MS: optionalString(
              runtimeEnv.DORIS_STREAM_LOAD_REQUEST_TIMEOUT_MS,
            ),
            DORIS_STREAM_LOAD_MAX_BODY_BYTES: optionalString(
              runtimeEnv.DORIS_STREAM_LOAD_MAX_BODY_BYTES,
            ),
          },
          resolveDorisNodeEnv(nodeEnv, runtimeEnv.DORIS_LOCAL_DEV_MODE),
        );
  const streamLoadTransport =
    input.streamLoadTransport ?? new DorisStreamLoadClient(streamConfig!);
  const databaseName = input.databaseName ?? streamConfig!.database;
  const storageService =
    input.storageService ??
    getS3EventStorageClient(runtimeEnv.LANGFUSE_S3_EVENT_UPLOAD_BUCKET);
  const workerId =
    input.workerId ??
    `doris-writer-${hostname()}-${process.pid}-${randomUUID()}`;
  const client = input.prismaClient ?? prisma;
  const eventCanonicalizer =
    input.eventCanonicalizer ?? createProductionEventCanonicalizer();
  const getAdmissionContext =
    input.getAdmissionContext ?? getWorkerAnalyticsAdmissionContext;
  const writer = new AnalyticsWriter({
    client,
    artifactStore: new CanonicalIngestionArtifactStore(
      new StorageServiceCanonicalObjectStore(storageService),
    ),
    doris: new DorisBatchSink(streamLoadTransport, databaseName),
    databaseName,
    canonicalPrefix: runtimeEnv.LANGFUSE_S3_EVENT_UPLOAD_PREFIX ?? "",
    workerId,
    getAdmissionContext,
  });
  const canonicalizer = new RawAnalyticsIngestionCanonicalizer({
    client,
    storageService,
    eventCanonicalizer,
    maskOtlp: input.maskOtlp ?? applyRuntimeIngestionMasking,
    loadCurrentEvent: createLegacyCurrentEventLoader({ client }),
  });

  return {
    writer,
    canonicalizer,
    processor: analyticsIngestionQueueProcessorBuilder({
      client,
      sink: writer,
      assertReady: assertDorisAnalyticsReady,
      reconcileUnresolved: (operation) =>
        writer.reconcileUnresolvedOperation(operation),
      canonicalize: (operation) => canonicalizer.canonicalize(operation),
      withOperationLock: withLegacyOperationLock,
      getAdmissionContext,
    }),
    reconcileRaw: async (limit, cursor) => {
      const result = await reconcileRawAnalyticsIngestionReceipts({
        client,
        storageService,
        rawPrefix: runtimeEnv.LANGFUSE_S3_EVENT_UPLOAD_PREFIX,
        limit,
        ...(cursor === undefined ? {} : { cursor }),
        admissionContext: getAdmissionContext(),
      });
      await expireUnreadyAnalyticsIngestionReceipts({
        client,
        limit,
        rawArtifactExists: async (rawObjectKey) =>
          (await storageService.downloadIfExists(rawObjectKey)) !== null,
      });
      return result;
    },
    workerId,
  };
}

async function applyRuntimeIngestionMasking(input: {
  readonly projectId: string;
  readonly resourceSpans: ResourceSpan[];
  readonly orgId?: string;
  readonly propagatedHeaders?: Readonly<Record<string, string>>;
}): Promise<ResourceSpan[]> {
  const result = await applyConfiguredIngestionMasking({
    data: input.resourceSpans,
    projectId: input.projectId,
    orgId: input.orgId,
    propagatedHeaders: input.propagatedHeaders
      ? { ...input.propagatedHeaders }
      : undefined,
  });
  if (!result.success) {
    throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
      tags: {
        phase: "ingestion_masking",
        reasonCode: "MASKING_CALLBACK_FAILED",
      },
    });
  }
  return result.data;
}

function createProductionEventCanonicalizer(): EventCanonicalizer {
  if (!redis) {
    throw new Error("Redis is required for Doris analytics enrichment");
  }
  const promptService = new PromptService(prisma, redis);
  const usageResolver = new AnalyticsGenerationUsageResolver();
  return new ProductionEventCanonicalizer({
    warnOnUsageTotalMismatch,
    resolvePrompt: async ({ projectId, promptName, promptVersion }) => {
      const prompt = await promptService.getPrompt({
        projectId,
        promptName,
        version: promptVersion,
        label: undefined,
      });
      return prompt
        ? { id: prompt.id, name: prompt.name, version: prompt.version }
        : null;
    },
    resolveGenerationUsage: usageResolver.resolve,
  });
}
