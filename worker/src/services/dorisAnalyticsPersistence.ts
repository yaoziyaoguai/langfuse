import { hostname } from "node:os";
import { randomUUID } from "node:crypto";

import type { PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  DorisStreamLoadClient,
  getS3EventStorageClient,
  PromptService,
  redis,
  parseDorisStreamLoadConfig,
  resolveDorisNodeEnv,
  type ResourceSpan,
  type StorageService,
} from "@langfuse/shared/src/server";

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
  readonly workerId: string;
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
  readonly maskOtlp?: (input: {
    readonly projectId: string;
    readonly resourceSpans: ResourceSpan[];
  }) => Promise<ResourceSpan[]>;
}): DorisAnalyticsPersistence {
  const runtimeEnv = input.runtimeEnv ?? env;
  const nodeEnv = runtimeEnv.NODE_ENV ?? "development";
  const streamConfig =
    input.streamLoadTransport && input.databaseName
      ? null
      : parseDorisStreamLoadConfig(
          {
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
  const writer = new AnalyticsWriter({
    client,
    artifactStore: new CanonicalIngestionArtifactStore(
      new StorageServiceCanonicalObjectStore(storageService),
    ),
    doris: new DorisBatchSink(streamLoadTransport, databaseName),
    databaseName,
    canonicalPrefix: runtimeEnv.LANGFUSE_S3_EVENT_UPLOAD_PREFIX ?? "",
    workerId,
  });
  const canonicalizer = new RawAnalyticsIngestionCanonicalizer({
    client,
    storageService,
    eventCanonicalizer,
    maskOtlp: input.maskOtlp,
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
    }),
    workerId,
  };
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
