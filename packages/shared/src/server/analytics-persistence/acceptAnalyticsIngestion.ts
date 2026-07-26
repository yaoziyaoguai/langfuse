import { createHash, randomUUID } from "node:crypto";

import type { AnalyticsIngestionOperation, PrismaClient } from "@prisma/client";

import { prisma } from "../../db";
import {
  AnalyticsIngestionReceiptConflictError,
  createAnalyticsIngestionReceipt,
  findAnalyticsIngestionOperationForProject,
} from "../repositories/analyticsIngestionOperations";
import { safeBlobKeySegment } from "../services/safeBlobKeySegment";
import type { StorageService } from "../services/StorageService";
import {
  lockAnalyticsAdmission,
  lockLegacyAnalyticsAdmission,
  type AnalyticsRuntimeAdmissionContext,
} from "./analyticsBackendAdmission";
import type { AnalyticsCapabilityName } from "./analyticsCapabilities";
import {
  analyticsDurableProvenanceFromRecord,
  analyticsProducerProvenanceFromAdmission,
  deserializeAnalyticsDurableProvenance,
  serializeAnalyticsDurableProvenance,
  type AnalyticsDurableProvenance,
} from "./analyticsDurableProvenance";
import { AnalyticsPersistenceError } from "./errors";

const RAW_FORMAT_VERSION = 1;
export const MAX_RAW_ANALYTICS_BYTES = 100 * 1024 * 1024;
const REPLAY_HORIZON_MS = 7 * 24 * 60 * 60 * 1_000;
const STATUS_RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;

export type RawAnalyticsIngestionEnvelope = {
  readonly formatVersion: typeof RAW_FORMAT_VERSION;
  readonly source:
    | "otlp"
    | "score"
    | "annotation-score"
    | "dataset-run-item"
    | "internal-event"
    | "legacy-event";
  readonly payload: unknown;
  readonly isLangfuseInternal?: boolean;
  readonly attribution: {
    readonly ingestionApiKey: string;
    readonly ingestionSdkName: string;
    readonly ingestionSdkVersion: string;
  };
};

export type RawAnalyticsIngestionReceiptSeed = {
  readonly operationId: string;
  readonly projectId: string;
  readonly sourceOperationId: string;
  readonly acceptedAt: Date;
  readonly acceptedAtNanos: bigint;
  readonly canonicalizerVersion: string;
  readonly schemaVersion: number;
  readonly analyticsProvenance?: AnalyticsDurableProvenance;
};

export type DecodedRawAnalyticsIngestionEnvelope =
  RawAnalyticsIngestionEnvelope & {
    readonly receipt: RawAnalyticsIngestionReceiptSeed | null;
  };

function validationError(): AnalyticsPersistenceError {
  return new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
}

function analyticsDeploymentProvenanceMatches(
  existing: AnalyticsDurableProvenance | null | undefined,
  current: AnalyticsDurableProvenance | null | undefined,
): boolean {
  if (!existing || !current) return !existing && !current;

  // 重试可落到同一 deployment 的另一台 Web；原始 producer 仍由 raw envelope 保留。
  return (
    existing.analyticsBackend === current.analyticsBackend &&
    existing.deploymentGeneration === current.deploymentGeneration &&
    existing.workloadEpochFingerprint === current.workloadEpochFingerprint &&
    existing.runtimeContractVersion === current.runtimeContractVersion &&
    existing.capability === current.capability &&
    existing.capabilityActivationGeneration ===
      current.capabilityActivationGeneration &&
    existing.capabilityContractVersion === current.capabilityContractVersion
  );
}

export function encodeRawAnalyticsIngestionEnvelope(
  envelope: RawAnalyticsIngestionEnvelope,
  receipt?: RawAnalyticsIngestionReceiptSeed,
): string {
  if (
    envelope.formatVersion !== RAW_FORMAT_VERSION ||
    ![
      "otlp",
      "score",
      "annotation-score",
      "dataset-run-item",
      "internal-event",
      "legacy-event",
    ].includes(envelope.source) ||
    !("payload" in envelope) ||
    (envelope.isLangfuseInternal !== undefined &&
      typeof envelope.isLangfuseInternal !== "boolean") ||
    !envelope.attribution ||
    typeof envelope.attribution.ingestionApiKey !== "string" ||
    typeof envelope.attribution.ingestionSdkName !== "string" ||
    typeof envelope.attribution.ingestionSdkVersion !== "string"
  ) {
    throw validationError();
  }
  if (receipt) assertReceiptSeed(receipt);
  try {
    const body = JSON.stringify({
      formatVersion: RAW_FORMAT_VERSION,
      source: envelope.source,
      payload: envelope.payload,
      ...(envelope.isLangfuseInternal === true
        ? { isLangfuseInternal: true }
        : {}),
      attribution: envelope.attribution,
      ...(receipt
        ? {
            receipt: {
              operationId: receipt.operationId,
              projectId: receipt.projectId,
              sourceOperationId: receipt.sourceOperationId,
              acceptedAt: receipt.acceptedAt.toISOString(),
              acceptedAtNanos: receipt.acceptedAtNanos.toString(),
              canonicalizerVersion: receipt.canonicalizerVersion,
              schemaVersion: receipt.schemaVersion,
              ...(receipt.analyticsProvenance
                ? {
                    analyticsProvenance: serializeAnalyticsDurableProvenance(
                      receipt.analyticsProvenance,
                    ),
                  }
                : {}),
            },
          }
        : {}),
    });
    if (!body) throw validationError();
    JSON.parse(body);
    return body;
  } catch (error) {
    if (error instanceof AnalyticsPersistenceError) throw error;
    throw validationError();
  }
}

export function decodeRawAnalyticsIngestionEnvelope(
  body: string,
): DecodedRawAnalyticsIngestionEnvelope {
  try {
    const parsed: unknown = JSON.parse(body);
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      Array.isArray(parsed) ||
      !("payload" in parsed)
    ) {
      throw validationError();
    }
    const envelope = parsed as Record<string, unknown>;
    const attribution = envelope.attribution;
    if (
      envelope.formatVersion !== RAW_FORMAT_VERSION ||
      (envelope.source !== "otlp" &&
        envelope.source !== "score" &&
        envelope.source !== "annotation-score" &&
        envelope.source !== "dataset-run-item" &&
        envelope.source !== "internal-event" &&
        envelope.source !== "legacy-event") ||
      (envelope.isLangfuseInternal !== undefined &&
        typeof envelope.isLangfuseInternal !== "boolean") ||
      typeof attribution !== "object" ||
      attribution === null ||
      Array.isArray(attribution)
    ) {
      throw validationError();
    }
    const typedAttribution = attribution as Record<string, unknown>;
    if (
      typeof typedAttribution.ingestionApiKey !== "string" ||
      typeof typedAttribution.ingestionSdkName !== "string" ||
      typeof typedAttribution.ingestionSdkVersion !== "string"
    ) {
      throw validationError();
    }
    const receipt = decodeReceiptSeed(envelope.receipt);
    return {
      formatVersion: RAW_FORMAT_VERSION,
      source: envelope.source,
      payload: envelope.payload,
      isLangfuseInternal: envelope.isLangfuseInternal === true,
      attribution: {
        ingestionApiKey: typedAttribution.ingestionApiKey,
        ingestionSdkName: typedAttribution.ingestionSdkName,
        ingestionSdkVersion: typedAttribution.ingestionSdkVersion,
      },
      receipt,
    };
  } catch (error) {
    if (error instanceof AnalyticsPersistenceError) throw error;
    throw validationError();
  }
}

function assertReceiptSeed(seed: RawAnalyticsIngestionReceiptSeed): void {
  if (
    !seed.operationId ||
    !seed.projectId ||
    !seed.sourceOperationId ||
    !Number.isFinite(seed.acceptedAt.getTime()) ||
    seed.acceptedAtNanos < 0n ||
    seed.acceptedAtNanos / 1_000_000n !== BigInt(seed.acceptedAt.getTime()) ||
    !seed.canonicalizerVersion ||
    !Number.isSafeInteger(seed.schemaVersion) ||
    seed.schemaVersion <= 0 ||
    (seed.analyticsProvenance !== undefined &&
      (() => {
        try {
          serializeAnalyticsDurableProvenance(seed.analyticsProvenance);
          return false;
        } catch {
          return true;
        }
      })())
  ) {
    throw validationError();
  }
}

function decodeReceiptSeed(
  value: unknown,
): RawAnalyticsIngestionReceiptSeed | null {
  if (value === undefined) return null;
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw validationError();
  }
  const seed = value as Record<string, unknown>;
  if (
    typeof seed.operationId !== "string" ||
    typeof seed.projectId !== "string" ||
    typeof seed.sourceOperationId !== "string" ||
    typeof seed.acceptedAt !== "string" ||
    typeof seed.acceptedAtNanos !== "string" ||
    typeof seed.canonicalizerVersion !== "string" ||
    typeof seed.schemaVersion !== "number"
  ) {
    throw validationError();
  }
  try {
    const decoded = {
      operationId: seed.operationId,
      projectId: seed.projectId,
      sourceOperationId: seed.sourceOperationId,
      acceptedAt: new Date(seed.acceptedAt),
      acceptedAtNanos: BigInt(seed.acceptedAtNanos),
      canonicalizerVersion: seed.canonicalizerVersion,
      schemaVersion: seed.schemaVersion,
      ...(seed.analyticsProvenance === undefined
        ? {}
        : {
            analyticsProvenance: deserializeAnalyticsDurableProvenance(
              seed.analyticsProvenance,
            ),
          }),
    };
    assertReceiptSeed(decoded);
    return decoded;
  } catch (error) {
    if (error instanceof AnalyticsPersistenceError) throw error;
    throw validationError();
  }
}

function sha256(body: string): string {
  return createHash("sha256").update(body, "utf8").digest("hex");
}

export function assertRawAnalyticsBodySize(
  body: string,
  maxBytes = MAX_RAW_ANALYTICS_BYTES,
): void {
  if (!Number.isSafeInteger(maxBytes) || maxBytes <= 0) {
    throw new TypeError("Invalid raw analytics byte limit");
  }
  if (Buffer.byteLength(body, "utf8") > maxBytes) {
    throw new AnalyticsPersistenceError("ANALYTICS_RESOURCE_EXHAUSTED", false, {
      tags: {
        phase: "raw_acceptance",
        reasonCode: "RAW_BYTES_EXCEEDED",
      },
    });
  }
}

function normalizePrefix(prefix: string): string {
  const normalized = prefix.replace(/^\/+/, "");
  return normalized && !normalized.endsWith("/")
    ? `${normalized}/`
    : normalized;
}

function rawConflict(operationId: string): AnalyticsPersistenceError {
  return new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false, {
    tags: { operationId, phase: "raw_reconciliation" },
  });
}

const PERMANENT_RAW_RECEIPT_ERRORS = new Set([
  "Project not found",
  "Analytics ingestion is fenced by project deletion",
  "Analytics ingestion admission does not match provenance",
  "Analytics backend deployment generation changed",
  "Analytics ingestion durable provenance changed",
  "Legacy unstamped analytics work is fenced by deployment",
  "Managed analytics ingestion requires provenance",
]);

function isPermanentRawReceiptError(error: unknown): boolean {
  return (
    error instanceof AnalyticsIngestionReceiptConflictError ||
    (error instanceof AnalyticsPersistenceError && !error.retryable) ||
    (error instanceof Error && PERMANENT_RAW_RECEIPT_ERRORS.has(error.message))
  );
}

function validateExistingRaw(input: {
  readonly body: string;
  readonly operationId: string;
  readonly projectId: string;
  readonly sourceOperationId: string;
  readonly canonicalizerVersion: string;
  readonly schemaVersion: number;
  readonly producerProvenance: AnalyticsDurableProvenance | null;
  readonly envelope: RawAnalyticsIngestionEnvelope;
}): RawAnalyticsIngestionReceiptSeed {
  let decoded: DecodedRawAnalyticsIngestionEnvelope;
  try {
    decoded = decodeRawAnalyticsIngestionEnvelope(input.body);
  } catch {
    throw rawConflict(input.operationId);
  }
  const seed = decoded.receipt;
  const existingEnvelope: RawAnalyticsIngestionEnvelope = {
    formatVersion: decoded.formatVersion,
    source: decoded.source,
    payload: decoded.payload,
    ...(decoded.isLangfuseInternal === true
      ? { isLangfuseInternal: true }
      : {}),
    attribution: decoded.attribution,
  };
  if (
    !seed ||
    seed.operationId !== input.operationId ||
    seed.projectId !== input.projectId ||
    seed.sourceOperationId !== input.sourceOperationId ||
    seed.canonicalizerVersion !== input.canonicalizerVersion ||
    seed.schemaVersion !== input.schemaVersion ||
    !analyticsDeploymentProvenanceMatches(
      seed.analyticsProvenance,
      input.producerProvenance,
    ) ||
    encodeRawAnalyticsIngestionEnvelope(existingEnvelope, seed) !==
      input.body ||
    sha256(encodeRawAnalyticsIngestionEnvelope(existingEnvelope)) !==
      sha256(encodeRawAnalyticsIngestionEnvelope(input.envelope))
  ) {
    throw rawConflict(input.operationId);
  }
  return seed;
}

function receiptSeedFromOperation(
  operation: AnalyticsIngestionOperation,
): RawAnalyticsIngestionReceiptSeed {
  const provenance = analyticsDurableProvenanceFromRecord(operation);
  return {
    operationId: operation.id,
    projectId: operation.projectId,
    sourceOperationId: operation.sourceOperationId,
    acceptedAt: operation.acceptedAt,
    acceptedAtNanos: operation.acceptedAtNanos,
    canonicalizerVersion: operation.canonicalizerVersion,
    schemaVersion: operation.schemaVersion,
    ...(provenance ? { analyticsProvenance: provenance } : {}),
  };
}

export async function captureAnalyticsFoundationProvenance(input: {
  readonly client?: PrismaClient;
  readonly admissionContext: AnalyticsRuntimeAdmissionContext | null;
  readonly requiredContract: {
    readonly schemaVersion: number;
    readonly canonicalizerVersion: string;
  };
  readonly capability?: AnalyticsCapabilityName;
}): Promise<AnalyticsDurableProvenance | null> {
  const client = input.client ?? prisma;
  return client.$transaction(async (transaction) => {
    if (!input.admissionContext) {
      await lockLegacyAnalyticsAdmission(transaction);
      return null;
    }
    const admission = await lockAnalyticsAdmission({
      transaction,
      runtimeLeaseId: input.admissionContext.runtimeLeaseId,
      expectedBackend: input.admissionContext.backend,
      expectedDeploymentGeneration: input.admissionContext.deploymentGeneration,
      ...(input.capability
        ? { capability: input.capability, action: "externalProducer" as const }
        : { action: "foundation" as const }),
      requiredContract: input.requiredContract,
    });
    return analyticsProducerProvenanceFromAdmission(
      admission,
      input.capability,
    );
  });
}

export async function acceptAnalyticsIngestion(input: {
  readonly projectId: string;
  readonly envelope: RawAnalyticsIngestionEnvelope;
  readonly canonicalizerVersion: string;
  readonly schemaVersion: number;
  readonly storageService: StorageService;
  readonly client?: PrismaClient;
  readonly operationId?: string;
  readonly sourceOperationId?: string;
  readonly acceptedAt?: Date;
  readonly acceptedAtNanos?: bigint;
  readonly rawPrefix?: string;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext | null;
  readonly capability?: AnalyticsCapabilityName;
  readonly captureFoundationProvenance?: typeof captureAnalyticsFoundationProvenance;
  readonly createReceipt?: typeof createAnalyticsIngestionReceipt;
  readonly findReceipt?: typeof findAnalyticsIngestionOperationForProject;
}): Promise<{ readonly operationId: string; readonly status: "ACCEPTED" }> {
  const operationId = input.operationId ?? randomUUID();
  const acceptedAt = input.acceptedAt ?? new Date();
  const acceptedAtNanos =
    input.acceptedAtNanos ?? BigInt(acceptedAt.getTime()) * 1_000_000n;
  if (
    !input.projectId ||
    !operationId ||
    !Number.isFinite(acceptedAt.getTime()) ||
    acceptedAtNanos / 1_000_000n !== BigInt(acceptedAt.getTime()) ||
    !input.canonicalizerVersion ||
    !Number.isSafeInteger(input.schemaVersion) ||
    input.schemaVersion <= 0
  ) {
    throw validationError();
  }
  // 在 admission 或 storage I/O 前先拒绝无法稳定序列化的 envelope。
  encodeRawAnalyticsIngestionEnvelope(input.envelope);

  const sourceOperationId = input.sourceOperationId ?? operationId;
  const admissionContext = input.admissionContext ?? null;
  const producerProvenance = await (
    input.captureFoundationProvenance ?? captureAnalyticsFoundationProvenance
  )({
    client: input.client,
    admissionContext,
    requiredContract: {
      schemaVersion: input.schemaVersion,
      canonicalizerVersion: input.canonicalizerVersion,
    },
    ...(input.capability ? { capability: input.capability } : {}),
  });
  let receiptSeed: RawAnalyticsIngestionReceiptSeed = {
    operationId,
    projectId: input.projectId,
    sourceOperationId,
    acceptedAt,
    acceptedAtNanos,
    canonicalizerVersion: input.canonicalizerVersion,
    schemaVersion: input.schemaVersion,
    ...(producerProvenance ? { analyticsProvenance: producerProvenance } : {}),
  };
  const rawObjectKey = `${normalizePrefix(input.rawPrefix ?? "")}analytics-ingestion/raw/${safeBlobKeySegment(input.projectId)}/${safeBlobKeySegment(operationId)}.json`;
  const createReceipt = input.createReceipt ?? createAnalyticsIngestionReceipt;
  const existingRaw = await input.storageService.downloadIfExists(rawObjectKey);
  let existingReceipt: AnalyticsIngestionOperation | null = null;
  if (existingRaw !== null) {
    receiptSeed = validateExistingRaw({
      body: existingRaw,
      operationId,
      projectId: input.projectId,
      sourceOperationId,
      canonicalizerVersion: input.canonicalizerVersion,
      schemaVersion: input.schemaVersion,
      producerProvenance,
      envelope: input.envelope,
    });
  } else {
    existingReceipt = await (
      input.findReceipt ?? findAnalyticsIngestionOperationForProject
    )({
      client: input.client,
      operationId,
      projectId: input.projectId,
    });
    if (existingReceipt) {
      receiptSeed = receiptSeedFromOperation(existingReceipt);
      if (
        existingReceipt.rawObjectKey !== rawObjectKey ||
        existingReceipt.sourceOperationId !== sourceOperationId ||
        existingReceipt.canonicalizerVersion !== input.canonicalizerVersion ||
        existingReceipt.schemaVersion !== input.schemaVersion ||
        !analyticsDeploymentProvenanceMatches(
          receiptSeed.analyticsProvenance,
          producerProvenance,
        )
      ) {
        throw rawConflict(operationId);
      }
    }
  }
  const persistedBody = encodeRawAnalyticsIngestionEnvelope(
    input.envelope,
    receiptSeed,
  );
  assertRawAnalyticsBodySize(persistedBody);
  if (
    existingReceipt &&
    existingReceipt.sourceChecksum !== sha256(persistedBody)
  ) {
    throw rawConflict(operationId);
  }
  const receipt = {
    client: input.client,
    operationId: receiptSeed.operationId,
    projectId: receiptSeed.projectId,
    sourceOperationId: receiptSeed.sourceOperationId,
    sourceChecksum: sha256(persistedBody),
    rawObjectKey,
    acceptedAt: receiptSeed.acceptedAt,
    acceptedAtNanos: receiptSeed.acceptedAtNanos,
    canonicalizerVersion: receiptSeed.canonicalizerVersion,
    schemaVersion: receiptSeed.schemaVersion,
    producerProvenance: receiptSeed.analyticsProvenance ?? null,
    admissionContext,
    recoverableUntil: new Date(
      receiptSeed.acceptedAt.getTime() + REPLAY_HORIZON_MS,
    ),
    statusExpiresAt: new Date(
      receiptSeed.acceptedAt.getTime() + STATUS_RETENTION_MS,
    ),
  };
  if (existingRaw === null) {
    await createReceipt({ ...receipt, publishReady: false });
    const uploadResult = await input.storageService.uploadFileIfAbsent({
      fileName: rawObjectKey,
      fileType: "application/json",
      data: persistedBody,
    });
    if (uploadResult === "already_exists") {
      const winner = await input.storageService.downloadIfExists(rawObjectKey);
      if (winner === null) {
        throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
          tags: { operationId, phase: "raw_reconciliation" },
        });
      }
      if (winner !== persistedBody) throw rawConflict(operationId);
    }
  }
  await createReceipt({
    ...receipt,
    publishReady: true,
    rawArtifactVerified: true,
  });
  return { operationId, status: "ACCEPTED" };
}

export async function reconcileRawAnalyticsIngestionReceipts(input: {
  readonly storageService: StorageService;
  readonly client?: PrismaClient;
  readonly rawPrefix?: string;
  readonly limit?: number;
  readonly cursor?: string;
  readonly admissionContext?: AnalyticsRuntimeAdmissionContext | null;
  readonly createReceipt?: typeof createAnalyticsIngestionReceipt;
  readonly findExistingRawObjectKeys?: (
    rawObjectKeys: readonly string[],
  ) => Promise<ReadonlySet<string>>;
}): Promise<{
  readonly scanned: number;
  readonly recovered: number;
  readonly existing: number;
  readonly invalid: number;
  readonly nextCursor?: string;
}> {
  const limit = input.limit ?? 100;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000) {
    throw new TypeError("Invalid raw analytics reconciliation limit");
  }
  const prefix = `${normalizePrefix(input.rawPrefix ?? "")}analytics-ingestion/raw/`;
  const page = await input.storageService.listFilesPage(prefix, {
    ...(input.cursor === undefined ? {} : { cursor: input.cursor }),
    limit,
  });
  const listedObjects = page.files.sort(
    (left, right) =>
      left.createdAt.getTime() - right.createdAt.getTime() ||
      left.file.localeCompare(right.file),
  );
  const createReceipt = input.createReceipt ?? createAnalyticsIngestionReceipt;
  const findExistingRawObjectKeys =
    input.findExistingRawObjectKeys ??
    (input.createReceipt
      ? null
      : async (rawObjectKeys: readonly string[]) => {
          const client = input.client ?? prisma;
          const existing = new Set<string>();
          for (let offset = 0; offset < rawObjectKeys.length; offset += 500) {
            const rows = await client.analyticsIngestionOperation.findMany({
              where: {
                rawObjectKey: {
                  in: rawObjectKeys.slice(offset, offset + 500),
                },
                outboxV2: { isNot: null },
              },
              select: { rawObjectKey: true },
            });
            for (const row of rows) existing.add(row.rawObjectKey);
          }
          return existing;
        });
  const existingRawObjectKeys = findExistingRawObjectKeys
    ? await findExistingRawObjectKeys(
        listedObjects.map((object) => object.file),
      )
    : new Set<string>();
  const objects = listedObjects
    .filter((object) => !existingRawObjectKeys.has(object.file))
    .slice(0, limit);
  let recovered = 0;
  let existing = 0;
  let invalid = 0;

  for (const object of objects) {
    const body = await input.storageService.download(object.file);
    let decoded: DecodedRawAnalyticsIngestionEnvelope;
    try {
      decoded = decodeRawAnalyticsIngestionEnvelope(body);
    } catch (error) {
      if (!(error instanceof AnalyticsPersistenceError)) throw error;
      invalid += 1;
      continue;
    }
    const seed = decoded.receipt;
    if (!seed) {
      invalid += 1;
      continue;
    }
    const expectedKey = `${prefix}${safeBlobKeySegment(seed.projectId)}/${safeBlobKeySegment(seed.operationId)}.json`;
    if (object.file !== expectedKey) {
      invalid += 1;
      continue;
    }
    let result: Awaited<ReturnType<typeof createReceipt>>;
    try {
      result = await createReceipt({
        client: input.client,
        operationId: seed.operationId,
        projectId: seed.projectId,
        sourceOperationId: seed.sourceOperationId,
        sourceChecksum: sha256(body),
        rawObjectKey: object.file,
        acceptedAt: seed.acceptedAt,
        acceptedAtNanos: seed.acceptedAtNanos,
        canonicalizerVersion: seed.canonicalizerVersion,
        schemaVersion: seed.schemaVersion,
        producerProvenance: seed.analyticsProvenance ?? null,
        admissionContext: input.admissionContext ?? null,
        recoverableUntil: new Date(
          seed.acceptedAt.getTime() + REPLAY_HORIZON_MS,
        ),
        statusExpiresAt: new Date(
          seed.acceptedAt.getTime() + STATUS_RETENTION_MS,
        ),
        publishReady: true,
        rawArtifactVerified: true,
      });
    } catch (error) {
      if (!isPermanentRawReceiptError(error)) throw error;
      invalid += 1;
      continue;
    }
    if (result.created) recovered += 1;
    else existing += 1;
  }

  return {
    scanned: objects.length,
    recovered,
    existing,
    invalid,
    ...(page.nextCursor === undefined ? {} : { nextCursor: page.nextCursor }),
  };
}
