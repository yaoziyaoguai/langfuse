import { createHash, randomUUID } from "node:crypto";

import type { PrismaClient } from "@prisma/client";

import { createAnalyticsIngestionReceipt } from "../repositories/analyticsIngestionOperations";
import { safeBlobKeySegment } from "../services/safeBlobKeySegment";
import type { StorageService } from "../services/StorageService";
import { AnalyticsPersistenceError } from "./errors";

const RAW_FORMAT_VERSION = 1;
export const CURRENT_ANALYTICS_CANONICALIZER_VERSION = "1";
export const CURRENT_ANALYTICS_SCHEMA_VERSION = 1;
export const MAX_RAW_ANALYTICS_BYTES = 100 * 1024 * 1024;
const REPLAY_HORIZON_MS = 7 * 24 * 60 * 60 * 1_000;
const STATUS_RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;

export type RawAnalyticsIngestionEnvelope = {
  readonly formatVersion: typeof RAW_FORMAT_VERSION;
  readonly source: "otlp" | "score" | "annotation-score" | "internal-event";
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
};

export type DecodedRawAnalyticsIngestionEnvelope =
  RawAnalyticsIngestionEnvelope & {
    readonly receipt: RawAnalyticsIngestionReceiptSeed | null;
  };

function validationError(): AnalyticsPersistenceError {
  return new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
}

export function encodeRawAnalyticsIngestionEnvelope(
  envelope: RawAnalyticsIngestionEnvelope,
  receipt?: RawAnalyticsIngestionReceiptSeed,
): string {
  if (
    envelope.formatVersion !== RAW_FORMAT_VERSION ||
    !["otlp", "score", "annotation-score", "internal-event"].includes(
      envelope.source,
    ) ||
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
        envelope.source !== "internal-event") ||
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
    seed.schemaVersion <= 0
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
  readonly createReceipt?: typeof createAnalyticsIngestionReceipt;
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

  const sourceOperationId = input.sourceOperationId ?? operationId;
  const body = encodeRawAnalyticsIngestionEnvelope(input.envelope, {
    operationId,
    projectId: input.projectId,
    sourceOperationId,
    acceptedAt,
    acceptedAtNanos,
    canonicalizerVersion: input.canonicalizerVersion,
    schemaVersion: input.schemaVersion,
  });
  assertRawAnalyticsBodySize(body);
  const sourceChecksum = sha256(body);
  const rawObjectKey = `${normalizePrefix(input.rawPrefix ?? "")}analytics-ingestion/raw/${safeBlobKeySegment(input.projectId)}/${safeBlobKeySegment(operationId)}.json`;
  const uploadResult = await input.storageService.uploadFileIfAbsent({
    fileName: rawObjectKey,
    fileType: "application/json",
    data: body,
  });
  if (uploadResult === "already_exists") {
    const existing = await input.storageService.downloadIfExists(rawObjectKey);
    if (existing === null) {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: { operationId, phase: "raw_reconciliation" },
      });
    }
    if (sha256(existing) !== sourceChecksum) {
      throw new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false, {
        tags: { operationId, phase: "raw_reconciliation" },
      });
    }
  }

  const createReceipt = input.createReceipt ?? createAnalyticsIngestionReceipt;
  await createReceipt({
    client: input.client,
    operationId,
    projectId: input.projectId,
    sourceOperationId,
    sourceChecksum,
    rawObjectKey,
    acceptedAt,
    acceptedAtNanos,
    canonicalizerVersion: input.canonicalizerVersion,
    schemaVersion: input.schemaVersion,
    recoverableUntil: new Date(acceptedAt.getTime() + REPLAY_HORIZON_MS),
    statusExpiresAt: new Date(acceptedAt.getTime() + STATUS_RETENTION_MS),
  });
  return { operationId, status: "ACCEPTED" };
}

export async function reconcileRawAnalyticsIngestionReceipts(input: {
  readonly storageService: StorageService;
  readonly client?: PrismaClient;
  readonly rawPrefix?: string;
  readonly limit?: number;
  readonly createReceipt?: typeof createAnalyticsIngestionReceipt;
}): Promise<{
  readonly scanned: number;
  readonly recovered: number;
  readonly existing: number;
  readonly invalid: number;
}> {
  const limit = input.limit ?? 100;
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1_000) {
    throw new TypeError("Invalid raw analytics reconciliation limit");
  }
  const prefix = `${normalizePrefix(input.rawPrefix ?? "")}analytics-ingestion/raw/`;
  const objects = (await input.storageService.listFiles(prefix)).slice(
    0,
    limit,
  );
  const createReceipt = input.createReceipt ?? createAnalyticsIngestionReceipt;
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
    const result = await createReceipt({
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
      recoverableUntil: new Date(seed.acceptedAt.getTime() + REPLAY_HORIZON_MS),
      statusExpiresAt: new Date(
        seed.acceptedAt.getTime() + STATUS_RETENTION_MS,
      ),
    });
    if (result.created) recovered += 1;
    else existing += 1;
  }

  return { scanned: objects.length, recovered, existing, invalid };
}
