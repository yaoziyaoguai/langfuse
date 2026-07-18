import { createHash, randomUUID } from "node:crypto";

import type { PrismaClient } from "@prisma/client";

import { createAnalyticsIngestionReceipt } from "../repositories/analyticsIngestionOperations";
import { safeBlobKeySegment } from "../services/safeBlobKeySegment";
import type { StorageService } from "../services/StorageService";
import { AnalyticsPersistenceError } from "./errors";

const RAW_FORMAT_VERSION = 1;
const REPLAY_HORIZON_MS = 7 * 24 * 60 * 60 * 1_000;
const STATUS_RETENTION_MS = 30 * 24 * 60 * 60 * 1_000;

export type RawAnalyticsIngestionEnvelope = {
  readonly formatVersion: typeof RAW_FORMAT_VERSION;
  readonly source: "otlp" | "score" | "internal-event";
  readonly payload: unknown;
  readonly attribution: {
    readonly ingestionApiKey: string;
    readonly ingestionSdkName: string;
    readonly ingestionSdkVersion: string;
  };
};

function validationError(): AnalyticsPersistenceError {
  return new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
}

export function encodeRawAnalyticsIngestionEnvelope(
  envelope: RawAnalyticsIngestionEnvelope,
): string {
  if (
    envelope.formatVersion !== RAW_FORMAT_VERSION ||
    !["otlp", "score", "internal-event"].includes(envelope.source) ||
    !("payload" in envelope) ||
    !envelope.attribution ||
    typeof envelope.attribution.ingestionApiKey !== "string" ||
    typeof envelope.attribution.ingestionSdkName !== "string" ||
    typeof envelope.attribution.ingestionSdkVersion !== "string"
  ) {
    throw validationError();
  }
  try {
    const body = JSON.stringify({
      formatVersion: RAW_FORMAT_VERSION,
      source: envelope.source,
      payload: envelope.payload,
      attribution: envelope.attribution,
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
): RawAnalyticsIngestionEnvelope {
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
        envelope.source !== "internal-event") ||
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
    return {
      formatVersion: RAW_FORMAT_VERSION,
      source: envelope.source,
      payload: envelope.payload,
      attribution: {
        ingestionApiKey: typedAttribution.ingestionApiKey,
        ingestionSdkName: typedAttribution.ingestionSdkName,
        ingestionSdkVersion: typedAttribution.ingestionSdkVersion,
      },
    };
  } catch (error) {
    if (error instanceof AnalyticsPersistenceError) throw error;
    throw validationError();
  }
}

function sha256(body: string): string {
  return createHash("sha256").update(body, "utf8").digest("hex");
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

  const body = encodeRawAnalyticsIngestionEnvelope(input.envelope);
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
    sourceOperationId: input.sourceOperationId ?? operationId,
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
