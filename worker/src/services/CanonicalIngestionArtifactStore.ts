import { createHash } from "node:crypto";

import {
  assertAnalyticsBatchBoundary,
  encodeEventIdentity,
  encodeFileReferenceIdentity,
  encodeScoreIdentity,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsEntity,
  type CanonicalAnalyticsEntityClaim,
} from "@langfuse/shared/analytics-persistence";
import {
  safeBlobKeySegment,
  type StorageService,
} from "@langfuse/shared/src/server";

const ARTIFACT_FORMAT_VERSION = 1;
const SHA256_HEX = /^[a-f0-9]{64}$/;
const DECIMAL_INTEGER = /^-?\d+$/;

export interface ConditionalCanonicalObjectStore {
  putIfAbsent(input: {
    readonly key: string;
    readonly body: string;
    readonly contentType: "application/json";
  }): Promise<"created" | "already_exists">;
  get(key: string): Promise<string | null>;
}

export class StorageServiceCanonicalObjectStore implements ConditionalCanonicalObjectStore {
  constructor(private readonly storageService: StorageService) {}

  putIfAbsent(input: {
    readonly key: string;
    readonly body: string;
    readonly contentType: "application/json";
  }): Promise<"created" | "already_exists"> {
    return this.storageService.uploadFileIfAbsent({
      fileName: input.key,
      fileType: input.contentType,
      data: input.body,
    });
  }

  get(key: string): Promise<string | null> {
    return this.storageService.downloadIfExists(key);
  }
}

export class CanonicalArtifactIntegrityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CanonicalArtifactIntegrityError";
  }
}

function canonicalEntityKey(entity: CanonicalAnalyticsEntity): string {
  switch (entity.kind) {
    case "event":
      return encodeEventIdentity(entity);
    case "score":
      return encodeScoreIdentity({
        projectId: entity.projectId,
        partitionDate: entity.partitionDate,
        scoreId: entity.scoreId,
      });
    case "fileReference":
      return encodeFileReferenceIdentity({
        projectId: entity.projectId,
        partitionDate: entity.partitionDate,
        entityType: entity.entityType,
        entityId: entity.entityId,
        fileId: entity.fileId,
      });
  }
}

function childSortKey(child: CanonicalAnalyticsEntityClaim): string {
  return [
    child.entity.kind,
    canonicalEntityKey(child.entity),
    child.entity.sourceVersion.toString(),
    child.entity.canonicalPayloadHash,
  ].join(":");
}

function stableJson(value: unknown, inArray = false): string {
  if (value === null) return "null";
  if (value === undefined) {
    if (inArray) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact contains an undefined array value",
      );
    }
    return "undefined";
  }
  if (typeof value === "bigint") return JSON.stringify(value.toString());
  if (typeof value === "string" || typeof value === "boolean") {
    return JSON.stringify(value);
  }
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact contains a non-finite number",
      );
    }
    return JSON.stringify(Object.is(value, -0) ? 0 : value);
  }
  if (Array.isArray(value)) {
    return `[${value.map((item) => stableJson(item, true)).join(",")}]`;
  }
  if (typeof value === "object") {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object)
      .filter((key) => object[key] !== undefined)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${stableJson(object[key])}`)
      .join(",")}}`;
  }
  throw new CanonicalArtifactIntegrityError(
    "Canonical artifact contains an unsupported value",
  );
}

function checksum(body: string): string {
  return createHash("sha256").update(body, "utf8").digest("hex");
}

function asRecord(value: unknown): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return value as Record<string, unknown>;
}

function decimalBigInt(value: unknown): bigint {
  if (typeof value !== "string" || !DECIMAL_INTEGER.test(value)) {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return BigInt(value);
}

function nullableDecimalBigInt(value: unknown): bigint | null {
  return value === null ? null : decimalBigInt(value);
}

function restoreEntity(value: unknown): CanonicalAnalyticsEntity {
  const entity = asRecord(value);
  const restoredBase = {
    ...entity,
    sourceVersion: decimalBigInt(entity.sourceVersion),
    systemTimestamp: decimalBigInt(entity.systemTimestamp),
  };

  switch (entity.kind) {
    case "event":
      return {
        ...restoredBase,
        startTime: decimalBigInt(entity.startTime),
        endTime: nullableDecimalBigInt(entity.endTime),
        completionStartTime: nullableDecimalBigInt(entity.completionStartTime),
      } as unknown as CanonicalAnalyticsEntity;
    case "score":
      return {
        ...restoredBase,
        timestamp: decimalBigInt(entity.timestamp),
      } as unknown as CanonicalAnalyticsEntity;
    case "fileReference":
      return restoredBase as unknown as CanonicalAnalyticsEntity;
    default:
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact is malformed",
      );
  }
}

export function encodeCanonicalArtifact(batch: CanonicalAnalyticsBatch): {
  readonly body: string;
  readonly checksum: string;
} {
  assertAnalyticsBatchBoundary(batch);
  const children = [...batch.children].sort((left, right) => {
    const leftKey = childSortKey(left);
    const rightKey = childSortKey(right);
    return leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : 0;
  });
  const body = stableJson({
    formatVersion: ARTIFACT_FORMAT_VERSION,
    batch: { ...batch, children },
  });
  return { body, checksum: checksum(body) };
}

export function decodeCanonicalArtifact(
  body: string,
  expectedChecksum: string,
): CanonicalAnalyticsBatch {
  if (!SHA256_HEX.test(expectedChecksum)) {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact checksum is invalid",
    );
  }
  if (checksum(body) !== expectedChecksum) {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact checksum does not match",
    );
  }

  try {
    const artifact = asRecord(JSON.parse(body));
    if (artifact.formatVersion !== ARTIFACT_FORMAT_VERSION) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact format is unsupported",
      );
    }
    const encodedBatch = asRecord(artifact.batch);
    if (!Array.isArray(encodedBatch.children)) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact is malformed",
      );
    }
    const restored = {
      ...encodedBatch,
      acceptedAt: decimalBigInt(encodedBatch.acceptedAt),
      children: encodedBatch.children.map((value) => {
        const claim = asRecord(value);
        return {
          ...claim,
          entity: restoreEntity(claim.entity),
          expectedSourceVersion: nullableDecimalBigInt(
            claim.expectedSourceVersion,
          ),
          fenceGeneration: decimalBigInt(claim.fenceGeneration),
          traceDeletionGeneration: decimalBigInt(claim.traceDeletionGeneration),
          projectDeletionGeneration: decimalBigInt(
            claim.projectDeletionGeneration,
          ),
        };
      }),
    } as unknown as CanonicalAnalyticsBatch;
    assertAnalyticsBatchBoundary(restored);
    return restored;
  } catch (error) {
    if (error instanceof CanonicalArtifactIntegrityError) throw error;
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
}

export function canonicalArtifactObjectKey(input: {
  readonly prefix: string;
  readonly projectId: string;
  readonly operationId: string;
  readonly fenceGeneration: bigint;
}): string {
  if (input.fenceGeneration <= 0n) {
    throw new RangeError("Canonical artifact fence must be positive");
  }
  const prefix = input.prefix.replace(/^\/+|\/+$/g, "");
  const segments = [
    prefix,
    "canonical-ingestion",
    safeBlobKeySegment(input.projectId),
    safeBlobKeySegment(input.operationId),
    `fence-${input.fenceGeneration}.json`,
  ].filter(Boolean);
  return segments.join("/");
}

export class CanonicalIngestionArtifactStore {
  constructor(private readonly objectStore: ConditionalCanonicalObjectStore) {}

  async putIfAbsent(
    key: string,
    batch: CanonicalAnalyticsBatch,
  ): Promise<{
    readonly outcome: "created" | "already_exists";
    readonly checksum: string;
  }> {
    const artifact = encodeCanonicalArtifact(batch);
    const outcome = await this.objectStore.putIfAbsent({
      key,
      body: artifact.body,
      contentType: "application/json",
    });
    if (outcome === "already_exists") {
      const existing = await this.objectStore.get(key);
      if (existing === null || checksum(existing) !== artifact.checksum) {
        throw new CanonicalArtifactIntegrityError(
          "Canonical artifact conditional-create collision",
        );
      }
    }
    return { outcome, checksum: artifact.checksum };
  }

  async get(
    key: string,
    expectedChecksum: string,
  ): Promise<CanonicalAnalyticsBatch> {
    if (!SHA256_HEX.test(expectedChecksum)) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact checksum is invalid",
      );
    }
    const body = await this.objectStore.get(key);
    if (body === null) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact is unavailable",
      );
    }
    return decodeCanonicalArtifact(body, expectedChecksum);
  }
}
