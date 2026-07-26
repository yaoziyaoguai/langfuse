import { createHash } from "node:crypto";

import {
  AnalyticsPersistenceError,
  assertAnalyticsBatchBoundary,
  encodeDatasetRunItemIdentity,
  encodeEventIdentity,
  encodeFileReferenceIdentity,
  encodeScoreIdentity,
  type AnalyticsSourceContract,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsDatasetRunItem,
  type CanonicalAnalyticsEntity,
  type CanonicalAnalyticsEntityClaim,
  type CanonicalAnalyticsEvent,
  type CanonicalAnalyticsFileReference,
  type CanonicalAnalyticsScore,
  type CanonicalJsonValue,
} from "@langfuse/shared/analytics-persistence";
import {
  safeBlobKeySegment,
  type StorageService,
} from "@langfuse/shared/src/server";

const ARTIFACT_FORMAT_VERSION = 1;
const SHARDED_ARTIFACT_FORMAT_VERSION = 2;
const MAX_CANONICAL_ARTIFACT_BYTES = 100 * 1024 * 1024;
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
  constructor(
    message: string,
    readonly reasonCode:
      | "ARTIFACT_UNAVAILABLE"
      | "ARTIFACT_INTEGRITY" = "ARTIFACT_INTEGRITY",
  ) {
    super(message);
    this.name = "CanonicalArtifactIntegrityError";
  }
}

function canonicalEntityKey(entity: CanonicalAnalyticsEntity): string {
  switch (entity.kind) {
    case "event":
      return encodeEventIdentity({
        projectId: entity.projectId,
        traceId: entity.traceId,
        spanId: entity.spanId,
      });
    case "score":
      return encodeScoreIdentity({
        projectId: entity.projectId,
        scoreId: entity.scoreId,
      });
    case "fileReference":
      return encodeFileReferenceIdentity({
        projectId: entity.projectId,
        entityType: entity.entityType,
        entityId: entity.entityId,
        fileId: entity.fileId,
      });
    case "datasetRunItem":
      return encodeDatasetRunItemIdentity({
        projectId: entity.projectId,
        runItemId: entity.runItemId,
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

function sortedChildren(
  batch: CanonicalAnalyticsBatch,
): CanonicalAnalyticsEntityClaim[] {
  return [...batch.children].sort((left, right) => {
    const leftKey = childSortKey(left);
    const rightKey = childSortKey(right);
    return leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : 0;
  });
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

function stringValue(record: Record<string, unknown>, key: string): string {
  const value = record[key];
  if (typeof value !== "string") {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return value;
}

function nullableString(
  record: Record<string, unknown>,
  key: string,
): string | null {
  const value = record[key];
  if (value !== null && typeof value !== "string") {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return value;
}

function booleanValue(record: Record<string, unknown>, key: string): boolean {
  const value = record[key];
  if (typeof value !== "boolean") {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return value;
}

function numberValue(record: Record<string, unknown>, key: string): number {
  const value = record[key];
  if (typeof value !== "number" || !Number.isFinite(value)) {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return value;
}

function nullableNumber(
  record: Record<string, unknown>,
  key: string,
): number | null {
  const value = record[key];
  if (
    value !== null &&
    (typeof value !== "number" || !Number.isFinite(value))
  ) {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return value;
}

function stringArray(
  record: Record<string, unknown>,
  key: string,
): readonly string[] {
  const value = record[key];
  if (!Array.isArray(value) || value.some((item) => typeof item !== "string")) {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return value;
}

function jsonValue(value: unknown): CanonicalJsonValue {
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (Array.isArray(value)) return value.map(jsonValue);
  const record = asRecord(value);
  return Object.fromEntries(
    Object.entries(record).map(([key, item]) => [key, jsonValue(item)]),
  );
}

function jsonRecord(
  record: Record<string, unknown>,
  key: string,
): Readonly<Record<string, CanonicalJsonValue>> {
  const value = asRecord(record[key]);
  return Object.fromEntries(
    Object.entries(value).map(([itemKey, item]) => [itemKey, jsonValue(item)]),
  );
}

function stringRecord(
  record: Record<string, unknown>,
  key: string,
): Readonly<Record<string, string>> {
  const value = asRecord(record[key]);
  const result: Record<string, string> = {};
  for (const [itemKey, item] of Object.entries(value)) {
    if (typeof item !== "string") {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact is malformed",
      );
    }
    result[itemKey] = item;
  }
  return result;
}

function numberRecord(
  record: Record<string, unknown>,
  key: string,
): Readonly<Record<string, number>> {
  const value = asRecord(record[key]);
  const result: Record<string, number> = {};
  for (const [itemKey, item] of Object.entries(value)) {
    if (typeof item !== "number" || !Number.isFinite(item)) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact is malformed",
      );
    }
    result[itemKey] = item;
  }
  return result;
}

function eventSourceContract(entity: Record<string, unknown>): "v4" | "otlp" {
  const sourceContract = stringValue(entity, "sourceContract");
  if (sourceContract !== "v4" && sourceContract !== "otlp") {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return sourceContract;
}

function restoreBase<TSource extends AnalyticsSourceContract>(
  entity: Record<string, unknown>,
  sourceContract: TSource,
) {
  if (entity.sourceContract !== sourceContract) {
    throw new CanonicalArtifactIntegrityError(
      "Canonical artifact is malformed",
    );
  }
  return {
    projectId: stringValue(entity, "projectId"),
    partitionDate: stringValue(entity, "partitionDate"),
    sourceContract,
    sourceVersion: decimalBigInt(entity.sourceVersion),
    canonicalizerVersion: stringValue(entity, "canonicalizerVersion"),
    schemaVersion: numberValue(entity, "schemaVersion"),
    canonicalPayloadHash: stringValue(entity, "canonicalPayloadHash"),
    systemTimestamp: decimalBigInt(entity.systemTimestamp),
    rawObjectKey: stringValue(entity, "rawObjectKey"),
    resolvedEnrichmentIds: stringRecord(entity, "resolvedEnrichmentIds"),
  };
}

function restoreEntity(value: unknown): CanonicalAnalyticsEntity {
  const entity = asRecord(value);

  switch (entity.kind) {
    case "event": {
      const restored: CanonicalAnalyticsEvent = {
        ...restoreBase(entity, eventSourceContract(entity)),
        kind: "event",
        traceId: stringValue(entity, "traceId"),
        spanId: stringValue(entity, "spanId"),
        parentSpanId: nullableString(entity, "parentSpanId"),
        type: stringValue(entity, "type"),
        name: stringValue(entity, "name"),
        environment: stringValue(entity, "environment"),
        version: nullableString(entity, "version"),
        release: nullableString(entity, "release"),
        traceName: nullableString(entity, "traceName"),
        startTime: decimalBigInt(entity.startTime),
        endTime: nullableDecimalBigInt(entity.endTime),
        completionStartTime: nullableDecimalBigInt(entity.completionStartTime),
        userId: nullableString(entity, "userId"),
        sessionId: nullableString(entity, "sessionId"),
        level: stringValue(entity, "level"),
        statusMessage: nullableString(entity, "statusMessage"),
        isAppRoot: booleanValue(entity, "isAppRoot"),
        bookmarked: booleanValue(entity, "bookmarked"),
        public: booleanValue(entity, "public"),
        tags: stringArray(entity, "tags"),
        input: jsonValue(entity.input),
        output: jsonValue(entity.output),
        metadata: jsonRecord(entity, "metadata"),
        providedModelName: nullableString(entity, "providedModelName"),
        internalModelId: nullableString(entity, "internalModelId"),
        promptId: nullableString(entity, "promptId"),
        promptName: nullableString(entity, "promptName"),
        promptVersion: nullableNumber(entity, "promptVersion"),
        modelParameters: jsonRecord(entity, "modelParameters"),
        providedUsageDetails: numberRecord(entity, "providedUsageDetails"),
        usageDetails: numberRecord(entity, "usageDetails"),
        providedCostDetails: numberRecord(entity, "providedCostDetails"),
        costDetails: numberRecord(entity, "costDetails"),
        totalCost: nullableNumber(entity, "totalCost"),
        toolDefinitions: stringRecord(entity, "toolDefinitions"),
        toolCalls: stringArray(entity, "toolCalls"),
        toolCallNames: stringArray(entity, "toolCallNames"),
        source: stringValue(entity, "source"),
        ingestionSdkName: stringValue(entity, "ingestionSdkName"),
        ingestionSdkVersion: stringValue(entity, "ingestionSdkVersion"),
        serviceName: nullableString(entity, "serviceName"),
        telemetrySdkLanguage: nullableString(entity, "telemetrySdkLanguage"),
        eventBytes: numberValue(entity, "eventBytes"),
        ...(Object.hasOwn(entity, "experimentId")
          ? {
              experimentId: nullableString(entity, "experimentId"),
              experimentName: nullableString(entity, "experimentName"),
              experimentMetadata: jsonRecord(entity, "experimentMetadata"),
              experimentDescription: nullableString(
                entity,
                "experimentDescription",
              ),
              experimentDatasetId: nullableString(
                entity,
                "experimentDatasetId",
              ),
              experimentItemId: nullableString(entity, "experimentItemId"),
              experimentItemVersion: nullableDecimalBigInt(
                entity.experimentItemVersion,
              ),
              experimentItemExpectedOutput: nullableString(
                entity,
                "experimentItemExpectedOutput",
              ),
              experimentItemMetadata: jsonRecord(
                entity,
                "experimentItemMetadata",
              ),
              experimentItemRootSpanId: nullableString(
                entity,
                "experimentItemRootSpanId",
              ),
            }
          : {}),
      };
      return restored;
    }
    case "score": {
      const dataType = stringValue(entity, "dataType");
      if (
        dataType !== "NUMERIC" &&
        dataType !== "BOOLEAN" &&
        dataType !== "CATEGORICAL" &&
        dataType !== "TEXT" &&
        dataType !== "CORRECTION"
      ) {
        throw new CanonicalArtifactIntegrityError(
          "Canonical artifact is malformed",
        );
      }
      const restored: CanonicalAnalyticsScore = {
        ...restoreBase(entity, "score"),
        kind: "score",
        scoreId: stringValue(entity, "scoreId"),
        traceId: nullableString(entity, "traceId"),
        observationId: nullableString(entity, "observationId"),
        sessionId: nullableString(entity, "sessionId"),
        timestamp: decimalBigInt(entity.timestamp),
        name: stringValue(entity, "name"),
        source: stringValue(entity, "source"),
        dataType,
        numericValue: nullableNumber(entity, "numericValue"),
        stringValue: nullableString(entity, "stringValue"),
        longStringValue: nullableString(entity, "longStringValue"),
        booleanValue:
          entity.booleanValue === null
            ? null
            : booleanValue(entity, "booleanValue"),
        comment: nullableString(entity, "comment"),
        authorUserId: nullableString(entity, "authorUserId"),
        configId: nullableString(entity, "configId"),
        queueId: nullableString(entity, "queueId"),
        environment: stringValue(entity, "environment"),
        metadata: jsonRecord(entity, "metadata"),
        ...(Object.hasOwn(entity, "datasetRunId")
          ? {
              datasetRunId: nullableString(entity, "datasetRunId"),
              executionTraceId: nullableString(entity, "executionTraceId"),
            }
          : {}),
      };
      return restored;
    }
    case "datasetRunItem": {
      const restored: CanonicalAnalyticsDatasetRunItem = {
        ...restoreBase(entity, "dataset-run-item"),
        kind: "datasetRunItem",
        runItemId: stringValue(entity, "runItemId"),
        datasetRunId: stringValue(entity, "datasetRunId"),
        datasetItemId: stringValue(entity, "datasetItemId"),
        datasetId: stringValue(entity, "datasetId"),
        traceId: stringValue(entity, "traceId"),
        observationId: nullableString(entity, "observationId"),
        error: nullableString(entity, "error"),
        createdAt: decimalBigInt(entity.createdAt),
        updatedAt: decimalBigInt(entity.updatedAt),
        datasetRunName: stringValue(entity, "datasetRunName"),
        datasetRunDescription: nullableString(entity, "datasetRunDescription"),
        datasetRunMetadata: jsonRecord(entity, "datasetRunMetadata"),
        datasetRunCreatedAt: decimalBigInt(entity.datasetRunCreatedAt),
        datasetItemVersion: nullableDecimalBigInt(entity.datasetItemVersion),
        datasetItemInput: jsonValue(entity.datasetItemInput),
        datasetItemExpectedOutput: jsonValue(entity.datasetItemExpectedOutput),
        datasetItemMetadata: jsonRecord(entity, "datasetItemMetadata"),
        datasetDeletionGeneration: decimalBigInt(
          entity.datasetDeletionGeneration,
        ),
        runDeletionGeneration: decimalBigInt(entity.runDeletionGeneration),
      };
      return restored;
    }
    case "fileReference": {
      const entityType = stringValue(entity, "entityType");
      if (entityType !== "EVENT" && entityType !== "SCORE") {
        throw new CanonicalArtifactIntegrityError(
          "Canonical artifact is malformed",
        );
      }
      const restored: CanonicalAnalyticsFileReference = {
        ...restoreBase(entity, "file-reference"),
        kind: "fileReference",
        entityType,
        entityId: stringValue(entity, "entityId"),
        owningTraceId: nullableString(entity, "owningTraceId"),
        fileId: stringValue(entity, "fileId"),
        eventId: nullableString(entity, "eventId"),
        bucketName: nullableString(entity, "bucketName"),
        bucketPath: nullableString(entity, "bucketPath"),
      };
      return restored;
    }
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
  const children = sortedChildren(batch);
  const body = stableJson({
    formatVersion: ARTIFACT_FORMAT_VERSION,
    batch: { ...batch, children },
  });
  return { body, checksum: checksum(body) };
}

function canonicalArtifactShardObjectKey(key: string, index: number): string {
  const suffix = `.part-${String(index).padStart(6, "0")}.json`;
  return key.endsWith(".json")
    ? `${key.slice(0, -5)}${suffix}`
    : `${key}${suffix}`;
}

function batchHeader(batch: CanonicalAnalyticsBatch): string {
  const { children: _children, ...header } = batch;
  return stableJson(header);
}

function emptyArtifactBytes(batch: CanonicalAnalyticsBatch): number {
  return Buffer.byteLength(
    stableJson({
      formatVersion: ARTIFACT_FORMAT_VERSION,
      batch: { ...batch, children: [] },
    }),
    "utf8",
  );
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
    const restored: CanonicalAnalyticsBatch = {
      projectId: stringValue(encodedBatch, "projectId"),
      operationId: stringValue(encodedBatch, "operationId"),
      canonicalizerVersion: stringValue(encodedBatch, "canonicalizerVersion"),
      schemaVersion: numberValue(encodedBatch, "schemaVersion"),
      acceptedAt: decimalBigInt(encodedBatch.acceptedAt),
      rawObjectKey: stringValue(encodedBatch, "rawObjectKey"),
      children: encodedBatch.children.map((value) => {
        const claim = asRecord(value);
        return {
          entity: restoreEntity(claim.entity),
          expectedSourceVersion: nullableDecimalBigInt(
            claim.expectedSourceVersion,
          ),
          fenceGeneration: decimalBigInt(claim.fenceGeneration),
          traceDeletionGeneration: decimalBigInt(claim.traceDeletionGeneration),
          projectDeletionGeneration: decimalBigInt(
            claim.projectDeletionGeneration,
          ),
          ...(Object.hasOwn(claim, "owningDatasetId")
            ? {
                owningDatasetId: nullableString(claim, "owningDatasetId"),
                owningDatasetRunId: nullableString(claim, "owningDatasetRunId"),
                datasetDeletionGeneration: decimalBigInt(
                  claim.datasetDeletionGeneration,
                ),
                runDeletionGeneration: decimalBigInt(
                  claim.runDeletionGeneration,
                ),
              }
            : {}),
        } satisfies CanonicalAnalyticsEntityClaim;
      }),
    };
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
  constructor(
    private readonly objectStore: ConditionalCanonicalObjectStore,
    private readonly maxArtifactBytes = MAX_CANONICAL_ARTIFACT_BYTES,
  ) {
    if (!Number.isSafeInteger(maxArtifactBytes) || maxArtifactBytes <= 0) {
      throw new TypeError("Invalid canonical artifact byte limit");
    }
  }

  async putIfAbsent(
    key: string,
    batch: CanonicalAnalyticsBatch,
  ): Promise<{
    readonly outcome: "created" | "already_exists";
    readonly checksum: string;
  }> {
    assertAnalyticsBatchBoundary(batch);
    if (this.artifactFits(batch)) {
      const artifact = encodeCanonicalArtifact(batch);
      const outcome = await this.putBodyIfAbsent(
        key,
        artifact.body,
        artifact.checksum,
      );
      return { outcome, checksum: artifact.checksum };
    }

    const shards = this.splitBatch(batch);
    const references: Array<{
      readonly key: string;
      readonly checksum: string;
    }> = [];
    for (const [index, shard] of shards.entries()) {
      const shardArtifact = encodeCanonicalArtifact(shard);
      const shardKey = canonicalArtifactShardObjectKey(key, index);
      await this.putBodyIfAbsent(
        shardKey,
        shardArtifact.body,
        shardArtifact.checksum,
      );
      references.push({ key: shardKey, checksum: shardArtifact.checksum });
    }
    const indexBody = stableJson({
      formatVersion: SHARDED_ARTIFACT_FORMAT_VERSION,
      shards: references,
    });
    if (Buffer.byteLength(indexBody, "utf8") > this.maxArtifactBytes) {
      throw this.artifactTooLarge(
        batch.operationId,
        "ARTIFACT_INDEX_BYTES_EXCEEDED",
      );
    }
    const indexChecksum = checksum(indexBody);
    const outcome = await this.putBodyIfAbsent(key, indexBody, indexChecksum);
    return { outcome, checksum: indexChecksum };
  }

  private artifactFits(batch: CanonicalAnalyticsBatch): boolean {
    const children = sortedChildren(batch);
    let bytes = emptyArtifactBytes(batch);
    for (const [index, child] of children.entries()) {
      bytes += Buffer.byteLength(stableJson(child), "utf8");
      if (index > 0) bytes += 1;
      if (bytes > this.maxArtifactBytes) return false;
    }
    return true;
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
        "ARTIFACT_UNAVAILABLE",
      );
    }
    return this.decodeStoredArtifact(key, body, expectedChecksum);
  }

  async getIfExists(key: string): Promise<{
    readonly batch: CanonicalAnalyticsBatch;
    readonly checksum: string;
  } | null> {
    const body = await this.objectStore.get(key);
    if (body === null) return null;
    const artifactChecksum = checksum(body);
    return {
      batch: await this.decodeStoredArtifact(key, body, artifactChecksum),
      checksum: artifactChecksum,
    };
  }

  private artifactTooLarge(
    operationId: string,
    reasonCode: string,
  ): AnalyticsPersistenceError {
    return new AnalyticsPersistenceError(
      "ANALYTICS_RESOURCE_EXHAUSTED",
      false,
      {
        tags: {
          operationId,
          phase: "canonical_artifact",
          reasonCode,
        },
      },
    );
  }

  private splitBatch(
    batch: CanonicalAnalyticsBatch,
  ): CanonicalAnalyticsBatch[] {
    const children = sortedChildren(batch);
    const emptyBytes = emptyArtifactBytes(batch);
    const shards: CanonicalAnalyticsBatch[] = [];
    let current: CanonicalAnalyticsEntityClaim[] = [];
    let currentBytes = emptyBytes;

    for (const child of children) {
      const childBytes = Buffer.byteLength(stableJson(child), "utf8");
      const separatorBytes = current.length === 0 ? 0 : 1;
      if (emptyBytes + childBytes > this.maxArtifactBytes) {
        throw this.artifactTooLarge(
          batch.operationId,
          "ARTIFACT_CHILD_BYTES_EXCEEDED",
        );
      }
      if (
        current.length > 0 &&
        currentBytes + separatorBytes + childBytes > this.maxArtifactBytes
      ) {
        shards.push({ ...batch, children: current });
        current = [];
        currentBytes = emptyBytes;
      }
      current.push(child);
      currentBytes += (current.length === 1 ? 0 : 1) + childBytes;
    }
    if (current.length > 0) shards.push({ ...batch, children: current });
    return shards;
  }

  private async putBodyIfAbsent(
    key: string,
    body: string,
    expectedChecksum: string,
  ): Promise<"created" | "already_exists"> {
    const outcome = await this.objectStore.putIfAbsent({
      key,
      body,
      contentType: "application/json",
    });
    if (outcome === "already_exists") {
      const existing = await this.objectStore.get(key);
      if (existing === null || checksum(existing) !== expectedChecksum) {
        throw new CanonicalArtifactIntegrityError(
          "Canonical artifact conditional-create collision",
        );
      }
    }
    return outcome;
  }

  private async decodeStoredArtifact(
    key: string,
    body: string,
    expectedChecksum: string,
  ): Promise<CanonicalAnalyticsBatch> {
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
    let document: Record<string, unknown>;
    try {
      document = asRecord(JSON.parse(body));
    } catch (error) {
      if (error instanceof CanonicalArtifactIntegrityError) throw error;
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact is malformed",
      );
    }
    if (document.formatVersion === ARTIFACT_FORMAT_VERSION) {
      return decodeCanonicalArtifact(body, expectedChecksum);
    }
    if (
      document.formatVersion !== SHARDED_ARTIFACT_FORMAT_VERSION ||
      !Array.isArray(document.shards) ||
      document.shards.length === 0
    ) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact format is unsupported",
      );
    }

    const batches: CanonicalAnalyticsBatch[] = [];
    for (const [index, value] of document.shards.entries()) {
      const reference = asRecord(value);
      const shardKey = reference.key;
      const shardChecksum = reference.checksum;
      if (
        typeof shardKey !== "string" ||
        shardKey !== canonicalArtifactShardObjectKey(key, index) ||
        typeof shardChecksum !== "string" ||
        !SHA256_HEX.test(shardChecksum)
      ) {
        throw new CanonicalArtifactIntegrityError(
          "Canonical artifact shard reference is invalid",
        );
      }
      const shardBody = await this.objectStore.get(shardKey);
      if (shardBody === null) {
        throw new CanonicalArtifactIntegrityError(
          "Canonical artifact shard is unavailable",
          "ARTIFACT_UNAVAILABLE",
        );
      }
      batches.push(decodeCanonicalArtifact(shardBody, shardChecksum));
    }
    const header = batchHeader(batches[0]!);
    if (batches.some((batch) => batchHeader(batch) !== header)) {
      throw new CanonicalArtifactIntegrityError(
        "Canonical artifact shard headers do not match",
      );
    }
    const restored: CanonicalAnalyticsBatch = {
      ...batches[0]!,
      children: batches.flatMap((batch) => batch.children),
    };
    assertAnalyticsBatchBoundary(restored);
    return restored;
  }
}
