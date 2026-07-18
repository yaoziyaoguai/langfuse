import { createHash } from "node:crypto";

import {
  assertAnalyticsBatchBoundary,
  encodeEventIdentity,
  encodeFileReferenceIdentity,
  encodeScoreIdentity,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsEntity,
  type CanonicalAnalyticsEntityClaim,
  type CanonicalJsonValue,
} from "@langfuse/shared/analytics-persistence";
import type {
  DorisStreamLoadReconciliation,
  DorisStreamLoadResult,
} from "@langfuse/shared/src/server";

const TABLE_BY_KIND = {
  event: "events_current",
  score: "scores_current",
  fileReference: "blob_storage_file_log",
} as const;

type AnalyticsEntityType = "EVENT" | "SCORE" | "FILE_REFERENCE";

export interface CanonicalCandidateDescriptor {
  readonly candidateKey: string;
  readonly entityType: AnalyticsEntityType;
  readonly entityKey: string;
  readonly owningTraceId: string | null;
  readonly partitionDate: string;
  readonly claim: CanonicalAnalyticsEntityClaim;
}

export interface PreparedDorisLoadBatch {
  readonly targetTable: (typeof TABLE_BY_KIND)[keyof typeof TABLE_BY_KIND];
  readonly partitionDate: string;
  readonly logicalBatchId: string;
  readonly candidateKeys: readonly string[];
  readonly ndjsonBody: string;
  readonly payloadHash: string;
  readonly rowCount: number;
}

export interface DorisStreamLoadTransport {
  load(input: {
    readonly database?: string;
    readonly table: string;
    readonly label: string;
    readonly ndjsonBody: string | Buffer;
  }): Promise<DorisStreamLoadResult>;
  reconcile(input: {
    readonly database?: string;
    readonly label: string;
  }): Promise<DorisStreamLoadReconciliation>;
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function entityType(entity: CanonicalAnalyticsEntity): AnalyticsEntityType {
  switch (entity.kind) {
    case "event":
      return "EVENT";
    case "score":
      return "SCORE";
    case "fileReference":
      return "FILE_REFERENCE";
  }
}

function entityKey(entity: CanonicalAnalyticsEntity): string {
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

function owningTraceId(entity: CanonicalAnalyticsEntity): string | null {
  return entity.kind === "event"
    ? entity.traceId
    : entity.kind === "score"
      ? entity.traceId
      : null;
}

function candidateBaseKey(claim: CanonicalAnalyticsEntityClaim): string {
  return [
    entityType(claim.entity),
    entityKey(claim.entity),
    claim.entity.sourceVersion.toString(),
    claim.entity.canonicalPayloadHash,
  ].join("\0");
}

export function describeCanonicalCandidates(
  batch: CanonicalAnalyticsBatch,
): readonly CanonicalCandidateDescriptor[] {
  assertAnalyticsBatchBoundary(batch);
  const sorted = [...batch.children].sort((left, right) => {
    const leftKey = candidateBaseKey(left);
    const rightKey = candidateBaseKey(right);
    return leftKey < rightKey ? -1 : leftKey > rightKey ? 1 : 0;
  });
  const duplicateCounts = new Map<string, number>();
  return sorted.map((claim) => {
    const baseKey = candidateBaseKey(claim);
    const ordinal = duplicateCounts.get(baseKey) ?? 0;
    duplicateCounts.set(baseKey, ordinal + 1);
    return {
      candidateKey: `${sha256(`langfuse-candidate-v1\0${baseKey}`)}-${String(
        ordinal,
      ).padStart(4, "0")}`,
      entityType: entityType(claim.entity),
      entityKey: entityKey(claim.entity),
      owningTraceId: owningTraceId(claim.entity),
      partitionDate: claim.entity.partitionDate,
      claim,
    };
  });
}

function stableJsonValue(value: CanonicalJsonValue): CanonicalJsonValue {
  if (Array.isArray(value)) return value.map(stableJsonValue);
  if (typeof value !== "object" || value === null) return value;
  const object = value as { readonly [key: string]: CanonicalJsonValue };
  return Object.fromEntries(
    Object.keys(object)
      .sort()
      .map((key) => [key, stableJsonValue(object[key]!)]),
  );
}

function stableRecord<T>(
  record: Readonly<Record<string, T>>,
): Record<string, T> {
  return Object.fromEntries(
    Object.keys(record)
      .sort()
      .map((key) => [key, record[key]!]),
  );
}

function stringifyIo(value: CanonicalJsonValue): string | null {
  if (value === null) return null;
  return typeof value === "string"
    ? value
    : JSON.stringify(stableJsonValue(value));
}

function floorDiv(dividend: bigint, divisor: bigint): bigint {
  const quotient = dividend / divisor;
  return dividend < 0n && dividend % divisor !== 0n ? quotient - 1n : quotient;
}

function dorisDateTime(nanos: bigint): string {
  const seconds = floorDiv(nanos, 1_000_000_000n);
  const nanosWithinSecond = nanos - seconds * 1_000_000_000n;
  const milliseconds = seconds * 1_000n;
  const asNumber = Number(milliseconds);
  if (!Number.isSafeInteger(asNumber)) {
    throw new RangeError("Canonical timestamp is outside the Doris range");
  }
  const date = new Date(asNumber);
  if (Number.isNaN(date.getTime())) {
    throw new RangeError("Canonical timestamp is outside the Doris range");
  }
  const wholeSeconds = date.toISOString().slice(0, 19).replace("T", " ");
  const micros = String(nanosWithinSecond / 1_000n).padStart(6, "0");
  return `${wholeSeconds}.${micros}`;
}

function tokenCount(value: number | undefined): string | null {
  if (value === undefined) return null;
  if (!Number.isSafeInteger(value) || value < 0) {
    throw new TypeError("Canonical token count is invalid");
  }
  return String(value);
}

function eventRow(
  entity: Extract<CanonicalAnalyticsEntity, { kind: "event" }>,
) {
  const input = stringifyIo(entity.input);
  const output = stringifyIo(entity.output);
  return {
    project_id: entity.projectId,
    partition_date: entity.partitionDate,
    trace_id: entity.traceId,
    span_id: entity.spanId,
    version_token: entity.sourceVersion.toString(),
    parent_span_id: entity.parentSpanId,
    type: entity.type,
    name: entity.name,
    environment: entity.environment,
    user_id: entity.userId,
    session_id: entity.sessionId,
    level: entity.level,
    status_message: entity.statusMessage,
    is_app_root: entity.isAppRoot,
    bookmarked: entity.bookmarked,
    public: entity.public,
    release: entity.release,
    version: entity.version,
    trace_name: entity.traceName,
    start_time: dorisDateTime(entity.startTime),
    end_time: entity.endTime === null ? null : dorisDateTime(entity.endTime),
    completion_start_time:
      entity.completionStartTime === null
        ? null
        : dorisDateTime(entity.completionStartTime),
    created_at: dorisDateTime(entity.systemTimestamp),
    updated_at: dorisDateTime(entity.systemTimestamp),
    provided_model_name: entity.providedModelName,
    internal_model_id: entity.internalModelId,
    prompt_id: entity.promptId,
    prompt_name: entity.promptName,
    prompt_version: entity.promptVersion,
    total_input_tokens: tokenCount(entity.usageDetails.input),
    total_output_tokens: tokenCount(entity.usageDetails.output),
    total_cost: entity.totalCost === null ? null : String(entity.totalCost),
    tags: [...entity.tags],
    metadata: stableJsonValue(entity.metadata),
    usage_details: stableRecord(entity.usageDetails),
    cost_details: stableRecord(entity.costDetails),
    provided_usage_details: stableRecord(entity.providedUsageDetails),
    provided_cost_details: stableRecord(entity.providedCostDetails),
    model_parameters: stableJsonValue(entity.modelParameters),
    tool_definitions: stableRecord(entity.toolDefinitions),
    tool_calls: [...entity.toolCalls],
    tool_call_names: [...entity.toolCallNames],
    input,
    output,
    input_preview: input?.slice(0, 200) ?? null,
    output_preview: output?.slice(0, 200) ?? null,
    source: entity.source,
    ingestion_sdk_name: entity.ingestionSdkName || "unknown",
    ingestion_sdk_version: entity.ingestionSdkVersion || "unknown",
    service_name: entity.serviceName,
    telemetry_sdk_language: entity.telemetrySdkLanguage,
    blob_storage_file_path: null,
    event_bytes: entity.eventBytes,
  };
}

function scoreRow(
  entity: Extract<CanonicalAnalyticsEntity, { kind: "score" }>,
) {
  return {
    project_id: entity.projectId,
    score_date: entity.partitionDate,
    score_id: entity.scoreId,
    version_token: entity.sourceVersion.toString(),
    trace_id: entity.traceId,
    observation_id: entity.observationId,
    session_id: entity.sessionId,
    name: entity.name,
    source: entity.source,
    data_type: entity.dataType,
    value: entity.numericValue,
    string_value: entity.stringValue,
    long_string_value: entity.longStringValue,
    boolean_value: entity.booleanValue,
    comment: entity.comment,
    author_user_id: entity.authorUserId,
    config_id: entity.configId,
    queue_id: entity.queueId,
    environment: entity.environment,
    metadata: stableJsonValue(entity.metadata),
    timestamp: dorisDateTime(entity.timestamp),
    created_at: dorisDateTime(entity.systemTimestamp),
    updated_at: dorisDateTime(entity.systemTimestamp),
  };
}

function fileReferenceRow(
  entity: Extract<CanonicalAnalyticsEntity, { kind: "fileReference" }>,
) {
  return {
    project_id: entity.projectId,
    file_date: entity.partitionDate,
    entity_type: entity.entityType,
    entity_id: entity.entityId,
    file_id: entity.fileId,
    version_token: entity.sourceVersion.toString(),
    event_id: entity.eventId,
    bucket_name: entity.bucketName,
    bucket_path: entity.bucketPath,
    created_at: dorisDateTime(entity.systemTimestamp),
    updated_at: dorisDateTime(entity.systemTimestamp),
  };
}

function row(entity: CanonicalAnalyticsEntity) {
  switch (entity.kind) {
    case "event":
      return eventRow(entity);
    case "score":
      return scoreRow(entity);
    case "fileReference":
      return fileReferenceRow(entity);
  }
}

export function prepareDorisLoadBatches(
  batch: CanonicalAnalyticsBatch,
  includedCandidateKeys?: ReadonlySet<string>,
): readonly PreparedDorisLoadBatch[] {
  const candidates = describeCanonicalCandidates(batch).filter(
    ({ candidateKey }) =>
      includedCandidateKeys === undefined ||
      includedCandidateKeys.has(candidateKey),
  );
  const groups = new Map<string, CanonicalCandidateDescriptor[]>();
  for (const candidate of candidates) {
    const table = TABLE_BY_KIND[candidate.claim.entity.kind];
    const key = `${table}\0${candidate.partitionDate}`;
    groups.set(key, [...(groups.get(key) ?? []), candidate]);
  }

  return [...groups.entries()]
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .map(([, group]) => {
      const sorted = [...group].sort((left, right) =>
        left.candidateKey < right.candidateKey
          ? -1
          : left.candidateKey > right.candidateKey
            ? 1
            : 0,
      );
      const targetTable = TABLE_BY_KIND[sorted[0]!.claim.entity.kind];
      const candidateKeys = sorted.map(({ candidateKey }) => candidateKey);
      const ndjsonBody = `${sorted
        .map(({ claim }) => JSON.stringify(row(claim.entity)))
        .join("\n")}\n`;
      const identityHash = sha256(candidateKeys.join("\0"));
      return {
        targetTable,
        partitionDate: sorted[0]!.partitionDate,
        logicalBatchId: `${targetTable}:${sorted[0]!.partitionDate}:${identityHash.slice(0, 16)}`,
        candidateKeys,
        ndjsonBody,
        payloadHash: sha256(ndjsonBody),
        rowCount: sorted.length,
      };
    });
}

export class DorisBatchSink {
  constructor(
    private readonly transport: DorisStreamLoadTransport,
    private readonly database?: string,
  ) {}

  load(
    batch: PreparedDorisLoadBatch,
    label: string,
  ): Promise<DorisStreamLoadResult> {
    return this.transport.load({
      database: this.database,
      table: batch.targetTable,
      label,
      ndjsonBody: batch.ndjsonBody,
    });
  }

  reconcile(label: string): Promise<DorisStreamLoadReconciliation> {
    return this.transport.reconcile({ database: this.database, label });
  }
}
