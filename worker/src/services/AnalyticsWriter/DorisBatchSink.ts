import { createHash } from "node:crypto";

import {
  AnalyticsPersistenceError,
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
import {
  recordDistribution,
  recordIncrement,
} from "@langfuse/shared/src/server";

const MAX_BATCH_BYTES = 100 * 1024 * 1024;
const MAX_INFLIGHT_LOADS = 4;
const GLOBAL_BUFFERED_BYTE_CAP = 512 * 1024 * 1024;

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
  readonly lookupId: string;
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
    readonly columns?: readonly string[];
    readonly mergeType?: "APPEND" | "DELETE";
  }): Promise<DorisStreamLoadResult>;
  reconcile(input: {
    readonly database?: string;
    readonly label: string;
  }): Promise<DorisStreamLoadReconciliation>;
}

type AdmissionTask<T> = {
  readonly bytes: number;
  readonly task: () => Promise<T>;
  readonly resolve: (value: T) => void;
  readonly reject: (error: unknown) => void;
};

export class AnalyticsLoadAdmissionController {
  private bufferedBytes = 0;
  private inflightLoads = 0;
  private readonly waiting: AdmissionTask<unknown>[] = [];

  constructor(
    private readonly limits: {
      readonly maxBatchBytes: number;
      readonly maxInflightLoads: number;
      readonly globalBufferedByteCap: number;
    } = {
      maxBatchBytes: MAX_BATCH_BYTES,
      maxInflightLoads: MAX_INFLIGHT_LOADS,
      globalBufferedByteCap: GLOBAL_BUFFERED_BYTE_CAP,
    },
  ) {
    if (
      !Number.isSafeInteger(limits.maxBatchBytes) ||
      limits.maxBatchBytes <= 0 ||
      !Number.isSafeInteger(limits.maxInflightLoads) ||
      limits.maxInflightLoads <= 0 ||
      !Number.isSafeInteger(limits.globalBufferedByteCap) ||
      limits.globalBufferedByteCap < limits.maxBatchBytes
    ) {
      throw new TypeError("Invalid analytics load admission limits");
    }
  }

  run<T>(bytes: number, task: () => Promise<T>): Promise<T> {
    if (!Number.isSafeInteger(bytes) || bytes <= 0) {
      return Promise.reject(
        new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false, {
          tags: { phase: "load_admission" },
        }),
      );
    }
    if (bytes > this.limits.maxBatchBytes) {
      recordIncrement("langfuse.analytics.ingestion.admission", 1, {
        status: "batch_too_large",
      });
      return Promise.reject(
        new AnalyticsPersistenceError("ANALYTICS_RESOURCE_EXHAUSTED", false, {
          tags: {
            phase: "load_admission",
            reasonCode: "BATCH_BYTES_EXCEEDED",
          },
        }),
      );
    }
    if (this.bufferedBytes + bytes > this.limits.globalBufferedByteCap) {
      recordIncrement("langfuse.analytics.ingestion.admission", 1, {
        status: "buffer_full",
      });
      return Promise.reject(
        new AnalyticsPersistenceError("ANALYTICS_RESOURCE_EXHAUSTED", true, {
          tags: {
            phase: "load_admission",
            reasonCode: "GLOBAL_BUFFER_FULL",
          },
        }),
      );
    }

    this.bufferedBytes += bytes;
    recordDistribution("langfuse.analytics.ingestion.buffered_bytes", bytes);
    return new Promise<T>((resolve, reject) => {
      this.waiting.push({
        bytes,
        task,
        resolve: resolve as (value: unknown) => void,
        reject,
      });
      this.drain();
    });
  }

  snapshot() {
    return {
      bufferedBytes: this.bufferedBytes,
      inflightLoads: this.inflightLoads,
      waitingLoads: this.waiting.length,
    };
  }

  private drain(): void {
    while (
      this.inflightLoads < this.limits.maxInflightLoads &&
      this.waiting.length > 0
    ) {
      const admitted = this.waiting.shift()!;
      this.inflightLoads += 1;
      Promise.resolve()
        .then(admitted.task)
        .then(admitted.resolve, admitted.reject)
        .finally(() => {
          this.inflightLoads -= 1;
          this.bufferedBytes -= admitted.bytes;
          this.drain();
        })
        .catch(admitted.reject);
    }
  }
}

const defaultAdmissionController = new AnalyticsLoadAdmissionController();

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
  }
}

function owningTraceId(entity: CanonicalAnalyticsEntity): string | null {
  return entity.kind === "event"
    ? entity.traceId
    : entity.kind === "score"
      ? entity.traceId
      : entity.owningTraceId;
}

function lookupId(entity: CanonicalAnalyticsEntity): string {
  switch (entity.kind) {
    case "event":
      return entity.spanId;
    case "score":
      return entity.scoreId;
    case "fileReference":
      return entity.fileId;
  }
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
      lookupId: lookupId(claim.entity),
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
  return JSON.stringify(stableJsonValue(value));
}

function previewIo(value: CanonicalJsonValue): string | null {
  if (value === null) return null;
  return (
    typeof value === "string" ? value : JSON.stringify(stableJsonValue(value))
  ).slice(0, 200);
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
    input_preview: previewIo(entity.input),
    output_preview: previewIo(entity.output),
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
  maxBatchBytes = MAX_BATCH_BYTES,
): readonly PreparedDorisLoadBatch[] {
  if (!Number.isSafeInteger(maxBatchBytes) || maxBatchBytes <= 0) {
    throw new TypeError("Invalid Doris load batch byte limit");
  }
  const candidates = describeCanonicalCandidates(batch).filter(
    ({ candidateKey }) =>
      includedCandidateKeys === undefined ||
      includedCandidateKeys.has(candidateKey),
  );
  const groups = new Map<string, CanonicalCandidateDescriptor[]>();
  for (const candidate of candidates) {
    const table = TABLE_BY_KIND[candidate.claim.entity.kind];
    const key = JSON.stringify([
      table,
      candidate.partitionDate,
      candidate.owningTraceId,
      candidate.claim.traceDeletionGeneration.toString(),
      candidate.claim.projectDeletionGeneration.toString(),
    ]);
    groups.set(key, [...(groups.get(key) ?? []), candidate]);
  }

  return [...groups.entries()]
    .sort(([left], [right]) => (left < right ? -1 : left > right ? 1 : 0))
    .flatMap(([, group]) => {
      const sorted = [...group].sort((left, right) =>
        left.candidateKey < right.candidateKey
          ? -1
          : left.candidateKey > right.candidateKey
            ? 1
            : 0,
      );
      const targetTable = TABLE_BY_KIND[sorted[0]!.claim.entity.kind];
      const chunks: CanonicalCandidateDescriptor[][] = [];
      let current: CanonicalCandidateDescriptor[] = [];
      let currentBytes = 0;
      for (const candidate of sorted) {
        const encoded = `${JSON.stringify(row(candidate.claim.entity))}\n`;
        const bytes = Buffer.byteLength(encoded, "utf8");
        if (bytes > maxBatchBytes) {
          throw new AnalyticsPersistenceError(
            "ANALYTICS_RESOURCE_EXHAUSTED",
            false,
            {
              tags: {
                phase: "load_preparation",
                reasonCode: "ROW_BYTES_EXCEEDED",
              },
            },
          );
        }
        if (current.length > 0 && currentBytes + bytes > maxBatchBytes) {
          chunks.push(current);
          current = [];
          currentBytes = 0;
        }
        current.push(candidate);
        currentBytes += bytes;
      }
      if (current.length > 0) chunks.push(current);

      return chunks.map((chunk) => {
        const candidateKeys = chunk.map(({ candidateKey }) => candidateKey);
        const ndjsonBody = chunk
          .map(({ claim }) => `${JSON.stringify(row(claim.entity))}\n`)
          .join("");
        const identityHash = sha256(candidateKeys.join("\0"));
        return {
          targetTable,
          partitionDate: chunk[0]!.partitionDate,
          logicalBatchId: `${targetTable}:${chunk[0]!.partitionDate}:${identityHash.slice(0, 16)}`,
          candidateKeys,
          ndjsonBody,
          payloadHash: sha256(ndjsonBody),
          rowCount: chunk.length,
        };
      });
    });
}

export class DorisBatchSink {
  constructor(
    private readonly transport: DorisStreamLoadTransport,
    private readonly database?: string,
    private readonly admissionController = defaultAdmissionController,
  ) {}

  async load(
    batch: PreparedDorisLoadBatch,
    label: string,
  ): Promise<DorisStreamLoadResult> {
    const bytes = Buffer.byteLength(batch.ndjsonBody, "utf8");
    return this.admissionController.run(bytes, () =>
      this.transport.load({
        database: this.database,
        table: batch.targetTable,
        label,
        ndjsonBody: batch.ndjsonBody,
      }),
    );
  }

  reconcile(label: string): Promise<DorisStreamLoadReconciliation> {
    return this.transport.reconcile({ database: this.database, label });
  }
}
