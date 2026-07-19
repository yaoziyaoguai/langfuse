import { createHash } from "node:crypto";

import type { AnalyticsIngestionOperation, PrismaClient } from "@prisma/client";
import {
  AnalyticsPersistenceError,
  canonicalPayloadHash,
  decodeRawAnalyticsIngestionEnvelope,
  deriveFileReferenceSourceTime,
  deriveOtlpSourceTime,
  deriveScoreSourceTime,
  deriveV4SourceTime,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsEntityClaim,
  type CanonicalAnalyticsFileReference,
  type CanonicalAnalyticsScore,
  type OtlpNanoTimestamp,
} from "@langfuse/shared/analytics-persistence";
import {
  createIngestionEventSchema,
  getProjectDeletionGeneration,
  getTraceDeletionGeneration,
  OtelIngestionProcessor,
  validateAndInflateScore,
  type ResourceSpan,
} from "@langfuse/shared/src/server";
import type { StorageService } from "@langfuse/shared/src/server";

import { EventCanonicalizer, toCanonicalRecord } from "./EventCanonicalizer";

type RawAnalyticsOperation = Pick<
  AnalyticsIngestionOperation,
  | "id"
  | "projectId"
  | "sourceChecksum"
  | "rawObjectKey"
  | "acceptedAtNanos"
  | "canonicalizerVersion"
  | "schemaVersion"
>;

type InternalEventPayload = {
  readonly eventData: Parameters<
    EventCanonicalizer["canonicalize"]
  >[0]["eventData"];
  readonly envelopeTimestamp: string;
};

function sha256(body: string): string {
  return createHash("sha256").update(body, "utf8").digest("hex");
}

function validationError(sourceContract?: string): AnalyticsPersistenceError {
  return new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false, {
    tags: sourceContract ? { sourceContract } : undefined,
  });
}

function asArray(value: unknown, sourceContract: string): unknown[] {
  if (!Array.isArray(value)) throw validationError(sourceContract);
  return value;
}

function exactOtlpTimestamp(value: unknown): OtlpNanoTimestamp {
  if (
    value === null ||
    value === undefined ||
    typeof value === "bigint" ||
    typeof value === "string"
  ) {
    return value;
  }
  if (
    typeof value === "object" &&
    "high" in value &&
    "low" in value &&
    typeof value.high === "number" &&
    typeof value.low === "number"
  ) {
    return { high: value.high, low: value.low };
  }
  throw validationError("otlp");
}

function scoreDataType(value: string): CanonicalAnalyticsScore["dataType"] {
  if (
    value === "NUMERIC" ||
    value === "BOOLEAN" ||
    value === "CATEGORICAL" ||
    value === "TEXT" ||
    value === "CORRECTION"
  ) {
    return value;
  }
  throw validationError("score");
}

export class RawAnalyticsIngestionCanonicalizer {
  private readonly getProjectGeneration: typeof getProjectDeletionGeneration;
  private readonly getTraceGeneration: typeof getTraceDeletionGeneration;
  private readonly validateScore: typeof validateAndInflateScore;

  constructor(
    private readonly dependencies: {
      readonly storageService: StorageService;
      readonly eventCanonicalizer: EventCanonicalizer;
      readonly client?: PrismaClient;
      readonly maskOtlp?: (input: {
        readonly projectId: string;
        readonly resourceSpans: ResourceSpan[];
      }) => Promise<ResourceSpan[]>;
      readonly getProjectDeletionGeneration?: typeof getProjectDeletionGeneration;
      readonly getTraceDeletionGeneration?: typeof getTraceDeletionGeneration;
      readonly validateAndInflateScore?: typeof validateAndInflateScore;
    },
  ) {
    this.getProjectGeneration =
      dependencies.getProjectDeletionGeneration ?? getProjectDeletionGeneration;
    this.getTraceGeneration =
      dependencies.getTraceDeletionGeneration ?? getTraceDeletionGeneration;
    this.validateScore =
      dependencies.validateAndInflateScore ?? validateAndInflateScore;
  }

  async canonicalize(
    operation: RawAnalyticsOperation,
  ): Promise<CanonicalAnalyticsBatch> {
    const body = await this.dependencies.storageService.download(
      operation.rawObjectKey,
    );
    if (sha256(body) !== operation.sourceChecksum) {
      throw new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false, {
        tags: { operationId: operation.id, phase: "raw_checksum" },
      });
    }
    const envelope = decodeRawAnalyticsIngestionEnvelope(body);
    const projectDeletionGeneration = await this.getProjectGeneration({
      client: this.dependencies.client,
      projectId: operation.projectId,
    });
    const traceGenerations = new Map<string, bigint>();
    const traceDeletionGeneration = async (traceId: string | null) => {
      if (!traceId) return 0n;
      const existing = traceGenerations.get(traceId);
      if (existing !== undefined) return existing;
      const generation = await this.getTraceGeneration({
        client: this.dependencies.client,
        projectId: operation.projectId,
        traceId,
      });
      traceGenerations.set(traceId, generation);
      return generation;
    };

    let children: CanonicalAnalyticsEntityClaim[];
    switch (envelope.source) {
      case "otlp":
        children = await this.canonicalizeOtlp({
          operation,
          payload: envelope.payload,
          attribution: envelope.attribution,
          isLangfuseInternal: envelope.isLangfuseInternal,
          projectDeletionGeneration,
          traceDeletionGeneration,
        });
        break;
      case "score":
        children = await this.canonicalizeScores({
          operation,
          payload: envelope.payload,
          projectDeletionGeneration,
          traceDeletionGeneration,
        });
        break;
      case "internal-event":
        children = await this.canonicalizeInternalEvents({
          operation,
          payload: envelope.payload,
          projectDeletionGeneration,
          traceDeletionGeneration,
        });
        break;
    }
    if (children.length === 0) throw validationError(envelope.source);
    return {
      projectId: operation.projectId,
      operationId: operation.id,
      canonicalizerVersion: operation.canonicalizerVersion,
      schemaVersion: operation.schemaVersion,
      acceptedAt: operation.acceptedAtNanos,
      rawObjectKey: operation.rawObjectKey,
      children,
    };
  }

  private async canonicalizeOtlp(input: {
    operation: RawAnalyticsOperation;
    payload: unknown;
    attribution: {
      ingestionApiKey: string;
      ingestionSdkName: string;
      ingestionSdkVersion: string;
    };
    isLangfuseInternal?: boolean;
    projectDeletionGeneration: bigint;
    traceDeletionGeneration: (traceId: string | null) => Promise<bigint>;
  }): Promise<CanonicalAnalyticsEntityClaim[]> {
    const rawResourceSpans = asArray(input.payload, "otlp") as ResourceSpan[];
    const resourceSpans = this.dependencies.maskOtlp
      ? await this.dependencies.maskOtlp({
          projectId: input.operation.projectId,
          resourceSpans: rawResourceSpans,
        })
      : rawResourceSpans;
    if (!Array.isArray(resourceSpans)) throw validationError("otlp");
    const processor = new OtelIngestionProcessor({
      projectId: input.operation.projectId,
      publicKey: input.attribution.ingestionApiKey,
      sdkName: input.attribution.ingestionSdkName,
      sdkVersion: input.attribution.ingestionSdkVersion,
      isLangfuseInternal: input.isLangfuseInternal,
    });
    const eventInputs = processor.processToEvent(resourceSpans);
    const rawSpans = resourceSpans.flatMap((resourceSpan) =>
      (resourceSpan.scopeSpans ?? []).flatMap(
        (scopeSpan) => scopeSpan.spans ?? [],
      ),
    );
    if (eventInputs.length !== rawSpans.length) throw validationError("otlp");

    const claims: CanonicalAnalyticsEntityClaim[] = [];
    for (let index = 0; index < eventInputs.length; index += 1) {
      const eventData = eventInputs[index];
      const rawSpan = rawSpans[index];
      if (!eventData || !rawSpan) throw validationError("otlp");
      const entity = await this.dependencies.eventCanonicalizer.canonicalize({
        eventData,
        rawObjectKey: input.operation.rawObjectKey,
        sourceTime: deriveOtlpSourceTime({
          startTimeUnixNano: exactOtlpTimestamp(rawSpan.startTimeUnixNano),
          endTimeUnixNano: exactOtlpTimestamp(rawSpan.endTimeUnixNano),
        }),
        systemTimestamp: input.operation.acceptedAtNanos,
        canonicalizerVersion: input.operation.canonicalizerVersion,
        schemaVersion: input.operation.schemaVersion,
      });
      claims.push({
        entity,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration: await input.traceDeletionGeneration(
          entity.traceId,
        ),
        projectDeletionGeneration: input.projectDeletionGeneration,
      });
    }
    return claims;
  }

  private async canonicalizeScores(input: {
    operation: RawAnalyticsOperation;
    payload: unknown;
    projectDeletionGeneration: bigint;
    traceDeletionGeneration: (traceId: string | null) => Promise<bigint>;
  }): Promise<CanonicalAnalyticsEntityClaim[]> {
    const ingestionSchema = createIngestionEventSchema(false);
    const claims: CanonicalAnalyticsEntityClaim[] = [];
    for (const value of asArray(input.payload, "score")) {
      const parsed = ingestionSchema.safeParse(value);
      if (!parsed.success || parsed.data.type !== "score-create") {
        throw new AnalyticsPersistenceError(
          "ANALYTICS_UNSUPPORTED_FEATURE",
          false,
          { tags: { sourceContract: "score" } },
        );
      }
      const scoreId = parsed.data.body.id;
      if (!scoreId) throw validationError("score");
      const validated = await this.validateScore({
        projectId: input.operation.projectId,
        scoreId,
        body: parsed.data.body,
      });
      const sourceTime = deriveScoreSourceTime({
        timestamp: parsed.data.timestamp,
      });
      const dataType = scoreDataType(validated.dataType);
      const resolvedEnrichmentIds: Readonly<Record<string, string>> =
        validated.configId ? { configId: validated.configId } : {};
      const scoreContent = {
        kind: "score" as const,
        projectId: input.operation.projectId,
        scoreId,
        traceId: validated.traceId ?? null,
        observationId: validated.observationId ?? null,
        sessionId: validated.sessionId ?? null,
        timestamp: sourceTime.timestamp,
        name: validated.name,
        source: validated.source,
        dataType,
        numericValue:
          dataType === "NUMERIC" || dataType === "BOOLEAN"
            ? validated.value
            : null,
        stringValue: validated.stringValue ?? null,
        longStringValue: validated.longStringValue || null,
        booleanValue: dataType === "BOOLEAN" ? validated.value === 1 : null,
        comment: validated.comment ?? null,
        authorUserId: null,
        configId: validated.configId ?? null,
        queueId: validated.queueId ?? null,
        environment: validated.environment,
        metadata: toCanonicalRecord(parsed.data.body.metadata ?? {}),
        sourceContract: sourceTime.sourceContract,
        sourceVersion: sourceTime.sourceVersion,
        partitionDate: sourceTime.partitionDate,
        canonicalizerVersion: input.operation.canonicalizerVersion,
        schemaVersion: input.operation.schemaVersion,
        systemTimestamp: input.operation.acceptedAtNanos,
        rawObjectKey: input.operation.rawObjectKey,
        resolvedEnrichmentIds,
      };
      const score: CanonicalAnalyticsScore = {
        ...scoreContent,
        canonicalPayloadHash: canonicalPayloadHash(scoreContent),
      };
      const traceDeletionGeneration = await input.traceDeletionGeneration(
        score.traceId,
      );
      claims.push({
        entity: score,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration,
        projectDeletionGeneration: input.projectDeletionGeneration,
      });

      const fileTime = deriveFileReferenceSourceTime({
        parentSourceVersion: score.sourceVersion,
        parentPartitionDate: score.partitionDate,
      });
      const fileContent = {
        kind: "fileReference" as const,
        projectId: input.operation.projectId,
        entityType: "SCORE" as const,
        entityId: scoreId,
        owningTraceId: score.traceId,
        fileId: input.operation.id,
        eventId: parsed.data.id,
        bucketName: null,
        bucketPath: input.operation.rawObjectKey,
        sourceContract: fileTime.sourceContract,
        sourceVersion: fileTime.sourceVersion,
        partitionDate: fileTime.partitionDate,
        canonicalizerVersion: input.operation.canonicalizerVersion,
        schemaVersion: input.operation.schemaVersion,
        systemTimestamp: input.operation.acceptedAtNanos,
        rawObjectKey: input.operation.rawObjectKey,
        resolvedEnrichmentIds: {},
      };
      const fileReference: CanonicalAnalyticsFileReference = {
        ...fileContent,
        canonicalPayloadHash: canonicalPayloadHash(fileContent),
      };
      claims.push({
        entity: fileReference,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration,
        projectDeletionGeneration: input.projectDeletionGeneration,
      });
    }
    return claims;
  }

  private async canonicalizeInternalEvents(input: {
    operation: RawAnalyticsOperation;
    payload: unknown;
    projectDeletionGeneration: bigint;
    traceDeletionGeneration: (traceId: string | null) => Promise<bigint>;
  }): Promise<CanonicalAnalyticsEntityClaim[]> {
    const claims: CanonicalAnalyticsEntityClaim[] = [];
    for (const value of asArray(input.payload, "v4")) {
      if (
        typeof value !== "object" ||
        value === null ||
        !("eventData" in value) ||
        !("envelopeTimestamp" in value) ||
        typeof (value as InternalEventPayload).envelopeTimestamp !== "string"
      ) {
        throw validationError("v4");
      }
      const item = value as InternalEventPayload;
      if (item.eventData.projectId !== input.operation.projectId) {
        throw validationError("v4");
      }
      const event = await this.dependencies.eventCanonicalizer.canonicalize({
        eventData: item.eventData,
        rawObjectKey: input.operation.rawObjectKey,
        sourceTime: deriveV4SourceTime({
          envelopeTimestamp: item.envelopeTimestamp,
          bodyStartTime: item.eventData.startTimeISO,
          bodyEndTime: item.eventData.endTimeISO,
        }),
        systemTimestamp: input.operation.acceptedAtNanos,
        canonicalizerVersion: input.operation.canonicalizerVersion,
        schemaVersion: input.operation.schemaVersion,
      });
      claims.push({
        entity: event,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration: await input.traceDeletionGeneration(
          event.traceId,
        ),
        projectDeletionGeneration: input.projectDeletionGeneration,
      });
    }
    return claims;
  }
}
