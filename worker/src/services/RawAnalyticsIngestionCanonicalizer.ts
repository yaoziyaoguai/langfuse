import { createHash } from "node:crypto";

import type { AnalyticsIngestionOperation, PrismaClient } from "@prisma/client";
import {
  AnalyticsPersistenceError,
  canonicalEntityPayloadHash,
  decodeRawAnalyticsIngestionEnvelope,
  deriveDatasetRunItemSourceTime,
  deriveFileReferenceSourceTime,
  deriveOtlpSourceTime,
  deriveScoreSourceTime,
  deriveV4SourceTime,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsDatasetRunItem,
  type CanonicalAnalyticsEntityClaim,
  type CanonicalAnalyticsEvent,
  type CanonicalAnalyticsFileReference,
  type CanonicalAnalyticsScore,
  type OtlpNanoTimestamp,
} from "@langfuse/shared/analytics-persistence";
import {
  createIngestionEventSchema,
  getDatasetDeletionGeneration,
  getDatasetRunDeletionState,
  getProjectDeletionGeneration,
  getTraceDeletionGeneration,
  OtelIngestionProcessor,
  validateAndInflateScore,
  type ResourceSpan,
} from "@langfuse/shared/src/server";
import type { StorageService } from "@langfuse/shared/src/server";

import {
  EventCanonicalizer,
  toCanonicalJson,
  toCanonicalRecord,
} from "./EventCanonicalizer";
import {
  LegacyEventCanonicalizer,
  type LoadCurrentLegacyEvent,
} from "./LegacyEventCanonicalizer";

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

type DatasetRunItemContext = {
  readonly run: {
    readonly name: string;
    readonly description: string | null;
    readonly metadata: unknown;
    readonly createdAt: Date;
  };
  readonly item: {
    readonly input: unknown;
    readonly expectedOutput: unknown;
    readonly metadata: unknown;
    readonly validFrom: Date;
  };
};

type LoadDatasetRunItemContext = (input: {
  readonly projectId: string;
  readonly datasetId: string;
  readonly datasetRunId: string;
  readonly datasetItemId: string;
  readonly datasetVersion: Date | null;
}) => Promise<DatasetRunItemContext | null>;

type DatasetDeletionSnapshot = {
  readonly owningDatasetId: string | null;
  readonly owningDatasetRunId: string | null;
  readonly datasetDeletionGeneration: bigint;
  readonly runDeletionGeneration: bigint;
};

type ResolveDatasetDeletionSnapshot = (input: {
  readonly projectId: string;
  readonly datasetId?: string | null;
  readonly datasetRunId?: string | null;
}) => Promise<DatasetDeletionSnapshot>;

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
  private readonly loadDatasetRunItemContext: LoadDatasetRunItemContext;
  private readonly resolveDatasetDeletionSnapshot: ResolveDatasetDeletionSnapshot;
  private readonly legacyEventCanonicalizer: LegacyEventCanonicalizer;

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
      readonly loadDatasetRunItemContext?: LoadDatasetRunItemContext;
      readonly resolveDatasetDeletionSnapshot?: ResolveDatasetDeletionSnapshot;
      readonly loadCurrentEvent?: LoadCurrentLegacyEvent;
    },
  ) {
    this.getProjectGeneration =
      dependencies.getProjectDeletionGeneration ?? getProjectDeletionGeneration;
    this.getTraceGeneration =
      dependencies.getTraceDeletionGeneration ?? getTraceDeletionGeneration;
    this.validateScore =
      dependencies.validateAndInflateScore ??
      (dependencies.client
        ? (input) => validateAndInflateScore(input, dependencies.client)
        : validateAndInflateScore);
    this.loadDatasetRunItemContext =
      dependencies.loadDatasetRunItemContext ??
      (async (input) => {
        if (!dependencies.client) {
          throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
            tags: { phase: "dataset_run_item_enrichment" },
          });
        }
        const [run, item] = await Promise.all([
          dependencies.client.datasetRuns.findFirst({
            where: {
              id: input.datasetRunId,
              datasetId: input.datasetId,
              projectId: input.projectId,
            },
            select: {
              name: true,
              description: true,
              metadata: true,
              createdAt: true,
            },
          }),
          dependencies.client.datasetItem.findFirst({
            where: {
              id: input.datasetItemId,
              datasetId: input.datasetId,
              projectId: input.projectId,
              status: "ACTIVE",
              isDeleted: false,
              ...(input.datasetVersion
                ? {
                    validFrom: { lte: input.datasetVersion },
                    OR: [
                      { validTo: null },
                      { validTo: { gt: input.datasetVersion } },
                    ],
                  }
                : { validTo: null }),
            },
            orderBy: { validFrom: "desc" },
            select: {
              input: true,
              expectedOutput: true,
              metadata: true,
              validFrom: true,
            },
          }),
        ]);
        return run && item ? { run, item } : null;
      });
    this.resolveDatasetDeletionSnapshot =
      dependencies.resolveDatasetDeletionSnapshot ??
      (async (input) => {
        if (!dependencies.client) {
          return {
            owningDatasetId: input.datasetId ?? null,
            owningDatasetRunId: input.datasetRunId ?? null,
            datasetDeletionGeneration: 0n,
            runDeletionGeneration: 0n,
          };
        }
        const runState = input.datasetRunId
          ? await getDatasetRunDeletionState({
              client: dependencies.client,
              projectId: input.projectId,
              datasetRunId: input.datasetRunId,
            })
          : null;
        let datasetId = input.datasetId ?? runState?.datasetId ?? null;
        if (!datasetId && input.datasetRunId && dependencies.client) {
          datasetId =
            (
              await dependencies.client.datasetRuns.findFirst({
                where: {
                  id: input.datasetRunId,
                  projectId: input.projectId,
                },
                select: { datasetId: true },
              })
            )?.datasetId ?? null;
        }
        return {
          owningDatasetId: datasetId,
          owningDatasetRunId: input.datasetRunId ?? null,
          datasetDeletionGeneration: datasetId
            ? await getDatasetDeletionGeneration({
                client: dependencies.client,
                projectId: input.projectId,
                datasetId,
              })
            : 0n,
          runDeletionGeneration: runState?.generation ?? 0n,
        };
      });
    this.legacyEventCanonicalizer = new LegacyEventCanonicalizer({
      loadCurrentEvent:
        dependencies.loadCurrentEvent ??
        (async () => {
          throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
            tags: { phase: "legacy_current_state" },
          });
        }),
    });
  }

  private datasetSnapshotForEvent(
    event: CanonicalAnalyticsEvent,
  ): Promise<DatasetDeletionSnapshot> {
    if (
      event.schemaVersion < 2 ||
      (!event.experimentDatasetId && !event.experimentId)
    ) {
      return Promise.resolve({
        owningDatasetId: null,
        owningDatasetRunId: null,
        datasetDeletionGeneration: 0n,
        runDeletionGeneration: 0n,
      });
    }
    return this.resolveDatasetDeletionSnapshot({
      projectId: event.projectId,
      datasetId: event.experimentDatasetId ?? null,
      datasetRunId: event.experimentId ?? null,
    });
  }

  async canonicalize(
    operation: RawAnalyticsOperation,
  ): Promise<CanonicalAnalyticsBatch> {
    let body: string | null;
    try {
      body = await this.dependencies.storageService.downloadIfExists(
        operation.rawObjectKey,
      );
    } catch {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: {
          operationId: operation.id,
          phase: "raw_artifact_storage",
        },
      });
    }
    if (body === null) {
      throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
        tags: {
          operationId: operation.id,
          phase: "raw_artifact",
          reasonCode: "RAW_ARTIFACT_PENDING",
        },
      });
    }
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
      case "annotation-score":
        children = await this.canonicalizeScores({
          operation,
          payload: envelope.payload,
          trustedAnnotation: true,
          projectDeletionGeneration,
          traceDeletionGeneration,
        });
        break;
      case "dataset-run-item":
        children = await this.canonicalizeDatasetRunItems({
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
      case "legacy-event":
        children = await this.canonicalizeLegacyEvents({
          operation,
          payload: envelope.payload,
          attribution: envelope.attribution,
          isLangfuseInternal: envelope.isLangfuseInternal === true,
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
        ...(await this.datasetSnapshotForEvent(entity)),
      });
    }
    return claims;
  }

  private async canonicalizeScores(input: {
    operation: RawAnalyticsOperation;
    payload: unknown;
    trustedAnnotation?: boolean;
    projectDeletionGeneration: bigint;
    traceDeletionGeneration: (traceId: string | null) => Promise<bigint>;
  }): Promise<CanonicalAnalyticsEntityClaim[]> {
    const ingestionSchema = createIngestionEventSchema(false);
    const claims: CanonicalAnalyticsEntityClaim[] = [];
    for (const rawValue of asArray(
      input.payload,
      input.trustedAnnotation ? "annotation-score" : "score",
    )) {
      const annotation = input.trustedAnnotation
        ? (rawValue as { event?: unknown; authorUserId?: string })
        : null;
      if (
        input.trustedAnnotation &&
        (typeof rawValue !== "object" ||
          rawValue === null ||
          typeof annotation?.authorUserId !== "string" ||
          !annotation.authorUserId)
      ) {
        throw validationError("annotation-score");
      }
      const value = annotation?.event ?? rawValue;
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
        authorUserId: annotation?.authorUserId ?? null,
        configId: validated.configId ?? null,
        queueId: validated.queueId ?? null,
        environment: validated.environment,
        metadata: toCanonicalRecord(parsed.data.body.metadata ?? {}),
        ...(input.operation.schemaVersion >= 2
          ? {
              datasetRunId: validated.datasetRunId ?? null,
              executionTraceId: validated.executionTraceId ?? null,
            }
          : {}),
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
        canonicalPayloadHash: canonicalEntityPayloadHash(scoreContent),
      };
      const traceDeletionGeneration = await input.traceDeletionGeneration(
        score.traceId,
      );
      const datasetSnapshot =
        input.operation.schemaVersion >= 2 && score.datasetRunId
          ? await this.resolveDatasetDeletionSnapshot({
              projectId: input.operation.projectId,
              datasetRunId: score.datasetRunId,
            })
          : {
              owningDatasetId: null,
              owningDatasetRunId: null,
              datasetDeletionGeneration: 0n,
              runDeletionGeneration: 0n,
            };
      claims.push({
        entity: score,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration,
        projectDeletionGeneration: input.projectDeletionGeneration,
        ...datasetSnapshot,
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
        canonicalPayloadHash: canonicalEntityPayloadHash(fileContent),
      };
      claims.push({
        entity: fileReference,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration,
        projectDeletionGeneration: input.projectDeletionGeneration,
        ...datasetSnapshot,
      });
    }
    return claims;
  }

  private async canonicalizeDatasetRunItems(input: {
    operation: RawAnalyticsOperation;
    payload: unknown;
    projectDeletionGeneration: bigint;
    traceDeletionGeneration: (traceId: string | null) => Promise<bigint>;
  }): Promise<CanonicalAnalyticsEntityClaim[]> {
    if (input.operation.schemaVersion < 2) {
      throw new AnalyticsPersistenceError(
        "ANALYTICS_UNSUPPORTED_FEATURE",
        false,
        { tags: { sourceContract: "dataset-run-item" } },
      );
    }
    const schema = createIngestionEventSchema(true);
    const claims: CanonicalAnalyticsEntityClaim[] = [];
    for (const value of asArray(input.payload, "dataset-run-item")) {
      const parsed = schema.safeParse(value);
      const runItemId = parsed.success ? parsed.data.body.id : null;
      if (
        !parsed.success ||
        parsed.data.type !== "dataset-run-item-create" ||
        !runItemId
      ) {
        throw validationError("dataset-run-item");
      }
      const body = parsed.data.body;
      const context = await this.loadDatasetRunItemContext({
        projectId: input.operation.projectId,
        datasetId: body.datasetId,
        datasetRunId: body.runId,
        datasetItemId: body.datasetItemId,
        datasetVersion: body.datasetVersion
          ? new Date(body.datasetVersion)
          : null,
      });
      if (!context) throw validationError("dataset-run-item");
      const sourceTime = deriveDatasetRunItemSourceTime({
        eventTimestamp: parsed.data.timestamp,
        createdAt: body.createdAt ?? parsed.data.timestamp,
      });
      const datasetSnapshot = await this.resolveDatasetDeletionSnapshot({
        projectId: input.operation.projectId,
        datasetId: body.datasetId,
        datasetRunId: body.runId,
      });
      const content = {
        kind: "datasetRunItem" as const,
        projectId: input.operation.projectId,
        runItemId,
        datasetRunId: body.runId,
        datasetItemId: body.datasetItemId,
        datasetId: body.datasetId,
        traceId: body.traceId,
        observationId: body.observationId ?? null,
        error: body.error ?? null,
        createdAt: sourceTime.createdAt,
        updatedAt: sourceTime.createdAt,
        datasetRunName: context.run.name,
        datasetRunDescription: context.run.description,
        datasetRunMetadata: toCanonicalRecord(context.run.metadata ?? {}),
        datasetRunCreatedAt:
          BigInt(context.run.createdAt.getTime()) * 1_000_000n,
        datasetItemVersion:
          BigInt(context.item.validFrom.getTime()) * 1_000_000n,
        datasetItemInput: toCanonicalJson(context.item.input),
        datasetItemExpectedOutput: toCanonicalJson(context.item.expectedOutput),
        datasetItemMetadata: toCanonicalRecord(context.item.metadata ?? {}),
        datasetDeletionGeneration: datasetSnapshot.datasetDeletionGeneration,
        runDeletionGeneration: datasetSnapshot.runDeletionGeneration,
        sourceContract: sourceTime.sourceContract,
        sourceVersion: sourceTime.sourceVersion,
        partitionDate: sourceTime.partitionDate,
        canonicalizerVersion: input.operation.canonicalizerVersion,
        schemaVersion: input.operation.schemaVersion,
        systemTimestamp: input.operation.acceptedAtNanos,
        rawObjectKey: input.operation.rawObjectKey,
        resolvedEnrichmentIds: {},
      };
      const entity: CanonicalAnalyticsDatasetRunItem = {
        ...content,
        canonicalPayloadHash: canonicalEntityPayloadHash(content),
      };
      claims.push({
        entity,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration: await input.traceDeletionGeneration(
          entity.traceId,
        ),
        projectDeletionGeneration: input.projectDeletionGeneration,
        ...datasetSnapshot,
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
        ...(await this.datasetSnapshotForEvent(event)),
      });
    }
    return claims;
  }

  private async canonicalizeLegacyEvents(input: {
    operation: RawAnalyticsOperation;
    payload: unknown;
    attribution: {
      ingestionApiKey: string;
      ingestionSdkName: string;
      ingestionSdkVersion: string;
    };
    isLangfuseInternal: boolean;
    projectDeletionGeneration: bigint;
    traceDeletionGeneration: (traceId: string | null) => Promise<bigint>;
  }): Promise<CanonicalAnalyticsEntityClaim[]> {
    const legacy = await this.legacyEventCanonicalizer.canonicalize({
      projectId: input.operation.projectId,
      payload: input.payload,
      attribution: input.attribution,
      isLangfuseInternal: input.isLangfuseInternal,
    });
    const event = await this.dependencies.eventCanonicalizer.canonicalize({
      eventData: legacy.eventData,
      rawObjectKey: input.operation.rawObjectKey,
      sourceTime: deriveV4SourceTime({
        envelopeTimestamp: legacy.envelopeTimestamp,
        bodyStartTime: legacy.eventData.startTimeISO,
        bodyEndTime: legacy.eventData.endTimeISO,
      }),
      systemTimestamp: input.operation.acceptedAtNanos,
      canonicalizerVersion: input.operation.canonicalizerVersion,
      schemaVersion: input.operation.schemaVersion,
    });
    return [
      {
        entity: event,
        expectedSourceVersion: legacy.expectedSourceVersion,
        fenceGeneration: 1n,
        traceDeletionGeneration: await input.traceDeletionGeneration(
          event.traceId,
        ),
        projectDeletionGeneration: input.projectDeletionGeneration,
        ...(await this.datasetSnapshotForEvent(event)),
      },
    ];
  }
}
