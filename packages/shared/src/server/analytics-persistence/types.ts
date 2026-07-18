export type CanonicalJsonPrimitive = string | number | boolean | null;
export type CanonicalJsonValue =
  | CanonicalJsonPrimitive
  | readonly CanonicalJsonValue[]
  | { readonly [key: string]: CanonicalJsonValue };

export type AnalyticsSourceContract =
  | "v4"
  | "otlp"
  | "score"
  | "file-reference";

interface CanonicalEntityBase {
  readonly projectId: string;
  readonly partitionDate: string;
  readonly sourceContract: AnalyticsSourceContract;
  readonly sourceVersion: bigint;
  readonly canonicalizerVersion: string;
  readonly schemaVersion: number;
  readonly canonicalPayloadHash: string;
  readonly systemTimestamp: bigint;
  readonly rawObjectKey: string;
  readonly resolvedEnrichmentIds: Readonly<Record<string, string>>;
}

export interface CanonicalAnalyticsEvent extends CanonicalEntityBase {
  readonly kind: "event";
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId: string | null;
  readonly type: string;
  readonly name: string;
  readonly environment: string;
  readonly version: string | null;
  readonly release: string | null;
  readonly traceName: string | null;
  readonly startTime: bigint;
  readonly endTime: bigint | null;
  readonly completionStartTime: bigint | null;
  readonly userId: string | null;
  readonly sessionId: string | null;
  readonly level: string;
  readonly statusMessage: string | null;
  readonly isAppRoot: boolean;
  readonly bookmarked: boolean;
  readonly public: boolean;
  readonly tags: readonly string[];
  readonly input: CanonicalJsonValue;
  readonly output: CanonicalJsonValue;
  readonly metadata: Readonly<Record<string, CanonicalJsonValue>>;
  readonly providedModelName: string | null;
  readonly internalModelId: string | null;
  readonly promptId: string | null;
  readonly promptName: string | null;
  readonly promptVersion: number | null;
  readonly modelParameters: Readonly<Record<string, CanonicalJsonValue>>;
  readonly providedUsageDetails: Readonly<Record<string, number>>;
  readonly usageDetails: Readonly<Record<string, number>>;
  readonly providedCostDetails: Readonly<Record<string, number>>;
  readonly costDetails: Readonly<Record<string, number>>;
  readonly totalCost: number | null;
  readonly toolDefinitions: Readonly<Record<string, string>>;
  readonly toolCalls: readonly string[];
  readonly toolCallNames: readonly string[];
  readonly source: string;
  readonly ingestionSdkName: string;
  readonly ingestionSdkVersion: string;
  readonly serviceName: string | null;
  readonly telemetrySdkLanguage: string | null;
  readonly eventBytes: number;
}

export interface CanonicalAnalyticsScore extends CanonicalEntityBase {
  readonly kind: "score";
  readonly scoreId: string;
  readonly traceId: string | null;
  readonly observationId: string | null;
  readonly sessionId: string | null;
  readonly timestamp: bigint;
  readonly name: string;
  readonly source: string;
  readonly dataType:
    | "NUMERIC"
    | "BOOLEAN"
    | "CATEGORICAL"
    | "TEXT"
    | "CORRECTION";
  readonly numericValue: number | null;
  readonly stringValue: string | null;
  readonly longStringValue: string | null;
  readonly booleanValue: boolean | null;
  readonly comment: string | null;
  readonly authorUserId: string | null;
  readonly configId: string | null;
  readonly queueId: string | null;
  readonly environment: string;
  readonly metadata: Readonly<Record<string, CanonicalJsonValue>>;
}

export interface CanonicalAnalyticsFileReference extends CanonicalEntityBase {
  readonly kind: "fileReference";
  readonly entityType: "EVENT" | "SCORE";
  readonly entityId: string;
  readonly owningTraceId: string | null;
  readonly fileId: string;
  readonly eventId: string | null;
  readonly bucketName: string | null;
  readonly bucketPath: string | null;
}

export type CanonicalAnalyticsEntity =
  | CanonicalAnalyticsEvent
  | CanonicalAnalyticsScore
  | CanonicalAnalyticsFileReference;

export interface CanonicalAnalyticsEntityClaim {
  readonly entity: CanonicalAnalyticsEntity;
  readonly expectedSourceVersion: bigint | null;
  readonly fenceGeneration: bigint;
  readonly traceDeletionGeneration: bigint;
  readonly projectDeletionGeneration: bigint;
}

export interface CanonicalAnalyticsBatch {
  readonly projectId: string;
  readonly operationId: string;
  readonly canonicalizerVersion: string;
  readonly schemaVersion: number;
  readonly acceptedAt: bigint;
  readonly rawObjectKey: string;
  readonly children: readonly CanonicalAnalyticsEntityClaim[];
}
