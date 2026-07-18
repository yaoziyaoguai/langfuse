import type { InternalTraceEventInput } from "@langfuse/shared/src/server";
import {
  AnalyticsPersistenceError,
  canonicalPayloadHash,
  normalizeVersionToken,
  partitionDateFromVersionToken,
  type CanonicalAnalyticsEvent,
  type CanonicalJsonValue,
  type CanonicalSourceTime,
} from "@langfuse/shared/analytics-persistence";

type CanonicalizationEventInput = Omit<
  InternalTraceEventInput,
  "input" | "output" | "promptVersion"
> & {
  readonly input?: unknown;
  readonly output?: unknown;
  readonly promptVersion?: string | number;
};

interface ResolvedPrompt {
  readonly id: string;
  readonly name: string;
  readonly version: number;
}

interface ResolvedGenerationUsage {
  readonly internalModelId?: string | null;
  readonly usageDetails?: Readonly<Record<string, number>>;
  readonly costDetails?: Readonly<Record<string, number>>;
  readonly totalCost?: number | null;
  readonly usagePricingTierId?: string | null;
  readonly usagePricingTierName?: string | null;
}

interface GenerationUsageInput {
  readonly projectId: string;
  readonly spanId: string;
  readonly traceId: string;
  readonly providedModelName: string;
  readonly providedUsageDetails: Readonly<Record<string, number>>;
  readonly providedCostDetails: Readonly<Record<string, number>>;
  readonly input?: string;
  readonly output?: string;
}

export interface EventCanonicalizerDependencies {
  readonly warnOnUsageTotalMismatch: (
    usage: Readonly<Record<string, number>>,
    identity: { readonly projectId: string; readonly spanId: string },
  ) => void;
  readonly resolvePrompt: (input: {
    readonly projectId: string;
    readonly promptName: string;
    readonly promptVersion: number;
  }) => Promise<ResolvedPrompt | null>;
  readonly resolveGenerationUsage: (
    input: GenerationUsageInput,
  ) => Promise<ResolvedGenerationUsage | null>;
}

export interface EnrichedAnalyticsEvent {
  readonly projectId: string;
  readonly traceId: string;
  readonly spanId: string;
  readonly parentSpanId: string | null;
  readonly type: string;
  readonly name: string;
  readonly environment: string;
  readonly version: string | null;
  readonly release: string | null;
  readonly traceName: string | null;
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
  readonly inputForUsage?: string;
  readonly outputForUsage?: string;
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
  readonly usagePricingTierId: string | null;
  readonly usagePricingTierName: string | null;
  readonly toolDefinitions: Readonly<Record<string, string>>;
  readonly toolCalls: readonly string[];
  readonly toolCallNames: readonly string[];
  readonly source: string;
  readonly ingestionSdkName: string;
  readonly ingestionSdkVersion: string;
  readonly serviceName: string | null;
  readonly telemetrySdkLanguage: string | null;
  readonly rawObjectKey: string;
  readonly eventBytes: number;
}

function parsePromptVersion(
  value: string | number | null | undefined,
): number | null {
  if (value == null || value === "") return null;
  const parsed = typeof value === "number" ? value : parseInt(value, 10);
  return Number.isInteger(parsed) && parsed >= 0 && parsed <= 65_535
    ? parsed
    : null;
}

export function toCanonicalJson(value: unknown): CanonicalJsonValue {
  if (value == null) return null;
  if (typeof value === "string" || typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (!Number.isFinite(value)) {
      throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
    }
    return Object.is(value, -0) ? 0 : value;
  }
  if (Array.isArray(value)) return value.map(toCanonicalJson);
  if (typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value as Record<string, unknown>)
        .filter(([, item]) => item !== undefined)
        .map(([key, item]) => [key, toCanonicalJson(item)]),
    );
  }
  throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
}

export function toCanonicalRecord(
  value: unknown,
): Readonly<Record<string, CanonicalJsonValue>> {
  const canonical = toCanonicalJson(value);
  if (
    canonical === null ||
    Array.isArray(canonical) ||
    typeof canonical !== "object"
  ) {
    throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
  }
  return canonical as Readonly<Record<string, CanonicalJsonValue>>;
}

function stringifyForUsage(value: unknown): string | undefined {
  if (value == null) return undefined;
  try {
    return typeof value === "string" ? value : JSON.stringify(value);
  } catch {
    throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
  }
}

function parseModelParameters(
  value: unknown,
): Readonly<Record<string, CanonicalJsonValue>> {
  try {
    const parsed =
      typeof value === "string" ? JSON.parse(value) : (value ?? {});
    return toCanonicalRecord(parsed);
  } catch (error) {
    if (error instanceof AnalyticsPersistenceError) throw error;
    throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
  }
}

function validationError(): AnalyticsPersistenceError {
  return new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
}

function validateSourceTime(sourceTime: CanonicalSourceTime): void {
  if (
    sourceTime.sourceContract !== "v4" &&
    sourceTime.sourceContract !== "otlp"
  ) {
    throw validationError();
  }
  try {
    normalizeVersionToken(sourceTime.sourceVersion);
    normalizeVersionToken(sourceTime.startTime);
    if (sourceTime.endTime !== null) {
      normalizeVersionToken(sourceTime.endTime);
    }
    if (
      partitionDateFromVersionToken(sourceTime.startTime) !==
      sourceTime.partitionDate
    ) {
      throw validationError();
    }
  } catch (error) {
    if (error instanceof AnalyticsPersistenceError) throw error;
    throw validationError();
  }
  if (
    (sourceTime.endTime !== null &&
      sourceTime.endTime < sourceTime.startTime) ||
    (sourceTime.sourceContract === "otlp" &&
      (sourceTime.endTime === null ||
        sourceTime.sourceVersion !== sourceTime.endTime))
  ) {
    throw new AnalyticsPersistenceError("ANALYTICS_INVALID_TIME_RANGE", false);
  }
}

export class EventCanonicalizer {
  constructor(private readonly dependencies: EventCanonicalizerDependencies) {}

  async enrich(input: {
    readonly eventData: CanonicalizationEventInput;
    readonly rawObjectKey: string;
  }): Promise<EnrichedAnalyticsEvent> {
    const { eventData } = input;
    const inputForUsage = stringifyForUsage(eventData.input);
    const outputForUsage = stringifyForUsage(eventData.output);
    const providedUsageDetails = eventData.providedUsageDetails ?? {};
    const providedCostDetails = eventData.providedCostDetails ?? {};
    this.dependencies.warnOnUsageTotalMismatch(providedUsageDetails, {
      projectId: eventData.projectId,
      spanId: eventData.spanId,
    });

    const promptVersion = parsePromptVersion(eventData.promptVersion);
    const [prompt, generationUsage] = await Promise.all([
      eventData.promptName && promptVersion !== null
        ? this.dependencies.resolvePrompt({
            projectId: eventData.projectId,
            promptName: eventData.promptName,
            promptVersion,
          })
        : null,
      eventData.modelName
        ? this.dependencies.resolveGenerationUsage({
            projectId: eventData.projectId,
            spanId: eventData.spanId,
            traceId: eventData.traceId,
            providedModelName: eventData.modelName,
            providedUsageDetails,
            providedCostDetails,
            input: inputForUsage,
            output: outputForUsage,
          })
        : null,
    ]);

    return {
      projectId: eventData.projectId,
      traceId: eventData.traceId,
      spanId: eventData.spanId,
      parentSpanId: eventData.parentSpanId ?? null,
      type: eventData.type ?? "SPAN",
      name: eventData.name ?? "",
      environment: eventData.environment ?? "default",
      version: eventData.version ?? null,
      release: eventData.release ?? null,
      traceName: eventData.traceName ?? null,
      userId: eventData.userId ?? null,
      sessionId: eventData.sessionId ?? null,
      level: eventData.level ?? "DEFAULT",
      statusMessage: eventData.statusMessage ?? null,
      isAppRoot: eventData.isAppRoot ?? false,
      bookmarked: eventData.bookmarked ?? false,
      public: eventData.public ?? false,
      tags: eventData.tags ?? [],
      input: toCanonicalJson(eventData.input),
      output: toCanonicalJson(eventData.output),
      inputForUsage,
      outputForUsage,
      metadata: toCanonicalRecord(eventData.metadata ?? {}),
      providedModelName: eventData.modelName ?? null,
      internalModelId: generationUsage?.internalModelId ?? null,
      promptId: prompt?.id ?? null,
      promptName: eventData.promptName ?? prompt?.name ?? null,
      promptVersion: prompt?.version ?? promptVersion,
      modelParameters: parseModelParameters(eventData.modelParameters),
      providedUsageDetails,
      usageDetails:
        generationUsage?.usageDetails ?? eventData.usageDetails ?? {},
      providedCostDetails,
      costDetails: generationUsage?.costDetails ?? eventData.costDetails ?? {},
      totalCost: generationUsage?.totalCost ?? null,
      usagePricingTierId: generationUsage?.usagePricingTierId ?? null,
      usagePricingTierName: generationUsage?.usagePricingTierName ?? null,
      toolDefinitions: eventData.toolDefinitions ?? {},
      toolCalls: eventData.toolCalls ?? [],
      toolCallNames: eventData.toolCallNames ?? [],
      source: eventData.source,
      ingestionSdkName: eventData.ingestionSdkName ?? "",
      ingestionSdkVersion: eventData.ingestionSdkVersion ?? "",
      serviceName: eventData.serviceName ?? null,
      telemetrySdkLanguage: eventData.telemetrySdkLanguage ?? null,
      rawObjectKey: input.rawObjectKey,
      eventBytes: eventData.eventBytes ?? 0,
    };
  }

  async canonicalize(input: {
    readonly eventData: CanonicalizationEventInput;
    readonly rawObjectKey: string;
    readonly sourceTime: CanonicalSourceTime;
    readonly systemTimestamp: bigint;
    readonly canonicalizerVersion: string;
    readonly schemaVersion: number;
  }): Promise<CanonicalAnalyticsEvent> {
    if (
      !input.eventData.projectId ||
      !input.eventData.traceId ||
      !input.eventData.spanId ||
      !input.rawObjectKey ||
      !input.canonicalizerVersion ||
      !Number.isSafeInteger(input.schemaVersion) ||
      input.schemaVersion <= 0
    ) {
      throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
    }
    try {
      normalizeVersionToken(input.systemTimestamp);
    } catch {
      throw validationError();
    }
    validateSourceTime(input.sourceTime);
    const enriched = await this.enrich(input);
    const {
      inputForUsage: _inputForUsage,
      outputForUsage: _outputForUsage,
      usagePricingTierId,
      usagePricingTierName: _usagePricingTierName,
      ...canonicalContent
    } = enriched;
    let completionStartTime: bigint | null = null;
    try {
      completionStartTime = input.eventData.completionStartTime
        ? normalizeVersionToken(input.eventData.completionStartTime)
        : null;
    } catch {
      throw validationError();
    }
    if (
      completionStartTime !== null &&
      (completionStartTime < input.sourceTime.startTime ||
        (input.sourceTime.endTime !== null &&
          completionStartTime > input.sourceTime.endTime))
    ) {
      throw new AnalyticsPersistenceError(
        "ANALYTICS_INVALID_TIME_RANGE",
        false,
      );
    }
    const hashInput = {
      kind: "event" as const,
      ...canonicalContent,
      sourceContract: input.sourceTime.sourceContract,
      sourceVersion: input.sourceTime.sourceVersion,
      partitionDate: input.sourceTime.partitionDate,
      startTime: input.sourceTime.startTime,
      endTime: input.sourceTime.endTime,
      completionStartTime,
      systemTimestamp: input.systemTimestamp,
      canonicalizerVersion: input.canonicalizerVersion,
      schemaVersion: input.schemaVersion,
      resolvedEnrichmentIds: Object.fromEntries(
        [
          ["promptId", enriched.promptId],
          ["modelId", enriched.internalModelId],
          ["usagePricingTierId", usagePricingTierId],
        ].filter((entry): entry is [string, string] => entry[1] !== null),
      ),
    };
    try {
      return {
        ...hashInput,
        canonicalPayloadHash: canonicalPayloadHash(hashInput),
      };
    } catch (error) {
      if (error instanceof AnalyticsPersistenceError) throw error;
      throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
    }
  }
}
