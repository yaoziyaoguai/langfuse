import type { EventsObservation, TraceDomain } from "../../../../domain";
import {
  ObservationLevelDomain,
  ObservationTypeDomain,
} from "../../../../domain/observations";
import type { ObservationFieldGroupPublicApi } from "../../../../domain/observation-field-groups";
import type { EventsObservationPublic } from "../../../queries/createGenerationsQuery";
import type { DorisObservation } from "./observations";
import type { DorisTrace } from "./traces";

function prefixedTotal(
  values: Readonly<Record<string, number>>,
  prefix: string,
): number | null {
  const matching = Object.entries(values).filter(([key]) =>
    key.startsWith(prefix),
  );
  return matching.length > 0
    ? matching.reduce((total, [, value]) => total + value, 0)
    : null;
}

export function reduceDorisUsageOrCostDetails(
  details: Readonly<Record<string, number>> | null | undefined,
): { input: number | null; output: number | null; total: number | null } {
  return {
    input: prefixedTotal(details ?? {}, "input"),
    output: prefixedTotal(details ?? {}, "output"),
    total: Number(details?.total ?? 0),
  };
}

function stringRecord(
  value: Readonly<Record<string, unknown>> | undefined,
): Record<string, string> | null {
  if (!value) return null;
  return Object.fromEntries(
    Object.entries(value).map(([key, item]) => [key, String(item)]),
  );
}

export function toDorisEventsObservation(
  observation: DorisObservation,
): EventsObservation {
  return {
    id: observation.id,
    traceId: observation.traceId,
    projectId: observation.projectId,
    environment: observation.environment,
    type: ObservationTypeDomain.parse(observation.type),
    startTime: observation.startTime,
    endTime: observation.endTime,
    name: observation.name,
    metadata: (observation.metadata ?? {}) as EventsObservation["metadata"],
    parentObservationId: observation.parentObservationId,
    level: ObservationLevelDomain.parse(observation.level ?? "DEFAULT"),
    statusMessage: observation.statusMessage,
    version: observation.version,
    createdAt: observation.createdAt,
    updatedAt: observation.updatedAt,
    model: observation.providedModelName,
    internalModelId: observation.internalModelId,
    modelParameters: (observation.modelParameters ??
      null) as EventsObservation["modelParameters"],
    input: (observation.input ?? null) as EventsObservation["input"],
    output: (observation.output ?? null) as EventsObservation["output"],
    completionStartTime: observation.completionStartTime,
    promptId: observation.promptId,
    promptName: observation.promptName,
    promptVersion: observation.promptVersion,
    latency: observation.latency,
    timeToFirstToken: observation.timeToFirstToken,
    providedUsageDetails: { ...observation.providedUsageDetails },
    usageDetails: { ...observation.usageDetails },
    costDetails: { ...observation.costDetails },
    providedCostDetails: { ...observation.providedCostDetails },
    inputCost: prefixedTotal(observation.costDetails, "input"),
    outputCost: prefixedTotal(observation.costDetails, "output"),
    totalCost: observation.totalCost,
    inputUsage:
      prefixedTotal(observation.usageDetails, "input") ??
      observation.totalInputTokens ??
      0,
    outputUsage:
      prefixedTotal(observation.usageDetails, "output") ??
      observation.totalOutputTokens ??
      0,
    totalUsage: observation.totalUsage,
    usagePricingTierId: null,
    usagePricingTierName: null,
    toolDefinitions: stringRecord(observation.toolDefinitions),
    toolCalls: observation.toolCalls ? [...observation.toolCalls] : null,
    toolCallNames: observation.toolCallNames
      ? [...observation.toolCallNames]
      : null,
    userId: observation.userId,
    sessionId: observation.sessionId,
    traceName: observation.traceName,
    release: observation.release,
    tags: [...observation.tags],
    bookmarked: observation.bookmarked,
    public: observation.public,
  };
}

export function projectDorisObservation(
  observation: DorisObservation,
  fields: readonly ObservationFieldGroupPublicApi[],
): EventsObservationPublic {
  const value = toDorisEventsObservation(observation);
  const selected = new Set(fields);
  return {
    id: value.id,
    traceId: value.traceId,
    projectId: value.projectId,
    type: value.type,
    startTime: value.startTime,
    endTime: value.endTime,
    parentObservationId: value.parentObservationId,
    modelId: null,
    inputPrice: null,
    outputPrice: null,
    totalPrice: null,
    ...(selected.has("basic") && {
      name: value.name,
      level: value.level,
      statusMessage: value.statusMessage,
      version: value.version,
      environment: value.environment,
      bookmarked: value.bookmarked,
      public: value.public,
      userId: value.userId,
      sessionId: value.sessionId,
    }),
    ...(selected.has("time") && {
      completionStartTime: value.completionStartTime,
      createdAt: value.createdAt,
      updatedAt: value.updatedAt,
    }),
    ...(selected.has("io") && { input: value.input, output: value.output }),
    ...(selected.has("metadata") && { metadata: value.metadata }),
    ...(selected.has("model") && {
      model: value.model,
      internalModelId: value.internalModelId,
      modelParameters: value.modelParameters,
    }),
    ...(selected.has("usage") && {
      providedUsageDetails: value.providedUsageDetails,
      usageDetails: value.usageDetails,
      costDetails: value.costDetails,
      providedCostDetails: value.providedCostDetails,
      inputCost: value.inputCost,
      outputCost: value.outputCost,
      totalCost: value.totalCost,
      inputUsage: value.inputUsage,
      outputUsage: value.outputUsage,
      totalUsage: value.totalUsage,
      usagePricingTierId: value.usagePricingTierId,
      usagePricingTierName: value.usagePricingTierName,
    }),
    ...(selected.has("prompt") && {
      promptId: value.promptId,
      promptName: value.promptName,
      promptVersion: value.promptVersion,
    }),
    ...(selected.has("metrics") && {
      latency: value.latency,
      timeToFirstToken: value.timeToFirstToken,
    }),
    ...(selected.has("trace_context") && {
      tags: value.tags,
      release: value.release,
      traceName: value.traceName,
    }),
  };
}

export function toDorisTraceDomain(
  trace: DorisTrace,
  control: { readonly bookmarked: boolean; readonly public: boolean },
): TraceDomain {
  return {
    id: trace.id,
    projectId: trace.projectId,
    name: trace.name,
    timestamp: trace.timestamp,
    environment: trace.environment,
    tags: [...trace.tags],
    bookmarked: control.bookmarked,
    public: control.public,
    release: trace.release,
    version: trace.version,
    userId: trace.userId,
    sessionId: trace.sessionId,
    input: (trace.input ?? trace.inputPreview) as TraceDomain["input"],
    output: (trace.output ?? trace.outputPreview) as TraceDomain["output"],
    metadata: (trace.metadata ?? {}) as TraceDomain["metadata"],
    createdAt: trace.timestamp,
    updatedAt: trace.endTime,
  };
}
