import type { FullEventsObservation } from "@langfuse/shared/src/server";

import type { BatchExportEventsRow } from "./types";

export function mapObservationToBatchExportEvent(
  observation: FullEventsObservation,
  scores: Record<string, string[] | number[]>,
  comments: unknown[],
): BatchExportEventsRow {
  if (!observation.traceId) {
    throw new TypeError("Batch export event is missing its trace ID");
  }
  return {
    id: observation.id,
    traceId: observation.traceId,
    traceName: observation.traceName,
    type: observation.type,
    name: observation.name ?? "",
    startTime: observation.startTime,
    endTime: observation.endTime,
    completionStartTime: observation.completionStartTime,
    environment: observation.environment,
    version: observation.version,
    userId: observation.userId,
    sessionId: observation.sessionId,
    level: observation.level,
    statusMessage: observation.statusMessage,
    promptName: observation.promptName,
    promptId: observation.promptId,
    promptVersion: observation.promptVersion,
    modelId: observation.internalModelId,
    providedModelName: observation.model,
    modelParameters: observation.modelParameters,
    usageDetails: observation.usageDetails,
    costDetails: observation.costDetails,
    totalCost: observation.totalCost,
    input: observation.input,
    output: observation.output,
    metadata: observation.metadata,
    latencyMs:
      observation.latency === null ? null : observation.latency * 1_000,
    timeToFirstTokenMs:
      observation.timeToFirstToken === null
        ? null
        : observation.timeToFirstToken * 1_000,
    tags: observation.traceTags,
    release: observation.release ?? null,
    parentObservationId: observation.parentObservationId,
    scores,
    comments,
  };
}
