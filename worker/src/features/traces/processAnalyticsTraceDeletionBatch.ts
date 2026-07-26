import type { SerializedAnalyticsDurableProvenance } from "@langfuse/shared/src/server";

import {
  processAnalyticsTraceDelete,
  type TraceDeletionReference,
} from "./processAnalyticsTraceDelete";

type SerializedTraceDeletionReference = {
  readonly operationId: string;
  readonly traceId: string;
  readonly generation: string;
  readonly analyticsProvenance?: SerializedAnalyticsDurableProvenance;
};

/**
 * Processes only authoritative deletion references. Legacy unstamped jobs stay
 * on the ClickHouse compatibility path and are never upgraded by a consumer.
 */
export async function processAnalyticsTraceDeletionBatch(input: {
  readonly projectId: string;
  readonly traceIds: readonly string[];
  readonly deletionOperations?: readonly SerializedTraceDeletionReference[];
}): Promise<void> {
  const traceIds = [...new Set(input.traceIds)];
  const requested = new Set(traceIds);
  const provided = new Map<string, TraceDeletionReference>();

  for (const reference of input.deletionOperations ?? []) {
    if (!requested.has(reference.traceId) || provided.has(reference.traceId)) {
      throw new Error("Trace deletion queue references are invalid");
    }
    provided.set(reference.traceId, {
      operationId: reference.operationId,
      traceId: reference.traceId,
      generation: BigInt(reference.generation),
      analyticsProvenance: reference.analyticsProvenance,
    });
  }

  const missingTraceIds = traceIds.filter((traceId) => !provided.has(traceId));
  if (missingTraceIds.length > 0) {
    throw new Error("Managed trace deletion queue references are missing");
  }

  for (const traceId of traceIds) {
    const reference = provided.get(traceId);
    if (!reference) throw new Error("Trace deletion operation is missing");
    await processAnalyticsTraceDelete(input.projectId, reference);
  }
}
