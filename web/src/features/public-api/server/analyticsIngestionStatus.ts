import { toEventIdentity } from "@langfuse/shared/src/server";
import type { getAnalyticsIngestionStatusForProject } from "@langfuse/shared/src/server";

import { buildObservationUrl, buildTraceUrl } from "@/src/utils/product-url";

type AnalyticsIngestionStatus = NonNullable<
  Awaited<ReturnType<typeof getAnalyticsIngestionStatusForProject>>
>;

function candidateEntityLink(input: {
  operation: AnalyticsIngestionStatus;
  candidate: AnalyticsIngestionStatus["candidates"][number];
  loadStatusById: ReadonlyMap<
    string,
    AnalyticsIngestionStatus["loads"][number]["status"]
  >;
}): string | null {
  const visible =
    input.candidate.disposition === "NOOP" ||
    (input.candidate.loadBatchId !== null &&
      input.loadStatusById.get(input.candidate.loadBatchId) === "VISIBLE");
  if (!visible) return null;

  if (input.candidate.entityType === "EVENT") {
    try {
      const identity = toEventIdentity(input.candidate.entityKey);
      if (identity.projectId !== input.operation.projectId) return null;
      return buildObservationUrl({
        projectId: input.operation.projectId,
        traceId: identity.traceId,
        observationId: identity.spanId,
      });
    } catch {
      return null;
    }
  }
  if (input.candidate.entityType === "SCORE" && input.candidate.owningTraceId) {
    return buildTraceUrl({
      projectId: input.operation.projectId,
      traceId: input.candidate.owningTraceId,
    });
  }
  return null;
}

function recoveryGuidance(operation: AnalyticsIngestionStatus): string {
  if (operation.terminalAt === null) {
    return "Persistence is asynchronous. Retry this status request with the same operation ID.";
  }
  if (operation.status === "VISIBLE") {
    return "All supported analytics children are visible.";
  }
  if (operation.status === "COMPLETED_WITH_CANCELLATIONS") {
    return "Persistence completed; deleted children remain cancelled.";
  }
  if (operation.status === "CANCELLED_BY_DELETION") {
    return "Persistence was cancelled by a durable deletion tombstone and should not be replayed.";
  }
  return "Inspect reasonCode and contact the deployment operator before replaying.";
}

function loadEntityType(
  operation: AnalyticsIngestionStatus,
  loadBatchId: string,
) {
  const entityType = operation.candidates.find(
    (candidate) => candidate.loadBatchId === loadBatchId,
  )?.entityType;
  if (!entityType) {
    throw new Error("Analytics load batch has no public candidate");
  }
  return entityType;
}

export function serializeAnalyticsIngestionStatus(
  operation: AnalyticsIngestionStatus,
) {
  const loadStatusById = new Map(
    operation.loads.map((load) => [load.id, load.status]),
  );
  return {
    operationId: operation.operationId,
    status: operation.status,
    manifest: operation.manifest,
    outbox: operation.outbox,
    acceptedAt: operation.acceptedAt.toISOString(),
    recoverableUntil: operation.recoverableUntil.toISOString(),
    statusExpiresAt: operation.statusExpiresAt.toISOString(),
    visibleAt: operation.visibleAt?.toISOString() ?? null,
    terminalAt: operation.terminalAt?.toISOString() ?? null,
    reasonCode: operation.reasonCode,
    candidates: operation.candidates.map((candidate) => ({
      candidateKey: candidate.candidateKey,
      entityType: candidate.entityType,
      disposition: candidate.disposition,
      loadBatchId: candidate.loadBatchId,
      reasonCode: candidate.reasonCode,
      entityLink: candidateEntityLink({
        operation,
        candidate,
        loadStatusById,
      }),
    })),
    loads: operation.loads.map((load) => ({
      id: load.id,
      entityType: loadEntityType(operation, load.id),
      status: load.status,
      totalRows: load.totalRows,
      filteredRows: load.filteredRows,
      lastErrorCode: load.lastErrorCode,
      visibleAt: load.visibleAt?.toISOString() ?? null,
    })),
    guidance: recoveryGuidance(operation),
  };
}
