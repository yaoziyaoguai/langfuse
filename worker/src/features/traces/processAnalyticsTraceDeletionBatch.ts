import {
  scheduleTraceDeletionOperations,
  type AnalyticsDeletionRequester,
} from "@langfuse/shared/src/server";
import { prisma } from "@langfuse/shared/src/db";

import {
  processAnalyticsTraceDelete,
  type TraceDeletionReference,
} from "./processAnalyticsTraceDelete";

type SerializedTraceDeletionReference = {
  readonly operationId: string;
  readonly traceId: string;
  readonly generation: string;
};

const SYSTEM_REQUESTER: AnalyticsDeletionRequester = {
  principalType: "system",
  principalId: "analytics-trace-deletion-worker",
};

/**
 * Resolves rolling-deployment jobs that predate deletion-operation payloads,
 * then processes each fenced operation with bounded concurrency.
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
    });
  }

  const missingTraceIds = traceIds.filter((traceId) => !provided.has(traceId));
  if (missingTraceIds.length > 0) {
    const project = await prisma.project.findFirstOrThrow({
      where: { id: input.projectId, deletedAt: null },
      select: { orgId: true },
    });
    const scheduled = await scheduleTraceDeletionOperations({
      projectId: input.projectId,
      organizationId: project.orgId,
      traceIds: missingTraceIds,
      requester: SYSTEM_REQUESTER,
    });
    for (const item of scheduled) {
      provided.set(item.traceId, {
        operationId: item.operation.id,
        traceId: item.traceId,
        generation: item.generation,
      });
    }
  }

  for (const traceId of traceIds) {
    const reference = provided.get(traceId);
    if (!reference) throw new Error("Trace deletion operation is missing");
    await processAnalyticsTraceDelete(input.projectId, reference);
  }
}
