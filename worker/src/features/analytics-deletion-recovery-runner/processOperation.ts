import type { AnalyticsDeletionOperation } from "@prisma/client";
import {
  analyticsDurableProvenanceFromRecord,
  serializeAnalyticsDurableProvenance,
} from "@langfuse/shared/src/server";

import { processAnalyticsProjectDelete } from "../projects/processAnalyticsProjectDelete";
import { markPendingTraceDeletionsCompleted } from "../traces/markPendingTraceDeletionsCompleted";
import { processAnalyticsTraceDelete } from "../traces/processAnalyticsTraceDelete";

type RecoveryDependencies = {
  readonly processTrace: typeof processAnalyticsTraceDelete;
  readonly processProject: typeof processAnalyticsProjectDelete;
  readonly markPendingTraceCompleted: typeof markPendingTraceDeletionsCompleted;
};

const defaultDependencies: RecoveryDependencies = {
  processTrace: processAnalyticsTraceDelete,
  processProject: processAnalyticsProjectDelete,
  markPendingTraceCompleted: markPendingTraceDeletionsCompleted,
};

export async function processAnalyticsDeletionRecoveryOperation(
  operation: AnalyticsDeletionOperation,
  dependencies: RecoveryDependencies = defaultDependencies,
): Promise<void> {
  const provenance = analyticsDurableProvenanceFromRecord(operation);
  const analyticsProvenance = provenance
    ? serializeAnalyticsDurableProvenance(provenance)
    : undefined;
  if (operation.scope === "TRACE") {
    if (!operation.traceId) {
      throw new Error("Trace deletion recovery is missing traceId");
    }
    await dependencies.processTrace(operation.projectId, {
      operationId: operation.id,
      traceId: operation.traceId,
      generation: operation.generation,
      analyticsProvenance,
    });
    await dependencies.markPendingTraceCompleted({
      projectId: operation.projectId,
      traceIds: [operation.traceId],
    });
    return;
  }

  if (operation.scope === "PROJECT") {
    await dependencies.processProject({
      projectId: operation.projectId,
      organizationId: operation.organizationId,
      reference: {
        operationId: operation.id,
        generation: operation.generation,
        analyticsProvenance,
      },
    });
    return;
  }

  operation.scope satisfies never;
  throw new Error("Unsupported analytics deletion recovery scope");
}
