import type { AnalyticsDeletionOperation } from "@prisma/client";

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
  if (operation.scope === "TRACE") {
    if (!operation.traceId) {
      throw new Error("Trace deletion recovery is missing traceId");
    }
    await dependencies.processTrace(operation.projectId, {
      operationId: operation.id,
      traceId: operation.traceId,
      generation: operation.generation,
    });
    await dependencies.markPendingTraceCompleted({
      projectId: operation.projectId,
      traceIds: [operation.traceId],
    });
    return;
  }

  await dependencies.processProject({
    projectId: operation.projectId,
    organizationId: operation.organizationId,
    reference: {
      operationId: operation.id,
      generation: operation.generation,
    },
  });
}
