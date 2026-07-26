import { prisma } from "@langfuse/shared/src/db";

import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";
import { withAnalyticsDeletionWorkFence } from "../analytics-deletion/analyticsDeletionWorkFence";
import { processClickhouseTraceDelete } from "../traces/processClickhouseTraceDelete";
import { processPostgresTraceDelete } from "../traces/processPostgresTraceDelete";

type LegacyTraceDeletionBackend = "admission" | "postgres" | "clickhouse";

export type LegacyTraceDeletionFailure = {
  readonly backend: LegacyTraceDeletionBackend;
  readonly errorName: string;
  readonly reason: unknown;
};

type Dependencies = {
  readonly processPostgres: typeof processPostgresTraceDelete;
  readonly processClickhouse: typeof processClickhouseTraceDelete;
  readonly withFence: (run: () => Promise<void>) => Promise<void>;
};

const defaultDependencies: Dependencies = {
  processPostgres: (projectId, traceIds) =>
    processPostgresTraceDelete(projectId, traceIds),
  processClickhouse: (projectId, traceIds) =>
    processClickhouseTraceDelete(projectId, traceIds),
  withFence: (run) =>
    withAnalyticsDeletionWorkFence({
      client: prisma,
      operation: null,
      serializedProvenance: undefined,
      admissionContext: getWorkerAnalyticsAdmissionContext(),
      selectedBackend: "clickhouse",
      claimKind: "analytics-deletion-operation",
      run,
    }),
};

function errorName(error: unknown): string {
  return error instanceof Error ? error.name : typeof error;
}

export async function processLegacyTraceDeletionBatch(
  input: {
    readonly projectId: string;
    readonly traceIds: string[];
  },
  dependencies: Dependencies = defaultDependencies,
): Promise<readonly LegacyTraceDeletionFailure[]> {
  const tasks = [
    {
      backend: "postgres" as const,
      run: () => dependencies.processPostgres(input.projectId, input.traceIds),
    },
    {
      backend: "clickhouse" as const,
      run: () =>
        dependencies.processClickhouse(input.projectId, input.traceIds),
    },
  ];
  const state: { results?: readonly PromiseSettledResult<void>[] } = {};
  try {
    await dependencies.withFence(async () => {
      state.results = await Promise.allSettled(tasks.map(({ run }) => run()));
    });
    if (!state.results) {
      throw new Error("Legacy deletion fence completed without running IO");
    }
  } catch (error) {
    return [
      { backend: "admission", errorName: errorName(error), reason: error },
    ];
  }

  return tasks.flatMap(({ backend }, index) => {
    const result = state.results![index]!;
    return result.status === "rejected"
      ? [
          {
            backend,
            errorName: errorName(result.reason),
            reason: result.reason,
          },
        ]
      : [];
  });
}
