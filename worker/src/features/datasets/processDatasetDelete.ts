import {
  AnalyticsPersistenceError,
  analyticsDatasetDeletionReferenceMatches,
  claimAnalyticsDatasetDeletionOperation,
  completeAnalyticsDatasetDeletionOperation,
  deleteDatasetMediaLinksByDatasetId,
  findAnalyticsDatasetDeletionOperation,
  markAnalyticsDatasetDeletionBarrierVisible,
  type DatasetQueueEventType,
} from "@langfuse/shared/src/server";

type DatasetDeletionOperation = NonNullable<
  Awaited<ReturnType<typeof findAnalyticsDatasetDeletionOperation>>
>;

type ProcessDatasetDeleteDependencies = {
  readonly workerId?: string;
  readonly findOperation?: typeof findAnalyticsDatasetDeletionOperation;
  readonly claimOperation?: typeof claimAnalyticsDatasetDeletionOperation;
  readonly markBarrierVisible?: typeof markAnalyticsDatasetDeletionBarrierVisible;
  readonly completeOperation?: typeof completeAnalyticsDatasetDeletionOperation;
  readonly writeBarrier?: (input: {
    readonly operationId: string;
    readonly projectId: string;
    readonly datasetId: string;
    readonly datasetGeneration: bigint | null;
    readonly runGenerations: Readonly<Record<string, bigint>>;
    readonly lease: { readonly owner: string; readonly fence: bigint };
    readonly createdAt: Date;
  }) => Promise<void>;
  readonly cleanupRunItems?: (input: {
    readonly projectId: string;
    readonly datasetId: string;
    readonly datasetRunIds: readonly string[];
    readonly deleteDataset: boolean;
  }) => Promise<void>;
  readonly deleteMediaLinks?: typeof deleteDatasetMediaLinksByDatasetId;
};

function conflict(): never {
  throw new AnalyticsPersistenceError("ANALYTICS_CONFLICT", false, {
    tags: { phase: "dataset_deletion_contract" },
  });
}

function unavailable(): never {
  throw new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
    tags: { phase: "dataset_deletion_lease" },
  });
}

function assertManagedPayload(
  payload: DatasetQueueEventType,
  operation: DatasetDeletionOperation,
): {
  readonly datasetGeneration: bigint | null;
  readonly runGenerations: Readonly<Record<string, bigint>>;
} {
  const reference = payload.analyticsDeletion;
  if (
    !reference ||
    reference.operationId !== operation.id ||
    operation.projectId !== payload.projectId ||
    operation.datasetId !== payload.datasetId ||
    operation.scope !==
      (payload.deletionType === "dataset" ? "DATASET" : "DATASET_RUNS") ||
    !analyticsDatasetDeletionReferenceMatches({ operation, reference })
  ) {
    conflict();
  }
  const datasetGeneration =
    reference.datasetGeneration === null
      ? null
      : BigInt(reference.datasetGeneration);
  const runGenerations = Object.fromEntries(
    Object.entries(reference.runGenerations).map(([runId, generation]) => [
      runId,
      BigInt(generation),
    ]),
  );
  if (
    payload.deletionType === "dataset-runs" &&
    [...payload.datasetRunIds].sort().join("\0") !==
      [...operation.datasetRunIds].sort().join("\0")
  ) {
    conflict();
  }
  return { datasetGeneration, runGenerations };
}

export async function processDatasetDelete(
  jobPayload: DatasetQueueEventType,
  dependencies: ProcessDatasetDeleteDependencies = {},
): Promise<void> {
  if (!jobPayload.analyticsDeletion) {
    throw new AnalyticsPersistenceError(
      "ANALYTICS_UNSUPPORTED_FEATURE",
      false,
      { tags: { sourceContract: "legacy-dataset-deletion" } },
    );
  }
  const findOperation =
    dependencies.findOperation ?? findAnalyticsDatasetDeletionOperation;
  const operation = await findOperation({
    operationId: jobPayload.analyticsDeletion.operationId,
    projectId: jobPayload.projectId,
  });
  if (!operation) conflict();
  if (operation.status === "COMPLETED") return;
  const generations = assertManagedPayload(jobPayload, operation);
  const owner =
    dependencies.workerId ??
    `dataset-delete-${process.pid}-${jobPayload.analyticsDeletion.operationId}`;
  const claimOperation =
    dependencies.claimOperation ?? claimAnalyticsDatasetDeletionOperation;
  const claimed = await claimOperation({
    operationId: operation.id,
    projectId: operation.projectId,
    owner,
  });
  if (!claimed) unavailable();
  const lease = { owner, fence: claimed.workerFence };

  if (!claimed.logicallyInvisible) {
    const writeBarrier =
      dependencies.writeBarrier ??
      (await import("./dorisDatasetDeletionLifecycle.js"))
        .writeDorisDatasetDeletionBarrier;
    await writeBarrier({
      operationId: claimed.id,
      projectId: claimed.projectId,
      datasetId: claimed.datasetId,
      ...generations,
      lease,
      createdAt: claimed.createdAt,
    });
    const markBarrierVisible =
      dependencies.markBarrierVisible ??
      markAnalyticsDatasetDeletionBarrierVisible;
    if (
      !(await markBarrierVisible({
        operationId: claimed.id,
        projectId: claimed.projectId,
        lease,
      }))
    ) {
      unavailable();
    }
  }

  const cleanupRunItems =
    dependencies.cleanupRunItems ??
    (await import("./dorisDatasetDeletionLifecycle.js"))
      .cleanupDorisDatasetRunItems;
  await cleanupRunItems({
    projectId: claimed.projectId,
    datasetId: claimed.datasetId,
    datasetRunIds: claimed.datasetRunIds,
    deleteDataset: claimed.scope === "DATASET",
  });
  if (claimed.scope === "DATASET") {
    await (dependencies.deleteMediaLinks ?? deleteDatasetMediaLinksByDatasetId)(
      {
        projectId: claimed.projectId,
        datasetId: claimed.datasetId,
      },
    );
  }
  const completeOperation =
    dependencies.completeOperation ?? completeAnalyticsDatasetDeletionOperation;
  if (
    !(await completeOperation({
      operationId: claimed.id,
      projectId: claimed.projectId,
      lease,
    }))
  ) {
    unavailable();
  }
}
