import type { AnalyticsEntityHead, PrismaClient } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";
import {
  analyticsProjectRetentionStateId,
  getS3EventStorageClient,
  type AnalyticsRuntimeAdmissionContext,
} from "@langfuse/shared/src/server";

import { env } from "../../../env";
import { getDorisAnalyticsLifecycleRuntime } from "../../../services/dorisAnalyticsLifecycle";
import { processDorisGlobalRetentionStep } from "../../doris-global-retention";

type ProjectRetentionDependencies = {
  client?: PrismaClient;
  processStep?: typeof processDorisGlobalRetentionStep;
  deleteDorisHeads?: (
    operationId: string,
    heads: readonly AnalyticsEntityHead[],
  ) => Promise<void>;
  onCutoffPublished?: (input: { cutoffDate: Date }) => Promise<void>;
  scheduleContinuation?: (input: {
    retentionDays: number;
    delayMs: number;
  }) => Promise<void>;
};

export async function processDorisProjectRetention(
  input: {
    projectId: string;
    queuedRetentionDays: number;
    admissionContext?: AnalyticsRuntimeAdmissionContext | null;
  },
  dependencies: ProjectRetentionDependencies = {},
) {
  const client = dependencies.client ?? prisma;
  const stateId = analyticsProjectRetentionStateId(input.projectId);
  const [project, state] = await Promise.all([
    client.project.findUnique({
      where: { id: input.projectId },
      select: { retentionDays: true },
    }),
    client.analyticsRetentionState.findUnique({
      where: { id: stateId },
      select: { activeRunId: true },
    }),
  ]);
  if (!project?.retentionDays && !state?.activeRunId) {
    return { outcome: "idle" as const };
  }
  // 关闭设置只能阻止新 run；已经发布的 cutoff 必须继续收敛。
  const retentionDays = project?.retentionDays
    ? project.retentionDays
    : input.queuedRetentionDays;
  const result = await (
    dependencies.processStep ?? processDorisGlobalRetentionStep
  )({
    projectId: input.projectId,
    stateId,
    retentionDays,
    drainMs: env.LANGFUSE_DORIS_GLOBAL_RETENTION_DRAIN_MS,
    batchSize: env.LANGFUSE_DORIS_GLOBAL_RETENTION_BATCH_SIZE,
    admissionContext: input.admissionContext ?? null,
    onCutoffPublished: (cutoffDate) =>
      dependencies.onCutoffPublished?.({ cutoffDate }) ?? Promise.resolve(),
    dependencies: {
      client,
      deleteDorisHeads:
        dependencies.deleteDorisHeads ??
        ((operationId, heads) =>
          deleteDorisRetentionBatch(operationId, heads, { client })),
    },
  });
  if (result.outcome !== "idle" && result.outcome !== "completed") {
    await dependencies.scheduleContinuation?.({
      retentionDays,
      delayMs: result.outcome === "waiting" ? 60_000 : 1_000,
    });
  }
  return result;
}

export async function deleteDorisRetentionBatch(
  operationId: string,
  heads: readonly AnalyticsEntityHead[],
  dependencies: {
    client?: PrismaClient;
    deleteObjects?: (keys: string[]) => Promise<void>;
    deleteMaterialized?: (
      operationId: string,
      heads: readonly AnalyticsEntityHead[],
    ) => Promise<void>;
  } = {},
): Promise<void> {
  if (heads.length === 0) return;
  const projectId = heads[0]!.projectId;
  if (heads.some((head) => head.projectId !== projectId)) {
    throw new Error("Retention batch contains multiple projects");
  }
  const client = dependencies.client ?? prisma;
  const operationIds = [...new Set(heads.map((head) => head.operationId))];
  const survivingHeads = await client.analyticsEntityHead.findMany({
    where: {
      projectId,
      operationId: { in: operationIds },
      id: { notIn: heads.map((head) => head.id) },
    },
    select: { operationId: true },
    distinct: ["operationId"],
  });
  const protectedOperations = new Set(
    survivingHeads.map((head) => head.operationId),
  );
  const expiredOperations = operationIds.filter(
    (id) => !protectedOperations.has(id),
  );
  if (expiredOperations.length > 0) {
    const artifacts = await client.analyticsIngestionOperation.findMany({
      where: {
        projectId,
        id: { in: expiredOperations },
        status: "VISIBLE",
      },
      select: { rawObjectKey: true, canonicalObjectKey: true },
    });
    const keys = [
      ...new Set(
        artifacts.flatMap(({ rawObjectKey, canonicalObjectKey }) =>
          canonicalObjectKey
            ? [rawObjectKey, canonicalObjectKey]
            : [rawObjectKey],
        ),
      ),
    ];
    if (keys.length > 0) {
      await (
        dependencies.deleteObjects ??
        ((paths) =>
          getS3EventStorageClient(
            env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
          ).deleteFiles(paths))
      )(keys);
    }
  }
  // 对象删除可重试；只有 Doris 删除确认可见后，调用者才清理 PG heads。
  await (
    dependencies.deleteMaterialized ??
    ((id, selected) =>
      getDorisAnalyticsLifecycleRuntime().materializedDeletion.deleteHeads(
        id,
        selected,
      ))
  )(operationId, heads);
}
