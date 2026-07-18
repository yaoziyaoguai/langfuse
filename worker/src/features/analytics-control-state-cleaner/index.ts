import type { PrismaClient } from "@prisma/client";

import { prisma } from "@langfuse/shared/src/db";
import { recordIncrement } from "@langfuse/shared/src/server";

const POST_REPLAY_SAFETY_DELAY_MS = 24 * 60 * 60 * 1_000;

/**
 * 只压缩已经被可验证 checkpoint 覆盖的成功终态 child ledger。operation 状态和
 * entity head 继续保留；生产周期调度必须等 U8 checkpoint/restore 闭环后才启用。
 */
export async function compactAnalyticsControlState(input: {
  readonly client?: PrismaClient;
  readonly now?: Date;
  readonly limit?: number;
}): Promise<{
  readonly operationsCompacted: number;
  readonly childRowsDeleted: number;
}> {
  const client = input.client ?? prisma;
  const now = input.now ?? new Date();
  const limit = input.limit ?? 100;
  if (
    !Number.isFinite(now.getTime()) ||
    !Number.isSafeInteger(limit) ||
    limit < 1 ||
    limit > 1_000
  ) {
    throw new TypeError("Invalid analytics control-state cleanup input");
  }
  const checkpoint = await client.analyticsCheckpointGeneration.findFirst({
    where: {
      status: "SEALED",
      sealedAt: { not: null },
      keyId: { not: null },
      manifestHash: { not: null },
      signature: { not: null },
    },
    orderBy: { generation: "desc" },
  });
  if (!checkpoint) {
    return { operationsCompacted: 0, childRowsDeleted: 0 };
  }

  const replaySafetyCutoff = new Date(
    now.getTime() - POST_REPLAY_SAFETY_DELAY_MS,
  );
  const eligible = await client.analyticsIngestionOperation.findMany({
    where: {
      terminalAt: { not: null },
      status: {
        in: [
          "VISIBLE",
          "CANCELLED_BY_DELETION",
          "COMPLETED_WITH_CANCELLATIONS",
        ],
      },
      acceptedAt: {
        lte: checkpoint.operationHighWatermarkAcceptedAt,
      },
      recoverableUntil: { lte: replaySafetyCutoff },
      statusExpiresAt: { gt: now },
      candidates: { some: {} },
      loadBatches: {
        every: { createdAt: { lte: checkpoint.loadHighWatermarkCreatedAt } },
      },
    },
    orderBy: [{ recoverableUntil: "asc" }, { id: "asc" }],
    select: { id: true, projectId: true },
    take: limit,
  });

  let operationsCompacted = 0;
  let childRowsDeleted = 0;
  for (const candidate of eligible) {
    const compacted = await client.$transaction(async (transaction) => {
      const currentCheckpoint =
        await transaction.analyticsCheckpointGeneration.findFirst({
          where: {
            generation: checkpoint.generation,
            status: "SEALED",
            sealedAt: { not: null },
            keyId: { not: null },
            manifestHash: { not: null },
            signature: { not: null },
          },
        });
      if (!currentCheckpoint) return null;
      const operation = await transaction.analyticsIngestionOperation.findFirst(
        {
          where: {
            id: candidate.id,
            projectId: candidate.projectId,
            terminalAt: { not: null },
            status: {
              in: [
                "VISIBLE",
                "CANCELLED_BY_DELETION",
                "COMPLETED_WITH_CANCELLATIONS",
              ],
            },
            acceptedAt: {
              lte: currentCheckpoint.operationHighWatermarkAcceptedAt,
            },
            recoverableUntil: { lte: replaySafetyCutoff },
            statusExpiresAt: { gt: now },
            candidates: { some: {} },
            loadBatches: {
              every: {
                createdAt: {
                  lte: currentCheckpoint.loadHighWatermarkCreatedAt,
                },
              },
            },
          },
          include: { candidates: true, loadBatches: true },
        },
      );
      if (!operation) return null;
      const candidateCount = operation.candidates.length;
      const loadBatchCount = operation.loadBatches.length;
      const deletedCandidates =
        await transaction.analyticsIngestionCandidate.deleteMany({
          where: { operationId: operation.id, projectId: operation.projectId },
        });
      const deletedLoads = await transaction.analyticsLoadBatch.deleteMany({
        where: { operationId: operation.id, projectId: operation.projectId },
      });
      if (
        deletedCandidates.count !== candidateCount ||
        deletedLoads.count !== loadBatchCount
      ) {
        throw new Error(
          "Analytics control-state child compaction was incomplete",
        );
      }
      await transaction.analyticsIngestionOperation.update({
        where: { id: operation.id },
        data: {
          candidateManifest: {
            compacted: true,
            candidateCount,
            checkpointGeneration: currentCheckpoint.generation.toString(),
          },
          frozenManifest: {
            compacted: true,
            candidateCount,
            loadBatchCount,
            checkpointGeneration: currentCheckpoint.generation.toString(),
            compactedAt: now.toISOString(),
          },
        },
      });
      return candidateCount + loadBatchCount;
    });
    if (compacted === null) continue;
    operationsCompacted += 1;
    childRowsDeleted += compacted;
  }
  if (operationsCompacted > 0) {
    recordIncrement(
      "langfuse.analytics.control_state.compacted",
      operationsCompacted,
    );
  }
  return { operationsCompacted, childRowsDeleted };
}

export class AnalyticsControlStateCleaner {
  private timeout: NodeJS.Timeout | null = null;

  constructor(
    private readonly dependencies: {
      readonly intervalMs: number;
      readonly runOnce?: typeof compactAnalyticsControlState;
    },
  ) {
    if (
      !Number.isSafeInteger(dependencies.intervalMs) ||
      dependencies.intervalMs < 1_000
    ) {
      throw new TypeError("Invalid analytics control-state cleaner interval");
    }
  }

  start(): void {
    if (this.timeout) return;
    const tick = async () => {
      try {
        await (this.dependencies.runOnce ?? compactAnalyticsControlState)({});
      } finally {
        if (this.timeout) {
          this.timeout = setTimeout(tick, this.dependencies.intervalMs);
          this.timeout.unref();
        }
      }
    };
    this.timeout = setTimeout(tick, this.dependencies.intervalMs);
    this.timeout.unref();
  }

  stop(): void {
    if (this.timeout) clearTimeout(this.timeout);
    this.timeout = null;
  }
}
