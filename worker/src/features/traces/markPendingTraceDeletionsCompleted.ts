import type { Prisma } from "@prisma/client";
import { prisma } from "@langfuse/shared/src/db";

type PendingDeletionClient = Pick<Prisma.TransactionClient, "pendingDeletion">;

export async function markPendingTraceDeletionsCompleted(input: {
  readonly projectId: string;
  readonly traceIds: readonly string[];
  readonly client?: PendingDeletionClient;
}): Promise<number> {
  const traceIds = [...new Set(input.traceIds)];
  if (traceIds.length === 0) return 0;
  const result = await (input.client ?? prisma).pendingDeletion.updateMany({
    where: {
      projectId: input.projectId,
      object: "trace",
      objectId: { in: traceIds },
      isDeleted: false,
    },
    data: { isDeleted: true },
  });
  return result.count;
}
