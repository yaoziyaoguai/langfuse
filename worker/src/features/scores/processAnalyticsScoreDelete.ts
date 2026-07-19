import { createHash } from "node:crypto";

import { prisma } from "@langfuse/shared/src/db";

import { getDorisAnalyticsLifecycleRuntime } from "../../services/dorisAnalyticsLifecycle";

export async function processAnalyticsScoreDelete(
  projectId: string,
  scoreIds: readonly string[],
): Promise<void> {
  const uniqueScoreIds = [...new Set(scoreIds)].sort();
  const heads = await prisma.analyticsEntityHead.findMany({
    where: {
      projectId,
      entityType: "SCORE",
      lookupId: { in: uniqueScoreIds },
    },
  });
  if (heads.length === 0) return;

  const operationId = `score-delete-${createHash("sha256")
    .update(`${projectId}\0${uniqueScoreIds.join("\0")}`, "utf8")
    .digest("hex")}`;
  await getDorisAnalyticsLifecycleRuntime().materializedDeletion.deleteHeads(
    operationId,
    heads,
  );
}
