import type { AnalyticsIngestionOperation, Prisma } from "@prisma/client";

/**
 * Serializes manifest creation, load claims, and terminalization for one
 * ingestion operation. Fenced load outcomes and reconciliation remain CAS-only
 * because a LOADING/UNKNOWN row already prevents terminalization.
 */
export async function findAndLockAnalyticsIngestionOperation(
  transaction: Prisma.TransactionClient,
  input: { readonly operationId: string; readonly projectId: string },
): Promise<AnalyticsIngestionOperation | null> {
  const locked = await transaction.$queryRaw<Array<{ id: string }>>`
    SELECT id
    FROM analytics_ingestion_operations
    WHERE id = ${input.operationId} AND project_id = ${input.projectId}
    FOR UPDATE
  `;
  if (locked.length !== 1) {
    return null;
  }
  return transaction.analyticsIngestionOperation.findFirst({
    where: { id: input.operationId, projectId: input.projectId },
  });
}

export async function lockAnalyticsIngestionOperation(
  transaction: Prisma.TransactionClient,
  input: { readonly operationId: string; readonly projectId: string },
): Promise<AnalyticsIngestionOperation> {
  const operation = await findAndLockAnalyticsIngestionOperation(
    transaction,
    input,
  );
  if (!operation) {
    throw new Error("Analytics ingestion operation does not exist");
  }
  return operation;
}
