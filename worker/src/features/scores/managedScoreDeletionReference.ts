import { UnrecoverableError } from "bullmq";

import type { SerializedAnalyticsDurableProvenance } from "@langfuse/shared/src/server";

type ManagedScoreDeletionFields = {
  readonly deletionOperationId?: string;
  readonly deletionGeneration?: string;
  readonly analyticsProvenance?: SerializedAnalyticsDurableProvenance;
};

export function validateManagedScoreDeletionReference(
  fields: ManagedScoreDeletionFields,
  expectedOperationId: string,
): SerializedAnalyticsDurableProvenance | undefined {
  const values = [
    fields.deletionOperationId,
    fields.deletionGeneration,
    fields.analyticsProvenance,
  ];
  const present = values.filter((value) => value !== undefined).length;
  if (present === 0) return undefined;
  if (
    present !== values.length ||
    fields.deletionOperationId !== expectedOperationId ||
    fields.deletionGeneration !==
      fields.analyticsProvenance?.deploymentGeneration
  ) {
    throw new UnrecoverableError(
      "Managed score deletion reference does not match the queue delivery",
    );
  }
  return fields.analyticsProvenance;
}
