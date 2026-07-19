import {
  applyFullMapping,
  BatchActionStatus,
  type MappingError,
  type ObservationAddToDatasetConfig,
} from "@langfuse/shared";
import { prisma } from "@langfuse/shared/src/db";
import {
  createManyDatasetItems,
  logger,
  traceException,
} from "@langfuse/shared/src/server";

const CHUNK_SIZE = 100;

export type ObservationForDatasetMapping = {
  id: string;
  traceId: string;
  input: unknown;
  output: unknown;
  metadata: unknown;
};

function formatMappingErrors(
  observationId: string,
  errors: MappingError[],
): string {
  return `Observation ${observationId}: Mapping failed - ${errors.map(({ message }) => message).join("; ")}`;
}

async function processChunk(params: {
  projectId: string;
  datasetId: string;
  mapping: ObservationAddToDatasetConfig["mapping"];
  observations: ObservationForDatasetMapping[];
}): Promise<{ processed: number; failed: number; errors: string[] }> {
  const items: Array<{
    datasetId: string;
    input: unknown;
    expectedOutput: unknown;
    metadata: unknown;
    sourceTraceId: string;
    sourceObservationId: string;
  }> = [];
  const mappingErrors: string[] = [];

  for (const observation of params.observations) {
    const mapped = applyFullMapping({
      observation: {
        input: observation.input,
        output: observation.output,
        metadata: observation.metadata,
      },
      mapping: params.mapping,
    });
    if (mapped.errors.length > 0) {
      mappingErrors.push(formatMappingErrors(observation.id, mapped.errors));
      continue;
    }
    items.push({
      datasetId: params.datasetId,
      input: mapped.input,
      expectedOutput: mapped.expectedOutput ?? undefined,
      metadata: mapped.metadata ?? undefined,
      sourceTraceId: observation.traceId,
      sourceObservationId: observation.id,
    });
  }

  if (items.length === 0) {
    return {
      processed: 0,
      failed: mappingErrors.length,
      errors: mappingErrors,
    };
  }

  try {
    const result = await createManyDatasetItems({
      projectId: params.projectId,
      items,
      normalizeOpts: { sanitizeControlChars: true },
      validateOpts: { normalizeUndefinedToNull: true },
      allowPartialSuccess: true,
    });
    if (!result.success) {
      return {
        processed: 0,
        failed: items.length + mappingErrors.length,
        errors: [
          ...mappingErrors,
          ...(result.validationErrors ?? []).map(
            (error) =>
              `Item ${error.itemIndex}: ${error.field} - ${error.errors.map(({ message }) => message).join(", ")}`,
          ),
        ],
      };
    }
    return {
      processed: result.successCount,
      failed: result.failedCount + mappingErrors.length,
      errors: [
        ...mappingErrors,
        ...(result.validationErrors ?? []).map(
          (error) =>
            `Item ${error.itemIndex}: ${error.field} - ${error.errors.map(({ message }) => message).join(", ")}`,
        ),
      ],
    };
  } catch (error) {
    logger.error("Failed to create dataset items in batch action", error);
    traceException(error);
    return {
      processed: 0,
      failed: items.length + mappingErrors.length,
      errors: [
        ...mappingErrors,
        `Failed to create chunk: ${error instanceof Error ? error.message : "Unknown error"}`,
      ],
    };
  }
}

export async function processAddObservationsToDataset(params: {
  projectId: string;
  batchActionId: string;
  config: ObservationAddToDatasetConfig;
  observations: ObservationForDatasetMapping[];
}): Promise<void> {
  await prisma.batchAction.update({
    where: { id: params.batchActionId },
    data: {
      status: BatchActionStatus.Processing,
      totalCount: params.observations.length,
    },
  });

  let processed = 0;
  let failed = 0;
  const errors: string[] = [];
  for (
    let offset = 0;
    offset < params.observations.length;
    offset += CHUNK_SIZE
  ) {
    const result = await processChunk({
      projectId: params.projectId,
      datasetId: params.config.datasetId,
      mapping: params.config.mapping,
      observations: params.observations.slice(offset, offset + CHUNK_SIZE),
    });
    processed += result.processed;
    failed += result.failed;
    errors.push(...result.errors.slice(0, Math.max(0, 20 - errors.length)));
    await prisma.batchAction.update({
      where: { id: params.batchActionId },
      data: { processedCount: processed, failedCount: failed },
    });
  }

  const status =
    failed === 0
      ? BatchActionStatus.Completed
      : processed === 0
        ? BatchActionStatus.Failed
        : BatchActionStatus.Partial;
  await prisma.batchAction.update({
    where: { id: params.batchActionId },
    data: {
      status,
      finishedAt: new Date(),
      processedCount: processed,
      failedCount: failed,
      log:
        errors.length > 0
          ? `${failed} items failed validation. Sample errors:\n${errors.join("\n")}`
          : null,
    },
  });
}
