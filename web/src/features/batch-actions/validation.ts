import z from "zod";
import {
  AddToDatasetMappingSchema,
  ObservationAddToDatasetConfigSchema,
  BatchActionQuerySchema,
  BatchEvalSourceTable,
  BatchEvalSourceTableSchema,
  type BatchActionQuery,
} from "@langfuse/shared";

export function scopeBatchEvaluationQuery(
  query: BatchActionQuery,
  sourceTable: BatchEvalSourceTable,
): BatchActionQuery {
  if (sourceTable === BatchEvalSourceTable.EVENTS) return query;

  const filters = query.filter ?? [];
  const alreadyScoped = filters.some(
    (filter) =>
      filter.column === "isExperimentItemRootSpan" &&
      filter.operator === "=" &&
      filter.value === true,
  );
  if (alreadyScoped) return query;

  return {
    ...query,
    filter: [
      ...filters,
      {
        column: "isExperimentItemRootSpan",
        operator: "=",
        value: true,
        type: "boolean",
      },
    ],
  };
}

export const CreateObservationAddToDatasetActionSchema = z.object({
  projectId: z.string(),
  query: BatchActionQuerySchema,
  config: ObservationAddToDatasetConfigSchema,
});

export const CreateObservationBatchEvaluationActionSchema = z.object({
  projectId: z.string(),
  query: BatchActionQuerySchema,
  evaluatorIds: z.array(z.string()).min(1),
  sourceTable: BatchEvalSourceTableSchema.default("events"),
});

export const ValidateBatchAddToDatasetMappingSchema = z.object({
  projectId: z.string(),
  observationId: z.string(),
  traceId: z.string(),
  datasetId: z.string(),
  mapping: AddToDatasetMappingSchema,
});

export const GetBatchActionByIdSchema = z.object({
  projectId: z.string(),
  batchActionId: z.string(),
});

export const ListBatchActionsSchema = z.object({
  projectId: z.string(),
  page: z.number(),
  limit: z.number(),
});
