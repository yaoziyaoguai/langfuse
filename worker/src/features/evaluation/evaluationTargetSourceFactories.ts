import type { ObservationForEval } from "@langfuse/shared";
import { prisma } from "@langfuse/shared/src/db";
import {
  buildDorisTraceReadQuery,
  checkObservationExists,
  checkTraceExistsAndGetTimestamp,
  getDatasetItemIdsByTraceId,
  getDorisTelemetryRepositories,
  getObservationForTraceIdByName,
  getTraceByIdFromEventsTable,
  getTraceByIdFromTracesTable,
  toDorisEventsObservation,
  toDorisTraceDomain,
} from "@langfuse/shared/src/server";

import { env } from "../../env";
import { ClickHouseEvaluationTargetSource } from "./ClickHouseEvaluationTargetSource";
import { DorisEvaluationTargetSource } from "./DorisEvaluationTargetSource";

export function createClickHouseEvaluationTargetSource() {
  return new ClickHouseEvaluationTargetSource({
    getTrace:
      env.LANGFUSE_MIGRATION_V4_WRITE_MODE === "events_only"
        ? getTraceByIdFromEventsTable
        : getTraceByIdFromTracesTable,
    getObservationsByName: getObservationForTraceIdByName,
    checkTraceExists: checkTraceExistsAndGetTimestamp,
    checkObservationExists: ({ projectId, observationId }) =>
      checkObservationExists(projectId, observationId, new Date()),
    getDatasetItemsByTraceId: ({ projectId, traceId }) =>
      getDatasetItemIdsByTraceId({ projectId, traceId, filter: [] }),
    // Managed evaluation dispatches are Doris-only. ClickHouse observation
    // evaluation continues to use its existing S3-backed queue payload.
    getObservationForEvaluation: async () => undefined,
  });
}

export function toDorisObservationForEvaluation(
  observation: Awaited<
    ReturnType<
      ReturnType<typeof getDorisTelemetryRepositories>["observations"]["get"]
    >
  >,
): ObservationForEval | undefined {
  if (!observation) return undefined;
  return {
    span_id: observation.id,
    trace_id: observation.traceId,
    project_id: observation.projectId,
    parent_span_id: observation.parentObservationId,
    type: observation.type,
    name: observation.name ?? "",
    environment: observation.environment,
    version: observation.version,
    level: observation.level ?? "DEFAULT",
    status_message: observation.statusMessage,
    trace_name: observation.traceName,
    user_id: observation.userId,
    session_id: observation.sessionId,
    tags: [...observation.tags],
    release: observation.release,
    provided_model_name: observation.providedModelName,
    model_parameters: observation.modelParameters ?? null,
    prompt_id: observation.promptId,
    prompt_name: observation.promptName,
    prompt_version: observation.promptVersion,
    provided_usage_details: { ...observation.providedUsageDetails },
    provided_cost_details: { ...observation.providedCostDetails },
    usage_details: { ...observation.usageDetails },
    cost_details: { ...observation.costDetails },
    tool_definitions: { ...(observation.toolDefinitions ?? {}) },
    tool_calls: [...(observation.toolCalls ?? [])],
    tool_call_names: [...(observation.toolCallNames ?? [])],
    tool_call_count: observation.toolCalls?.length ?? 0,
    experiment_id: observation.experimentId ?? null,
    experiment_name: observation.experimentName ?? null,
    experiment_description: observation.experimentDescription ?? null,
    experiment_dataset_id: observation.experimentDatasetId ?? null,
    experiment_item_id: observation.experimentItemId ?? null,
    experiment_item_expected_output:
      observation.experimentItemExpectedOutput ?? null,
    experiment_item_metadata: observation.experimentItemMetadata ?? null,
    experiment_item_root_span_id: observation.experimentItemRootSpanId ?? null,
    input: observation.input ?? null,
    output: observation.output ?? null,
    metadata: { ...(observation.metadata ?? {}) },
  };
}

export function createDorisEvaluationTargetSource() {
  const repositories = getDorisTelemetryRepositories();
  return new DorisEvaluationTargetSource({
    repositories: getDorisTelemetryRepositories,
    toTraceDomain: toDorisTraceDomain,
    toObservationDomain: toDorisEventsObservation,
    getTraceControl: ({ projectId, traceId }) =>
      prisma.traceControlState.findUnique({
        where: { projectId_traceId: { projectId, traceId } },
        select: { bookmarked: true, public: true },
      }),
    checkTraceExists: async (request) => {
      const filter = [
        ...request.filter,
        {
          type: "string" as const,
          column: "id",
          operator: "=" as const,
          value: request.traceId,
        },
        ...(request.maxTimeStamp
          ? [
              {
                type: "datetime" as const,
                column: "timestamp",
                operator: "<=" as const,
                value: request.maxTimeStamp,
              },
            ]
          : []),
        ...(request.exactTimestamp
          ? [
              {
                type: "datetime" as const,
                column: "timestamp",
                operator: ">=" as const,
                value: request.exactTimestamp,
              },
              {
                type: "datetime" as const,
                column: "timestamp",
                operator: "<" as const,
                value: new Date(request.exactTimestamp.getTime() + 1),
              },
            ]
          : []),
      ];
      const query = await buildDorisTraceReadQuery(request.projectId, filter);
      if (query.impossible) return { exists: false };
      const page = await repositories.traces.list({
        projectId: request.projectId,
        range: query.range,
        filters: query.filters,
        limit: 1,
      });
      const trace = page.items[0];
      return trace
        ? { exists: true, timestamp: trace.timestamp }
        : { exists: false };
    },
    checkObservationExists: async ({ projectId, observationId }) =>
      Boolean(
        await repositories.observations.get({ projectId, observationId }),
      ),
    getDatasetItemsByTraceId: async ({ projectId, traceId }) => {
      const rows = await prisma.datasetRunItems.findMany({
        where: { projectId, traceId },
        select: {
          datasetItemId: true,
          observationId: true,
          datasetRun: { select: { datasetId: true } },
        },
      });
      return rows.map((row) => ({
        id: row.datasetItemId,
        datasetId: row.datasetRun.datasetId,
        observationId: row.observationId,
      }));
    },
    getObservationForEvaluation: async ({
      projectId,
      traceId,
      observationId,
    }) =>
      toDorisObservationForEvaluation(
        await repositories.observations.get({
          projectId,
          traceId,
          observationId,
        }),
      ),
  });
}
