import { prisma } from "@langfuse/shared/src/db";
import {
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
  });
}

export function createDorisEvaluationTargetSource() {
  return new DorisEvaluationTargetSource({
    repositories: getDorisTelemetryRepositories,
    toTraceDomain: toDorisTraceDomain,
    toObservationDomain: toDorisEventsObservation,
    getTraceControl: ({ projectId, traceId }) =>
      prisma.traceControlState.findUnique({
        where: { projectId_traceId: { projectId, traceId } },
        select: { bookmarked: true, public: true },
      }),
  });
}
