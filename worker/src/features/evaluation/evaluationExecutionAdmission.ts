import type { AnalyticsBackend } from "@langfuse/shared/analytics-backend";
import { prisma } from "@langfuse/shared/src/db";
import {
  AnalyticsEvaluationDispatchProvenanceError,
  type AnalyticsEvaluationDispatchEventType,
  type AnalyticsRuntimeAdmissionContext,
  validateAnalyticsEvaluationExecution,
} from "@langfuse/shared/src/server";

import { getWorkerAnalyticsAdmissionContext } from "../../analyticsRuntime";

type EvaluationExecutionAdmissionInput = {
  readonly backend: AnalyticsBackend;
  readonly projectId: string;
  readonly jobExecutionId: string;
  readonly analyticsEvaluationDispatch?: AnalyticsEvaluationDispatchEventType;
};

type EvaluationExecutionAdmissionDependencies = {
  readonly getAdmissionContext: () => AnalyticsRuntimeAdmissionContext | null;
  readonly validate: typeof validateAnalyticsEvaluationExecution;
};

export async function assertEvaluationExecutionAdmission(
  input: EvaluationExecutionAdmissionInput,
  overrides: Partial<EvaluationExecutionAdmissionDependencies> = {},
): Promise<void> {
  const dependencies: EvaluationExecutionAdmissionDependencies = {
    getAdmissionContext: getWorkerAnalyticsAdmissionContext,
    validate: validateAnalyticsEvaluationExecution,
    ...overrides,
  };
  const envelope = input.analyticsEvaluationDispatch;

  if (input.backend === "clickhouse") {
    if (envelope) {
      throw new AnalyticsEvaluationDispatchProvenanceError(
        "Doris-managed evaluation cannot execute on ClickHouse",
      );
    }
    return;
  }
  if (!envelope) {
    throw new AnalyticsEvaluationDispatchProvenanceError(
      "Doris evaluation execution requires an authoritative dispatch envelope",
    );
  }
  if (envelope.projectId !== input.projectId) {
    throw new AnalyticsEvaluationDispatchProvenanceError(
      "Evaluation dispatch project does not match the execution",
    );
  }
  const admissionContext = dependencies.getAdmissionContext();
  if (!admissionContext) {
    throw new AnalyticsEvaluationDispatchProvenanceError(
      "Evaluation execution runtime is not admitted",
    );
  }

  await dependencies.validate({
    client: prisma,
    admissionContext,
    envelope,
    jobExecutionId: input.jobExecutionId,
  });
}
