import {
  withAnalyticsBatchActionPublicationAdmission,
  type AnalyticsQueuePublicationGuard,
} from "@/src/server/analyticsQueuePublicationAdmission";
import { withAnalyticsEvaluationMutationAdmission } from "@/src/features/evals/server/evaluationMutationAdmission";

export function runHistoricalEvaluationMutation<T>(input: {
  readonly scheduleHistoricalEvaluation: boolean;
  readonly resourceIdentity: string;
  readonly mutate: (guard: AnalyticsQueuePublicationGuard) => Promise<T>;
}): Promise<T> {
  if (!input.scheduleHistoricalEvaluation) {
    return withAnalyticsEvaluationMutationAdmission({
      resourceIdentity: input.resourceIdentity,
      mutate: input.mutate,
    });
  }

  return withAnalyticsBatchActionPublicationAdmission({
    actionId: "eval-create",
    resourceIdentity: input.resourceIdentity,
    publish: input.mutate,
  });
}
