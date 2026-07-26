import {
  withAnalyticsQueuePublicationAdmission,
  type AnalyticsQueuePublicationGuard,
} from "@/src/server/analyticsQueuePublicationAdmission";

/**
 * Serializes evaluator configuration writes with the Doris capability row.
 * This makes DRAINING the linearization boundary for the replay cutoff.
 */
export function withAnalyticsEvaluationMutationAdmission<T>(input: {
  readonly resourceIdentity: string;
  readonly mutate: (guard: AnalyticsQueuePublicationGuard) => Promise<T>;
}): Promise<T> {
  return withAnalyticsQueuePublicationAdmission({
    claimKind: "evaluation-config-mutation",
    resourceIdentity: input.resourceIdentity,
    supportedBackends: ["clickhouse", "doris"],
    unsupportedMessage: "",
    capabilities: ["evaluations"],
    publish: input.mutate,
  });
}
