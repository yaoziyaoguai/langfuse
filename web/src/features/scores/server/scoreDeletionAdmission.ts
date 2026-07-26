import {
  withAnalyticsQueuePublicationAdmission,
  type AnalyticsQueuePublicationGuard,
} from "@/src/server/analyticsQueuePublicationAdmission";

type ScoreDeletionAdmissionInput<T> = {
  readonly resourceIdentity: string;
  readonly publish: (guard: AnalyticsQueuePublicationGuard) => Promise<T>;
};

export async function withScoreDeletionAdmission<T>(
  input: ScoreDeletionAdmissionInput<T>,
): Promise<T> {
  return withAnalyticsQueuePublicationAdmission({
    claimKind: "score-delete-publish",
    resourceIdentity: input.resourceIdentity,
    supportedBackends: ["clickhouse", "doris"],
    unsupportedMessage: "",
    requiresManagedDoris: true,
    publish: input.publish,
  });
}

export function managedScoreDeletionReference(
  guard: AnalyticsQueuePublicationGuard,
  deletionOperationId: string,
) {
  return guard.durableProvenance
    ? {
        deletionOperationId,
        deletionGeneration: guard.durableProvenance.deploymentGeneration,
        analyticsProvenance: guard.durableProvenance,
      }
    : {};
}
