import {
  configuredCommunityAnalyticsQueueInventory,
  type CommunityAnalyticsQueueInventoryEntry,
} from "./analyticsQueueInventory";
import {
  ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES,
  probeAnalyticsScoreDeletionQueues,
  type AnalyticsScoreDeletionQueueDrainEvidence,
  type AnalyticsScoreDeletionQueueDrainScope,
} from "./analyticsScoreDeletionDrain";

export async function createEmptyAnalyticsQueueDrainEvidence(input: {
  readonly scope: AnalyticsScoreDeletionQueueDrainScope;
  readonly queueNamespaceFingerprint: string;
  readonly inventory?: readonly CommunityAnalyticsQueueInventoryEntry[];
  readonly countsByQueue?: Readonly<
    Record<string, Readonly<Record<string, number>>>
  >;
}): Promise<AnalyticsScoreDeletionQueueDrainEvidence> {
  const counts = Object.fromEntries(
    ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES.map((type) => [type, 0]),
  );
  return probeAnalyticsScoreDeletionQueues({
    queues: (
      input.inventory ?? configuredCommunityAnalyticsQueueInventory()
    ).map((entry) => ({
      ...entry,
      getJobCounts: async () => ({
        ...counts,
        ...input.countsByQueue?.[entry.name],
      }),
    })),
    queueNamespaceFingerprint: input.queueNamespaceFingerprint,
    scope: input.scope,
  });
}
