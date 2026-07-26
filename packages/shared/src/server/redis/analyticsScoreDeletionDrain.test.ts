import { describe, expect, it, vi } from "vitest";

import { QueueName } from "../queues";
import type { CommunityAnalyticsQueueInventoryEntry } from "./analyticsQueueInventory";
import {
  ANALYTICS_QUEUE_DRAIN_BLOCKING_JOB_TYPES,
  ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES,
  assertAnalyticsQueueDrainEvidence,
  createAnalyticsScoreDeletionDrainProbe,
  probeAnalyticsScoreDeletionQueues,
  type AnalyticsScoreDeletionDrainQueue,
} from "./analyticsScoreDeletionDrain";

const queueNamespaceFingerprint = "a".repeat(64);
const scope = {
  backend: "clickhouse" as const,
  deploymentGeneration: 7n,
  workloadEpochFingerprint: "c".repeat(64),
};
const inventory: readonly CommunityAnalyticsQueueInventoryEntry[] = [
  {
    family: QueueName.IngestionQueue,
    name: QueueName.IngestionQueue,
    shardIndex: 0,
  },
  {
    family: QueueName.IngestionQueue,
    name: `${QueueName.IngestionQueue}-1`,
    shardIndex: 1,
  },
  {
    family: QueueName.ScoreDelete,
    name: QueueName.ScoreDelete,
    shardIndex: null,
  },
];
const emptyCounts = Object.fromEntries(
  ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES.map((type) => [type, 0]),
);

function queue(
  descriptor: CommunityAnalyticsQueueInventoryEntry,
  counts: Record<string, number> = emptyCounts,
): AnalyticsScoreDeletionDrainQueue {
  return {
    ...descriptor,
    getJobCounts: vi.fn(async () => counts),
  };
}

function queues(
  overrides: Readonly<Record<string, Record<string, number>>> = {},
): readonly AnalyticsScoreDeletionDrainQueue[] {
  return inventory.map((descriptor) =>
    queue(descriptor, overrides[descriptor.name] ?? emptyCounts),
  );
}

describe("community analytics queue drain", () => {
  it.each(ANALYTICS_QUEUE_DRAIN_BLOCKING_JOB_TYPES)(
    "blocks a switch when any physical shard has %s work",
    async (jobType) => {
      const evidence = await probeAnalyticsScoreDeletionQueues({
        queues: queues({
          [`${QueueName.IngestionQueue}-1`]: {
            ...emptyCounts,
            [jobType]: 1,
          },
        }),
        queueNamespaceFingerprint,
        scope,
      });

      expect(evidence).toMatchObject({ empty: false, pendingJobs: 1 });
      expect(
        evidence.queues.find(
          (entry) => entry.name === `${QueueName.IngestionQueue}-1`,
        )?.counts[jobType],
      ).toBe(1);
    },
  );

  it("allows completed history and permanent repeat metadata while recording both", async () => {
    const inspectedQueues = queues({
      [QueueName.IngestionQueue]: {
        ...emptyCounts,
        completed: 17,
        repeat: 1,
      },
      [QueueName.ScoreDelete]: {
        ...emptyCounts,
        completed: 23,
      },
    });

    const evidence = await probeAnalyticsScoreDeletionQueues({
      queues: inspectedQueues,
      queueNamespaceFingerprint,
      scope,
    });

    expect(evidence).toMatchObject({ empty: true, pendingJobs: 0 });
    expect(evidence.queues[0]?.counts).toMatchObject({
      completed: 17,
      repeat: 1,
    });
    expect(inspectedQueues[0]?.getJobCounts).toHaveBeenCalledWith(
      ...ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES,
    );
  });

  it("fails closed when any physical queue is unavailable or Redis rejects", async () => {
    await expect(
      probeAnalyticsScoreDeletionQueues({
        queues: null,
        queueNamespaceFingerprint,
        scope,
      }),
    ).rejects.toThrow(/unavailable/i);

    const inspectedQueues = queues();
    vi.mocked(inspectedQueues[1]!.getJobCounts).mockRejectedValue(
      new Error("redis shard unavailable"),
    );
    await expect(
      probeAnalyticsScoreDeletionQueues({
        queues: inspectedQueues,
        queueNamespaceFingerprint,
        scope,
      }),
    ).rejects.toThrow("redis shard unavailable");
  });

  it.each([undefined, -1, 1.5, Number.NaN])(
    "fails closed when BullMQ returns invalid count %s",
    async (count) => {
      await expect(
        probeAnalyticsScoreDeletionQueues({
          queues: queues({
            [QueueName.ScoreDelete]: {
              ...emptyCounts,
              waiting: count as number,
            },
          }),
          queueNamespaceFingerprint,
          scope,
        }),
      ).rejects.toThrow(/invalid waiting job count/i);
    },
  );

  it("creates one read-only inspector session for the full physical inventory", async () => {
    const close = vi.fn(async () => undefined);
    const createInspectionSession = vi.fn(() => ({
      queues: queues(),
      close,
    }));
    const probe = createAnalyticsScoreDeletionDrainProbe({
      getInventory: () => inventory,
      createInspectionSession,
      getQueueNamespaceFingerprint: () => queueNamespaceFingerprint,
    });

    await probe.close();
    expect(createInspectionSession).not.toHaveBeenCalled();

    await expect(probe.verify(scope)).resolves.toMatchObject({
      backend: "clickhouse",
      deploymentGeneration: "7",
      workloadEpochFingerprint: scope.workloadEpochFingerprint,
      empty: true,
    });
    await probe.verify(scope);

    expect(createInspectionSession).toHaveBeenCalledOnce();
    expect(createInspectionSession).toHaveBeenCalledWith(inventory);
    await probe.close();
    expect(close).toHaveBeenCalledOnce();
  });

  it("produces deterministic, payload-free evidence bound to inventory and scope", async () => {
    const first = await probeAnalyticsScoreDeletionQueues({
      queues: queues(),
      queueNamespaceFingerprint,
      scope,
    });
    const second = await probeAnalyticsScoreDeletionQueues({
      queues: queues(),
      queueNamespaceFingerprint,
      scope,
    });
    const nextGeneration = await probeAnalyticsScoreDeletionQueues({
      queues: queues(),
      queueNamespaceFingerprint,
      scope: { ...scope, deploymentGeneration: 8n },
    });
    const changedShardCount = await probeAnalyticsScoreDeletionQueues({
      queues: queues().slice(0, -1),
      queueNamespaceFingerprint,
      scope,
    });

    expect(first.evidenceDigest).toBe(second.evidenceDigest);
    expect(first.evidenceDigest).not.toBe(nextGeneration.evidenceDigest);
    expect(first.evidenceDigest).not.toBe(changedShardCount.evidenceDigest);
    expect(JSON.stringify(first)).not.toContain("payload");
  });

  it("rejects evidence when pendingJobs, empty, counts, or digest are inconsistent", async () => {
    const evidence = await probeAnalyticsScoreDeletionQueues({
      queues: queues(),
      queueNamespaceFingerprint,
      scope,
    });

    expect(() =>
      assertAnalyticsQueueDrainEvidence({
        evidence,
        expectedInventory: inventory,
      }),
    ).not.toThrow();
    expect(() =>
      assertAnalyticsQueueDrainEvidence({
        evidence: { ...evidence, pendingJobs: 1 },
        expectedInventory: inventory,
      }),
    ).toThrow(/pending/i);
    expect(() =>
      assertAnalyticsQueueDrainEvidence({
        evidence: { ...evidence, empty: false },
        expectedInventory: inventory,
      }),
    ).toThrow(/empty/i);
    expect(() =>
      assertAnalyticsQueueDrainEvidence({
        evidence: { ...evidence, evidenceDigest: "f".repeat(64) },
        expectedInventory: inventory,
      }),
    ).toThrow(/digest/i);
    expect(() =>
      assertAnalyticsQueueDrainEvidence({
        evidence,
        expectedInventory: inventory.slice(0, -1),
      }),
    ).toThrow(/inventory/i);
  });
});
