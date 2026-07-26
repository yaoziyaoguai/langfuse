import { createHash } from "node:crypto";

import { Queue, type JobType } from "bullmq";

import type { AnalyticsBackend } from "../analytics-persistence/analyticsBackend";
import { logger } from "../logger";
import {
  configuredCommunityAnalyticsQueueInventory,
  type CommunityAnalyticsQueueInventoryEntry,
} from "./analyticsQueueInventory";
import { fingerprintConfiguredAnalyticsQueueNamespace } from "./analyticsQueueNamespace";
import {
  createAnalyticsQueuePublisherOptionsWithRedis,
  getQueuePrefix,
  redisErrorForLogging,
} from "./redis";

export const ANALYTICS_QUEUE_DRAIN_BLOCKING_JOB_TYPES = [
  "waiting",
  "paused",
  "delayed",
  "prioritized",
  "active",
  "waiting-children",
  "failed",
] as const satisfies readonly JobType[];

export const ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES = [
  ...ANALYTICS_QUEUE_DRAIN_BLOCKING_JOB_TYPES,
  "completed",
  "repeat",
] as const satisfies readonly JobType[];

/** @deprecated Use ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES. */
export const ANALYTICS_SCORE_DELETION_DRAIN_JOB_TYPES =
  ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES;

type AnalyticsQueueDrainEvidenceJobType =
  (typeof ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES)[number];

export type AnalyticsScoreDeletionDrainQueue =
  CommunityAnalyticsQueueInventoryEntry & {
    getJobCounts(...types: JobType[]): Promise<Record<string, number>>;
  };

export type AnalyticsScoreDeletionQueueDrainEvidence = {
  readonly backend: AnalyticsBackend;
  readonly deploymentGeneration: string;
  readonly workloadEpochFingerprint: string;
  readonly empty: boolean;
  readonly pendingJobs: number;
  readonly queueNamespaceFingerprint: string;
  readonly evidenceDigest: string;
  readonly queues: readonly {
    readonly family: CommunityAnalyticsQueueInventoryEntry["family"];
    readonly name: string;
    readonly shardIndex: number | null;
    readonly counts: Readonly<
      Record<AnalyticsQueueDrainEvidenceJobType, number>
    >;
  }[];
};

export type AnalyticsScoreDeletionQueueDrainScope = {
  readonly backend: AnalyticsBackend;
  readonly deploymentGeneration: bigint;
  readonly workloadEpochFingerprint: string;
};

export type AnalyticsQueueDrainInspectionSession = {
  readonly queues: readonly AnalyticsScoreDeletionDrainQueue[];
  readonly close: () => Promise<void>;
};

function normalizeCounts(
  counts: Record<string, number>,
): Record<AnalyticsQueueDrainEvidenceJobType, number> {
  return Object.fromEntries(
    ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES.map((type) => {
      const count = counts[type];
      if (!Number.isSafeInteger(count) || count < 0) {
        throw new TypeError(`Invalid ${type} job count returned by BullMQ`);
      }
      return [type, count];
    }),
  ) as Record<AnalyticsQueueDrainEvidenceJobType, number>;
}

function assertInventoryEntries(
  actual: readonly CommunityAnalyticsQueueInventoryEntry[],
  expected?: readonly CommunityAnalyticsQueueInventoryEntry[],
): void {
  if (actual.length === 0) {
    throw new Error("Analytics queue inventory is empty");
  }
  const names = new Set<string>();
  for (const entry of actual) {
    if (
      !entry.name ||
      entry.name.includes(":") ||
      (entry.shardIndex !== null &&
        (!Number.isSafeInteger(entry.shardIndex) || entry.shardIndex < 0)) ||
      names.has(entry.name)
    ) {
      throw new TypeError("Invalid analytics queue inventory");
    }
    names.add(entry.name);
  }
  if (expected && JSON.stringify(actual) !== JSON.stringify(expected)) {
    throw new Error("Analytics queue drain inventory does not match");
  }
}

function pendingJobsInQueues(
  queues: AnalyticsScoreDeletionQueueDrainEvidence["queues"],
): number {
  return queues.reduce(
    (total, queue) =>
      total +
      ANALYTICS_QUEUE_DRAIN_BLOCKING_JOB_TYPES.reduce(
        (queueTotal, type) => queueTotal + queue.counts[type],
        0,
      ),
    0,
  );
}

function digestAnalyticsQueueDrainEvidence(
  evidence: Omit<AnalyticsScoreDeletionQueueDrainEvidence, "evidenceDigest">,
): string {
  return createHash("sha256")
    .update(
      JSON.stringify({
        version: 3,
        backend: evidence.backend,
        deploymentGeneration: evidence.deploymentGeneration,
        workloadEpochFingerprint: evidence.workloadEpochFingerprint,
        queueNamespaceFingerprint: evidence.queueNamespaceFingerprint,
        queues: evidence.queues,
      }),
    )
    .digest("hex");
}

export function assertAnalyticsQueueDrainEvidence(input: {
  readonly evidence: AnalyticsScoreDeletionQueueDrainEvidence;
  readonly expectedInventory: readonly CommunityAnalyticsQueueInventoryEntry[];
}): void {
  assertInventoryEntries(input.expectedInventory);
  const evidenceInventory = input.evidence.queues.map(
    ({ family, name, shardIndex }) => ({ family, name, shardIndex }),
  );
  assertInventoryEntries(evidenceInventory, input.expectedInventory);

  const normalizedQueues = input.evidence.queues.map((queue) => ({
    family: queue.family,
    name: queue.name,
    shardIndex: queue.shardIndex,
    counts: normalizeCounts(queue.counts),
  }));
  if (
    JSON.stringify(normalizedQueues) !== JSON.stringify(input.evidence.queues)
  ) {
    throw new TypeError("Analytics queue drain counts are not canonical");
  }

  const pendingJobs = pendingJobsInQueues(input.evidence.queues);
  if (input.evidence.pendingJobs !== pendingJobs) {
    throw new Error("Analytics queue drain pending job count is inconsistent");
  }
  if (input.evidence.empty !== (pendingJobs === 0)) {
    throw new Error("Analytics queue drain empty flag is inconsistent");
  }
  const { evidenceDigest, ...unsignedEvidence } = input.evidence;
  if (
    !/^[a-f0-9]{64}$/.test(evidenceDigest) ||
    digestAnalyticsQueueDrainEvidence(unsignedEvidence) !== evidenceDigest
  ) {
    throw new Error("Analytics queue drain evidence digest is inconsistent");
  }
}

export async function probeAnalyticsScoreDeletionQueues(input: {
  readonly queues: readonly AnalyticsScoreDeletionDrainQueue[] | null;
  readonly queueNamespaceFingerprint: string;
  readonly scope: AnalyticsScoreDeletionQueueDrainScope;
}): Promise<AnalyticsScoreDeletionQueueDrainEvidence> {
  if (!input.queues) {
    throw new Error("Analytics queues are unavailable");
  }
  assertInventoryEntries(input.queues);
  if (!/^[a-f0-9]{64}$/.test(input.queueNamespaceFingerprint)) {
    throw new TypeError("Invalid analytics queue namespace fingerprint");
  }
  if (
    input.scope.deploymentGeneration < 0n ||
    !/^[a-f0-9]{64}$/.test(input.scope.workloadEpochFingerprint)
  ) {
    throw new TypeError("Invalid analytics queue drain scope");
  }

  const queues = await Promise.all(
    input.queues.map(async (queue) => ({
      family: queue.family,
      name: queue.name,
      shardIndex: queue.shardIndex,
      counts: normalizeCounts(
        await queue.getJobCounts(...ANALYTICS_QUEUE_DRAIN_EVIDENCE_JOB_TYPES),
      ),
    })),
  );
  const pendingJobs = pendingJobsInQueues(queues);
  const unsignedEvidence = {
    backend: input.scope.backend,
    deploymentGeneration: input.scope.deploymentGeneration.toString(),
    workloadEpochFingerprint: input.scope.workloadEpochFingerprint,
    empty: pendingJobs === 0,
    pendingJobs,
    queueNamespaceFingerprint: input.queueNamespaceFingerprint,
    queues,
  } as const;

  return {
    ...unsignedEvidence,
    evidenceDigest: digestAnalyticsQueueDrainEvidence(unsignedEvidence),
  };
}

/**
 * Builds read-only BullMQ Queue inspectors. Business queue singletons are not
 * used because some of them register repeat jobs as a getInstance side effect.
 */
export function createBullMQAnalyticsQueueDrainInspectionSession(
  inventory: readonly CommunityAnalyticsQueueInventoryEntry[],
): AnalyticsQueueDrainInspectionSession | null {
  assertInventoryEntries(inventory);
  const redisOptions = createAnalyticsQueuePublisherOptionsWithRedis(
    inventory[0]!.name,
  );
  if (!redisOptions) return null;

  const bullQueues: Queue[] = [];
  try {
    for (const descriptor of inventory) {
      const bullQueue = new Queue(descriptor.name, {
        connection: redisOptions.connection,
        prefix: getQueuePrefix(descriptor.name),
        ...(redisOptions.skipVersionCheck === undefined
          ? {}
          : { skipVersionCheck: redisOptions.skipVersionCheck }),
      });
      bullQueue.on("error", (error) => {
        logger.error(
          `Analytics queue drain inspector ${descriptor.name} error`,
          redisErrorForLogging(error),
        );
      });
      bullQueues.push(bullQueue);
    }
  } catch (error) {
    redisOptions.connection.disconnect();
    throw error;
  }

  let closed = false;
  return {
    queues: inventory.map((descriptor, index) => ({
      ...descriptor,
      getJobCounts: (...types) => bullQueues[index]!.getJobCounts(...types),
    })),
    close: async () => {
      if (closed) return;
      closed = true;
      const results = await Promise.allSettled(
        bullQueues.map((queue) => queue.close()),
      );
      redisOptions.connection.disconnect();
      const rejection = results.find(
        (result): result is PromiseRejectedResult =>
          result.status === "rejected",
      );
      if (rejection) throw rejection.reason;
    },
  };
}

export function createAnalyticsScoreDeletionDrainProbe(
  dependencies: {
    readonly getInventory?: () => readonly CommunityAnalyticsQueueInventoryEntry[];
    readonly createInspectionSession?: (
      inventory: readonly CommunityAnalyticsQueueInventoryEntry[],
    ) => AnalyticsQueueDrainInspectionSession | null;
    readonly getQueueNamespaceFingerprint?: () => string;
  } = {},
): {
  readonly verify: (
    scope: AnalyticsScoreDeletionQueueDrainScope,
  ) => Promise<AnalyticsScoreDeletionQueueDrainEvidence>;
  readonly close: () => Promise<void>;
} {
  const getInventory =
    dependencies.getInventory ?? configuredCommunityAnalyticsQueueInventory;
  const createInspectionSession =
    dependencies.createInspectionSession ??
    createBullMQAnalyticsQueueDrainInspectionSession;
  const getQueueNamespaceFingerprint =
    dependencies.getQueueNamespaceFingerprint ??
    fingerprintConfiguredAnalyticsQueueNamespace;
  let inspectionSession:
    | AnalyticsQueueDrainInspectionSession
    | null
    | undefined;
  let openedInventory:
    | readonly CommunityAnalyticsQueueInventoryEntry[]
    | undefined;

  return {
    verify: async (scope) => {
      const inventory = getInventory();
      assertInventoryEntries(inventory);
      if (inspectionSession === undefined) {
        openedInventory = inventory;
        inspectionSession = createInspectionSession(inventory);
      } else {
        assertInventoryEntries(inventory, openedInventory);
      }
      if (inspectionSession) {
        assertInventoryEntries(inspectionSession.queues, inventory);
      }
      return probeAnalyticsScoreDeletionQueues({
        queues: inspectionSession?.queues ?? null,
        queueNamespaceFingerprint: getQueueNamespaceFingerprint(),
        scope,
      });
    },
    close: async () => {
      const currentSession = inspectionSession;
      inspectionSession = undefined;
      openedInventory = undefined;
      await currentSession?.close();
    },
  };
}
