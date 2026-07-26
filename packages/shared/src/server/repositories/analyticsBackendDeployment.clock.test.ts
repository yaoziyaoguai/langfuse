import { createHash } from "node:crypto";

import { describe, expect, it, vi } from "vitest";
import { QueueName } from "../queues";
import { createEmptyAnalyticsQueueDrainEvidence } from "../redis/analyticsQueueDrain.test-helper";

vi.mock("../../db", () => ({ prisma: {} }));

import {
  adoptExistingAnalyticsBackend,
  digestAnalyticsRuntimeInventory,
  resolveAnalyticsBackendStartup,
  switchAnalyticsBackend,
} from "./analyticsBackendDeployment";

const digest = (value: string): string => value.repeat(64);

describe("analytics backend deployment database clock", () => {
  it("requires an explicit fresh-install attestation before creating generation one", async () => {
    const createMarker = vi.fn().mockResolvedValue({
      id: "global",
      backend: "DORIS",
      generation: 1n,
      workloadEpochFingerprint: digest("a"),
      foundationContractVersion: 1,
    });
    const transaction = {
      $queryRaw: vi
        .fn()
        .mockResolvedValueOnce([])
        .mockResolvedValueOnce([])
        .mockResolvedValue([{ now: new Date("2040-01-01T00:00:00.000Z") }]),
      analyticsBackendDeploymentState: {
        findUnique: vi.fn().mockResolvedValue(null),
        create: createMarker,
      },
      analyticsIngestionOperation: { count: vi.fn().mockResolvedValue(0) },
      analyticsDeletionOperation: { count: vi.fn().mockResolvedValue(0) },
      analyticsEntityHead: { count: vi.fn().mockResolvedValue(0) },
      analyticsCheckpointGeneration: { count: vi.fn().mockResolvedValue(0) },
      analyticsRetentionRun: { count: vi.fn().mockResolvedValue(0) },
      batchExport: { count: vi.fn().mockResolvedValue(0) },
      jobExecution: { count: vi.fn().mockResolvedValue(0) },
      analyticsIntegrationState: { count: vi.fn().mockResolvedValue(0) },
      analyticsIntegrationPendingDelivery: {
        count: vi.fn().mockResolvedValue(0),
      },
      analyticsIntegrationExecution: { count: vi.fn().mockResolvedValue(0) },
      analyticsCapabilityActivation: {
        updateMany: vi.fn().mockResolvedValue({ count: 6 }),
      },
      analyticsBackendDeploymentTransition: {
        create: vi.fn().mockResolvedValue({}),
      },
    };
    const client = {
      $transaction: vi.fn(
        async (callback: (value: typeof transaction) => Promise<unknown>) =>
          callback(transaction),
      ),
    };

    await expect(
      resolveAnalyticsBackendStartup({
        client: client as never,
        backend: "doris",
        workloadEpochFingerprint: digest("a"),
        queueNamespaceFingerprint: digest("f"),
        foundationContractVersion: 1,
        allowFreshInitialization: false,
        freshDeploymentEvidence: {
          selectedBackendEmpty: true,
          evidenceDigest: digest("b"),
        },
      }),
    ).resolves.toEqual({ mode: "ADOPTION_REQUIRED" });
    expect(createMarker).not.toHaveBeenCalled();
  });

  it("derives adoption lease cutoffs from the locked transaction clock", async () => {
    const databaseNow = new Date("2040-01-01T00:00:00.000Z");
    const inventory = [
      { instanceId: "web-1", component: "web" as const },
      { instanceId: "worker-1", component: "worker" as const },
    ];
    const queueNamespaceFingerprint = digest("f");
    const claimCount = vi.fn().mockResolvedValue(0);
    const createMarker = vi.fn(async ({ data }) => ({
      ...data,
      updatedAt: data.createdAt,
    }));
    const transaction = {
      $queryRaw: vi.fn().mockResolvedValue([{ now: databaseNow }]),
      analyticsBackendDeploymentState: {
        findUnique: vi.fn().mockResolvedValue(null),
        create: createMarker,
      },
      analyticsRuntimeLease: {
        findMany: vi.fn().mockResolvedValue([
          {
            instanceId: "web-1",
            component: "WEB",
            backend: "CLICKHOUSE",
            workloadEpochFingerprint: digest("a"),
            queueNamespaceFingerprint,
            foundationContractVersion: 1,
            state: "QUIESCED",
          },
          {
            instanceId: "worker-1",
            component: "WORKER",
            backend: "CLICKHOUSE",
            workloadEpochFingerprint: digest("a"),
            queueNamespaceFingerprint,
            foundationContractVersion: 1,
            state: "QUIESCED",
          },
        ]),
      },
      analyticsBackendClaimLease: { count: claimCount },
      analyticsIngestionOperation: { count: vi.fn().mockResolvedValue(0) },
      analyticsIngestionOutboxV2: { count: vi.fn().mockResolvedValue(0) },
      analyticsLoadBatch: { count: vi.fn().mockResolvedValue(0) },
      analyticsDeletionOperation: { count: vi.fn().mockResolvedValue(0) },
      analyticsCheckpointGeneration: { count: vi.fn().mockResolvedValue(0) },
      analyticsRetentionRun: { count: vi.fn().mockResolvedValue(0) },
      batchAction: { count: vi.fn().mockResolvedValue(0) },
      batchExport: { count: vi.fn().mockResolvedValue(0) },
      jobExecution: { count: vi.fn().mockResolvedValue(0) },
      jobConfiguration: { count: vi.fn().mockResolvedValue(0) },
      analyticsIntegrationState: { count: vi.fn().mockResolvedValue(0) },
      analyticsIntegrationPendingDelivery: {
        count: vi.fn().mockResolvedValue(0),
      },
      analyticsIntegrationExecution: { count: vi.fn().mockResolvedValue(0) },
      analyticsBackendDeploymentTransition: {
        create: vi.fn().mockResolvedValue({}),
      },
    };
    const client = {
      $transaction: vi.fn(
        async (callback: (value: typeof transaction) => Promise<unknown>) =>
          callback(transaction),
      ),
    };
    const verifyScoreDeletionQueuesEmpty = vi.fn().mockResolvedValue(
      await createEmptyAnalyticsQueueDrainEvidence({
        scope: {
          backend: "clickhouse",
          deploymentGeneration: 0n,
          workloadEpochFingerprint: digest("a"),
        },
        queueNamespaceFingerprint,
      }),
    );

    await adoptExistingAnalyticsBackend({
      client: client as never,
      expectedBackend: "clickhouse",
      workloadEpochFingerprint: digest("a"),
      foundationContractVersion: 1,
      expectedInventory: inventory,
      expectedInventoryDigest: digestAnalyticsRuntimeInventory(inventory),
      verifyScoreDeletionQueuesEmpty,
      drainAttestationDigest: digest("b"),
      denyProbeAttestationDigest: digest("c"),
    });

    expect(claimCount).toHaveBeenCalledWith({
      where: {
        releasedAt: null,
        leaseExpiresAt: {
          gt: new Date(databaseNow.getTime() - 60_000),
        },
      },
    });
    expect(createMarker).toHaveBeenCalledWith({
      data: expect.objectContaining({ createdAt: databaseNow }),
    });
    expect(verifyScoreDeletionQueuesEmpty).toHaveBeenCalledWith({
      backend: "clickhouse",
      deploymentGeneration: 0n,
      workloadEpochFingerprint: digest("a"),
    });
    expect(transaction.$queryRaw).toHaveBeenCalledTimes(3);
  });

  it("samples the switch clock after the locked emptiness probe", async () => {
    const databaseNow = new Date("2040-02-01T00:00:00.000Z");
    const events: string[] = [];
    const inventory = [
      { instanceId: "web-1", component: "web" as const },
      { instanceId: "worker-1", component: "worker" as const },
    ];
    const marker = {
      id: "global",
      backend: "CLICKHOUSE",
      generation: 7n,
      workloadEpochFingerprint: digest("a"),
      queueNamespaceFingerprint: digest("7"),
      foundationContractVersion: 1,
    };
    const claimCount = vi.fn().mockResolvedValue(0);
    const createTransition = vi.fn().mockResolvedValue({});
    const transaction = {
      $queryRaw: vi.fn().mockImplementation(async () => {
        events.push("sql");
        return [{ now: databaseNow }];
      }),
      analyticsBackendDeploymentState: {
        findUnique: vi.fn().mockResolvedValue(marker),
        updateMany: vi.fn().mockResolvedValue({ count: 1 }),
        findUniqueOrThrow: vi.fn().mockResolvedValue({
          ...marker,
          backend: "DORIS",
          generation: 8n,
          workloadEpochFingerprint: digest("b"),
        }),
      },
      analyticsRuntimeLease: {
        count: vi.fn().mockResolvedValue(0),
        findMany: vi.fn().mockResolvedValue([
          {
            instanceId: "web-1",
            component: "WEB",
            state: "QUIESCED",
            queueNamespaceFingerprint: marker.queueNamespaceFingerprint,
          },
          {
            instanceId: "worker-1",
            component: "WORKER",
            state: "QUIESCED",
            queueNamespaceFingerprint: marker.queueNamespaceFingerprint,
          },
        ]),
      },
      analyticsBackendClaimLease: { count: claimCount },
      analyticsIngestionOperation: { count: vi.fn().mockResolvedValue(0) },
      analyticsIngestionOutboxV2: { count: vi.fn().mockResolvedValue(0) },
      analyticsLoadBatch: { count: vi.fn().mockResolvedValue(0) },
      analyticsDeletionOperation: { count: vi.fn().mockResolvedValue(0) },
      analyticsEntityHead: { count: vi.fn().mockResolvedValue(0) },
      analyticsCheckpointGeneration: { count: vi.fn().mockResolvedValue(0) },
      analyticsRetentionRun: { count: vi.fn().mockResolvedValue(0) },
      batchAction: { count: vi.fn().mockResolvedValue(0) },
      batchExport: { count: vi.fn().mockResolvedValue(0) },
      jobExecution: { count: vi.fn().mockResolvedValue(0) },
      jobConfiguration: { count: vi.fn().mockResolvedValue(0) },
      analyticsIntegrationState: { count: vi.fn().mockResolvedValue(0) },
      analyticsIntegrationPendingDelivery: {
        count: vi.fn().mockResolvedValue(0),
      },
      analyticsIntegrationExecution: { count: vi.fn().mockResolvedValue(0) },
      analyticsCapabilityActivation: {
        count: vi.fn().mockResolvedValue(0),
        updateMany: vi.fn().mockResolvedValue({ count: 6 }),
      },
      analyticsBackendDeploymentTransition: {
        create: createTransition,
      },
    };
    const client = {
      $transaction: vi.fn(
        async (callback: (value: typeof transaction) => Promise<unknown>) =>
          callback(transaction),
      ),
    };
    const verifyBackendEmptiness = vi.fn(async () => {
      events.push("probe");
      return {
        source: {
          backend: "clickhouse" as const,
          empty: true,
          evidenceDigest: digest("c"),
        },
        target: {
          backend: "doris" as const,
          empty: true,
          evidenceDigest: digest("d"),
        },
      };
    });
    const queueDrainEvidence = await createEmptyAnalyticsQueueDrainEvidence({
      scope: {
        backend: "clickhouse",
        deploymentGeneration: 7n,
        workloadEpochFingerprint: digest("a"),
      },
      queueNamespaceFingerprint: marker.queueNamespaceFingerprint,
    });
    const verifyScoreDeletionQueuesEmpty = vi.fn(async () => {
      events.push("queue-probe");
      return queueDrainEvidence;
    });

    await switchAnalyticsBackend({
      client: client as never,
      expectedBackend: "clickhouse",
      expectedGeneration: 7n,
      expectedWorkloadEpochFingerprint: digest("a"),
      targetBackend: "doris",
      targetWorkloadEpochFingerprint: digest("b"),
      targetFoundationContractVersion: 1,
      expectedQuiescedInventory: inventory,
      verifyBackendEmptiness,
      verifyScoreDeletionQueuesEmpty,
      externalDrainAttestationDigest: digest("e"),
      denyProbeAttestationDigest: digest("f"),
    });

    expect(events.slice(0, 4)).toEqual(["sql", "sql", "probe", "sql"]);
    expect(claimCount).toHaveBeenCalledWith({
      where: {
        releasedAt: null,
        leaseExpiresAt: {
          gt: new Date(databaseNow.getTime() - 60_000),
        },
      },
    });
    expect(verifyScoreDeletionQueuesEmpty).toHaveBeenCalledOnce();
    const expectedDrainEvidenceDigest = createHash("sha256")
      .update(
        JSON.stringify({
          attestation: digest("e"),
          counts: {
            ingestionOperations: 0,
            ingestionOutbox: 0,
            loadBatches: 0,
            deletionOperations: 0,
            checkpoints: 0,
            retentionRuns: 0,
            batchActions: 0,
            batchExports: 0,
            jobExecutions: 0,
            integrationDeliveries: 0,
            integrationExecutions: 0,
            integrationBootstraps: 0,
          },
          sourceEmptyEvidenceDigest: digest("c"),
          targetEmptyEvidenceDigest: digest("d"),
          scoreDeletionQueueDrainEvidenceDigest:
            queueDrainEvidence.evidenceDigest,
          queueNamespaceFingerprint: marker.queueNamespaceFingerprint,
          scoreDeletionQueuePendingJobs: 0,
        }),
      )
      .digest("hex");
    expect(createTransition).toHaveBeenCalledWith({
      data: expect.objectContaining({
        drainEvidenceDigest: expectedDrainEvidenceDigest,
      }),
    });

    verifyScoreDeletionQueuesEmpty.mockResolvedValueOnce({
      ...queueDrainEvidence,
      queues: [],
      evidenceDigest: digest("8"),
    });
    await expect(
      switchAnalyticsBackend({
        client: client as never,
        expectedBackend: "clickhouse",
        expectedGeneration: 7n,
        expectedWorkloadEpochFingerprint: digest("a"),
        targetBackend: "doris",
        targetWorkloadEpochFingerprint: digest("b"),
        targetFoundationContractVersion: 1,
        expectedQuiescedInventory: inventory,
        verifyBackendEmptiness,
        verifyScoreDeletionQueuesEmpty,
        externalDrainAttestationDigest: digest("e"),
        denyProbeAttestationDigest: digest("f"),
      }),
    ).rejects.toThrow(/inventory/i);

    verifyScoreDeletionQueuesEmpty.mockResolvedValueOnce(
      await createEmptyAnalyticsQueueDrainEvidence({
        scope: {
          backend: "clickhouse",
          deploymentGeneration: 7n,
          workloadEpochFingerprint: digest("a"),
        },
        queueNamespaceFingerprint: marker.queueNamespaceFingerprint,
        countsByQueue: {
          [QueueName.ScoreDelete]: { waiting: 1 },
        },
      }),
    );
    await expect(
      switchAnalyticsBackend({
        client: client as never,
        expectedBackend: "clickhouse",
        expectedGeneration: 7n,
        expectedWorkloadEpochFingerprint: digest("a"),
        targetBackend: "doris",
        targetWorkloadEpochFingerprint: digest("b"),
        targetFoundationContractVersion: 1,
        expectedQuiescedInventory: inventory,
        verifyBackendEmptiness,
        verifyScoreDeletionQueuesEmpty,
        externalDrainAttestationDigest: digest("e"),
        denyProbeAttestationDigest: digest("f"),
      }),
    ).rejects.toThrow(/pending analytics queue work/i);
    expect(
      transaction.analyticsBackendDeploymentState.updateMany,
    ).toHaveBeenCalledOnce();
  });
});
