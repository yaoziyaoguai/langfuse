import { Prisma, PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)("analytics checkpoints", () => {
  let checkpoints: typeof import("./analyticsCheckpoints.js");
  let ingestion: typeof import("./analyticsIngestionOperations.js");
  let deletion: typeof import("./analyticsDeletionOperations.js");
  const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const organizationId = `checkpoint-org-${suffix}`;
  const projectId = `checkpoint-project-${suffix}`;
  const createdCheckpointGenerations: bigint[] = [];

  beforeAll(async () => {
    checkpoints = await import("./analyticsCheckpoints.js");
    ingestion = await import("./analyticsIngestionOperations.js");
    deletion = await import("./analyticsDeletionOperations.js");
    await prisma.organization.create({
      data: { id: organizationId, name: "Doris checkpoint test" },
    });
    await prisma.project.create({
      data: {
        id: projectId,
        name: "Doris checkpoint test",
        orgId: organizationId,
      },
    });
  }, 120_000);

  afterAll(async () => {
    if (createdCheckpointGenerations.length > 0) {
      await prisma.analyticsCheckpointGeneration.deleteMany({
        where: { generation: { in: createdCheckpointGenerations } },
      });
    }
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.analyticsBackendClaimLease.deleteMany();
    await prisma.analyticsRuntimeCapabilityContract.deleteMany();
    await prisma.analyticsRuntimeLease.deleteMany();
    await prisma.analyticsBackendDeploymentTransition.deleteMany();
    await prisma.analyticsBackendDeploymentState.deleteMany();
    await prisma.$disconnect();
  }, 30_000);

  it("fences post-cut mutation dispatch while pre-cut work drains", async () => {
    const acceptedAt = new Date("2026-07-18T12:00:00.000Z");
    const acceptedAtNanos = 1_784_376_000_000_000_000n;
    const preCutOperationId = `checkpoint-pre-${suffix}`;
    const preCutLoadId = `checkpoint-load-${suffix}`;
    await ingestion.createAnalyticsIngestionReceipt({
      client: prisma,
      operationId: preCutOperationId,
      projectId,
      sourceOperationId: `checkpoint-pre-source-${suffix}`,
      sourceChecksum: "a".repeat(64),
      rawObjectKey: `events/${projectId}/raw/pre.json`,
      acceptedAt,
      acceptedAtNanos,
      canonicalizerVersion: "1",
      schemaVersion: 1,
      recoverableUntil: new Date("2026-07-25T12:00:00.000Z"),
      statusExpiresAt: new Date("2026-08-24T12:00:00.000Z"),
    });
    await prisma.analyticsLoadBatch.create({
      data: {
        id: preCutLoadId,
        operationId: preCutOperationId,
        projectId,
        databaseName: "langfuse",
        targetTable: "events_current",
        logicalBatchId: `checkpoint-logical-${suffix}`,
        fenceGeneration: 1n,
        label: `checkpoint_label_${suffix}`.replaceAll("-", "_").slice(0, 128),
        payloadHash: "b".repeat(64),
        canonicalObjectKey: `events/${projectId}/canonical/pre.json`,
      },
    });

    const now = new Date("2026-07-18T12:01:00.000Z");
    const checkpoint = await checkpoints.beginAnalyticsCheckpoint({
      client: prisma,
      leaseOwner: `checkpoint-worker-${suffix}`,
      leaseMs: 60_000,
      now,
    });
    createdCheckpointGenerations.push(checkpoint.generation);
    expect(checkpoint).toMatchObject({
      status: "PREPARING",
      operationHighWatermarkAcceptedAtNanos: acceptedAtNanos,
    });

    await expect(
      checkpoints.beginAnalyticsCheckpoint({
        client: prisma,
        leaseOwner: `other-worker-${suffix}`,
        leaseMs: 60_000,
        now: new Date(now.getTime() + 1_000),
      }),
    ).rejects.toThrow("Analytics checkpoint is already active");

    const postCutOperationId = `checkpoint-post-${suffix}`;
    const postCutReceipt = await ingestion.createAnalyticsIngestionReceipt({
      client: prisma,
      operationId: postCutOperationId,
      projectId,
      sourceOperationId: `checkpoint-post-source-${suffix}`,
      sourceChecksum: "c".repeat(64),
      rawObjectKey: `events/${projectId}/raw/post.json`,
      acceptedAt: new Date("2026-07-18T12:01:01.000Z"),
      acceptedAtNanos: 1_784_376_061_000_000_000n,
      canonicalizerVersion: "1",
      schemaVersion: 1,
      recoverableUntil: new Date("2026-07-25T12:01:01.000Z"),
      statusExpiresAt: new Date("2026-08-24T12:01:01.000Z"),
    });
    expect(postCutReceipt.operation.checkpointGeneration).toBe(
      checkpoint.generation,
    );

    await expect(
      checkpoints.acquireAnalyticsMutationPermit({
        client: prisma,
        mutation: {
          kind: "ingestion",
          checkpointGeneration: 0n,
          operationAcceptedAtNanos: acceptedAtNanos,
        },
        now: new Date(now.getTime() + 2_000),
      }),
    ).resolves.toMatchObject({ outcome: "allowed" });
    await expect(
      checkpoints.acquireAnalyticsMutationPermit({
        client: prisma,
        mutation: {
          kind: "ingestion",
          checkpointGeneration: checkpoint.generation,
          operationAcceptedAtNanos: 1_784_376_061_000_000_000n,
        },
        now: new Date(now.getTime() + 2_000),
      }),
    ).resolves.toMatchObject({
      outcome: "held",
      checkpointGeneration: checkpoint.generation,
    });

    const scheduled = await deletion.scheduleTraceDeletionOperations({
      client: prisma,
      projectId,
      organizationId,
      traceIds: [`checkpoint-trace-${suffix}`],
      requester: { principalType: "system", principalId: "checkpoint-test" },
      now: new Date(now.getTime() + 3_000),
    });
    expect(scheduled[0]?.operation.checkpointGeneration).toBe(
      checkpoint.generation,
    );
    await expect(
      checkpoints.acquireAnalyticsMutationPermit({
        client: prisma,
        mutation: {
          kind: "deletion",
          checkpointGeneration: checkpoint.generation,
          createdAt: scheduled[0]!.operation.createdAt,
        },
        now: new Date(now.getTime() + 4_000),
      }),
    ).resolves.toMatchObject({ outcome: "held" });

    await expect(
      checkpoints.getAnalyticsCheckpointDrainState({
        client: prisma,
        generation: checkpoint.generation,
      }),
    ).resolves.toMatchObject({
      nonterminalOperations: 1,
      nonterminalLoads: 1,
      nonterminalDeletions: 0,
      drained: false,
    });

    await prisma.analyticsLoadBatch.update({
      where: { id: preCutLoadId },
      data: { status: "VISIBLE", visibleAt: new Date(now.getTime() + 5_000) },
    });
    await prisma.analyticsIngestionOperation.update({
      where: { id: preCutOperationId },
      data: {
        status: "VISIBLE",
        visibleAt: new Date(now.getTime() + 5_000),
        terminalAt: new Date(now.getTime() + 5_000),
      },
    });

    await expect(
      checkpoints.getAnalyticsCheckpointDrainState({
        client: prisma,
        generation: checkpoint.generation,
      }),
    ).resolves.toMatchObject({
      nonterminalOperations: 0,
      nonterminalLoads: 0,
      nonterminalDeletions: 0,
      drained: true,
    });

    await expect(
      checkpoints.abortAnalyticsCheckpoint({
        client: prisma,
        generation: checkpoint.generation,
        leaseOwner: checkpoint.leaseOwner,
        reasonCode: "TEST_COMPLETE",
        now: new Date(now.getTime() + 6_000),
      }),
    ).resolves.toBe(true);
  });

  it("keeps deployment ahead of checkpoint locks when a switch is queued", async () => {
    const deployment = await import("./analyticsBackendDeployment.js");
    const generation = BigInt(Date.now()) * 1_000n + 17n;
    const leaseOwner = `checkpoint-lock-order-${suffix}`;
    await prisma.analyticsCheckpointGeneration.create({
      data: {
        generation,
        status: "PREPARING",
        leaseOwner,
        leaseExpiresAt: new Date(Date.now() + 120_000),
        operationHighWatermarkAcceptedAt: new Date(0),
        operationHighWatermarkAcceptedAtNanos: 0n,
        loadHighWatermarkCreatedAt: new Date(0),
        deletionHighWatermarkCreatedAt: new Date(0),
      },
    });

    let deploymentHeld!: () => void;
    const deploymentHeldPromise = new Promise<void>((resolve) => {
      deploymentHeld = resolve;
    });
    let attemptCheckpoint!: () => void;
    const attemptCheckpointPromise = new Promise<void>((resolve) => {
      attemptCheckpoint = resolve;
    });
    const holder = prisma.$transaction(async (transaction) => {
      await deployment.acquireAnalyticsDeploymentSharedLock(transaction);
      deploymentHeld();
      await attemptCheckpointPromise;
      await checkpoints.acquireAnalyticsCheckpointTransactionLock(transaction);
    });

    const waitForAdvisoryWaiters = async (minimum: number) => {
      for (let attempt = 0; attempt < 100; attempt += 1) {
        const [row] = await prisma.$queryRaw<readonly { count: number }[]>(
          Prisma.sql`SELECT COUNT(*)::int AS count
                     FROM pg_locks
                     WHERE locktype = 'advisory'
                       AND granted = false
                       AND database = (
                         SELECT oid FROM pg_database
                         WHERE datname = current_database()
                       )`,
        );
        if ((row?.count ?? 0) >= minimum) return;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }
      throw new Error("Timed out waiting for advisory lock contention");
    };

    let switcher: Promise<void> | undefined;
    let capture: Promise<string> | undefined;
    try {
      await deploymentHeldPromise;
      switcher = prisma.$transaction(async (transaction) => {
        await deployment.acquireAnalyticsDeploymentExclusiveLock(transaction);
      });
      await waitForAdvisoryWaiters(1);
      capture = checkpoints.withAnalyticsCheckpointIoFence({
        client: prisma,
        generation,
        leaseOwner,
        transactionTimeoutMs: 10_000,
        execute: async () => "captured",
      });
      await waitForAdvisoryWaiters(2);
      attemptCheckpoint();

      await expect(Promise.all([holder, switcher, capture])).resolves.toEqual([
        undefined,
        undefined,
        "captured",
      ]);
    } finally {
      attemptCheckpoint();
      const pendingOperations: Promise<unknown>[] = [holder];
      if (switcher) pendingOperations.push(switcher);
      if (capture) pendingOperations.push(capture);
      await Promise.allSettled(pendingOperations);
      await prisma.analyticsCheckpointGeneration.deleteMany({
        where: { generation },
      });
    }
  });

  it("keeps one shared fence across every artifact capture", async () => {
    const generation = BigInt(Date.now()) * 1_000n + 23n;
    const leaseOwner = `checkpoint-artifact-fence-${suffix}`;
    await prisma.analyticsCheckpointGeneration.create({
      data: {
        generation,
        status: "PREPARING",
        leaseOwner,
        leaseExpiresAt: new Date(Date.now() + 120_000),
        operationHighWatermarkAcceptedAt: new Date(0),
        operationHighWatermarkAcceptedAtNanos: 0n,
        loadHighWatermarkCreatedAt: new Date(0),
        deletionHighWatermarkCreatedAt: new Date(0),
      },
    });

    let firstCaptureStarted!: () => void;
    const firstCaptureStartedPromise = new Promise<void>((resolve) => {
      firstCaptureStarted = resolve;
    });
    let continueCaptures!: () => void;
    const continueCapturesPromise = new Promise<void>((resolve) => {
      continueCaptures = resolve;
    });
    const order: string[] = [];
    const capture = checkpoints.withAnalyticsCheckpointIoFence({
      client: prisma,
      generation,
      leaseOwner,
      transactionTimeoutMs: 10_000,
      execute: async () => {
        order.push("postgres");
        firstCaptureStarted();
        await continueCapturesPromise;
        order.push("doris");
        await Promise.resolve();
        order.push("lifecycle");
      },
    });

    let retention: Promise<unknown> | undefined;
    try {
      await firstCaptureStartedPromise;
      retention = prisma.$transaction(async (transaction) => {
        const permit =
          await checkpoints.acquireAnalyticsRetentionMutationPermit({
            transaction,
          });
        order.push("retention");
        return permit;
      });
      for (let attempt = 0; attempt < 100; attempt += 1) {
        const [row] = await prisma.$queryRaw<readonly { count: number }[]>(
          Prisma.sql`SELECT COUNT(*)::int AS count
                     FROM pg_locks
                     WHERE locktype = 'advisory'
                       AND granted = false
                       AND database = (
                         SELECT oid FROM pg_database
                         WHERE datname = current_database()
                       )`,
        );
        if ((row?.count ?? 0) >= 1) break;
        if (attempt === 99) {
          throw new Error("Timed out waiting for retention fence contention");
        }
        await new Promise((resolve) => setTimeout(resolve, 10));
      }

      expect(order).toEqual(["postgres"]);
      continueCaptures();
      await expect(capture).resolves.toBeUndefined();
      await expect(retention).resolves.toMatchObject({
        outcome: "held",
        checkpointGeneration: generation,
      });
      expect(order).toEqual(["postgres", "doris", "lifecycle", "retention"]);
    } finally {
      continueCaptures();
      await Promise.allSettled([capture, ...(retention ? [retention] : [])]);
      await prisma.analyticsCheckpointGeneration.deleteMany({
        where: { generation },
      });
    }
  });

  it("preserves the original producer across recovery and fences stale-generation IO", async () => {
    await prisma.analyticsLoadBatch.deleteMany({ where: { projectId } });
    await prisma.analyticsIngestionOutboxV2.deleteMany({
      where: { operation: { projectId } },
    });
    await prisma.analyticsIngestionOperation.deleteMany({
      where: { projectId },
    });
    await prisma.analyticsDeletionOperation.deleteMany({
      where: { projectId },
    });
    await prisma.analyticsDeletionTombstone.deleteMany({
      where: { projectId },
    });
    await prisma.analyticsCheckpointGeneration.deleteMany({
      where: { generation: { in: createdCheckpointGenerations } },
    });
    createdCheckpointGenerations.length = 0;

    const deployment = await import("./analyticsBackendDeployment.js");
    const leases = await import("./analyticsRuntimeLeases.js");
    const now = new Date();
    const workloadEpochFingerprint =
      deployment.fingerprintAnalyticsWorkloadEpoch(
        `checkpoint-provenance-${suffix}`,
      );
    const startup = await deployment.resolveAnalyticsBackendStartup({
      client: prisma,
      backend: "doris",
      workloadEpochFingerprint,
      queueNamespaceFingerprint: "f".repeat(64),
      foundationContractVersion: 1,
      allowFreshInitialization: true,
      freshDeploymentEvidence: {
        selectedBackendEmpty: true,
        evidenceDigest: "e".repeat(64),
      },
      now,
    });
    if (startup.mode !== "READY") throw new Error("Expected fresh marker");
    const register = (instanceId: string) =>
      leases.registerAnalyticsRuntimeLease({
        client: prisma,
        component: "worker",
        instanceId,
        backend: "doris",
        deploymentGeneration: startup.marker.generation,
        workloadEpochFingerprint,
        queueNamespaceFingerprint: "f".repeat(64),
        buildId: "u0-checkpoint-provenance",
        foundationContractVersion: 1,
        acceptedSchemaVersion: { min: 1, max: 1 },
        acceptedCanonicalVersion: { min: 1, max: 1 },
        capabilityContracts: [],
        leaseMs: 120_000,
        now,
      });
    const producer = await register(`checkpoint-producer-${suffix}`);
    const recovery = await register(`checkpoint-recovery-${suffix}`);
    await expect(
      checkpoints.beginAnalyticsCheckpoint({
        client: prisma,
        leaseOwner: `unstamped-checkpoint-${suffix}`,
        leaseMs: 60_000,
        now,
      }),
    ).rejects.toThrow("Legacy unstamped analytics work is fenced");
    const producerContext = {
      runtimeLeaseId: producer.lease.id,
      backend: "doris" as const,
      deploymentGeneration: startup.marker.generation,
    };
    const checkpoint = await checkpoints.beginAnalyticsCheckpoint({
      client: prisma,
      leaseOwner: `checkpoint-producer-${suffix}`,
      leaseMs: 60_000,
      admissionContext: producerContext,
      now,
    });
    createdCheckpointGenerations.push(checkpoint.generation);
    expect(checkpoint).toMatchObject({
      analyticsBackend: "DORIS",
      deploymentGeneration: startup.marker.generation,
      workloadEpochFingerprint,
      runtimeContractVersion: 1,
      producerRuntimeLeaseId: producer.lease.id,
    });

    const analyticsProvenance = {
      analyticsBackend: "DORIS",
      deploymentGeneration: startup.marker.generation.toString(),
      workloadEpochFingerprint,
      runtimeContractVersion: 1,
      producerRuntimeLeaseId: producer.lease.id,
    };
    await expect(
      checkpoints.recordAnalyticsCheckpointArtifacts({
        client: prisma,
        generation: checkpoint.generation,
        leaseOwner: checkpoint.leaseOwner,
        postgresSnapshotId: "postgres-snapshot",
        postgresWalLsn: "0/1",
        dorisSnapshotId: "doris-snapshot",
        artifactDigests: { postgres: "a".repeat(64) },
        manifest: {
          version: 1,
          analyticsProvenance: {
            ...analyticsProvenance,
            producerRuntimeLeaseId: recovery.lease.id,
          },
        },
        keyId: "checkpoint-key",
        manifestHash: "b".repeat(64),
        signature: "signature",
        admissionContext: producerContext,
        now: new Date(now.getTime() + 500),
      }),
    ).rejects.toThrow("manifest provenance changed");
    await expect(
      prisma.analyticsCheckpointGeneration.findUniqueOrThrow({
        where: { generation: checkpoint.generation },
      }),
    ).resolves.toMatchObject({
      producerRuntimeLeaseId: producer.lease.id,
      manifest: null,
    });
    await expect(
      checkpoints.recordAnalyticsCheckpointArtifacts({
        client: prisma,
        generation: checkpoint.generation,
        leaseOwner: checkpoint.leaseOwner,
        postgresSnapshotId: "postgres-snapshot",
        postgresWalLsn: "0/1",
        dorisSnapshotId: "doris-snapshot",
        artifactDigests: { postgres: "a".repeat(64) },
        manifest: { version: 1, analyticsProvenance },
        keyId: "checkpoint-key",
        manifestHash: "b".repeat(64),
        signature: "signature",
        admissionContext: producerContext,
        now: new Date(now.getTime() + 1_000),
      }),
    ).resolves.toBe(true);

    const recoveryContext = {
      runtimeLeaseId: recovery.lease.id,
      backend: "doris" as const,
      deploymentGeneration: startup.marker.generation,
    };
    await prisma.analyticsCheckpointGeneration.update({
      where: { generation: checkpoint.generation },
      data: { leaseExpiresAt: new Date(0) },
    });
    const claimed =
      await checkpoints.claimAnalyticsCheckpointAnchorReconciliation({
        client: prisma,
        generation: checkpoint.generation,
        leaseOwner: `checkpoint-recovery-owner-${suffix}`,
        leaseMs: 60_000,
        admissionContext: recoveryContext,
      });
    expect(claimed).toMatchObject({
      leaseOwner: `checkpoint-recovery-owner-${suffix}`,
      producerRuntimeLeaseId: producer.lease.id,
    });

    await prisma.analyticsBackendDeploymentState.update({
      where: { id: "global" },
      data: { generation: { increment: 1 } },
    });
    const externalIo = vi.fn().mockResolvedValue("not-run");
    await expect(
      checkpoints.withAnalyticsCheckpointIoFence({
        client: prisma,
        generation: checkpoint.generation,
        leaseOwner: `checkpoint-recovery-owner-${suffix}`,
        admissionContext: recoveryContext,
        transactionTimeoutMs: 10_000,
        execute: externalIo,
      }),
    ).rejects.toThrow("deployment generation changed");
    expect(externalIo).not.toHaveBeenCalled();
    await expect(
      prisma.analyticsCheckpointGeneration.findUniqueOrThrow({
        where: { generation: checkpoint.generation },
      }),
    ).resolves.toMatchObject({
      producerRuntimeLeaseId: producer.lease.id,
    });
  });
});
