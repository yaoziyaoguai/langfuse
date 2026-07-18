import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)("analytics ingestion operations", () => {
  let repository: typeof import("./analyticsIngestionOperations.js");
  let loadRepository: typeof import("./analyticsLoadBatches.js");
  const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const organizationId = `ingestion-org-${suffix}`;
  const projectId = `ingestion-project-${suffix}`;

  beforeAll(async () => {
    repository = await import("./analyticsIngestionOperations.js");
    loadRepository = await import("./analyticsLoadBatches.js");
    await prisma.organization.create({
      data: { id: organizationId, name: "Doris ingestion operation test" },
    });
    await prisma.project.create({
      data: {
        id: projectId,
        name: "Doris ingestion operation test",
        orgId: organizationId,
      },
    });
  }, 30_000);

  afterAll(async () => {
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.$disconnect();
  }, 30_000);

  it("atomically creates one receipt and outbox row per source operation", async () => {
    const input = {
      client: prisma,
      operationId: `operation-${suffix}`,
      projectId,
      sourceOperationId: `source-${suffix}`,
      sourceChecksum: "a".repeat(64),
      rawObjectKey: `events/${projectId}/raw/source.json`,
      acceptedAt: new Date("2026-07-18T12:00:00.000Z"),
      acceptedAtNanos: 1_784_376_000_000_000_000n,
      canonicalizerVersion: "1",
      schemaVersion: 3,
      recoverableUntil: new Date("2026-07-25T12:00:00.000Z"),
      statusExpiresAt: new Date("2026-08-24T12:00:00.000Z"),
    };

    const [first, replay] = await Promise.all([
      repository.createAnalyticsIngestionReceipt(input),
      repository.createAnalyticsIngestionReceipt(input),
    ]);

    expect(first.operation.id).toBe(input.operationId);
    expect(replay.operation.id).toBe(input.operationId);
    expect(first.created || replay.created).toBe(true);
    await expect(
      prisma.analyticsIngestionOutbox.count({
        where: { operationId: input.operationId },
      }),
    ).resolves.toBe(1);

    const claimed = await repository.claimAnalyticsIngestionOutbox({
      client: prisma,
      workerId: "outbox-worker-a",
      now: new Date("2026-07-18T12:00:01.000Z"),
      lockedUntil: new Date("2026-07-18T12:01:01.000Z"),
      limit: 10,
    });
    expect(claimed).toMatchObject([{ operationId: input.operationId }]);
    await expect(
      repository.claimAnalyticsIngestionOutbox({
        client: prisma,
        workerId: "outbox-worker-b",
        now: new Date("2026-07-18T12:00:02.000Z"),
        lockedUntil: new Date("2026-07-18T12:01:02.000Z"),
        limit: 10,
      }),
    ).resolves.toEqual([]);
    await expect(
      repository.markAnalyticsIngestionOutboxPublished({
        client: prisma,
        operationId: input.operationId,
        workerId: "outbox-worker-a",
        now: new Date("2026-07-18T12:00:03.000Z"),
      }),
    ).resolves.toBe(true);

    await expect(
      repository.createAnalyticsIngestionReceipt({
        ...input,
        sourceChecksum: "b".repeat(64),
      }),
    ).rejects.toThrow("Ingestion source operation conflicts with its receipt");
    await expect(
      repository.createAnalyticsIngestionReceipt({
        ...input,
        acceptedAtNanos: input.acceptedAtNanos + 1n,
      }),
    ).rejects.toThrow("Ingestion source operation conflicts with its receipt");
  });

  it("requires absence reconciliation before an expired lease can advance its fence", async () => {
    const operationId = `takeover-operation-${suffix}`;
    await repository.createAnalyticsIngestionReceipt({
      client: prisma,
      operationId,
      projectId,
      sourceOperationId: `takeover-source-${suffix}`,
      sourceChecksum: "c".repeat(64),
      rawObjectKey: `events/${projectId}/raw/takeover.json`,
      acceptedAt: new Date("2026-07-18T12:00:00.000Z"),
      acceptedAtNanos: 1_784_376_000_000_000_000n,
      canonicalizerVersion: "1",
      schemaVersion: 3,
      recoverableUntil: new Date("2026-07-25T12:00:00.000Z"),
      statusExpiresAt: new Date("2026-08-24T12:00:00.000Z"),
    });

    const first = await repository.reserveCanonicalizationFence({
      client: prisma,
      operationId,
      projectId,
      expectedFence: 0n,
      nextFence: 1n,
      leaseOwner: "worker-a",
      leaseUntil: new Date("2026-07-18T12:01:00.000Z"),
      now: new Date("2026-07-18T12:00:00.000Z"),
      reservedObjectKey: `canonical/${operationId}/fence-1.json`,
      confirmedAbsentObjectKey: null,
    });
    expect(first).toMatchObject({ outcome: "reserved", fence: 1n });

    await expect(
      repository.reserveCanonicalizationFence({
        client: prisma,
        operationId,
        projectId,
        expectedFence: 1n,
        nextFence: 2n,
        leaseOwner: "worker-b",
        leaseUntil: new Date("2026-07-18T12:03:00.000Z"),
        now: new Date("2026-07-18T12:02:00.000Z"),
        reservedObjectKey: `canonical/${operationId}/fence-2.json`,
        confirmedAbsentObjectKey: null,
      }),
    ).resolves.toMatchObject({ outcome: "reconciliation_required" });

    await expect(
      repository.reserveCanonicalizationFence({
        client: prisma,
        operationId,
        projectId,
        expectedFence: 1n,
        nextFence: 2n,
        leaseOwner: "worker-b",
        leaseUntil: new Date("2026-07-18T12:03:00.000Z"),
        now: new Date("2026-07-18T12:02:00.000Z"),
        reservedObjectKey: `canonical/${operationId}/fence-2.json`,
        confirmedAbsentObjectKey: `canonical/${operationId}/fence-1.json`,
      }),
    ).resolves.toMatchObject({ outcome: "reserved", fence: 2n });
  });

  it("publishes the canonical pointer and complete candidate manifest atomically", async () => {
    const operationId = `publish-operation-${suffix}`;
    const reservedObjectKey = `canonical/${operationId}/fence-1.json`;
    await repository.createAnalyticsIngestionReceipt({
      client: prisma,
      operationId,
      projectId,
      sourceOperationId: `publish-source-${suffix}`,
      sourceChecksum: "d".repeat(64),
      rawObjectKey: `events/${projectId}/raw/publish.json`,
      acceptedAt: new Date("2026-07-18T12:00:00.000Z"),
      acceptedAtNanos: 1_784_376_000_000_000_000n,
      canonicalizerVersion: "1",
      schemaVersion: 3,
      recoverableUntil: new Date("2026-07-25T12:00:00.000Z"),
      statusExpiresAt: new Date("2026-08-24T12:00:00.000Z"),
    });
    await repository.reserveCanonicalizationFence({
      client: prisma,
      operationId,
      projectId,
      expectedFence: 0n,
      nextFence: 1n,
      leaseOwner: "worker-a",
      leaseUntil: new Date("2026-07-18T12:01:00.000Z"),
      now: new Date("2026-07-18T12:00:00.000Z"),
      reservedObjectKey,
      confirmedAbsentObjectKey: null,
    });

    const candidates = [
      {
        candidateKey: "candidate-1",
        entityType: "EVENT" as const,
        entityKey: "event-key-1",
        owningTraceId: "trace-1",
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
        sourceVersion: 1_752_753_600_000_000_001n,
        canonicalPayloadHash: "e".repeat(64),
        traceDeletionGeneration: 0n,
        projectDeletionGeneration: 0n,
      },
    ];

    await expect(
      repository.publishCanonicalArtifact({
        client: prisma,
        operationId,
        projectId,
        fence: 2n,
        leaseOwner: "worker-a",
        canonicalObjectKey: reservedObjectKey,
        artifactChecksum: "f".repeat(64),
        candidates,
      }),
    ).resolves.toMatchObject({ outcome: "stale_fence" });

    await expect(
      repository.publishCanonicalArtifact({
        client: prisma,
        operationId,
        projectId,
        fence: 1n,
        leaseOwner: "worker-a",
        canonicalObjectKey: reservedObjectKey,
        artifactChecksum: "f".repeat(64),
        candidates,
      }),
    ).resolves.toMatchObject({ outcome: "published" });

    const operation =
      await prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: operationId },
        include: { candidates: true },
      });
    expect(operation).toMatchObject({
      manifestState: "CANDIDATE_PUBLISHED",
      canonicalObjectKey: reservedObjectKey,
      canonicalArtifactChecksum: "f".repeat(64),
      status: "QUEUED",
    });
    expect(operation.candidates).toHaveLength(1);
    expect(operation.candidateManifest).toEqual({
      count: 1,
      keys: ["candidate-1"],
    });

    const loadBatchId = `load-batch-${suffix}`;
    const freezeInput = {
      client: prisma,
      operationId,
      projectId,
      fence: 1n,
      canonicalObjectKey: reservedObjectKey,
      dispositions: [
        {
          candidateKey: "candidate-1",
          disposition: "LOAD_REQUIRED" as const,
          loadBatchId,
          reasonCode: null,
          quarantineExpiresAt: null,
        },
      ],
      loadBatches: [
        {
          id: loadBatchId,
          databaseName: "langfuse_poc",
          targetTable: "events_current",
          logicalBatchId: "events-2026-07-17-0001",
          attempt: 0,
          label: `lf_events_${suffix.replaceAll("-", "_")}`.slice(0, 128),
          payloadHash: "1".repeat(64),
          partitionDate: new Date("2026-07-17T00:00:00.000Z"),
          candidateKeys: ["candidate-1"],
        },
      ],
    };

    await expect(
      repository.freezeAnalyticsIngestionManifest({
        ...freezeInput,
        fence: 2n,
      }),
    ).resolves.toMatchObject({ outcome: "stale_fence" });
    await expect(
      repository.freezeAnalyticsIngestionManifest(freezeInput),
    ).resolves.toMatchObject({ outcome: "frozen" });
    await expect(
      repository.freezeAnalyticsIngestionManifest(freezeInput),
    ).resolves.toMatchObject({ outcome: "already_frozen" });

    const frozen = await prisma.analyticsIngestionOperation.findUniqueOrThrow({
      where: { id: operationId },
      include: { candidates: true, loadBatches: true },
    });
    expect(frozen).toMatchObject({
      manifestState: "FROZEN",
      status: "PERSISTED",
    });
    expect(frozen.candidates).toMatchObject([
      { disposition: "LOAD_REQUIRED", loadBatchId },
    ]);
    expect(frozen.loadBatches).toMatchObject([
      { id: loadBatchId, status: "PENDING", fenceGeneration: 1n },
    ]);

    await expect(
      loadRepository.claimAnalyticsLoadBatch({
        client: prisma,
        loadBatchId,
        projectId,
        expectedFence: 1n,
        nextFence: 2n,
        leaseOwner: "load-worker-a",
        leaseUntil: new Date("2026-07-18T12:05:00.000Z"),
        now: new Date("2026-07-18T12:04:00.000Z"),
      }),
    ).resolves.toMatchObject({ outcome: "claimed", fence: 2n });

    await expect(
      loadRepository.recordAnalyticsLoadOutcome({
        client: prisma,
        loadBatchId,
        projectId,
        fence: 1n,
        leaseOwner: "load-worker-a",
        outcome: "VISIBLE",
        transactionId: "stale-transaction",
        totalRows: 1,
        filteredRows: 0,
        errorCode: null,
        now: new Date("2026-07-18T12:04:10.000Z"),
      }),
    ).resolves.toBe(false);

    await expect(
      loadRepository.recordAnalyticsLoadOutcome({
        client: prisma,
        loadBatchId,
        projectId,
        fence: 2n,
        leaseOwner: "load-worker-a",
        outcome: "UNKNOWN",
        transactionId: null,
        totalRows: null,
        filteredRows: null,
        errorCode: "PUBLISH_TIMEOUT",
        now: new Date("2026-07-18T12:04:10.000Z"),
      }),
    ).resolves.toBe(true);

    await expect(
      loadRepository.claimAnalyticsLoadBatch({
        client: prisma,
        loadBatchId,
        projectId,
        expectedFence: 2n,
        nextFence: 3n,
        leaseOwner: "load-worker-b",
        leaseUntil: new Date("2026-07-18T12:06:00.000Z"),
        now: new Date("2026-07-18T12:05:00.000Z"),
      }),
    ).resolves.toMatchObject({ outcome: "reconciliation_required" });

    await expect(
      loadRepository.recordAnalyticsLoadReconciliation({
        client: prisma,
        loadBatchId,
        projectId,
        fence: 2n,
        status: "VISIBLE",
        transactionId: "transaction-1",
        totalRows: 1,
        filteredRows: 0,
        now: new Date("2026-07-18T12:05:10.000Z"),
      }),
    ).resolves.toBe(true);

    await expect(
      loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId,
        projectId,
        now: new Date("2026-07-18T12:05:20.000Z"),
      }),
    ).resolves.toMatchObject({ outcome: "completed", status: "VISIBLE" });
  });
});
