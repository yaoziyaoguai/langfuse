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
      prisma.analyticsIngestionOutboxV2.count({
        where: { operationId: input.operationId },
      }),
    ).resolves.toBe(1);
    await expect(
      repository.getAnalyticsIngestionStatusForProject({
        client: prisma,
        operationId: input.operationId,
        projectId,
      }),
    ).resolves.toMatchObject({
      operationId: input.operationId,
      status: "ACCEPTED",
      manifest: "PENDING",
      outbox: "PENDING",
      candidates: [],
      loads: [],
    });
    await expect(
      repository.getAnalyticsIngestionStatusForProject({
        client: prisma,
        operationId: input.operationId,
        projectId: "other-project",
      }),
    ).resolves.toBeNull();

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
        generation: 1,
        workerId: "outbox-worker-a",
        now: new Date("2026-07-18T12:00:03.000Z"),
      }),
    ).resolves.toBe(true);
    await expect(
      repository.resolveAnalyticsIngestionAttemptFailure({
        client: prisma,
        operationId: input.operationId,
        projectId,
        reasonCode: "MAX_RETRIES_EXHAUSTED",
        expectedGeneration: 1,
        retryDelayMs: 1_000,
      }),
    ).resolves.toBe("requeued");
    await expect(
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: input.operationId },
      }),
    ).resolves.toMatchObject({
      status: "RETRYING",
      lastErrorCode: "MAX_RETRIES_EXHAUSTED",
      terminalAt: null,
    });
    await expect(
      repository.markAnalyticsIngestionTerminalFailure({
        client: prisma,
        operationId: input.operationId,
        projectId,
        status: "UNRECOVERABLE",
        reasonCode: "ANALYTICS_VALIDATION_ERROR",
        expectedGeneration: 2,
        now: new Date("2026-07-18T12:00:04.000Z"),
      }),
    ).resolves.toBe(true);
    await expect(
      repository.markAnalyticsIngestionTerminalFailure({
        client: prisma,
        operationId: input.operationId,
        projectId,
        status: "QUARANTINED",
        reasonCode: "ANALYTICS_CONFLICT",
        expectedGeneration: 2,
      }),
    ).resolves.toBe(false);
    await expect(
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: input.operationId },
      }),
    ).resolves.toMatchObject({
      status: "UNRECOVERABLE",
      lastErrorCode: "ANALYTICS_VALIDATION_ERROR",
      terminalAt: new Date("2026-07-18T12:00:04.000Z"),
    });

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

  it("atomically hands nonterminal legacy outbox rows to V2", async () => {
    const activeOperationId = `legacy-active-operation-${suffix}`;
    const terminalOperationId = `legacy-terminal-operation-${suffix}`;
    const acceptedAt = new Date("2026-07-18T12:00:00.000Z");
    const common = {
      projectId,
      acceptedAt,
      acceptedAtNanos: 1_784_376_000_000_000_000n,
      canonicalizerVersion: "1",
      schemaVersion: 3,
      recoverableUntil: new Date("2026-07-25T12:00:00.000Z"),
      statusExpiresAt: new Date("2026-08-24T12:00:00.000Z"),
    };
    await prisma.analyticsIngestionOperation.createMany({
      data: [
        {
          ...common,
          id: activeOperationId,
          sourceOperationId: `legacy-active-source-${suffix}`,
          sourceChecksum: "e".repeat(64),
          rawObjectKey: `events/${projectId}/raw/legacy-active.json`,
        },
        {
          ...common,
          id: terminalOperationId,
          sourceOperationId: `legacy-terminal-source-${suffix}`,
          sourceChecksum: "f".repeat(64),
          rawObjectKey: `events/${projectId}/raw/legacy-terminal.json`,
          status: "UNRECOVERABLE",
          terminalAt: acceptedAt,
        },
      ],
    });
    await prisma.analyticsIngestionOutbox.createMany({
      data: [
        {
          operationId: activeOperationId,
          attempts: 3,
          status: "PUBLISHED",
          lockedBy: "doris-handoff:crashed-worker",
          lockedUntil: new Date("2026-07-18T12:04:00.000Z"),
          publishedAt: acceptedAt,
        },
        { operationId: terminalOperationId, attempts: 5 },
      ],
    });

    await expect(
      repository.getAnalyticsIngestionStatusForProject({
        client: prisma,
        operationId: activeOperationId,
        projectId,
      }),
    ).resolves.toMatchObject({ outbox: "PUBLISHED" });

    const handoffResults = await Promise.all([
      repository.handoffLegacyAnalyticsIngestionOutbox({
        client: prisma,
        now: new Date("2026-07-18T12:05:00.000Z"),
        limit: 10,
      }),
      repository.handoffLegacyAnalyticsIngestionOutbox({
        client: prisma,
        now: new Date("2026-07-18T12:05:00.000Z"),
        limit: 10,
      }),
    ]);
    expect(handoffResults.reduce((sum, count) => sum + count, 0)).toBe(1);
    await expect(
      prisma.analyticsIngestionOutbox.findMany({
        where: {
          operationId: { in: [activeOperationId, terminalOperationId] },
        },
        orderBy: { operationId: "asc" },
        select: { operationId: true, attempts: true },
      }),
    ).resolves.toEqual([{ operationId: terminalOperationId, attempts: 5 }]);
    await expect(
      prisma.analyticsIngestionOutboxV2.findUniqueOrThrow({
        where: { operationId: activeOperationId },
      }),
    ).resolves.toMatchObject({
      status: "PENDING",
      generation: 1,
      attempts: 3,
      nextAttemptAt: new Date("2026-07-18T12:05:00.000Z"),
    });
    await expect(
      prisma.analyticsIngestionOutboxV2.count({
        where: { operationId: activeOperationId },
      }),
    ).resolves.toBe(1);
    await expect(
      repository.getAnalyticsIngestionStatusForProject({
        client: prisma,
        operationId: activeOperationId,
        projectId,
      }),
    ).resolves.toMatchObject({ outbox: "PENDING" });
    await expect(
      repository.handoffLegacyAnalyticsIngestionOutbox({
        client: prisma,
        now: new Date("2026-07-18T12:06:00.000Z"),
        limit: 10,
      }),
    ).resolves.toBe(0);
  });

  it("recovers only stale published outbox rows for inactive nonterminal operations", async () => {
    const staleOperationId = `stale-published-operation-${suffix}`;
    const activeLoadOperationId = `active-load-operation-${suffix}`;
    const liveLeaseOperationId = `live-lease-operation-${suffix}`;
    const recentOperationId = `recent-published-operation-${suffix}`;
    const terminalOperationId = `terminal-published-operation-${suffix}`;
    const old = new Date("2026-07-18T11:00:00.000Z");
    const recent = new Date("2026-07-18T12:04:00.000Z");
    const now = new Date("2026-07-18T12:05:00.000Z");
    const updatedBefore = new Date("2026-07-18T12:03:00.000Z");

    await prisma.analyticsIngestionOperation.createMany({
      data: [
        {
          id: activeLoadOperationId,
          projectId,
          sourceOperationId: `active-load-source-${suffix}`,
          sourceChecksum: "4".repeat(64),
          rawObjectKey: `events/${projectId}/raw/active-load.json`,
          acceptedAt: old,
          acceptedAtNanos: 1_784_372_400_000_000_000n,
          canonicalizerVersion: "1",
          schemaVersion: 3,
          recoverableUntil: new Date("2026-07-25T11:00:00.000Z"),
          statusExpiresAt: new Date("2026-08-25T11:00:00.000Z"),
          status: "PERSISTED",
          updatedAt: old,
        },
        {
          id: staleOperationId,
          projectId,
          sourceOperationId: `stale-published-source-${suffix}`,
          sourceChecksum: "1".repeat(64),
          rawObjectKey: `events/${projectId}/raw/stale-published.json`,
          acceptedAt: old,
          acceptedAtNanos: 1_784_372_400_000_000_000n,
          canonicalizerVersion: "1",
          schemaVersion: 3,
          recoverableUntil: new Date("2026-07-25T11:00:00.000Z"),
          statusExpiresAt: new Date("2026-08-25T11:00:00.000Z"),
          status: "RETRYING",
          updatedAt: old,
        },
        {
          id: liveLeaseOperationId,
          projectId,
          sourceOperationId: `live-lease-source-${suffix}`,
          sourceChecksum: "6".repeat(64),
          rawObjectKey: `events/${projectId}/raw/live-lease.json`,
          acceptedAt: old,
          acceptedAtNanos: 1_784_372_400_000_000_000n,
          canonicalizerVersion: "1",
          schemaVersion: 3,
          recoverableUntil: new Date("2026-07-25T11:00:00.000Z"),
          statusExpiresAt: new Date("2026-08-25T11:00:00.000Z"),
          status: "PERSISTED",
          updatedAt: old,
        },
        {
          id: recentOperationId,
          projectId,
          sourceOperationId: `recent-published-source-${suffix}`,
          sourceChecksum: "2".repeat(64),
          rawObjectKey: `events/${projectId}/raw/recent-published.json`,
          acceptedAt: old,
          acceptedAtNanos: 1_784_372_400_000_000_000n,
          canonicalizerVersion: "1",
          schemaVersion: 3,
          recoverableUntil: new Date("2026-07-25T11:00:00.000Z"),
          statusExpiresAt: new Date("2026-08-25T11:00:00.000Z"),
          status: "RETRYING",
          updatedAt: recent,
        },
        {
          id: terminalOperationId,
          projectId,
          sourceOperationId: `terminal-published-source-${suffix}`,
          sourceChecksum: "3".repeat(64),
          rawObjectKey: `events/${projectId}/raw/terminal-published.json`,
          acceptedAt: old,
          acceptedAtNanos: 1_784_372_400_000_000_000n,
          canonicalizerVersion: "1",
          schemaVersion: 3,
          recoverableUntil: new Date("2026-07-25T11:00:00.000Z"),
          statusExpiresAt: new Date("2026-08-25T11:00:00.000Z"),
          status: "UNRECOVERABLE",
          terminalAt: old,
          updatedAt: old,
        },
      ],
    });
    await prisma.analyticsIngestionOutboxV2.createMany({
      data: [
        activeLoadOperationId,
        liveLeaseOperationId,
        staleOperationId,
        recentOperationId,
        terminalOperationId,
      ].map((operationId) => ({
        operationId,
        status: "PUBLISHED",
        publishedAt: old,
        updatedAt: old,
      })),
    });
    await prisma.analyticsLoadBatch.create({
      data: {
        id: `active-load-${suffix}`,
        operationId: activeLoadOperationId,
        projectId,
        databaseName: "langfuse",
        targetTable: "events_current",
        logicalBatchId: "active-load",
        fenceGeneration: 1n,
        label: `active_load_${suffix}`,
        payloadHash: "5".repeat(64),
        canonicalObjectKey: `events/${projectId}/canonical/active-load.json`,
        status: "UNKNOWN",
        updatedAt: recent,
      },
    });
    await prisma.analyticsLoadBatch.create({
      data: {
        id: `live-lease-load-${suffix}`,
        operationId: liveLeaseOperationId,
        projectId,
        databaseName: "langfuse",
        targetTable: "events_current",
        logicalBatchId: "live-lease-load",
        fenceGeneration: 1n,
        leaseOwner: "active-worker",
        leaseExpiresAt: new Date("2026-07-18T12:06:00.000Z"),
        label: `live_lease_load_${suffix}`,
        payloadHash: "7".repeat(64),
        canonicalObjectKey: `events/${projectId}/canonical/live-lease.json`,
        status: "LOADING",
        updatedAt: old,
      },
    });

    await expect(
      repository.recoverStalePublishedAnalyticsIngestionOutbox({
        client: prisma,
        now,
        updatedBefore,
        limit: 10,
      }),
    ).resolves.toBe(1);
    await expect(
      prisma.analyticsIngestionOutboxV2.findMany({
        where: {
          operationId: {
            in: [
              activeLoadOperationId,
              liveLeaseOperationId,
              staleOperationId,
              recentOperationId,
              terminalOperationId,
            ],
          },
        },
        orderBy: { operationId: "asc" },
        select: {
          operationId: true,
          status: true,
          generation: true,
          nextAttemptAt: true,
          publishedAt: true,
        },
      }),
    ).resolves.toEqual([
      {
        operationId: activeLoadOperationId,
        status: "PUBLISHED",
        generation: 1,
        nextAttemptAt: expect.any(Date),
        publishedAt: old,
      },
      {
        operationId: liveLeaseOperationId,
        status: "PUBLISHED",
        generation: 1,
        nextAttemptAt: expect.any(Date),
        publishedAt: old,
      },
      {
        operationId: recentOperationId,
        status: "PUBLISHED",
        generation: 1,
        nextAttemptAt: expect.any(Date),
        publishedAt: old,
      },
      {
        operationId: staleOperationId,
        status: "PENDING",
        generation: 1,
        nextAttemptAt: now,
        publishedAt: null,
      },
      {
        operationId: terminalOperationId,
        status: "PUBLISHED",
        generation: 1,
        nextAttemptAt: expect.any(Date),
        publishedAt: old,
      },
    ]);
  });

  it("requeues an exhausted operation while a Doris label remains unresolved", async () => {
    const operationId = `unknown-operation-${suffix}`;
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: operationId,
        projectId,
        sourceOperationId: `unknown-source-${suffix}`,
        sourceChecksum: "c".repeat(64),
        rawObjectKey: `events/${projectId}/raw/unknown.json`,
        acceptedAt: new Date("2026-07-10T12:00:00.000Z"),
        acceptedAtNanos: 1_783_684_800_000_000_000n,
        canonicalizerVersion: "1",
        schemaVersion: 3,
        recoverableUntil: new Date("2026-07-17T12:00:00.000Z"),
        statusExpiresAt: new Date("2026-08-17T12:00:00.000Z"),
        status: "RETRYING",
        outboxV2: { create: { status: "PUBLISHED" } },
        loadBatches: {
          create: {
            id: `unknown-load-${suffix}`,
            projectId,
            databaseName: "langfuse",
            targetTable: "events_current",
            logicalBatchId: "events-2026-07-10-000000",
            fenceGeneration: 1n,
            label: `unknown-load-${suffix}`,
            payloadHash: "d".repeat(64),
            canonicalObjectKey: `events/${projectId}/canonical.json`,
            status: "UNKNOWN",
          },
        },
      },
    });

    await expect(
      repository.resolveAnalyticsIngestionAttemptFailure({
        client: prisma,
        operationId,
        projectId,
        reasonCode: "MAX_RETRIES_EXHAUSTED",
        expectedGeneration: 1,
        now: new Date("2026-07-18T12:00:00.000Z"),
      }),
    ).resolves.toBe("requeued");
    await expect(
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: operationId },
        include: { outboxV2: true, loadBatches: true },
      }),
    ).resolves.toMatchObject({
      status: "RETRYING",
      terminalAt: null,
      outboxV2: {
        status: "PENDING",
        generation: 2,
        nextAttemptAt: expect.toSatisfy(
          (value: Date) =>
            value >= new Date("2026-07-18T12:00:03.750Z") &&
            value <= new Date("2026-07-18T12:00:06.250Z"),
        ),
      },
      loadBatches: [{ status: "UNKNOWN" }],
    });

    await expect(
      repository.resolveAnalyticsIngestionAttemptFailure({
        client: prisma,
        operationId,
        projectId,
        reasonCode: "ANALYTICS_UNAVAILABLE",
        expectedGeneration: 2,
        now: new Date("2026-07-18T12:01:00.000Z"),
      }),
    ).resolves.toBe("requeued");
    await expect(
      prisma.analyticsIngestionOutboxV2.findUniqueOrThrow({
        where: { operationId },
      }),
    ).resolves.toMatchObject({
      status: "PENDING",
      generation: 3,
      nextAttemptAt: expect.toSatisfy(
        (value: Date) =>
          value >= new Date("2026-07-18T12:01:07.500Z") &&
          value <= new Date("2026-07-18T12:01:12.500Z"),
      ),
    });
  });

  it("preserves visible children when exhaustion terminalizes pending loads", async () => {
    const operationId = `partial-operation-${suffix}`;
    const visibleLoadId = `partial-visible-load-${suffix}`;
    const pendingLoadId = `partial-pending-load-${suffix}`;
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: operationId,
        projectId,
        sourceOperationId: `partial-source-${suffix}`,
        sourceChecksum: "9".repeat(64),
        rawObjectKey: `events/${projectId}/raw/partial.json`,
        acceptedAt: new Date("2026-07-10T12:00:00.000Z"),
        acceptedAtNanos: 1_783_684_800_000_000_000n,
        canonicalizerVersion: "1",
        schemaVersion: 3,
        canonicalizationFence: 1n,
        canonicalObjectKey: `events/${projectId}/canonical/partial.json`,
        manifestState: "FROZEN",
        status: "RETRYING",
        recoverableUntil: new Date("2026-07-17T12:00:00.000Z"),
        statusExpiresAt: new Date("2026-08-17T12:00:00.000Z"),
        outboxV2: { create: { status: "PUBLISHED" } },
        loadBatches: {
          create: [
            {
              id: visibleLoadId,
              projectId,
              databaseName: "langfuse",
              targetTable: "events_current",
              logicalBatchId: "partial-visible",
              fenceGeneration: 1n,
              label: `partial_visible_${suffix}`,
              payloadHash: "a".repeat(64),
              canonicalObjectKey: `events/${projectId}/canonical/partial.json`,
              status: "VISIBLE",
              totalRows: 1,
              filteredRows: 0,
              visibleAt: new Date("2026-07-18T11:00:00.000Z"),
            },
            {
              id: pendingLoadId,
              projectId,
              databaseName: "langfuse",
              targetTable: "events_current",
              logicalBatchId: "partial-pending",
              fenceGeneration: 1n,
              label: `partial_pending_${suffix}`,
              payloadHash: "b".repeat(64),
              canonicalObjectKey: `events/${projectId}/canonical/partial.json`,
            },
          ],
        },
        candidates: {
          create: [
            {
              projectId,
              candidateKey: `partial-visible-${suffix}`,
              entityType: "EVENT",
              entityKey: `visible-entity-${suffix}`,
              partitionDate: new Date("2026-07-17T00:00:00.000Z"),
              sourceVersion: 1n,
              canonicalPayloadHash: "c".repeat(64),
              disposition: "LOAD_REQUIRED",
              loadBatchId: visibleLoadId,
            },
            {
              projectId,
              candidateKey: `partial-pending-${suffix}`,
              entityType: "EVENT",
              entityKey: `pending-entity-${suffix}`,
              partitionDate: new Date("2026-07-17T00:00:00.000Z"),
              sourceVersion: 1n,
              canonicalPayloadHash: "d".repeat(64),
              disposition: "LOAD_REQUIRED",
              loadBatchId: pendingLoadId,
            },
          ],
        },
      },
    });

    await expect(
      repository.resolveAnalyticsIngestionAttemptFailure({
        client: prisma,
        operationId,
        projectId,
        reasonCode: "MAX_RETRIES_EXHAUSTED",
        expectedGeneration: 1,
        now: new Date("2026-07-18T12:00:00.000Z"),
      }),
    ).resolves.toBe("terminalized");
    await expect(
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: operationId },
        include: {
          candidates: { orderBy: { candidateKey: "desc" } },
          loadBatches: { orderBy: { logicalBatchId: "desc" } },
        },
      }),
    ).resolves.toMatchObject({
      status: "PARTIAL_FAILED",
      visibleAt: new Date("2026-07-18T12:00:00.000Z"),
      candidates: [
        { loadBatchId: visibleLoadId, disposition: "LOAD_REQUIRED" },
        { loadBatchId: pendingLoadId, disposition: "QUARANTINED" },
      ],
      loadBatches: [
        { id: visibleLoadId, status: "VISIBLE" },
        { id: pendingLoadId, status: "FAILED" },
      ],
    });
  });

  it("does not claim a pending load after a concurrent terminal fence commits", async () => {
    const operationId = `terminal-race-operation-${suffix}`;
    const loadBatchId = `terminal-race-load-${suffix}`;
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: operationId,
        projectId,
        sourceOperationId: `terminal-race-source-${suffix}`,
        sourceChecksum: "8".repeat(64),
        rawObjectKey: `events/${projectId}/raw/terminal-race.json`,
        acceptedAt: new Date("2026-07-10T12:00:00.000Z"),
        acceptedAtNanos: 1_783_684_800_000_000_000n,
        canonicalizerVersion: "1",
        schemaVersion: 3,
        manifestState: "FROZEN",
        status: "PERSISTED",
        recoverableUntil: new Date("2026-07-17T12:00:00.000Z"),
        statusExpiresAt: new Date("2026-08-17T12:00:00.000Z"),
        loadBatches: {
          create: {
            id: loadBatchId,
            projectId,
            databaseName: "langfuse",
            targetTable: "events_current",
            logicalBatchId: "terminal-race",
            fenceGeneration: 1n,
            label: `terminal_race_${suffix}`,
            payloadHash: "7".repeat(64),
            canonicalObjectKey: `events/${projectId}/canonical/terminal-race.json`,
          },
        },
      },
    });

    let releaseLock!: () => void;
    const release = new Promise<void>((resolve) => {
      releaseLock = resolve;
    });
    let reportLocked!: () => void;
    const locked = new Promise<void>((resolve) => {
      reportLocked = resolve;
    });
    const terminalCommit = prisma.$transaction(async (transaction) => {
      await transaction.$queryRaw`
        SELECT id
        FROM analytics_ingestion_operations
        WHERE id = ${operationId} AND project_id = ${projectId}
        FOR UPDATE
      `;
      reportLocked();
      await release;
      await transaction.analyticsIngestionOperation.update({
        where: { id: operationId },
        data: {
          status: "UNRECOVERABLE",
          lastErrorCode: "ANALYTICS_UNRECOVERABLE",
          terminalAt: new Date("2026-07-18T12:00:00.000Z"),
        },
      });
    });
    await locked;

    let claimSettled = false;
    const claim = loadRepository
      .claimAnalyticsLoadBatch({
        client: prisma,
        loadBatchId,
        projectId,
        expectedFence: 1n,
        nextFence: 2n,
        leaseOwner: "terminal-race-worker",
        leaseUntil: new Date("2026-07-18T12:02:00.000Z"),
        now: new Date("2026-07-18T12:01:00.000Z"),
      })
      .finally(() => {
        claimSettled = true;
      });
    await new Promise((resolve) => setTimeout(resolve, 50));
    expect(claimSettled).toBe(false);

    releaseLock();
    await terminalCommit;
    await expect(claim).resolves.toMatchObject({ outcome: "terminal" });
    await expect(
      prisma.analyticsLoadBatch.findUniqueOrThrow({
        where: { id: loadBatchId },
      }),
    ).resolves.toMatchObject({ status: "PENDING", fenceGeneration: 1n });
  });

  it("cancels a claimed trace-isolated load immediately before Stream Load", async () => {
    const operationId = `delete-race-operation-${suffix}`;
    const loadBatchId = `delete-race-load-${suffix}`;
    const traceId = `delete-race-trace-${suffix}`;
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: operationId,
        projectId,
        sourceOperationId: `delete-race-source-${suffix}`,
        sourceChecksum: "d".repeat(64),
        rawObjectKey: `events/${projectId}/raw/delete-race.json`,
        acceptedAt: new Date("2026-07-18T12:00:00.000Z"),
        acceptedAtNanos: 1_784_376_000_000_000_000n,
        canonicalizerVersion: "1",
        schemaVersion: 3,
        canonicalizationFence: 1n,
        canonicalObjectKey: `events/${projectId}/canonical/delete-race.json`,
        manifestState: "FROZEN",
        status: "PERSISTED",
        recoverableUntil: new Date("2026-07-25T12:00:00.000Z"),
        statusExpiresAt: new Date("2026-08-24T12:00:00.000Z"),
      },
    });
    await prisma.analyticsLoadBatch.create({
      data: {
        id: loadBatchId,
        operationId,
        projectId,
        databaseName: "langfuse",
        targetTable: "events_current",
        logicalBatchId: `delete-race-logical-${suffix}`,
        fenceGeneration: 2n,
        status: "LOADING",
        leaseOwner: "delete-race-worker",
        leaseExpiresAt: new Date("2026-07-18T12:01:00.000Z"),
        label: `delete_race_${suffix}`,
        payloadHash: "e".repeat(64),
        canonicalObjectKey: `events/${projectId}/canonical/delete-race.json`,
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
      },
    });
    await prisma.analyticsIngestionCandidate.create({
      data: {
        operationId,
        projectId,
        candidateKey: `delete-race-candidate-${suffix}`,
        entityType: "EVENT",
        entityKey: `delete-race-entity-${suffix}`,
        owningTraceId: traceId,
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
        sourceVersion: 1n,
        canonicalPayloadHash: "f".repeat(64),
        disposition: "LOAD_REQUIRED",
        loadBatchId,
        traceDeletionGeneration: 0n,
        projectDeletionGeneration: 0n,
      },
    });
    await prisma.analyticsDeletionTombstone.create({
      data: {
        projectId,
        traceId,
        generation: 1n,
        status: "SCHEDULED",
      },
    });

    await expect(
      loadRepository.cancelAnalyticsLoadBatchIfDeleted({
        client: prisma,
        loadBatchId,
        projectId,
        claimedFence: 2n,
        leaseOwner: "delete-race-worker",
      }),
    ).resolves.toEqual({ outcome: "cancelled" });
    await expect(
      prisma.analyticsLoadBatch.findUniqueOrThrow({
        where: { id: loadBatchId },
      }),
    ).resolves.toMatchObject({
      status: "CANCELLED_BY_DELETION",
      lastErrorCode: "DELETION_BARRIER",
    });
    await expect(
      prisma.analyticsIngestionCandidate.findFirstOrThrow({
        where: { operationId },
      }),
    ).resolves.toMatchObject({
      disposition: "CANCELLED_BY_DELETION",
      reasonCode: "DELETION_BARRIER",
    });
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
