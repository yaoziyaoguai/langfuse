import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)("analytics control state", () => {
  let controlState: typeof import("./analytics-control-state.js");
  const prisma = new PrismaClient({
    datasourceUrl: controlDatabaseUrl,
  });
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const organizationId = `org-${suffix}`;
  const projectId = `project-${suffix}`;
  const operationId = `operation-${suffix}`;
  const deletionOperationId = `deletion-${suffix}`;

  beforeAll(async () => {
    controlState = await import("./analytics-control-state.js");
    await prisma.organization.create({
      data: { id: organizationId, name: "Doris control-state test" },
    });
    await prisma.project.create({
      data: {
        id: projectId,
        name: "Doris control-state test",
        orgId: organizationId,
      },
    });
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: operationId,
        projectId,
        sourceOperationId: `source-${suffix}`,
        sourceChecksum: "source-checksum",
        rawObjectKey: `raw/${suffix}`,
        acceptedAt: new Date("2026-07-18T12:00:00.000Z"),
        acceptedAtNanos: 1_784_376_000_000_000_000n,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 1,
        recoverableUntil: new Date("2030-01-01T00:00:00.000Z"),
        statusExpiresAt: new Date("2030-01-08T00:00:00.000Z"),
      },
    });
    await prisma.analyticsDeletionOperation.create({
      data: {
        id: deletionOperationId,
        scope: "PROJECT",
        organizationId,
        projectId,
        generation: 1n,
        requesterPrincipalType: "api_key",
        requesterPrincipalId: `principal-${suffix}`,
        statusExpiresAt: new Date("2030-01-08T00:00:00.000Z"),
      },
    });
  }, 30_000);

  afterAll(async () => {
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.analyticsDeletionOperation.deleteMany({
      where: { id: deletionOperationId },
    });
    await prisma.analyticsProjectDeletionGeneration.deleteMany({
      where: { projectId },
    });
    await prisma.$disconnect();
  }, 30_000);

  it("atomically claims one payload per source version and preserves its partition", async () => {
    const base = {
      client: prisma,
      projectId,
      operationId,
      entityType: "EVENT" as const,
      entityKey: `event-${suffix}`,
      lookupId: `span-${suffix}`,
      owningTraceId: `trace-${suffix}`,
      expectedSourceVersion: null,
      canonicalizerVersion: "r1a-v1",
      fenceGeneration: 1n,
      traceDeletionGeneration: 0n,
      projectDeletionGeneration: 0n,
    };

    const results = await Promise.all([
      controlState.claimAnalyticsEntityHead({
        ...base,
        sourceVersion: 10n,
        canonicalPayloadHash: "hash-a",
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
      }),
      controlState.claimAnalyticsEntityHead({
        ...base,
        sourceVersion: 10n,
        canonicalPayloadHash: "hash-b",
        partitionDate: new Date("2026-07-17T00:00:00.000Z"),
      }),
    ]);

    expect(results.map(({ outcome }) => outcome).sort()).toEqual([
      "conflict",
      "won",
    ]);

    const winner = results.find(({ outcome }) => outcome === "won");
    expect(winner).toBeDefined();

    const samePayloadReplay = await controlState.claimAnalyticsEntityHead({
      ...base,
      sourceVersion: winner!.head.sourceVersion,
      canonicalPayloadHash: winner!.head.canonicalPayloadHash,
      partitionDate: winner!.head.partitionDate,
    });
    expect(samePayloadReplay.outcome).toBe("noop");

    const laterVersion = await controlState.claimAnalyticsEntityHead({
      ...base,
      expectedSourceVersion: winner!.head.sourceVersion,
      sourceVersion: winner!.head.sourceVersion + 1n,
      canonicalPayloadHash: "later-hash",
      partitionDate: winner!.head.partitionDate,
      fenceGeneration: 2n,
    });
    expect(laterVersion.outcome).toBe("won");
    expect(laterVersion.head.sourceVersion).toBe(11n);

    const supersededVersion = await controlState.claimAnalyticsEntityHead({
      ...base,
      expectedSourceVersion: null,
      sourceVersion: 9n,
      canonicalPayloadHash: "old-hash",
      partitionDate: winner!.head.partitionDate,
    });
    expect(supersededVersion.outcome).toBe("superseded");
    expect(supersededVersion.head.sourceVersion).toBe(11n);

    const crossDayMutation = await controlState.claimAnalyticsEntityHead({
      ...base,
      expectedSourceVersion: laterVersion.head.sourceVersion,
      sourceVersion: laterVersion.head.sourceVersion + 1n,
      canonicalPayloadHash: "new-hash",
      partitionDate: new Date("2026-07-18T00:00:00.000Z"),
      fenceGeneration: 3n,
    });
    expect(crossDayMutation.outcome).toBe("partition_conflict");

    const locatorMutation = await controlState.claimAnalyticsEntityHead({
      ...base,
      lookupId: `different-span-${suffix}`,
      expectedSourceVersion: laterVersion.head.sourceVersion,
      sourceVersion: laterVersion.head.sourceVersion + 1n,
      canonicalPayloadHash: "different-locator-hash",
      partitionDate: laterVersion.head.partitionDate,
      fenceGeneration: 3n,
    });
    expect(locatorMutation.outcome).toBe("locator_conflict");

    const persisted = await prisma.analyticsEntityHead.findUniqueOrThrow({
      where: {
        projectId_entityType_entityKey: {
          projectId,
          entityType: "EVENT",
          entityKey: base.entityKey,
        },
      },
    });
    expect(persisted.partitionDate).toEqual(winner!.head.partitionDate);
    expect(persisted.sourceVersion).toBe(11n);
    expect(persisted.lookupId).toBe(base.lookupId);
  });

  it("converges concurrent first claims to the highest source version", async () => {
    const base = {
      client: prisma,
      projectId,
      operationId,
      entityType: "EVENT" as const,
      entityKey: `concurrent-version-event-${suffix}`,
      lookupId: `concurrent-span-${suffix}`,
      owningTraceId: `trace-${suffix}`,
      expectedSourceVersion: null,
      canonicalizerVersion: "r1a-v1",
      fenceGeneration: 1n,
      traceDeletionGeneration: 0n,
      projectDeletionGeneration: 0n,
      partitionDate: new Date("2026-07-17T00:00:00.000Z"),
    };

    const results = await Promise.all([
      controlState.claimAnalyticsEntityHead({
        ...base,
        sourceVersion: 10n,
        canonicalPayloadHash: "version-10",
      }),
      controlState.claimAnalyticsEntityHead({
        ...base,
        sourceVersion: 11n,
        canonicalPayloadHash: "version-11",
      }),
    ]);

    expect(results[1]?.outcome).toBe("won");
    await expect(
      prisma.analyticsEntityHead.findUniqueOrThrow({
        where: {
          projectId_entityType_entityKey: {
            projectId,
            entityType: "EVENT",
            entityKey: base.entityKey,
          },
        },
      }),
    ).resolves.toMatchObject({
      sourceVersion: 11n,
      canonicalPayloadHash: "version-11",
    });
  });

  it("rejects stale ingestion and deletion worker fences", async () => {
    await expect(
      controlState.advanceAnalyticsIngestionOperation({
        client: prisma,
        operationId,
        projectId,
        expectedFence: 0n,
        nextFence: 1n,
        status: "QUEUED",
      }),
    ).resolves.toBe(true);
    await expect(
      controlState.advanceAnalyticsIngestionOperation({
        client: prisma,
        operationId,
        projectId,
        expectedFence: 0n,
        nextFence: 2n,
        status: "VISIBLE",
      }),
    ).resolves.toBe(false);

    await expect(
      controlState.advanceAnalyticsDeletionOperation({
        client: prisma,
        operationId: deletionOperationId,
        organizationId,
        expectedFence: 0n,
        nextFence: 1n,
        status: "RETRYING",
        phase: "physical_delete",
      }),
    ).resolves.toBe(true);
    await expect(
      controlState.advanceAnalyticsDeletionOperation({
        client: prisma,
        operationId: deletionOperationId,
        organizationId,
        expectedFence: 0n,
        nextFence: 2n,
        status: "COMPLETED",
        phase: "completed",
      }),
    ).resolves.toBe(false);

    await expect(
      controlState.advanceTraceDeletionTombstone({
        client: prisma,
        projectId,
        traceId: `deleted-trace-${suffix}`,
        generation: 5n,
        status: "RETRYING",
      }),
    ).resolves.toMatchObject({ advanced: true, generation: 5n });
    await expect(
      controlState.advanceTraceDeletionTombstone({
        client: prisma,
        projectId,
        traceId: `deleted-trace-${suffix}`,
        generation: 4n,
        status: "COMPLETED",
      }),
    ).resolves.toMatchObject({ advanced: false, generation: 5n });

    const [ingestion, deletion] = await Promise.all([
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: operationId },
      }),
      prisma.analyticsDeletionOperation.findUniqueOrThrow({
        where: { id: deletionOperationId },
      }),
    ]);
    expect(ingestion).toMatchObject({
      canonicalizationFence: 1n,
      status: "QUEUED",
    });
    expect(deletion).toMatchObject({
      workerFence: 1n,
      status: "RETRYING",
      phase: "physical_delete",
    });
    await expect(
      prisma.analyticsDeletionTombstone.findUniqueOrThrow({
        where: {
          projectId_traceId: {
            projectId,
            traceId: `deleted-trace-${suffix}`,
          },
        },
      }),
    ).resolves.toMatchObject({ generation: 5n, status: "RETRYING" });
  });

  it("keeps deletion generation and organization-scoped status after project deletion", async () => {
    await expect(
      controlState.advanceProjectDeletionGeneration({
        client: prisma,
        projectId,
        generation: 3n,
      }),
    ).resolves.toMatchObject({ advanced: true, generation: 3n });
    await expect(
      controlState.advanceProjectDeletionGeneration({
        client: prisma,
        projectId,
        generation: 2n,
      }),
    ).resolves.toMatchObject({ advanced: false, generation: 3n });

    await prisma.project.delete({ where: { id: projectId } });

    await expect(
      controlState.getProjectDeletionGeneration({ client: prisma, projectId }),
    ).resolves.toBe(3n);
    await expect(
      controlState.findDeletionOperationForOrganization({
        client: prisma,
        operationId: deletionOperationId,
        organizationId,
      }),
    ).resolves.toMatchObject({ id: deletionOperationId, projectId });

    await prisma.project.create({
      data: {
        id: projectId,
        name: "Recreated project id",
        orgId: organizationId,
      },
    });
    await expect(
      controlState.advanceProjectDeletionGeneration({
        client: prisma,
        projectId,
        generation: 1n,
      }),
    ).resolves.toMatchObject({ advanced: false, generation: 3n });
  });

  it("does not let ingestion initialization overwrite a newer trace-control revision", async () => {
    const traceId = `control-trace-${suffix}`;
    await controlState.initializeTraceControlState({
      client: prisma,
      projectId,
      traceId,
      initializedByOperationId: operationId,
      bookmarked: false,
      public: false,
    });
    await expect(
      controlState.mutateTraceControlState({
        client: prisma,
        projectId,
        traceId,
        expectedRevision: 0n,
        bookmarked: true,
        public: true,
        mutationSource: "api",
      }),
    ).resolves.toBe(true);

    const initialization = await controlState.initializeTraceControlState({
      client: prisma,
      projectId,
      traceId,
      initializedByOperationId: operationId,
      bookmarked: false,
      public: false,
    });
    expect(initialization.created).toBe(false);
    expect(initialization.state).toMatchObject({
      bookmarked: true,
      public: true,
      revision: 1n,
      lastMutationSource: "api",
    });

    await expect(
      controlState.getTraceControlState({ client: prisma, projectId, traceId }),
    ).resolves.toMatchObject({ public: true, revision: 1n });
  });
});
