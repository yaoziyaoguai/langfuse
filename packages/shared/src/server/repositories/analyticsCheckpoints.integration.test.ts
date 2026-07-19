import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

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
  }, 30_000);

  afterAll(async () => {
    if (createdCheckpointGenerations.length > 0) {
      await prisma.analyticsCheckpointGeneration.deleteMany({
        where: { generation: { in: createdCheckpointGenerations } },
      });
    }
    await prisma.organization.deleteMany({ where: { id: organizationId } });
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
});
