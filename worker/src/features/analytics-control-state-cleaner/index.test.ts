import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import { AnalyticsControlStateCleaner, compactAnalyticsControlState } from ".";

describe("AnalyticsControlStateCleaner lifecycle", () => {
  it("drains an in-flight compaction during shutdown", async () => {
    let finishRun:
      | ((result: {
          operationsCompacted: number;
          childRowsDeleted: number;
        }) => void)
      | undefined;
    const runOnce = vi.fn(
      () =>
        new Promise<{
          operationsCompacted: number;
          childRowsDeleted: number;
        }>((resolve) => {
          finishRun = resolve;
        }),
    );
    const cleaner = new AnalyticsControlStateCleaner({
      intervalMs: 60_000,
      runOnce,
    });

    cleaner.start();
    await vi.waitFor(() => expect(runOnce).toHaveBeenCalledOnce());
    const draining = cleaner.stopAndDrain();
    finishRun?.({ operationsCompacted: 0, childRowsDeleted: 0 });
    await draining;

    expect(runOnce).toHaveBeenCalledOnce();
  });
});

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)("analytics control-state cleaner", () => {
  const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const organizationId = `cleaner-org-${suffix}`;
  const projectId = `cleaner-project-${suffix}`;
  const checkpointGeneration = BigInt(Date.now());

  async function createOperation(input: {
    id: string;
    acceptedAt: Date;
    recoverableUntil: Date;
    status: "VISIBLE" | "RETRYING" | "QUARANTINED";
    terminalAt: Date | null;
  }) {
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: input.id,
        projectId,
        sourceOperationId: `source-${input.id}`,
        sourceChecksum: "a".repeat(64),
        rawObjectKey: `raw/${input.id}.json`,
        acceptedAt: input.acceptedAt,
        acceptedAtNanos: BigInt(input.acceptedAt.getTime()) * 1_000_000n,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 3,
        canonicalizationFence: 1n,
        canonicalObjectKey: `canonical/${input.id}.json`,
        manifestState: "FROZEN",
        status: input.status,
        terminalAt: input.terminalAt,
        recoverableUntil: input.recoverableUntil,
        statusExpiresAt: new Date("2026-09-30T00:00:00.000Z"),
      },
    });
    const loadBatchId = `load-${input.id}`;
    await prisma.analyticsLoadBatch.create({
      data: {
        id: loadBatchId,
        operationId: input.id,
        projectId,
        databaseName: "langfuse",
        targetTable: "events_current",
        logicalBatchId: `logical-${input.id}`,
        fenceGeneration: 1n,
        label: `cleaner_${input.id}`,
        payloadHash: "b".repeat(64),
        canonicalObjectKey: `canonical/${input.id}.json`,
        partitionDate: new Date("2026-07-01T00:00:00.000Z"),
        status: input.status === "VISIBLE" ? "VISIBLE" : "FAILED",
        visibleAt:
          input.status === "VISIBLE"
            ? new Date("2026-07-09T00:00:00.000Z")
            : null,
        totalRows: input.status === "VISIBLE" ? 1 : null,
        filteredRows: input.status === "VISIBLE" ? 0 : null,
        createdAt: new Date("2026-07-09T00:00:00.000Z"),
      },
    });
    await prisma.analyticsIngestionCandidate.create({
      data: {
        operationId: input.id,
        projectId,
        candidateKey: `candidate-${input.id}`,
        entityType: "EVENT",
        entityKey: `entity-${input.id}`,
        owningTraceId: `trace-${input.id}`,
        partitionDate: new Date("2026-07-01T00:00:00.000Z"),
        sourceVersion: 1n,
        canonicalPayloadHash: "c".repeat(64),
        disposition:
          input.status === "QUARANTINED" ? "QUARANTINED" : "LOAD_REQUIRED",
        loadBatchId,
      },
    });
  }

  beforeAll(async () => {
    await prisma.organization.create({
      data: { id: organizationId, name: "Analytics cleaner test" },
    });
    await prisma.project.create({
      data: {
        id: projectId,
        name: "Analytics cleaner test",
        orgId: organizationId,
      },
    });
    await createOperation({
      id: `eligible-${suffix}`,
      acceptedAt: new Date("2026-07-01T00:00:00.000Z"),
      recoverableUntil: new Date("2026-07-08T00:00:00.000Z"),
      status: "VISIBLE",
      terminalAt: new Date("2026-07-09T00:00:00.000Z"),
    });
    await createOperation({
      id: `active-${suffix}`,
      acceptedAt: new Date("2026-07-01T00:00:00.000Z"),
      recoverableUntil: new Date("2026-07-08T00:00:00.000Z"),
      status: "RETRYING",
      terminalAt: null,
    });
    await createOperation({
      id: `quarantined-${suffix}`,
      acceptedAt: new Date("2026-07-01T00:00:00.000Z"),
      recoverableUntil: new Date("2026-07-08T00:00:00.000Z"),
      status: "QUARANTINED",
      terminalAt: new Date("2026-07-09T00:00:00.000Z"),
    });
    await createOperation({
      id: `uncheckpointed-${suffix}`,
      acceptedAt: new Date("2026-07-15T00:00:00.000Z"),
      recoverableUntil: new Date("2026-07-22T00:00:00.000Z"),
      status: "VISIBLE",
      terminalAt: new Date("2026-07-23T00:00:00.000Z"),
    });
    await prisma.analyticsEntityHead.create({
      data: {
        projectId,
        operationId: `eligible-${suffix}`,
        entityType: "EVENT",
        entityKey: `entity-eligible-${suffix}`,
        owningTraceId: `trace-eligible-${suffix}`,
        sourceVersion: 1n,
        canonicalPayloadHash: "c".repeat(64),
        partitionDate: new Date("2026-07-01T00:00:00.000Z"),
        canonicalizerVersion: "r1a-v1",
        fenceGeneration: 1n,
      },
    });
    await prisma.analyticsCheckpointGeneration.create({
      data: {
        generation: checkpointGeneration,
        status: "SEALED",
        leaseOwner: "checkpoint-test",
        leaseExpiresAt: new Date("2026-08-02T00:00:00.000Z"),
        operationHighWatermarkAcceptedAt: new Date("2026-07-10T00:00:00.000Z"),
        operationHighWatermarkAcceptedAtNanos:
          BigInt(new Date("2026-07-10T00:00:00.000Z").getTime()) * 1_000_000n,
        loadHighWatermarkCreatedAt: new Date("2026-07-10T00:00:00.000Z"),
        deletionHighWatermarkCreatedAt: new Date("2026-07-10T00:00:00.000Z"),
        keyId: "test-key",
        manifestHash: "d".repeat(64),
        signature: "test-signature",
        sealedAt: new Date("2026-07-11T00:00:00.000Z"),
      },
    });
  }, 30_000);

  afterAll(async () => {
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.analyticsCheckpointGeneration.deleteMany({
      where: { generation: checkpointGeneration },
    });
    await prisma.$disconnect();
  }, 30_000);

  it("compacts only successful checkpointed children beyond the safety delay", async () => {
    await expect(
      compactAnalyticsControlState({
        client: prisma,
        now: new Date("2026-08-01T00:00:00.000Z"),
        limit: 10,
      }),
    ).resolves.toEqual({ operationsCompacted: 1, childRowsDeleted: 2 });

    const operations = await prisma.analyticsIngestionOperation.findMany({
      where: { projectId },
      include: { candidates: true, loadBatches: true, entityHeads: true },
    });
    const eligible = operations.find(({ id }) => id === `eligible-${suffix}`)!;
    expect(eligible.candidates).toHaveLength(0);
    expect(eligible.loadBatches).toHaveLength(0);
    expect(eligible.entityHeads).toHaveLength(1);
    expect(eligible.frozenManifest).toMatchObject({
      compacted: true,
      candidateCount: 1,
      loadBatchCount: 1,
      checkpointGeneration: checkpointGeneration.toString(),
    });
    for (const operation of operations.filter(
      ({ id }) => id !== `eligible-${suffix}`,
    )) {
      expect(operation.candidates).toHaveLength(1);
      expect(operation.loadBatches).toHaveLength(1);
    }
  });
});
