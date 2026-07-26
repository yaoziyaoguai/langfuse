import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)("analytics deletion operations", () => {
  const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const organizationId = `deletion-org-${suffix}`;
  const projectId = `deletion-project-${suffix}`;
  const projectDeletionProjectId = `project-deletion-${suffix}`;
  const traceId = `trace-${suffix}`;
  let repository: typeof import("./analyticsDeletionOperations.js");

  beforeAll(async () => {
    repository = await import("./analyticsDeletionOperations.js");
    await prisma.organization.create({
      data: { id: organizationId, name: "Deletion operation test" },
    });
    await prisma.project.create({
      data: { id: projectId, orgId: organizationId, name: "Deletion test" },
    });
  });

  afterAll(async () => {
    await prisma.project.deleteMany({ where: { orgId: organizationId } });
    await prisma.analyticsDeletionOperation.deleteMany({
      where: { organizationId },
    });
    await prisma.analyticsProjectDeletionGeneration.deleteMany({
      where: { projectId: { contains: suffix } },
    });
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.$disconnect();
  });

  it("schedules one idempotent trace operation and advances truthful phases", async () => {
    const request = {
      client: prisma,
      projectId,
      organizationId,
      traceIds: [traceId, traceId],
      requester: { principalType: "user" as const, principalId: "user-1" },
      now: new Date("2026-07-18T00:00:00.000Z"),
    };
    const first = await repository.scheduleTraceDeletionOperations(request);
    const repeated = await repository.scheduleTraceDeletionOperations(request);

    expect(first).toHaveLength(1);
    expect(repeated[0]?.operation.id).toBe(first[0]?.operation.id);
    expect(first[0]).toMatchObject({ traceId, generation: 1n });
    await expect(
      repository.markDeletionBarrierVisible({
        client: prisma,
        operationId: first[0]!.operation.id,
        projectId,
        scope: "TRACE",
        traceId,
        generation: 1n,
        barrierLabel: "lf_trace_delete_test",
        now: new Date("2026-07-18T00:00:01.000Z"),
      }),
    ).resolves.toBe(true);
    await expect(
      repository.completeDeletionOperation({
        client: prisma,
        operationId: first[0]!.operation.id,
        projectId,
        scope: "TRACE",
        traceId,
        generation: 1n,
        now: new Date("2026-07-18T00:00:02.000Z"),
      }),
    ).resolves.toBe(true);
    await expect(
      repository.findDeletionOperationForProject({
        client: prisma,
        operationId: first[0]!.operation.id,
        projectId,
      }),
    ).resolves.toMatchObject({
      status: "COMPLETED",
      phase: "completed",
      logicallyInvisible: true,
    });
  });

  it("retains one project generation and operation outside the project row", async () => {
    await prisma.project.create({
      data: {
        id: projectDeletionProjectId,
        orgId: organizationId,
        name: "Project deletion operation test",
      },
    });
    const request = {
      client: prisma,
      projectId: projectDeletionProjectId,
      organizationId,
      requester: { principalType: "user" as const, principalId: "owner-1" },
      now: new Date("2026-07-18T01:00:00.000Z"),
    };
    const first = await repository.scheduleProjectDeletionOperation(request);
    const repeated = await repository.scheduleProjectDeletionOperation(request);
    expect(repeated.id).toBe(first.id);
    expect(first).toMatchObject({ scope: "PROJECT", generation: 1n });
    await expect(
      repository.findLatestProjectDeletionOperation({
        client: prisma,
        projectId: projectDeletionProjectId,
        organizationId,
      }),
    ).resolves.toMatchObject({ id: first.id, generation: 1n });
  });

  it("completes outstanding trace operations behind the project deletion fence", async () => {
    const supersededProjectId = `superseded-project-${suffix}`;
    await prisma.project.create({
      data: {
        id: supersededProjectId,
        orgId: organizationId,
        name: "Superseded trace deletion test",
      },
    });
    const supersededTraceId = `superseded-${suffix}`;
    const [traceOperation] = await repository.scheduleTraceDeletionOperations({
      client: prisma,
      projectId: supersededProjectId,
      organizationId,
      traceIds: [supersededTraceId],
      requester: { principalType: "system", principalId: "test" },
    });
    const projectOperation = await repository.scheduleProjectDeletionOperation({
      client: prisma,
      projectId: supersededProjectId,
      organizationId,
      requester: { principalType: "system", principalId: "test" },
    });
    const claimed = await repository.claimDeletionOperation({
      client: prisma,
      operationId: projectOperation.id,
      projectId: supersededProjectId,
      owner: "project-worker",
    });
    await repository.markDeletionBarrierVisible({
      client: prisma,
      operationId: projectOperation.id,
      projectId: supersededProjectId,
      scope: "PROJECT",
      generation: projectOperation.generation,
      barrierLabel: "project-barrier",
      lease: { owner: "project-worker", fence: claimed!.workerFence },
    });

    await expect(
      repository.finalizeProjectDeletionOperation({
        client: prisma,
        projectOperationId: projectOperation.id,
        projectId: supersededProjectId,
        organizationId,
        projectGeneration: projectOperation.generation,
        lease: { owner: "project-worker", fence: claimed!.workerFence },
      }),
    ).resolves.toBe(true);
    await expect(
      prisma.analyticsDeletionOperation.findUniqueOrThrow({
        where: { id: traceOperation!.operation.id },
      }),
    ).resolves.toMatchObject({
      status: "COMPLETED",
      phase: "completed_by_project_deletion",
      logicallyInvisible: true,
    });
  });

  it("rechecks pre-barrier ingestion while holding the Project finalize lock", async () => {
    const drainProjectId = `drain-project-${suffix}`;
    await prisma.project.create({
      data: {
        id: drainProjectId,
        orgId: organizationId,
        name: "Project ingestion drain test",
      },
    });
    const projectOperation = await repository.scheduleProjectDeletionOperation({
      client: prisma,
      projectId: drainProjectId,
      organizationId,
      requester: { principalType: "system", principalId: "test" },
    });
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: `drain-ingestion-${suffix}`,
        acceptanceSequence: projectOperation.ingestionBarrierSequence! - 1n,
        projectId: drainProjectId,
        sourceOperationId: `drain-source-${suffix}`,
        sourceChecksum: "a".repeat(64),
        rawObjectKey: `raw/drain-${suffix}`,
        acceptedAt: new Date("2026-07-18T00:30:00.000Z"),
        acceptedAtNanos: 1_784_378_200_000_000_000n,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 1,
        recoverableUntil: new Date("2026-07-25T00:00:00.000Z"),
        statusExpiresAt: new Date("2026-08-18T00:00:00.000Z"),
      },
    });
    const claimed = await repository.claimDeletionOperation({
      client: prisma,
      operationId: projectOperation.id,
      projectId: drainProjectId,
      owner: "drain-worker",
    });
    await repository.markDeletionBarrierVisible({
      client: prisma,
      operationId: projectOperation.id,
      projectId: drainProjectId,
      scope: "PROJECT",
      generation: projectOperation.generation,
      barrierLabel: "drain-project-barrier",
      lease: { owner: "drain-worker", fence: claimed!.workerFence },
    });

    await expect(
      repository.finalizeProjectDeletionOperation({
        client: prisma,
        projectOperationId: projectOperation.id,
        projectId: drainProjectId,
        organizationId,
        projectGeneration: projectOperation.generation,
        lease: { owner: "drain-worker", fence: claimed!.workerFence },
      }),
    ).resolves.toBe(false);
    await expect(
      prisma.project.findUnique({ where: { id: drainProjectId } }),
    ).resolves.toMatchObject({ id: drainProjectId });
  });

  it("does not complete while pre-barrier ingestion remains nonterminal", async () => {
    const operationId = `ingestion-${suffix}`;
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: operationId,
        projectId,
        sourceOperationId: `source-${suffix}`,
        sourceChecksum: "checksum",
        rawObjectKey: `raw/${suffix}`,
        acceptedAt: new Date("2026-07-18T00:30:00.000Z"),
        acceptedAtNanos: 1_784_378_200_000_000_000n,
        createdAt: new Date("2026-07-18T00:30:00.000Z"),
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 1,
        recoverableUntil: new Date("2026-07-25T00:00:00.000Z"),
        statusExpiresAt: new Date("2026-08-18T00:00:00.000Z"),
      },
    });
    const check = {
      client: prisma,
      projectId,
      barrierCreatedAt: new Date("2026-07-18T01:00:00.000Z"),
    };
    await expect(repository.hasPreBarrierIngestionWork(check)).resolves.toBe(
      true,
    );
    await prisma.analyticsIngestionOperation.update({
      where: { id: operationId },
      data: { status: "VISIBLE" },
    });
    await expect(repository.hasPreBarrierIngestionWork(check)).resolves.toBe(
      false,
    );
  });

  it("fences stale deletion workers from changing operation or tombstone state", async () => {
    const fencedTraceId = `fenced-${suffix}`;
    const [scheduled] = await repository.scheduleTraceDeletionOperations({
      client: prisma,
      projectId,
      organizationId,
      traceIds: [fencedTraceId],
      requester: { principalType: "system", principalId: "test" },
    });
    const first = await repository.claimDeletionOperation({
      client: prisma,
      operationId: scheduled!.operation.id,
      projectId,
      owner: "worker-1",
      now: new Date("2026-07-18T02:00:00.000Z"),
      leaseMs: 1_000,
    });
    expect(first).toMatchObject({ leaseOwner: "worker-1", workerFence: 1n });
    await expect(
      repository.claimDeletionOperation({
        client: prisma,
        operationId: scheduled!.operation.id,
        projectId,
        owner: "worker-2",
        now: new Date("2026-07-18T02:00:00.500Z"),
      }),
    ).resolves.toBeNull();
    const second = await repository.claimDeletionOperation({
      client: prisma,
      operationId: scheduled!.operation.id,
      projectId,
      owner: "worker-2",
      now: new Date("2026-07-18T02:00:01.001Z"),
    });
    expect(second).toMatchObject({ leaseOwner: "worker-2", workerFence: 2n });

    await expect(
      repository.markDeletionBarrierVisible({
        client: prisma,
        operationId: scheduled!.operation.id,
        projectId,
        scope: "TRACE",
        traceId: fencedTraceId,
        generation: scheduled!.generation,
        barrierLabel: "stale-barrier",
        lease: { owner: "worker-1", fence: 1n },
      }),
    ).resolves.toBe(false);
    await expect(
      prisma.analyticsDeletionTombstone.findUniqueOrThrow({
        where: { projectId_traceId: { projectId, traceId: fencedTraceId } },
      }),
    ).resolves.toMatchObject({
      status: "RETRYING",
      barrierLabel: null,
      barrierVisibleAt: null,
    });

    await expect(
      repository.markDeletionBarrierVisible({
        client: prisma,
        operationId: scheduled!.operation.id,
        projectId,
        scope: "TRACE",
        traceId: fencedTraceId,
        generation: scheduled!.generation,
        barrierLabel: "current-barrier",
        lease: { owner: "worker-2", fence: 2n },
      }),
    ).resolves.toBe(true);
    await expect(
      repository.completeDeletionOperation({
        client: prisma,
        operationId: scheduled!.operation.id,
        projectId,
        scope: "TRACE",
        traceId: fencedTraceId,
        generation: scheduled!.generation,
        lease: { owner: "worker-1", fence: 1n },
      }),
    ).resolves.toBe(false);
    await expect(
      prisma.analyticsDeletionTombstone.findUniqueOrThrow({
        where: { projectId_traceId: { projectId, traceId: fencedTraceId } },
      }),
    ).resolves.toMatchObject({ status: "SCHEDULED", completedAt: null });
  });
});
