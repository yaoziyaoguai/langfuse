import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import type { DorisAnalyticsLifecycleRuntime } from "../../services/dorisAnalyticsLifecycle";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)("Doris trace deletion barriers", () => {
  const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const organizationId = `trace-barrier-org-${suffix}`;
  const projectId = `trace-barrier-project-${suffix}`;
  const traceId = `trace-barrier-trace-${suffix}`;

  beforeAll(async () => {
    await prisma.organization.create({
      data: { id: organizationId, name: "Trace barrier test" },
    });
    await prisma.project.create({
      data: { id: projectId, orgId: organizationId, name: "Barrier test" },
    });
  });

  afterAll(async () => {
    await prisma.project.deleteMany({ where: { id: projectId } });
    await prisma.analyticsDeletionOperation.deleteMany({
      where: { projectId },
    });
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.$disconnect();
  });

  it("keeps the operation retrying and truthfully visible when Doris cannot prove the barrier", async () => {
    const { scheduleTraceDeletionOperations } =
      await import("@langfuse/shared/src/server");
    const { processAnalyticsTraceDelete } =
      await import("./processAnalyticsTraceDelete");
    const [scheduled] = await scheduleTraceDeletionOperations({
      client: prisma,
      projectId,
      organizationId,
      traceIds: [traceId],
      requester: { principalType: "system", principalId: "barrier-test" },
    });
    const lifecycle = {
      store: {
        publishTraceTombstone: vi.fn().mockResolvedValue({
          projectId,
          traceId,
          generation: scheduled!.generation,
          visible: false,
          barrierLabel: "not-visible",
        }),
      },
      materializedDeletion: {
        deleteHeads: vi.fn(),
      },
    } as unknown as DorisAnalyticsLifecycleRuntime;

    await expect(
      processAnalyticsTraceDelete(
        projectId,
        {
          operationId: scheduled!.operation.id,
          traceId,
          generation: scheduled!.generation,
        },
        lifecycle,
      ),
    ).rejects.toThrow("Trace deletion barrier is not visible");

    await expect(
      prisma.analyticsDeletionOperation.findUniqueOrThrow({
        where: { id: scheduled!.operation.id },
      }),
    ).resolves.toMatchObject({
      status: "RETRYING",
      phase: "visibility_barrier",
      logicallyInvisible: false,
      leaseOwner: null,
      cancellationReasonCode: "BARRIER_NOT_VISIBLE",
    });
    expect(lifecycle.materializedDeletion.deleteHeads).not.toHaveBeenCalled();
  });
});
