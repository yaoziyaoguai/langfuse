import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import { prisma as sharedPrisma } from "../../../../packages/shared/src/db";
import {
  fingerprintAnalyticsWorkloadEpoch,
  resolveAnalyticsBackendStartup,
} from "../../../../packages/shared/src/server/repositories/analyticsBackendDeployment";
import { registerAnalyticsRuntimeLease } from "../../../../packages/shared/src/server/repositories/analyticsRuntimeLeases";
import { scheduleTraceDeletionOperations } from "../../../../packages/shared/src/server/repositories/analyticsDeletionOperations";
import {
  analyticsDurableProvenanceFromRecord,
  serializeAnalyticsDurableProvenance,
} from "../../../../packages/shared/src/server/analytics-persistence/analyticsDurableProvenance";
import type { AnalyticsRuntimeAdmissionContext } from "../../../../packages/shared/src/server/analytics-persistence/analyticsBackendAdmission";
import type { DorisAnalyticsLifecycleRuntime } from "../../services/dorisAnalyticsLifecycle";

vi.mock("@langfuse/shared/src/db", async () => ({
  prisma: (await import("../../../../packages/shared/src/db")).prisma,
}));
vi.mock("@langfuse/shared/src/server", async () => ({
  ...(await import("../../../../packages/shared/src/server/analytics-persistence/analyticsBackendAdmission")),
  ...(await import("../../../../packages/shared/src/server/analytics-persistence/analyticsDurableProvenance")),
  ...(await import("../../../../packages/shared/src/server/repositories/analyticsCheckpoints")),
  ...(await import("../../../../packages/shared/src/server/repositories/analyticsDeletionOperations")),
  ...(await import("../../../../packages/shared/src/server/repositories/analyticsRuntimeLeases")),
  isDorisAnalyticsBackend: () => true,
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
    info: vi.fn(),
    warn: vi.fn(),
  },
}));
vi.mock("../../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(() => null),
}));
vi.mock("../../services/dorisAnalyticsLifecycle", () => ({
  getDorisAnalyticsLifecycleRuntime: vi.fn(() => {
    throw new Error("Unexpected default Doris lifecycle");
  }),
}));
vi.mock("./processClickhouseTraceDelete", () => ({
  deleteMediaItemsForTraces: vi.fn(),
  processClickhouseTraceDelete: vi.fn(),
}));
vi.mock("./processPostgresTraceDelete", () => ({
  processPostgresTraceDelete: vi.fn(),
}));

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)("Doris trace deletion barriers", () => {
  const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
  const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
  const organizationId = `trace-barrier-org-${suffix}`;
  const projectId = `trace-barrier-project-${suffix}`;
  const traceId = `trace-barrier-trace-${suffix}`;
  const queueNamespaceFingerprint = "f".repeat(64);
  let producerAdmissionContext: AnalyticsRuntimeAdmissionContext;
  let workerAdmissionContext: AnalyticsRuntimeAdmissionContext;

  beforeAll(async () => {
    await prisma.organization.create({
      data: { id: organizationId, name: "Trace barrier test" },
    });
    await prisma.project.create({
      data: { id: projectId, orgId: organizationId, name: "Barrier test" },
    });
    const now = new Date();
    const workloadEpochFingerprint = fingerprintAnalyticsWorkloadEpoch(
      `trace-barrier-${suffix}`,
    );
    const startup = await resolveAnalyticsBackendStartup({
      client: prisma,
      backend: "doris",
      workloadEpochFingerprint,
      queueNamespaceFingerprint,
      foundationContractVersion: 1,
      allowFreshInitialization: true,
      freshDeploymentEvidence: {
        selectedBackendEmpty: true,
        evidenceDigest: "a".repeat(64),
      },
      now,
    });
    if (startup.mode !== "READY") throw new Error("Expected fresh marker");
    const register = (instanceId: string, component: "web" | "worker") =>
      registerAnalyticsRuntimeLease({
        client: prisma,
        component,
        instanceId,
        backend: "doris",
        deploymentGeneration: startup.marker.generation,
        workloadEpochFingerprint,
        queueNamespaceFingerprint,
        buildId: "trace-barrier-test",
        foundationContractVersion: 1,
        acceptedSchemaVersion: { min: 1, max: 3 },
        acceptedCanonicalVersion: { min: 1, max: 1 },
        capabilityContracts: [],
        leaseMs: 600_000,
        now,
      });
    const producer = await register(`trace-barrier-web-${suffix}`, "web");
    const worker = await register(`trace-barrier-worker-${suffix}`, "worker");
    producerAdmissionContext = {
      runtimeLeaseId: producer.lease.id,
      backend: "doris",
      deploymentGeneration: startup.marker.generation,
    };
    workerAdmissionContext = {
      runtimeLeaseId: worker.lease.id,
      backend: "doris",
      deploymentGeneration: startup.marker.generation,
    };
  });

  afterAll(async () => {
    await prisma.project.deleteMany({ where: { id: projectId } });
    await prisma.analyticsDeletionOperation.deleteMany({
      where: { projectId },
    });
    await prisma.analyticsBackendClaimLease.deleteMany();
    await prisma.analyticsRuntimeCapabilityContract.deleteMany();
    await prisma.analyticsRuntimeLease.deleteMany();
    await prisma.analyticsBackendDeploymentTransition.deleteMany();
    await prisma.analyticsBackendDeploymentState.deleteMany();
    await prisma.organization.deleteMany({ where: { id: organizationId } });
    await prisma.$disconnect();
    await sharedPrisma.$disconnect();
  });

  it("keeps the operation retrying and truthfully visible when Doris cannot prove the barrier", async () => {
    const { processAnalyticsTraceDelete } =
      await import("./processAnalyticsTraceDelete");
    const [scheduled] = await scheduleTraceDeletionOperations({
      client: prisma,
      projectId,
      organizationId,
      traceIds: [traceId],
      requester: { principalType: "system", principalId: "barrier-test" },
      analyticsAdmissionContext: producerAdmissionContext,
    });
    const provenance = analyticsDurableProvenanceFromRecord(
      scheduled!.operation,
    );
    if (!provenance) throw new Error("Expected managed deletion provenance");
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
          analyticsProvenance: serializeAnalyticsDurableProvenance(provenance),
        },
        lifecycle,
        workerAdmissionContext,
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
