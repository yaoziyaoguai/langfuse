import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;
if (process.env.DORIS_POC_ENABLED === "1" && !controlDatabaseUrl) {
  throw new Error("Doris harness must provide an isolated control database");
}

describe.skipIf(!controlDatabaseUrl)(
  "analytics deletion deployment provenance",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const queueNamespaceFingerprint = "f".repeat(64);
    const organizationId = `deletion-provenance-org-${suffix}`;
    const projectId = `deletion-provenance-project-${suffix}`;
    let deployment: typeof import("./analyticsBackendDeployment.js");
    let leases: typeof import("./analyticsRuntimeLeases.js");
    let deletion: typeof import("./analyticsDeletionOperations.js");

    beforeAll(async () => {
      deployment = await import("./analyticsBackendDeployment.js");
      leases = await import("./analyticsRuntimeLeases.js");
      deletion = await import("./analyticsDeletionOperations.js");
      await prisma.organization.create({
        data: { id: organizationId, name: "Deletion provenance test" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          orgId: organizationId,
          name: "Deletion provenance test",
        },
      });
    });

    beforeEach(async () => {
      await prisma.analyticsDeletionOperation.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsDeletionTombstone.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsProjectDeletionGeneration.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsBackendClaimLease.deleteMany();
      await prisma.analyticsRuntimeCapabilityContract.deleteMany();
      await prisma.analyticsRuntimeLease.deleteMany();
      await prisma.analyticsBackendDeploymentTransition.deleteMany();
      await prisma.analyticsBackendDeploymentState.deleteMany();
    });

    afterAll(async () => {
      await prisma.analyticsDeletionOperation.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsDeletionTombstone.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsProjectDeletionGeneration.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsBackendClaimLease.deleteMany();
      await prisma.analyticsRuntimeCapabilityContract.deleteMany();
      await prisma.analyticsRuntimeLease.deleteMany();
      await prisma.analyticsBackendDeploymentTransition.deleteMany();
      await prisma.analyticsBackendDeploymentState.deleteMany();
      await prisma.project.deleteMany({ where: { id: projectId } });
      await prisma.organization.deleteMany({ where: { id: organizationId } });
      await prisma.$disconnect();
    });

    it("revalidates the current runtime without replacing the original producer", async () => {
      const now = new Date();
      const workloadEpochFingerprint =
        deployment.fingerprintAnalyticsWorkloadEpoch(
          `deletion-provenance-${suffix}`,
        );
      const startup = await deployment.resolveAnalyticsBackendStartup({
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
        leases.registerAnalyticsRuntimeLease({
          client: prisma,
          component,
          instanceId,
          backend: "doris",
          deploymentGeneration: startup.marker.generation,
          workloadEpochFingerprint,
          queueNamespaceFingerprint,
          buildId: "u0-deletion-provenance",
          foundationContractVersion: 1,
          acceptedSchemaVersion: { min: 1, max: 3 },
          acceptedCanonicalVersion: { min: 1, max: 1 },
          capabilityContracts: [],
          leaseMs: 120_000,
          now,
        });
      const producer = await register("web-producer", "web");
      const recovery = await register("worker-recovery", "worker");
      const producerContext = {
        runtimeLeaseId: producer.lease.id,
        backend: "doris" as const,
        deploymentGeneration: startup.marker.generation,
      };
      const recoveryContext = {
        runtimeLeaseId: recovery.lease.id,
        backend: "doris" as const,
        deploymentGeneration: startup.marker.generation,
      };

      const [first] = await deletion.scheduleTraceDeletionOperations({
        client: prisma,
        projectId,
        organizationId,
        traceIds: [`trace-${suffix}`],
        requester: { principalType: "user", principalId: "owner-1" },
        analyticsAdmissionContext: producerContext,
        now: new Date(now.getTime() + 1_000),
      });
      const [repeated] = await deletion.scheduleTraceDeletionOperations({
        client: prisma,
        projectId,
        organizationId,
        traceIds: [`trace-${suffix}`],
        requester: { principalType: "system", principalId: "recovery" },
        analyticsAdmissionContext: recoveryContext,
        now: new Date(now.getTime() + 2_000),
      });

      expect(repeated?.operation.id).toBe(first?.operation.id);
      expect(repeated?.operation).toMatchObject({
        analyticsBackend: "DORIS",
        deploymentGeneration: startup.marker.generation,
        workloadEpochFingerprint,
        runtimeContractVersion: 1,
        producerRuntimeLeaseId: producer.lease.id,
      });

      await prisma.analyticsDeletionOperation.update({
        where: { id: first!.operation.id },
        data: { workloadEpochFingerprint: "b".repeat(64) },
      });
      await expect(
        deletion.scheduleTraceDeletionOperations({
          client: prisma,
          projectId,
          organizationId,
          traceIds: [`trace-${suffix}`],
          requester: { principalType: "system", principalId: "recovery" },
          analyticsAdmissionContext: recoveryContext,
          now: new Date(now.getTime() + 3_000),
        }),
      ).rejects.toThrow("Analytics deletion durable provenance changed");
      await expect(
        prisma.analyticsDeletionOperation.count({ where: { projectId } }),
      ).resolves.toBe(1);
    });
  },
);
