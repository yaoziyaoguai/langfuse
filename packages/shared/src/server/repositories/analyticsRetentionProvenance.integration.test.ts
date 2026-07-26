import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;
if (process.env.DORIS_POC_ENABLED === "1" && !controlDatabaseUrl) {
  throw new Error("Doris harness must provide an isolated control database");
}

describe.skipIf(!controlDatabaseUrl)(
  "analytics retention deployment provenance",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const queueNamespaceFingerprint = "f".repeat(64);
    let deployment: typeof import("./analyticsBackendDeployment.js");
    let leases: typeof import("./analyticsRuntimeLeases.js");
    let retention: typeof import("./analyticsRetention.js");

    beforeAll(async () => {
      deployment = await import("./analyticsBackendDeployment.js");
      leases = await import("./analyticsRuntimeLeases.js");
      retention = await import("./analyticsRetention.js");
    });

    beforeEach(async () => {
      await prisma.analyticsRetentionState.deleteMany();
      await prisma.analyticsRetentionRun.deleteMany();
      await prisma.analyticsBackendClaimLease.deleteMany();
      await prisma.analyticsRuntimeCapabilityContract.deleteMany();
      await prisma.analyticsRuntimeLease.deleteMany();
      await prisma.analyticsBackendDeploymentTransition.deleteMany();
      await prisma.analyticsBackendDeploymentState.deleteMany();
    });

    afterAll(async () => {
      await prisma.$disconnect();
    });

    it("revalidates a recovery lease without replacing the original producer", async () => {
      const now = new Date();
      const workloadEpochFingerprint =
        deployment.fingerprintAnalyticsWorkloadEpoch(
          `retention-provenance-${suffix}`,
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

      const register = (instanceId: string) =>
        leases.registerAnalyticsRuntimeLease({
          client: prisma,
          component: "worker",
          instanceId,
          backend: "doris",
          deploymentGeneration: startup.marker.generation,
          workloadEpochFingerprint,
          queueNamespaceFingerprint,
          buildId: "u0-retention-provenance",
          foundationContractVersion: 1,
          acceptedSchemaVersion: { min: 1, max: 3 },
          acceptedCanonicalVersion: { min: 1, max: 1 },
          capabilityContracts: [],
          leaseMs: 120_000,
          now,
        });
      const producer = await register("retention-producer");
      const recovery = await register("retention-recovery");
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

      const created = await retention.startOrResumeAnalyticsRetention({
        client: prisma,
        retentionDays: 30,
        admissionContext: producerContext,
      });
      expect(created).toMatchObject({
        analyticsBackend: "DORIS",
        deploymentGeneration: startup.marker.generation,
        workloadEpochFingerprint,
        runtimeContractVersion: 1,
        producerRuntimeLeaseId: producer.lease.id,
      });

      await expect(
        retention.startOrResumeAnalyticsRetention({
          client: prisma,
          retentionDays: 29,
          admissionContext: recoveryContext,
        }),
      ).resolves.toMatchObject({
        id: created?.id,
        producerRuntimeLeaseId: producer.lease.id,
      });
      await expect(
        retention.startOrResumeAnalyticsRetention({
          client: prisma,
          retentionDays: 29,
        }),
      ).rejects.toThrow(/legacy|fenced|deployment/i);
    });
  },
);
