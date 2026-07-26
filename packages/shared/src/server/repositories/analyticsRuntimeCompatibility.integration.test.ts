import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;
if (process.env.DORIS_POC_ENABLED === "1" && !controlDatabaseUrl) {
  throw new Error("Doris harness must provide an isolated control database");
}

describe.skipIf(!controlDatabaseUrl)(
  "analytics runtime compatibility gate",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const queueNamespaceFingerprint = "f".repeat(64);
    let deployment: typeof import("./analyticsBackendDeployment.js");
    let leases: typeof import("./analyticsRuntimeLeases.js");
    let admission: typeof import("../analytics-persistence/analyticsBackendAdmission.js");

    beforeAll(async () => {
      deployment = await import("./analyticsBackendDeployment.js");
      leases = await import("./analyticsRuntimeLeases.js");
      admission =
        await import("../analytics-persistence/analyticsBackendAdmission.js");
    });

    beforeEach(async () => {
      await prisma.analyticsBackendClaimLease.deleteMany();
      await prisma.analyticsRuntimeCapabilityContract.deleteMany();
      await prisma.analyticsRuntimeLease.deleteMany();
      await prisma.analyticsBackendDeploymentTransition.deleteMany();
      await prisma.analyticsBackendDeploymentState.deleteMany();
    });

    afterAll(async () => {
      await prisma.$disconnect();
    });

    it("requires compatible live inventory and a quiesced rollback build", async () => {
      const now = new Date();
      const workloadEpochFingerprint =
        deployment.fingerprintAnalyticsWorkloadEpoch(
          `runtime-compatibility-${suffix}`,
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
      const register = (input: {
        instanceId: string;
        component: "web" | "worker";
        buildId: string;
      }) =>
        leases.registerAnalyticsRuntimeLease({
          client: prisma,
          ...input,
          backend: "doris",
          deploymentGeneration: startup.marker.generation,
          workloadEpochFingerprint,
          queueNamespaceFingerprint,
          foundationContractVersion: 1,
          acceptedSchemaVersion: { min: 1, max: 2 },
          acceptedCanonicalVersion: { min: 1, max: 2 },
          capabilityContracts: [],
          leaseMs: 120_000,
          now,
        });
      await register({
        instanceId: "compat-web",
        component: "web",
        buildId: "release-a",
      });
      const worker = await register({
        instanceId: "compat-worker",
        component: "worker",
        buildId: "release-a",
      });
      const rollbackWeb = await register({
        instanceId: "compat-rollback-web",
        component: "web",
        buildId: "rollback-a",
      });
      const rollbackWorker = await register({
        instanceId: "compat-rollback-worker",
        component: "worker",
        buildId: "rollback-a",
      });
      await leases.markAnalyticsRuntimeQuiesced({
        client: prisma,
        runtimeLeaseId: rollbackWeb.lease.id,
        now: new Date(now.getTime() + 1_000),
      });
      await leases.markAnalyticsRuntimeQuiesced({
        client: prisma,
        runtimeLeaseId: rollbackWorker.lease.id,
        now: new Date(now.getTime() + 1_000),
      });

      const gate = {
        client: prisma,
        expectedRuntimeInstanceIds: ["compat-web", "compat-worker"],
        requiredSchemaVersions: [1, 2],
        requiredCanonicalVersions: [1, 2],
        rollbackBuildId: "rollback-a",
        now: new Date(now.getTime() + 2_000),
      } as const;
      await expect(
        leases.assertAnalyticsContractRolloutReady(gate),
      ).resolves.toBeUndefined();
      await expect(
        leases.assertAnalyticsContractRolloutReady({
          ...gate,
          requiredSchemaVersions: [1, 2, 3],
        }),
      ).rejects.toThrow(/LIVE_RUNTIME_INCOMPATIBLE/);
      await expect(
        leases.assertAnalyticsContractRolloutReady({
          ...gate,
          rollbackBuildId: "untested",
        }),
      ).rejects.toThrow(/ROLLBACK_NOT_ATTESTED/);

      await expect(
        prisma.$transaction((transaction) =>
          admission.lockAnalyticsAdmission({
            transaction,
            runtimeLeaseId: worker.lease.id,
            expectedBackend: "doris",
            expectedDeploymentGeneration: startup.marker.generation,
            action: "foundation",
            requiredContract: {
              schemaVersion: 2,
              canonicalizerVersion: "2",
            },
          }),
        ),
      ).resolves.toMatchObject({ admittingRuntimeLeaseId: worker.lease.id });
      await expect(
        prisma.$transaction((transaction) =>
          admission.lockAnalyticsAdmission({
            transaction,
            runtimeLeaseId: worker.lease.id,
            expectedBackend: "doris",
            expectedDeploymentGeneration: startup.marker.generation,
            action: "foundation",
            requiredContract: {
              schemaVersion: 3,
              canonicalizerVersion: "2",
            },
          }),
        ),
      ).rejects.toThrow(/does not accept work contract/);
    });
  },
);
