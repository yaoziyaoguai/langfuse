import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;
if (process.env.DORIS_POC_ENABLED === "1" && !controlDatabaseUrl) {
  throw new Error("Doris harness must provide an isolated control database");
}

describe.skipIf(!controlDatabaseUrl)(
  "analytics ingestion deployment provenance",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const queueNamespaceFingerprint = "f".repeat(64);
    const organizationId = `ingestion-provenance-org-${suffix}`;
    const projectId = `ingestion-provenance-project-${suffix}`;
    let deployment: typeof import("./analyticsBackendDeployment.js");
    let leases: typeof import("./analyticsRuntimeLeases.js");
    let ingestion: typeof import("./analyticsIngestionOperations.js");
    let admission: typeof import("../analytics-persistence/acceptAnalyticsIngestion.js");

    beforeAll(async () => {
      deployment = await import("./analyticsBackendDeployment.js");
      leases = await import("./analyticsRuntimeLeases.js");
      ingestion = await import("./analyticsIngestionOperations.js");
      admission =
        await import("../analytics-persistence/acceptAnalyticsIngestion.js");
      await prisma.organization.create({
        data: { id: organizationId, name: "Ingestion provenance test" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          orgId: organizationId,
          name: "Ingestion provenance test",
        },
      });
    });

    beforeEach(async () => {
      await prisma.analyticsIngestionOutboxV2.deleteMany();
      await prisma.analyticsIngestionOperation.deleteMany({
        where: { projectId },
      });
      await prisma.analyticsBackendClaimLease.deleteMany();
      await prisma.analyticsRuntimeCapabilityContract.deleteMany();
      await prisma.analyticsRuntimeLease.deleteMany();
      await prisma.analyticsBackendDeploymentTransition.deleteMany();
      await prisma.analyticsBackendDeploymentState.deleteMany();
    });

    afterAll(async () => {
      await prisma.organization.deleteMany({ where: { id: organizationId } });
      await prisma.$disconnect();
    });

    it("revalidates the current lease while preserving the original producer", async () => {
      const now = new Date();
      const workloadEpochFingerprint =
        deployment.fingerprintAnalyticsWorkloadEpoch(
          `ingestion-provenance-${suffix}`,
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
          buildId: "u0-ingestion-provenance",
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
      const producerProvenance =
        await admission.captureAnalyticsFoundationProvenance({
          client: prisma,
          admissionContext: producerContext,
          requiredContract: {
            schemaVersion: 3,
            canonicalizerVersion: "1",
          },
        });
      if (!producerProvenance) throw new Error("Expected managed provenance");
      const acceptedAt = new Date(now.getTime() + 1_000);
      const input = {
        client: prisma,
        operationId: `managed-ingestion-${suffix}`,
        projectId,
        sourceOperationId: `managed-source-${suffix}`,
        sourceChecksum: "b".repeat(64),
        rawObjectKey: `analytics-ingestion/raw/${projectId}/managed.json`,
        acceptedAt,
        acceptedAtNanos: BigInt(acceptedAt.getTime()) * 1_000_000n,
        canonicalizerVersion: "1",
        schemaVersion: 3,
        recoverableUntil: new Date(acceptedAt.getTime() + 60_000),
        statusExpiresAt: new Date(acceptedAt.getTime() + 120_000),
        producerProvenance,
      };

      await expect(
        ingestion.createAnalyticsIngestionReceipt({
          ...input,
          admissionContext: producerContext,
        }),
      ).resolves.toMatchObject({ created: true });
      await expect(
        ingestion.createAnalyticsIngestionReceipt({
          ...input,
          admissionContext: {
            runtimeLeaseId: recovery.lease.id,
            backend: "doris",
            deploymentGeneration: startup.marker.generation,
          },
        }),
      ).resolves.toMatchObject({ created: false });
      await expect(
        prisma.analyticsIngestionOperation.findUniqueOrThrow({
          where: { id: input.operationId },
        }),
      ).resolves.toMatchObject({
        analyticsBackend: "DORIS",
        deploymentGeneration: startup.marker.generation,
        workloadEpochFingerprint,
        runtimeContractVersion: 1,
        producerRuntimeLeaseId: producer.lease.id,
      });

      await expect(
        ingestion.createAnalyticsIngestionReceipt({
          ...input,
          operationId: `tampered-ingestion-${suffix}`,
          sourceOperationId: `tampered-source-${suffix}`,
          producerProvenance: {
            ...producerProvenance,
            workloadEpochFingerprint: "c".repeat(64),
          },
          admissionContext: {
            runtimeLeaseId: recovery.lease.id,
            backend: "doris",
            deploymentGeneration: startup.marker.generation,
          },
        }),
      ).rejects.toThrow(/provenance/i);
      await expect(
        prisma.analyticsIngestionOperation.count({
          where: { id: `tampered-ingestion-${suffix}` },
        }),
      ).resolves.toBe(0);

      await prisma.analyticsRuntimeCapabilityContract.createMany({
        data: [
          {
            runtimeLeaseId: producer.lease.id,
            capability: "DATASET_RUN_INGESTION",
            supportedContractVersion: 1,
            installedRoles: ["PRODUCER"],
          },
          {
            runtimeLeaseId: recovery.lease.id,
            capability: "DATASET_RUN_INGESTION",
            supportedContractVersion: 1,
            installedRoles: ["PRODUCER", "CONSUMER", "RECOVERY"],
          },
        ],
      });
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "DATASET_RUN_INGESTION" },
        data: {
          backend: "DORIS",
          deploymentGeneration: startup.marker.generation,
          generation: 1n,
          contractVersion: 1,
          minimumRuntimeContract: 1,
          status: "ACTIVE",
          activatedAt: new Date(),
        },
      });
      try {
        const capabilityProvenance =
          await admission.captureAnalyticsFoundationProvenance({
            client: prisma,
            admissionContext: producerContext,
            capability: "datasetRunIngestion",
            requiredContract: {
              schemaVersion: 3,
              canonicalizerVersion: "1",
            },
          });
        if (!capabilityProvenance) {
          throw new Error("Expected capability provenance");
        }
        const capabilityOperationId = `managed-dataset-run-${suffix}`;
        await expect(
          ingestion.createAnalyticsIngestionReceipt({
            ...input,
            operationId: capabilityOperationId,
            sourceOperationId: `managed-dataset-run-source-${suffix}`,
            producerProvenance: capabilityProvenance,
            admissionContext: producerContext,
          }),
        ).resolves.toMatchObject({ created: true });
        await expect(
          prisma.analyticsIngestionOperation.findUniqueOrThrow({
            where: { id: capabilityOperationId },
          }),
        ).resolves.toMatchObject({
          capability: "DATASET_RUN_INGESTION",
          capabilityActivationGeneration: 1n,
          capabilityContractVersion: 1,
        });

        await prisma.analyticsCapabilityActivation.update({
          where: { capability: "DATASET_RUN_INGESTION" },
          data: { status: "DRAINING" },
        });
        await expect(
          ingestion.createAnalyticsIngestionReceipt({
            ...input,
            operationId: capabilityOperationId,
            sourceOperationId: `managed-dataset-run-source-${suffix}`,
            producerProvenance: capabilityProvenance,
            admissionContext: producerContext,
          }),
        ).rejects.toThrow(/active/i);
      } finally {
        await prisma.analyticsCapabilityActivation.update({
          where: { capability: "DATASET_RUN_INGESTION" },
          data: {
            deploymentGeneration: 0n,
            status: "DISABLED",
            activatedAt: null,
          },
        });
      }
    });
  },
);
