import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)(
  "U6 analytics capability lifecycle",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const organizationId = `u6-capability-org-${suffix}`;
    const projectId = `u6-capability-project-${suffix}`;
    const datasetId = `u6-capability-dataset-${suffix}`;
    const webLeaseId = `u6-capability-web-${suffix}`;
    const workerLeaseId = `u6-capability-worker-${suffix}`;
    const epoch = "e".repeat(64);
    const queueNamespace = "q".repeat(64);
    let activations: typeof import("./analyticsCapabilityActivations.js");
    let coreBatchExports: typeof import("./analyticsCoreBatchExportsCapability.js");
    let batchExports: typeof import("./batchExportManifests.js");
    let u6: typeof import("./analyticsU6Capabilities.js");
    let experiments: typeof import("./experimentExecutions.js");

    beforeAll(async () => {
      activations = await import("./analyticsCapabilityActivations.js");
      coreBatchExports =
        await import("./analyticsCoreBatchExportsCapability.js");
      batchExports = await import("./batchExportManifests.js");
      u6 = await import("./analyticsU6Capabilities.js");
      experiments = await import("./experimentExecutions.js");
      await prisma.analyticsBackendDeploymentState.deleteMany();
      await prisma.analyticsCapabilityActivation.updateMany({
        data: {
          backend: "DORIS",
          deploymentGeneration: 0n,
          generation: 1n,
          status: "DISABLED",
          captureEnabled: false,
          captureRequired: false,
          rescanRequired: false,
          bootstrapCompletedGeneration: null,
          bootstrapEvidenceDigest: null,
          bootstrapCompletedAt: null,
          activatedAt: null,
          drainingAt: null,
        },
      });
      await prisma.organization.create({
        data: { id: organizationId, name: "U6 capability lifecycle" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          orgId: organizationId,
          name: "U6 capability lifecycle",
        },
      });
      await prisma.dataset.create({
        data: {
          id: datasetId,
          projectId,
          name: "U6 capability lifecycle",
        },
      });
      await prisma.analyticsBackendDeploymentState.create({
        data: {
          backend: "DORIS",
          generation: 1n,
          workloadEpochFingerprint: epoch,
          queueNamespaceFingerprint: queueNamespace,
          foundationContractVersion: 1,
        },
      });
      const capabilityContracts = [
        {
          capability: "CORE_BATCH_EXPORTS" as const,
          supportedContractVersion: 1,
          installedRoles: ["PRODUCER" as const],
        },
        {
          capability: "EXPERIMENTS" as const,
          supportedContractVersion: 1,
          installedRoles: ["PRODUCER" as const],
        },
        {
          capability: "DATASET_RUN_EXPORTS" as const,
          supportedContractVersion: 1,
          installedRoles: ["PRODUCER" as const],
        },
        {
          capability: "DATASET_RUN_INGESTION" as const,
          supportedContractVersion: 1,
          installedRoles: ["PRODUCER" as const],
        },
      ];
      const future = new Date(Date.now() + 3_600_000);
      await prisma.analyticsRuntimeLease.create({
        data: {
          id: webLeaseId,
          component: "WEB",
          instanceId: webLeaseId,
          backend: "DORIS",
          deploymentGeneration: 1n,
          workloadEpochFingerprint: epoch,
          queueNamespaceFingerprint: queueNamespace,
          buildId: "test-build",
          foundationContractVersion: 1,
          acceptedSchemaVersionMin: 1,
          acceptedSchemaVersionMax: 10,
          acceptedCanonicalVersionMin: 1,
          acceptedCanonicalVersionMax: 10,
          state: "ACTIVE",
          heartbeatAt: new Date(),
          leaseExpiresAt: future,
          capabilityContracts: { create: capabilityContracts },
        },
      });
      await prisma.analyticsRuntimeLease.create({
        data: {
          id: workerLeaseId,
          component: "WORKER",
          instanceId: workerLeaseId,
          backend: "DORIS",
          deploymentGeneration: 1n,
          workloadEpochFingerprint: epoch,
          queueNamespaceFingerprint: queueNamespace,
          buildId: "test-build",
          foundationContractVersion: 1,
          acceptedSchemaVersionMin: 1,
          acceptedSchemaVersionMax: 10,
          acceptedCanonicalVersionMin: 1,
          acceptedCanonicalVersionMax: 10,
          state: "ACTIVE",
          heartbeatAt: new Date(),
          leaseExpiresAt: future,
          capabilityContracts: {
            create: capabilityContracts.map((contract) => ({
              ...contract,
              installedRoles:
                contract.capability === "CORE_BATCH_EXPORTS" ||
                contract.capability === "EXPERIMENTS" ||
                contract.capability === "DATASET_RUN_EXPORTS"
                  ? (["CONSUMER", "RECOVERY"] as const)
                  : (["PRODUCER", "CONSUMER", "RECOVERY"] as const),
            })),
          },
        },
      });
    }, 60_000);

    afterAll(async () => {
      await prisma.organization.deleteMany({ where: { id: organizationId } });
      await prisma.analyticsRuntimeLease.deleteMany({
        where: { id: { in: [webLeaseId, workerLeaseId] } },
      });
      await prisma.analyticsBackendDeploymentState.deleteMany();
      await prisma.analyticsCapabilityActivation.updateMany({
        data: {
          deploymentGeneration: 0n,
          generation: 1n,
          status: "DISABLED",
          captureEnabled: false,
          captureRequired: false,
          rescanRequired: false,
          bootstrapCompletedGeneration: null,
          bootstrapEvidenceDigest: null,
          bootstrapCompletedAt: null,
          activatedAt: null,
          drainingAt: null,
        },
      });
      await prisma.$disconnect();
    }, 60_000);

    const completeAndActivate = async (
      capability:
        | "coreBatchExports"
        | "experiments"
        | "datasetRunExports"
        | "datasetRunIngestion",
      activationGeneration: bigint,
    ) => {
      const completed = await activations.completeAnalyticsCapabilityBootstrap({
        client: prisma,
        capability,
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: activationGeneration,
        verifyDurableBootstrap:
          capability === "coreBatchExports"
            ? (transaction) =>
                coreBatchExports.verifyDurableCoreBatchExportsBootstrap(
                  transaction,
                  {
                    deploymentGeneration: 1n,
                    activationGeneration,
                  },
                )
            : (transaction) =>
                u6.verifyDurableU6CapabilityBootstrap(transaction, {
                  capability,
                  deploymentGeneration: 1n,
                  activationGeneration,
                }),
      });
      if (!completed.bootstrapEvidenceDigest) {
        throw new Error("Expected bootstrap evidence");
      }
      return activations.activateAnalyticsCapability({
        client: prisma,
        capability,
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: activationGeneration,
        expectedRuntimeInstanceIds: [webLeaseId, workerLeaseId],
        expectedBootstrapEvidenceDigest: completed.bootstrapEvidenceDigest,
      });
    };

    it("enforces dependency order, durable drain, and dependent-first rollback", async () => {
      const experimentDark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "experiments",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
      });
      await expect(
        completeAndActivate("experiments", experimentDark.generation),
      ).rejects.toThrow(/dependency/i);

      const ingestionDark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "datasetRunIngestion",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
      });
      await expect(
        completeAndActivate("datasetRunIngestion", ingestionDark.generation),
      ).resolves.toMatchObject({ status: "ACTIVE" });
      await expect(
        completeAndActivate("experiments", experimentDark.generation),
      ).resolves.toMatchObject({ status: "ACTIVE" });

      await expect(
        activations.beginAnalyticsCapabilityDrain({
          client: prisma,
          capability: "datasetRunIngestion",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: ingestionDark.generation,
        }),
      ).rejects.toThrow(/dependent/i);

      const run = await experiments.createDorisExperimentExecutionIntent({
        client: prisma,
        admissionContext: {
          runtimeLeaseId: webLeaseId,
          backend: "doris",
          deploymentGeneration: 1n,
        },
        projectId,
        datasetId,
        name: `pending-experiment-${suffix}`,
        metadata: {},
      });
      await activations.beginAnalyticsCapabilityDrain({
        client: prisma,
        capability: "experiments",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: experimentDark.generation,
      });
      await expect(
        activations.disableAnalyticsCapability({
          client: prisma,
          capability: "experiments",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: experimentDark.generation,
          captureRequired: false,
          rescanRequired: false,
          verifyDurableDrain: (transaction, provenance) =>
            u6.verifyDurableU6CapabilityDrain(transaction, provenance),
        }),
      ).rejects.toThrow(/durable work/i);
      await experiments.quarantineExperimentExecutionDispatch({
        client: prisma,
        projectId,
        runId: run.id,
        expectedGeneration: 1,
        failureCode: "TEST_DRAIN",
      });
      await expect(
        activations.disableAnalyticsCapability({
          client: prisma,
          capability: "experiments",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: experimentDark.generation,
          captureRequired: false,
          rescanRequired: false,
          verifyDurableDrain: (transaction, provenance) =>
            u6.verifyDurableU6CapabilityDrain(transaction, provenance),
        }),
      ).resolves.toMatchObject({ status: "DISABLED" });
      await expect(
        activations.beginAnalyticsCapabilityDrain({
          client: prisma,
          capability: "datasetRunIngestion",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: ingestionDark.generation,
        }),
      ).resolves.toMatchObject({ status: "DRAINING" });
      await expect(
        activations.disableAnalyticsCapability({
          client: prisma,
          capability: "datasetRunIngestion",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: ingestionDark.generation,
          captureRequired: false,
          rescanRequired: false,
          verifyDurableDrain: (transaction, provenance) =>
            u6.verifyDurableU6CapabilityDrain(transaction, provenance),
        }),
      ).resolves.toMatchObject({ status: "DISABLED" });

      const exportDark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "datasetRunExports",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
      });
      await expect(
        completeAndActivate("datasetRunExports", exportDark.generation),
      ).rejects.toThrow(/dependency/i);
      const coreDark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "coreBatchExports",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
      });
      await completeAndActivate("coreBatchExports", coreDark.generation);
      await expect(
        completeAndActivate("datasetRunExports", exportDark.generation),
      ).resolves.toMatchObject({ status: "ACTIVE" });
      await expect(
        activations.beginAnalyticsCapabilityDrain({
          client: prisma,
          capability: "coreBatchExports",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: coreDark.generation,
        }),
      ).rejects.toThrow(/dependent/i);

      await activations.beginAnalyticsCapabilityDrain({
        client: prisma,
        capability: "datasetRunExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: exportDark.generation,
      });
      await activations.disableAnalyticsCapability({
        client: prisma,
        capability: "datasetRunExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: exportDark.generation,
        captureRequired: false,
        rescanRequired: false,
        verifyDurableDrain: (transaction, provenance) =>
          u6.verifyDurableU6CapabilityDrain(transaction, provenance),
      });

      const pendingExport = await batchExports.createDorisBatchExportIntent({
        client: prisma,
        admissionContext: {
          runtimeLeaseId: webLeaseId,
          backend: "doris",
          deploymentGeneration: 1n,
        },
        projectId,
        userId: `u6-capability-user-${suffix}`,
        name: "pending core export",
        format: "CSV",
        query: { tableName: "traces" },
      });
      await activations.beginAnalyticsCapabilityDrain({
        client: prisma,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: coreDark.generation,
      });
      await expect(
        activations.disableAnalyticsCapability({
          client: prisma,
          capability: "coreBatchExports",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: coreDark.generation,
          captureRequired: false,
          rescanRequired: false,
          verifyDurableDrain: (transaction, provenance) =>
            coreBatchExports.verifyDurableCoreBatchExportsDrain(
              transaction,
              provenance,
            ),
        }),
      ).rejects.toThrow(/durable work/i);
      await batchExports.cancelBatchExport({
        client: prisma,
        projectId,
        batchExportId: pendingExport.id,
      });
      await expect(
        activations.disableAnalyticsCapability({
          client: prisma,
          capability: "coreBatchExports",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: coreDark.generation,
          captureRequired: false,
          rescanRequired: false,
          verifyDurableDrain: (transaction, provenance) =>
            coreBatchExports.verifyDurableCoreBatchExportsDrain(
              transaction,
              provenance,
            ),
        }),
      ).resolves.toMatchObject({ status: "DISABLED" });
    });
  },
);
