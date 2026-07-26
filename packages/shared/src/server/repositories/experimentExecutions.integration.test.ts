import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import type { AnalyticsRuntimeAdmissionContext } from "../analytics-persistence/analyticsBackendAdmission";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)(
  "Doris durable experiment execution",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const organizationId = `experiment-org-${suffix}`;
    const projectId = `experiment-project-${suffix}`;
    const datasetId = `experiment-dataset-${suffix}`;
    const epoch = "e".repeat(64);
    const queueNamespace = "q".repeat(64);
    const future = new Date(Date.now() + 3_600_000);
    let repository: typeof import("./experimentExecutions.js");
    let webContext: AnalyticsRuntimeAdmissionContext;
    let workerContext: AnalyticsRuntimeAdmissionContext;

    const createLease = async (input: {
      id: string;
      component: "WEB" | "WORKER";
      experimentRoles: Array<"PRODUCER" | "CONSUMER" | "RECOVERY">;
      datasetRunRoles: Array<"PRODUCER" | "CONSUMER" | "RECOVERY">;
    }): Promise<AnalyticsRuntimeAdmissionContext> => {
      await prisma.analyticsRuntimeLease.create({
        data: {
          id: input.id,
          component: input.component,
          instanceId: input.id,
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
            create: [
              {
                capability: "EXPERIMENTS",
                supportedContractVersion: 1,
                installedRoles: input.experimentRoles,
              },
              {
                capability: "DATASET_RUN_INGESTION",
                supportedContractVersion: 1,
                installedRoles: input.datasetRunRoles,
              },
            ],
          },
        },
      });
      return {
        runtimeLeaseId: input.id,
        backend: "doris",
        deploymentGeneration: 1n,
      };
    };

    beforeAll(async () => {
      repository = await import("./experimentExecutions.js");
      await prisma.organization.create({
        data: { id: organizationId, name: "Experiment execution test" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          orgId: organizationId,
          name: "Experiment execution test",
        },
      });
      await prisma.dataset.create({
        data: {
          id: datasetId,
          projectId,
          name: "Experiment execution dataset",
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
      for (const capability of [
        "EXPERIMENTS",
        "DATASET_RUN_INGESTION",
      ] as const) {
        await prisma.analyticsCapabilityActivation.update({
          where: { capability },
          data: {
            backend: "DORIS",
            deploymentGeneration: 1n,
            generation: 1n,
            contractVersion: 1,
            minimumRuntimeContract: 1,
            status: "ACTIVE",
            activatedAt: new Date(),
          },
        });
      }
      webContext = await createLease({
        id: `experiment-web-${suffix}`,
        component: "WEB",
        experimentRoles: ["PRODUCER"],
        datasetRunRoles: ["PRODUCER"],
      });
      workerContext = await createLease({
        id: `experiment-worker-${suffix}`,
        component: "WORKER",
        experimentRoles: ["CONSUMER", "RECOVERY"],
        datasetRunRoles: ["PRODUCER", "CONSUMER", "RECOVERY"],
      });
    }, 60_000);

    afterAll(async () => {
      await prisma.organization.deleteMany({ where: { id: organizationId } });
      await prisma.analyticsRuntimeLease.deleteMany({
        where: {
          id: {
            in: [webContext.runtimeLeaseId, workerContext.runtimeLeaseId],
          },
        },
      });
      await prisma.analyticsBackendDeploymentState.deleteMany();
      for (const capability of [
        "EXPERIMENTS",
        "DATASET_RUN_INGESTION",
      ] as const) {
        await prisma.analyticsCapabilityActivation.update({
          where: { capability },
          data: {
            deploymentGeneration: 0n,
            status: "DISABLED",
            activatedAt: null,
          },
        });
      }
      await prisma.$disconnect();
    }, 60_000);

    const createIntent = (name: string) =>
      repository.createDorisExperimentExecutionIntent({
        client: prisma,
        admissionContext: webContext,
        projectId,
        datasetId,
        name: `${name}-${suffix}`,
        metadata: { prompt_id: "prompt-1" },
      });

    it("atomically creates stamped intent and a pending outbox", async () => {
      const run = await createIntent("intent");
      expect(run).toMatchObject({
        analyticsBackend: "DORIS",
        deploymentGeneration: 1n,
        capabilityActivationGeneration: 1n,
        capabilityContractVersion: 1,
        datasetRunIngestionActivationGeneration: 1n,
        datasetRunIngestionContractVersion: 1,
        experimentExecutionState: "PENDING",
        experimentExecutionGeneration: 0n,
        experimentDispatchOutbox: {
          status: "PENDING",
          generation: 1,
        },
      });
    });

    it("publishes from the outbox exactly once", async () => {
      const run = await createIntent("publish");
      const publish = vi.fn().mockResolvedValue(undefined);
      await expect(
        repository.publishExperimentExecutionDispatch({
          client: prisma,
          admissionContext: webContext,
          action: "externalProducer",
          projectId,
          runId: run.id,
          expectedGeneration: 1,
          publish,
        }),
      ).resolves.toBe(true);
      expect(publish).toHaveBeenCalledWith(
        expect.objectContaining({
          projectId,
          datasetId,
          runId: run.id,
          dispatchGeneration: 1,
          analyticsBackend: "DORIS",
        }),
      );
      publish.mockClear();
      await expect(
        repository.publishExperimentExecutionDispatch({
          client: prisma,
          admissionContext: workerContext,
          action: "recovery",
          projectId,
          runId: run.id,
          expectedGeneration: 1,
          publish,
        }),
      ).resolves.toBe(false);
      expect(publish).not.toHaveBeenCalled();
    });

    it("claims, renews, completes, and deduplicates redelivery", async () => {
      const run = await createIntent("claim");
      const job = repository.buildManagedExperimentExecutionJob({
        datasetRun: run,
        dispatchGeneration: 1,
      });
      const claim = await repository.claimExperimentExecution({
        client: prisma,
        admissionContext: workerContext,
        job,
        leaseOwner: "worker-a",
        leaseMs: 60_000,
      });
      expect(claim).toMatchObject({
        claimId: expect.any(String),
        generation: 1n,
      });
      if ("completed" in claim) throw new Error("Unexpected completed claim");
      const renewed = await repository.renewExperimentExecutionClaim({
        client: prisma,
        admissionContext: workerContext,
        projectId,
        runId: run.id,
        claimId: claim.claimId,
        generation: claim.generation,
        leaseOwner: "worker-a",
        leaseMs: 120_000,
      });
      expect(renewed.getTime()).toBeGreaterThan(claim.leaseExpiresAt.getTime());
      await expect(
        repository.completeExperimentExecution({
          client: prisma,
          admissionContext: workerContext,
          projectId,
          runId: run.id,
          claimId: claim.claimId,
          generation: claim.generation,
          leaseOwner: "worker-a",
        }),
      ).resolves.toBe(true);
      await expect(
        repository.claimExperimentExecution({
          client: prisma,
          admissionContext: workerContext,
          job,
          leaseOwner: "worker-b",
          leaseMs: 60_000,
        }),
      ).resolves.toMatchObject({
        completed: { id: run.id, experimentExecutionState: "COMPLETED" },
      });
    });

    it("requires dataset-run ingestion to remain executable", async () => {
      const run = await createIntent("dataset-gate");
      const job = repository.buildManagedExperimentExecutionJob({
        datasetRun: run,
        dispatchGeneration: 1,
      });
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "DATASET_RUN_INGESTION" },
        data: { status: "DISABLED" },
      });
      await expect(
        repository.claimExperimentExecution({
          client: prisma,
          admissionContext: workerContext,
          job,
          leaseOwner: "worker-a",
          leaseMs: 60_000,
        }),
      ).rejects.toThrow(/not active/);
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "DATASET_RUN_INGESTION" },
        data: { status: "ACTIVE" },
      });
    });
  },
);
