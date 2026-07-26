import { PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import type { AnalyticsRuntimeAdmissionContext } from "../analytics-persistence/analyticsBackendAdmission";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;
if (process.env.DORIS_POC_ENABLED === "1" && !controlDatabaseUrl) {
  throw new Error("Doris harness must provide an isolated control database");
}

describe.skipIf(!controlDatabaseUrl)(
  "Doris batch export manifest fencing",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const organizationId = `batch-export-org-${suffix}`;
    const projectId = `batch-export-project-${suffix}`;
    const epoch = "e".repeat(64);
    const queueNamespace = "q".repeat(64);
    const future = new Date(Date.now() + 3_600_000);
    let repository: typeof import("./batchExportManifests.js");
    let webContext: AnalyticsRuntimeAdmissionContext;
    let workerAContext: AnalyticsRuntimeAdmissionContext;
    let workerBContext: AnalyticsRuntimeAdmissionContext;

    beforeAll(async () => {
      repository = await import("./batchExportManifests.js");
      await prisma.organization.create({
        data: { id: organizationId, name: "Batch export manifest test" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          orgId: organizationId,
          name: "Batch export manifest test",
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
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "CORE_BATCH_EXPORTS" },
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
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "DATASET_RUN_EXPORTS" },
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

      const createLease = async (
        id: string,
        component: "WEB" | "WORKER",
        roles: Array<"PRODUCER" | "CONSUMER" | "RECOVERY">,
      ) => {
        await prisma.analyticsRuntimeLease.create({
          data: {
            id,
            component,
            instanceId: id,
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
                  capability: "CORE_BATCH_EXPORTS",
                  supportedContractVersion: 1,
                  installedRoles: roles,
                },
                {
                  capability: "DATASET_RUN_EXPORTS",
                  supportedContractVersion: 1,
                  installedRoles: roles,
                },
              ],
            },
          },
        });
        return {
          runtimeLeaseId: id,
          backend: "doris" as const,
          deploymentGeneration: 1n,
        };
      };
      webContext = await createLease(`web-${suffix}`, "WEB", ["PRODUCER"]);
      workerAContext = await createLease(`worker-a-${suffix}`, "WORKER", [
        "CONSUMER",
        "RECOVERY",
      ]);
      workerBContext = await createLease(`worker-b-${suffix}`, "WORKER", [
        "CONSUMER",
        "RECOVERY",
      ]);
    }, 60_000);

    afterAll(async () => {
      await prisma.organization.deleteMany({ where: { id: organizationId } });
      await prisma.analyticsRuntimeLease.deleteMany({
        where: {
          id: {
            in: [
              webContext.runtimeLeaseId,
              workerAContext.runtimeLeaseId,
              workerBContext.runtimeLeaseId,
            ],
          },
        },
      });
      await prisma.analyticsBackendDeploymentState.deleteMany();
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "CORE_BATCH_EXPORTS" },
        data: {
          deploymentGeneration: 0n,
          status: "DISABLED",
          activatedAt: null,
        },
      });
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "DATASET_RUN_EXPORTS" },
        data: {
          deploymentGeneration: 0n,
          status: "DISABLED",
          activatedAt: null,
        },
      });
      await prisma.$disconnect();
    }, 60_000);

    it("atomically creates the stamped export and dispatch outbox", async () => {
      const batchExport = await repository.createDorisBatchExportIntent({
        client: prisma,
        admissionContext: webContext,
        projectId,
        userId: "user-1",
        name: "durable export",
        format: "JSONL",
        query: {
          tableName: "traces",
          filter: null,
          orderBy: null,
        },
      });

      expect(batchExport).toMatchObject({
        analyticsBackend: "DORIS",
        deploymentGeneration: 1n,
        capabilityActivationGeneration: 1n,
        capabilityContractVersion: 1,
        manifestState: "PREPARING",
        manifestGeneration: 0n,
        executionState: "PENDING",
      });
      await expect(
        prisma.batchExportDispatchOutbox.count({
          where: { batchExportId: batchExport.id, status: "PENDING" },
        }),
      ).resolves.toBe(1);
    });

    it("publishes only from the durable outbox and is idempotent", async () => {
      const batchExport = await repository.createDorisBatchExportIntent({
        client: prisma,
        admissionContext: webContext,
        projectId,
        userId: "user-1",
        name: "dispatch export",
        format: "JSONL",
        query: { tableName: "traces", filter: null, orderBy: null },
      });
      const publish = vi.fn().mockResolvedValue(undefined);

      await expect(
        repository.publishBatchExportDispatch({
          client: prisma,
          admissionContext: webContext,
          action: "externalProducer",
          batchExportId: batchExport.id,
          expectedGeneration: 1,
          publish,
        }),
      ).resolves.toBe(true);
      expect(publish).toHaveBeenCalledOnce();
      expect(publish).toHaveBeenCalledWith(
        expect.objectContaining({
          projectId,
          batchExportId: batchExport.id,
          dispatchGeneration: 1,
          analyticsBackend: "DORIS",
        }),
      );

      publish.mockClear();
      await expect(
        repository.publishBatchExportDispatch({
          client: prisma,
          admissionContext: workerAContext,
          action: "recovery",
          batchExportId: batchExport.id,
          expectedGeneration: 1,
          publish,
        }),
      ).resolves.toBe(false);
      expect(publish).not.toHaveBeenCalled();
      await expect(
        prisma.batchExportDispatchOutbox.findUniqueOrThrow({
          where: { batchExportId: batchExport.id },
        }),
      ).resolves.toMatchObject({ status: "PUBLISHED", generation: 1 });
    });

    it("requires the exact dataset-run export generation throughout dispatch and execution", async () => {
      const batchExport = await repository.createDorisBatchExportIntent({
        client: prisma,
        admissionContext: webContext,
        projectId,
        userId: "user-1",
        name: "dataset-run export",
        format: "JSONL",
        query: {
          tableName: "dataset_run_items",
          filter: null,
          orderBy: null,
        },
      });
      expect(batchExport).toMatchObject({
        datasetRunExportActivationGeneration: 1n,
        datasetRunExportContractVersion: 1,
      });

      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "DATASET_RUN_EXPORTS" },
        data: { status: "DISABLED", activatedAt: null },
      });
      try {
        await expect(
          repository.publishBatchExportDispatch({
            client: prisma,
            admissionContext: webContext,
            action: "externalProducer",
            batchExportId: batchExport.id,
            expectedGeneration: 1,
            publish: vi.fn().mockResolvedValue(undefined),
          }),
        ).rejects.toThrow();
        await expect(
          repository.claimBatchExportManifest({
            client: prisma,
            admissionContext: workerAContext,
            projectId,
            batchExportId: batchExport.id,
            leaseOwner: "worker-a",
            leaseMs: 60_000,
            claimId: "dataset-run-claim",
          }),
        ).rejects.toThrow();
      } finally {
        await prisma.analyticsCapabilityActivation.update({
          where: { capability: "DATASET_RUN_EXPORTS" },
          data: {
            generation: 2n,
            status: "ACTIVE",
            activatedAt: new Date(),
          },
        });
      }

      await expect(
        repository.claimBatchExportManifest({
          client: prisma,
          admissionContext: workerAContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-a",
          leaseMs: 60_000,
          claimId: "dataset-run-claim",
        }),
      ).rejects.toThrow(/provenance generation|no longer current/i);

      const currentGenerationExport =
        await repository.createDorisBatchExportIntent({
          client: prisma,
          admissionContext: webContext,
          projectId,
          userId: "user-1",
          name: "current dataset-run export",
          format: "JSONL",
          query: {
            tableName: "dataset_run_items",
            filter: null,
            orderBy: null,
          },
        });
      expect(currentGenerationExport).toMatchObject({
        datasetRunExportActivationGeneration: 2n,
        datasetRunExportContractVersion: 1,
      });
      await expect(
        repository.claimBatchExportManifest({
          client: prisma,
          admissionContext: workerAContext,
          projectId,
          batchExportId: currentGenerationExport.id,
          leaseOwner: "worker-a",
          leaseMs: 60_000,
          claimId: "current-dataset-run-claim",
        }),
      ).resolves.toMatchObject({ generation: 1n });
    });

    it("durably backs off a transient dispatch failure", async () => {
      const batchExport = await repository.createDorisBatchExportIntent({
        client: prisma,
        admissionContext: webContext,
        projectId,
        userId: "user-1",
        name: "deferred dispatch export",
        format: "JSONL",
        query: { tableName: "traces", filter: null, orderBy: null },
      });
      const before = new Date();

      await expect(
        repository.deferBatchExportDispatch({
          client: prisma,
          batchExportId: batchExport.id,
          expectedGeneration: 1,
        }),
      ).resolves.toBe(true);
      const deferred = await prisma.batchExportDispatchOutbox.findUniqueOrThrow(
        {
          where: { batchExportId: batchExport.id },
        },
      );
      expect(deferred).toMatchObject({
        status: "PENDING",
        attempts: 1,
      });
      expect(deferred.nextAttemptAt.getTime()).toBeGreaterThan(
        before.getTime(),
      );
      await expect(
        repository.findPendingBatchExportDispatchIds({
          client: prisma,
          now: before,
          limit: 100,
        }),
      ).resolves.not.toContainEqual(
        expect.objectContaining({ batchExportId: batchExport.id }),
      );
    });

    it("fences an active loser and a stale generation after lease recovery", async () => {
      const batchExport = await repository.createDorisBatchExportIntent({
        client: prisma,
        admissionContext: webContext,
        projectId,
        userId: "user-1",
        name: "race export",
        format: "JSONL",
        query: { tableName: "observations", filter: null, orderBy: null },
      });
      const first = await repository.claimBatchExportManifest({
        client: prisma,
        admissionContext: workerAContext,
        projectId,
        batchExportId: batchExport.id,
        leaseOwner: "worker-a",
        leaseMs: 60_000,
        claimId: "claim-a",
      });
      expect(first).toMatchObject({ generation: 1n, claimId: "claim-a" });
      await expect(
        repository.renewBatchExportManifestLease({
          client: prisma,
          admissionContext: workerAContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-a",
          leaseMs: 120_000,
          claimId: "claim-a",
          generation: 1n,
        }),
      ).resolves.toBeInstanceOf(Date);
      await expect(
        repository.claimBatchExportManifest({
          client: prisma,
          admissionContext: workerBContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-b",
          leaseMs: 60_000,
          claimId: "claim-b",
        }),
      ).rejects.toBeInstanceOf(repository.BatchExportManifestBusyError);

      await prisma.batchExport.update({
        where: { id: batchExport.id },
        data: { manifestLeaseExpiresAt: new Date(Date.now() - 1_000) },
      });
      const second = await repository.claimBatchExportManifest({
        client: prisma,
        admissionContext: workerBContext,
        projectId,
        batchExportId: batchExport.id,
        leaseOwner: "worker-b",
        leaseMs: 60_000,
        claimId: "claim-b",
      });
      expect(second).toMatchObject({ generation: 2n, claimId: "claim-b" });

      await expect(
        repository.failBatchExportManifest({
          client: prisma,
          admissionContext: workerAContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-a",
          claimId: "claim-a",
          generation: 1n,
          failureCode: "STALE_MANIFEST_FAILED",
          log: "The stale manifest writer resumed after recovery",
        }),
      ).resolves.toBe(false);
      await expect(
        prisma.batchExport.findFirstOrThrow({
          where: { id: batchExport.id, projectId },
        }),
      ).resolves.toMatchObject({
        status: "QUEUED",
        executionState: "PENDING",
        manifestGeneration: 2n,
        manifestClaimId: "claim-b",
      });

      await expect(
        repository.sealBatchExportManifest({
          client: prisma,
          admissionContext: workerAContext,
          projectId,
          batchExportId: batchExport.id,
          generation: 1n,
          claimId: "claim-a",
          objectKey: "loser",
          checksum: "a".repeat(64),
          rowCount: 1,
          byteCount: 10n,
          formatVersion: 1,
        }),
      ).rejects.toBeInstanceOf(repository.BatchExportManifestFencedError);

      const descriptor = {
        client: prisma,
        admissionContext: workerBContext,
        projectId,
        batchExportId: batchExport.id,
        generation: 2n,
        claimId: "claim-b",
        objectKey: "winner",
        checksum: "b".repeat(64),
        rowCount: 2,
        byteCount: 20n,
        formatVersion: 1,
      } as const;
      await expect(
        repository.sealBatchExportManifest(descriptor),
      ).resolves.toMatchObject({
        manifestState: "SEALED",
        manifestObjectKey: "winner",
        manifestGeneration: 2n,
      });
      await expect(
        repository.sealBatchExportManifest(descriptor),
      ).resolves.toMatchObject({
        manifestObjectKey: "winner",
      });

      const execution = await repository.claimBatchExportExecution({
        client: prisma,
        admissionContext: workerBContext,
        projectId,
        batchExportId: batchExport.id,
        leaseOwner: "worker-b",
        leaseMs: 60_000,
        claimId: "execution-b",
      });
      expect(execution).toMatchObject({
        generation: 1n,
        claimId: "execution-b",
        batchExport: { manifestObjectKey: "winner" },
      });
      await expect(
        repository.renewBatchExportExecutionLease({
          client: prisma,
          admissionContext: workerBContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-b",
          leaseMs: 120_000,
          claimId: "execution-b",
          generation: 1n,
        }),
      ).resolves.toBeInstanceOf(Date);
      await expect(
        repository.claimBatchExportExecution({
          client: prisma,
          admissionContext: workerAContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-a",
          leaseMs: 60_000,
          claimId: "execution-a",
        }),
      ).rejects.toBeInstanceOf(repository.BatchExportManifestBusyError);

      await prisma.batchExport.update({
        where: { id: batchExport.id, projectId },
        data: { executionLeaseExpiresAt: new Date(Date.now() - 1_000) },
      });
      const recoveredExecution = await repository.claimBatchExportExecution({
        client: prisma,
        admissionContext: workerAContext,
        projectId,
        batchExportId: batchExport.id,
        leaseOwner: "worker-a",
        leaseMs: 60_000,
        claimId: "execution-a",
      });
      expect(recoveredExecution).toMatchObject({
        generation: 2n,
        claimId: "execution-a",
      });
      await expect(
        repository.failBatchExportExecution({
          client: prisma,
          admissionContext: workerBContext,
          projectId,
          batchExportId: batchExport.id,
          executionClaimId: "execution-b",
          executionGeneration: 1n,
          failureCode: "STALE_WORKER_FAILED",
          log: "The stale worker resumed after recovery",
        }),
      ).resolves.toBe(false);
      await expect(
        prisma.batchExport.findFirstOrThrow({
          where: { id: batchExport.id, projectId },
        }),
      ).resolves.toMatchObject({
        status: "PROCESSING",
        executionState: "EXPORTING",
        executionGeneration: 2n,
        executionClaimId: "execution-a",
      });

      const completion = {
        client: prisma,
        admissionContext: workerAContext,
        projectId,
        batchExportId: batchExport.id,
        generation: 2n,
        claimId: "execution-a",
        url: "https://example.test/export",
        expiresAt: new Date(Date.now() + 60_000),
      } as const;
      await expect(
        repository.completeBatchExportExecution(completion),
      ).resolves.toMatchObject({
        status: "COMPLETED",
        executionState: "COMPLETED",
      });
      await expect(
        repository.completeBatchExportExecution(completion),
      ).resolves.toMatchObject({
        url: completion.url,
      });
    });

    it("records a claimed manifest failure and permits an immediate retry", async () => {
      const batchExport = await repository.createDorisBatchExportIntent({
        client: prisma,
        admissionContext: webContext,
        projectId,
        userId: "user-1",
        name: "retryable manifest export",
        format: "JSONL",
        query: { tableName: "traces", filter: null, orderBy: null },
      });
      const first = await repository.claimBatchExportManifest({
        client: prisma,
        admissionContext: workerAContext,
        projectId,
        batchExportId: batchExport.id,
        leaseOwner: "worker-a",
        leaseMs: 60_000,
        claimId: "failed-claim",
      });
      expect(first).toMatchObject({ generation: 1n });

      await expect(
        repository.failBatchExportManifest({
          client: prisma,
          admissionContext: workerAContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-a",
          claimId: "failed-claim",
          generation: 1n,
          failureCode: "MANIFEST_WRITE_FAILED",
          log: "Object storage write failed",
        }),
      ).resolves.toBe(true);
      await expect(
        prisma.batchExport.findFirstOrThrow({
          where: { id: batchExport.id, projectId },
        }),
      ).resolves.toMatchObject({
        status: "FAILED",
        executionState: "FAILED",
        manifestLeaseOwner: null,
        manifestLeaseExpiresAt: null,
      });

      await expect(
        repository.claimBatchExportManifest({
          client: prisma,
          admissionContext: workerBContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-b",
          leaseMs: 60_000,
          claimId: "retry-claim",
        }),
      ).resolves.toMatchObject({
        generation: 2n,
        claimId: "retry-claim",
        batchExport: {
          status: "QUEUED",
          executionState: "PENDING",
          failureCode: null,
          log: null,
          finishedAt: null,
        },
      });
    });

    it("cancels an active claim and permanently suppresses pending dispatch", async () => {
      const batchExport = await repository.createDorisBatchExportIntent({
        client: prisma,
        admissionContext: webContext,
        projectId,
        userId: "user-1",
        name: "cancelled export",
        format: "JSONL",
        query: { tableName: "traces", filter: null, orderBy: null },
      });
      await repository.claimBatchExportManifest({
        client: prisma,
        admissionContext: workerAContext,
        projectId,
        batchExportId: batchExport.id,
        leaseOwner: "worker-a",
        leaseMs: 60_000,
        claimId: "cancelled-claim",
      });

      await expect(
        repository.cancelBatchExport({
          client: prisma,
          projectId,
          batchExportId: batchExport.id,
        }),
      ).resolves.toMatchObject({
        status: "CANCELLED",
        executionState: "CANCELLED",
        manifestLeaseOwner: null,
      });
      await expect(
        repository.renewBatchExportManifestLease({
          client: prisma,
          admissionContext: workerAContext,
          projectId,
          batchExportId: batchExport.id,
          leaseOwner: "worker-a",
          leaseMs: 60_000,
          claimId: "cancelled-claim",
          generation: 1n,
        }),
      ).rejects.toBeInstanceOf(repository.BatchExportManifestFencedError);
      await expect(
        repository.publishBatchExportDispatch({
          client: prisma,
          admissionContext: workerAContext,
          action: "recovery",
          batchExportId: batchExport.id,
          expectedGeneration: 1,
          publish: vi.fn().mockResolvedValue(undefined),
        }),
      ).resolves.toBe(false);
      await expect(
        prisma.batchExportDispatchOutbox.findUniqueOrThrow({
          where: { batchExportId: batchExport.id },
        }),
      ).resolves.toMatchObject({ status: "CANCELLED" });
    });

    it("quarantines a legacy-shaped row instead of silently accepting it", async () => {
      const legacy = await prisma.batchExport.create({
        data: {
          projectId,
          userId: "user-1",
          name: "legacy export",
          format: "JSONL",
          query: { tableName: "traces", filter: null, orderBy: null },
          status: "QUEUED",
        },
      });

      await repository.quarantineBatchExport({
        client: prisma,
        projectId,
        batchExportId: legacy.id,
        failureCode: "LEGACY_PAYLOAD",
        log: "Legacy payload reached a Doris worker",
      });

      await expect(
        prisma.batchExport.findUniqueOrThrow({ where: { id: legacy.id } }),
      ).resolves.toMatchObject({
        status: "FAILED",
        executionState: "QUARANTINED",
        failureCode: "LEGACY_PAYLOAD",
      });
    });
  },
);
