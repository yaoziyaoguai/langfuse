import { Prisma, PrismaClient } from "@prisma/client";
import { createHash } from "node:crypto";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

import type { AnalyticsRuntimeAdmissionContext } from "../analytics-persistence/analyticsBackendAdmission";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)(
  "Doris analytics integration visible-operation capture",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const organizationId = `integration-delivery-org-${suffix}`;
    const projectId = `integration-delivery-project-${suffix}`;
    const runtimeLeaseId = `integration-delivery-worker-${suffix}`;
    const webRuntimeLeaseId = `integration-delivery-web-${suffix}`;
    const epoch = "i".repeat(64);
    const queueNamespace = "n".repeat(64);
    const admissionContext: AnalyticsRuntimeAdmissionContext = {
      runtimeLeaseId,
      backend: "doris",
      deploymentGeneration: 1n,
    };
    let loadRepository: typeof import("./analyticsLoadBatches.js");
    let deliveryRepository: typeof import("./analyticsIntegrationDeliveries.js");
    let activationRepository: typeof import("./analyticsCapabilityActivations.js");

    beforeAll(async () => {
      loadRepository = await import("./analyticsLoadBatches.js");
      deliveryRepository = await import("./analyticsIntegrationDeliveries.js");
      activationRepository =
        await import("./analyticsCapabilityActivations.js");
      await prisma.organization.create({
        data: { id: organizationId, name: "Integration delivery test" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          orgId: organizationId,
          name: "Integration delivery test",
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
        where: { capability: "ANALYTICS_INTEGRATIONS" },
        data: {
          backend: "DORIS",
          deploymentGeneration: 1n,
          generation: 11n,
          contractVersion: 1,
          minimumRuntimeContract: 1,
          status: "DARK",
          captureEnabled: true,
          captureStartedAt: new Date("2026-07-25T00:00:00.000Z"),
          captureExpiresAt: new Date("2026-07-26T00:00:00.000Z"),
          captureRowBudget: 1_000,
          captureRows: 0n,
        },
      });
      await prisma.analyticsRuntimeLease.create({
        data: {
          id: runtimeLeaseId,
          component: "WORKER",
          instanceId: runtimeLeaseId,
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
          leaseExpiresAt: new Date(Date.now() + 3_600_000),
          capabilityContracts: {
            create: {
              capability: "ANALYTICS_INTEGRATIONS",
              supportedContractVersion: 1,
              installedRoles: ["CAPTURE", "CONSUMER", "RECOVERY"],
            },
          },
        },
      });
      await prisma.analyticsIntegrationState.createMany({
        data: [
          {
            id: `posthog-${suffix}`,
            projectId,
            integrationType: "POSTHOG",
            generation: 3n,
            status: "BOOTSTRAPPING_DARK",
          },
          {
            id: `blob-${suffix}`,
            projectId,
            integrationType: "BLOB_STORAGE",
            generation: 5n,
            status: "BOOTSTRAPPING_DARK",
          },
        ],
      });
    }, 90_000);

    afterAll(async () => {
      await prisma.organization.deleteMany({ where: { id: organizationId } });
      await prisma.analyticsRuntimeLease.deleteMany({
        where: { id: { in: [runtimeLeaseId, webRuntimeLeaseId] } },
      });
      await prisma.analyticsBackendDeploymentState.deleteMany();
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "ANALYTICS_INTEGRATIONS" },
        data: {
          deploymentGeneration: 0n,
          generation: 1n,
          status: "DISABLED",
          captureEnabled: false,
          captureStartedAt: null,
          captureExpiresAt: null,
          captureRowBudget: null,
          captureRows: 0n,
          captureRequired: false,
          rescanRequired: false,
          cutoffState: Prisma.DbNull,
          cutoffActivationGeneration: null,
          cutoffDigest: null,
          bootstrapCompletedGeneration: null,
          bootstrapEvidenceDigest: null,
          bootstrapCompletedAt: null,
          activatedAt: null,
          drainingAt: null,
          disabledAt: null,
        },
      });
      await prisma.$disconnect();
    }, 90_000);

    const createCompletableOperation = async (
      label: string,
      targetProjectId = projectId,
    ) => {
      const operationId = `integration-operation-${label}-${suffix}`;
      const loadBatchId = `integration-load-${label}-${suffix}`;
      const eventCandidate = `event-${label}`;
      const scoreCandidate = `score-${label}`;
      await prisma.analyticsIngestionOperation.create({
        data: {
          id: operationId,
          projectId: targetProjectId,
          sourceOperationId: `integration-source-${label}-${suffix}`,
          sourceChecksum: "a".repeat(64),
          rawObjectKey: `raw/${operationId}.json`,
          acceptedAt: new Date("2026-07-25T00:00:00.000Z"),
          acceptedAtNanos: 1_785_283_200_000_000_000n,
          canonicalizerVersion: "2",
          schemaVersion: 4,
          analyticsBackend: "DORIS",
          deploymentGeneration: 1n,
          workloadEpochFingerprint: epoch,
          runtimeContractVersion: 1,
          producerRuntimeLeaseId: runtimeLeaseId,
          manifestState: "FROZEN",
          status: "PERSISTED",
          recoverableUntil: new Date("2026-08-01T00:00:00.000Z"),
          statusExpiresAt: new Date("2026-09-01T00:00:00.000Z"),
          candidates: {
            create: [
              {
                candidateKey: eventCandidate,
                entityType: "EVENT",
                entityKey: `event-entity-${label}`,
                owningTraceId: `trace-${label}`,
                partitionDate: new Date("2026-07-25T00:00:00.000Z"),
                sourceVersion: 1n,
                canonicalPayloadHash: "b".repeat(64),
                disposition: "LOAD_REQUIRED",
                loadBatchId,
              },
              {
                candidateKey: scoreCandidate,
                entityType: "SCORE",
                entityKey: `score-entity-${label}`,
                owningTraceId: `trace-${label}`,
                partitionDate: new Date("2026-07-25T00:00:00.000Z"),
                sourceVersion: 1n,
                canonicalPayloadHash: "c".repeat(64),
                disposition: "LOAD_REQUIRED",
                loadBatchId,
              },
            ],
          },
          loadBatches: {
            create: {
              id: loadBatchId,
              databaseName: "langfuse_test",
              targetTable: "events_current",
              logicalBatchId: `integration-${label}`,
              attempt: 0,
              fenceGeneration: 1n,
              label: `integration_${label}_${suffix}`.replaceAll("-", "_"),
              payloadHash: "d".repeat(64),
              canonicalObjectKey: `canonical/${operationId}.json`,
              partitionDate: new Date("2026-07-25T00:00:00.000Z"),
              status: "VISIBLE",
              totalRows: 2,
              filteredRows: 0,
              visibleAt: new Date("2026-07-25T00:00:01.000Z"),
            },
          },
        },
      });
      return { operationId, eventCandidate, scoreCandidate };
    };

    const targetsFor = ({
      eventCandidate,
      scoreCandidate,
      label,
    }: {
      eventCandidate: string;
      scoreCandidate: string;
      label: string;
    }) =>
      [
        {
          candidateKey: eventCandidate,
          deliveryKind: "TRACE" as const,
          entityKey: `trace-${label}`,
          estimatedBytes: 120,
        },
        {
          candidateKey: eventCandidate,
          deliveryKind: "GENERATION" as const,
          entityKey: `span-${label}`,
          estimatedBytes: 240,
        },
        {
          candidateKey: eventCandidate,
          deliveryKind: "OBSERVATION" as const,
          entityKey: `span-${label}`,
          estimatedBytes: 240,
        },
        {
          candidateKey: scoreCandidate,
          deliveryKind: "SCORE" as const,
          entityKey: `score-${label}`,
          estimatedBytes: 80,
        },
      ] as const;

    it("atomically captures generation-scoped suspended deliveries in DARK", async () => {
      const operation = await createCompletableOperation("dark");
      await expect(
        loadRepository.completeAnalyticsIngestionOperation({
          client: prisma,
          operationId: operation.operationId,
          projectId,
          now: new Date("2026-07-25T00:00:02.000Z"),
          integrationCapture: {
            admissionContext,
            targets: targetsFor({ ...operation, label: "dark" }),
          },
        }),
      ).resolves.toMatchObject({ outcome: "completed", status: "VISIBLE" });

      const deliveries =
        await prisma.analyticsIntegrationPendingDelivery.findMany({
          where: { operationId: operation.operationId },
          orderBy: [{ integrationType: "asc" }, { deliveryKind: "asc" }],
        });
      expect(deliveries).toHaveLength(8);
      expect(deliveries).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            integrationType: "POSTHOG",
            integrationGeneration: 3n,
            deliveryKind: "TRACE",
            status: "SUSPENDED",
            analyticsBackend: "DORIS",
            deploymentGeneration: 1n,
            capabilityActivationGeneration: 11n,
          }),
          expect.objectContaining({
            integrationType: "BLOB_STORAGE",
            integrationGeneration: 5n,
            deliveryKind: "SCORE",
            status: "SUSPENDED",
          }),
        ]),
      );
      await expect(
        prisma.analyticsIntegrationState.findMany({
          where: { projectId },
          orderBy: { integrationType: "asc" },
          select: {
            integrationType: true,
            pendingRows: true,
            pendingEstimatedBytes: true,
          },
        }),
      ).resolves.toEqual([
        {
          integrationType: "POSTHOG",
          pendingRows: 4n,
          pendingEstimatedBytes: 680n,
        },
        {
          integrationType: "BLOB_STORAGE",
          pendingRows: 4n,
          pendingEstimatedBytes: 680n,
        },
      ]);
      await expect(
        prisma.analyticsCapabilityActivation.findUniqueOrThrow({
          where: { capability: "ANALYTICS_INTEGRATIONS" },
          select: { captureRows: true },
        }),
      ).resolves.toEqual({ captureRows: 8n });
    });

    it("creates pending work in ACTIVE and preserves idempotent stable identities", async () => {
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "ANALYTICS_INTEGRATIONS" },
        data: { status: "ACTIVE", captureEnabled: false },
      });
      await prisma.analyticsIntegrationState.updateMany({
        where: { projectId },
        data: { status: "ACTIVE" },
      });
      const states = await prisma.analyticsIntegrationState.findMany({
        where: { projectId },
      });
      for (const state of states) {
        const manifest = {
          version: 1,
          integrationStateId: state.id,
          integrationGeneration: state.generation.toString(),
          projectId,
          items: [],
        };
        await prisma.analyticsIntegrationState.update({
          where: { id: state.id },
          data: {
            bootstrapManifest: manifest,
            bootstrapManifestKey: `postgres-json://analytics-integration-bootstrap/${state.id}/${state.generation}`,
            bootstrapManifestChecksum: createHash("sha256")
              .update(JSON.stringify(manifest))
              .digest("hex"),
            bootstrapManifestRows: 0n,
            bootstrapSealedAt: new Date("2026-07-25T00:00:30.000Z"),
          },
        });
      }
      const operation = await createCompletableOperation("active");
      const targets = targetsFor({ ...operation, label: "active" });
      await loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId: operation.operationId,
        projectId,
        now: new Date("2026-07-25T00:01:00.000Z"),
        integrationCapture: { admissionContext, targets },
      });

      const deliveries =
        await prisma.analyticsIntegrationPendingDelivery.findMany({
          where: { operationId: operation.operationId },
        });
      expect(deliveries).toHaveLength(8);
      expect(new Set(deliveries.map(({ id }) => id)).size).toBe(8);
      expect(deliveries.every(({ status }) => status === "PENDING")).toBe(true);
    });

    it("pauses only the overflowing integration and leaves ingestion visible", async () => {
      await prisma.analyticsIntegrationState.update({
        where: {
          projectId_integrationType: {
            projectId,
            integrationType: "POSTHOG",
          },
        },
        data: { pendingRows: 100_000n, status: "ACTIVE" },
      });
      const operation = await createCompletableOperation("overflow");
      await expect(
        loadRepository.completeAnalyticsIngestionOperation({
          client: prisma,
          operationId: operation.operationId,
          projectId,
          now: new Date("2026-07-25T00:02:00.000Z"),
          integrationCapture: {
            admissionContext,
            targets: targetsFor({ ...operation, label: "overflow" }),
          },
        }),
      ).resolves.toMatchObject({ outcome: "completed", status: "VISIBLE" });

      await expect(
        prisma.analyticsIntegrationState.findUniqueOrThrow({
          where: {
            projectId_integrationType: {
              projectId,
              integrationType: "POSTHOG",
            },
          },
        }),
      ).resolves.toMatchObject({
        status: "PAUSED_BACKLOG",
        rescanRequired: true,
        pendingRows: 100_000n,
      });
      await expect(
        prisma.analyticsIntegrationPendingDelivery.count({
          where: {
            operationId: operation.operationId,
            integrationType: "POSTHOG",
          },
        }),
      ).resolves.toBe(0);
      await expect(
        prisma.analyticsIntegrationPendingDelivery.count({
          where: {
            operationId: operation.operationId,
            integrationType: "BLOB_STORAGE",
          },
        }),
      ).resolves.toBe(4);
    });

    it("seals, retries, and atomically terminalizes an immutable execution manifest", async () => {
      const sealed = await deliveryRepository.sealAnalyticsIntegrationExecution(
        {
          client: prisma,
          admissionContext,
          projectId,
          integrationType: "BLOB_STORAGE",
          now: new Date("2026-07-25T00:03:00.000Z"),
        },
      );
      expect(sealed).toMatchObject({
        projectId,
        integrationType: "BLOB_STORAGE",
        integrationGeneration: "5",
        analyticsBackend: "DORIS",
        deploymentGeneration: "1",
        capabilityActivationGeneration: "11",
      });
      if (!sealed) throw new Error("Expected a sealed integration execution");

      await expect(
        deliveryRepository.publishAnalyticsIntegrationExecution({
          client: prisma,
          admissionContext,
          envelope: sealed,
          queueJobId: sealed.executionId,
          publish: async () => undefined,
          now: new Date("2026-07-25T00:03:01.000Z"),
        }),
      ).resolves.toBe(true);
      const firstClaim =
        await deliveryRepository.claimAnalyticsIntegrationExecution({
          client: prisma,
          admissionContext,
          envelope: sealed,
          workerId: "integration-worker-a",
          now: new Date("2026-07-25T00:03:02.000Z"),
        });
      expect(firstClaim?.deliveries).toHaveLength(8);
      expect(firstClaim?.manifest.items).toHaveLength(8);
      await expect(
        deliveryRepository.deferAnalyticsIntegrationExecution({
          client: prisma,
          executionId: sealed.executionId,
          workerId: "integration-worker-a",
          failureCode: "REMOTE_PARTIAL_FAILURE",
          now: new Date("2026-07-25T00:03:03.000Z"),
        }),
      ).resolves.toBe(true);
      await expect(
        deliveryRepository.findPublishableAnalyticsIntegrationExecutions({
          client: prisma,
          now: new Date("2026-07-25T00:03:04.000Z"),
          limit: 10,
        }),
      ).resolves.toEqual([]);

      const secondClaim =
        await deliveryRepository.claimAnalyticsIntegrationExecution({
          client: prisma,
          admissionContext,
          envelope: sealed,
          workerId: "integration-worker-b",
          now: new Date("2026-07-25T00:04:00.000Z"),
        });
      expect(secondClaim?.execution.attempts).toBe(2);
      await expect(
        deliveryRepository.renewAnalyticsIntegrationExecutionClaim({
          client: prisma,
          admissionContext,
          executionId: sealed.executionId,
          workerId: "integration-worker-b",
          now: new Date("2026-07-25T00:04:30.000Z"),
          leaseMs: 5 * 60 * 1000,
        }),
      ).resolves.toEqual(new Date("2026-07-25T00:09:30.000Z"));
      await expect(
        deliveryRepository.renewAnalyticsIntegrationExecutionClaim({
          client: prisma,
          admissionContext,
          executionId: sealed.executionId,
          workerId: "integration-worker-a",
          now: new Date("2026-07-25T00:04:31.000Z"),
        }),
      ).rejects.toThrow(/fenced/i);
      const sourceDeletedId = secondClaim?.manifest.items[0]?.deliveryIds[0];
      if (!sourceDeletedId) {
        throw new Error("Expected a source-deleted delivery identity");
      }
      await expect(
        deliveryRepository.completeAnalyticsIntegrationExecution({
          client: prisma,
          executionId: sealed.executionId,
          workerId: "integration-worker-b",
          sourceDeletedDeliveryIds: [sourceDeletedId],
          now: new Date("2026-07-25T00:04:32.000Z"),
        }),
      ).resolves.toBe(true);

      await expect(
        prisma.analyticsIntegrationPendingDelivery.groupBy({
          by: ["status"],
          where: { executionId: sealed.executionId },
          _count: true,
        }),
      ).resolves.toEqual(
        expect.arrayContaining([
          { status: "COMPLETED", _count: 7 },
          { status: "SOURCE_DELETED", _count: 1 },
        ]),
      );
      await expect(
        prisma.analyticsIntegrationState.findUniqueOrThrow({
          where: {
            projectId_integrationType: {
              projectId,
              integrationType: "BLOB_STORAGE",
            },
          },
          select: { pendingRows: true, pendingEstimatedBytes: true },
        }),
      ).resolves.toEqual({
        pendingRows: 4n,
        pendingEstimatedBytes: 680n,
      });
    });

    it("keeps DARK bootstrap local and activates a config only after external commit", async () => {
      const lifecycleProjectId = `integration-lifecycle-project-${suffix}`;
      const lifecycleNow = new Date();
      await prisma.project.create({
        data: {
          id: lifecycleProjectId,
          orgId: organizationId,
          name: "Integration lifecycle test",
          PosthogIntegration: {
            create: {
              encryptedPosthogApiKey: "encrypted-test-value",
              posthogHostName: "https://example.com",
              enabled: true,
            },
          },
        },
      });
      await prisma.analyticsRuntimeLease.create({
        data: {
          id: webRuntimeLeaseId,
          component: "WEB",
          instanceId: webRuntimeLeaseId,
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
          heartbeatAt: lifecycleNow,
          leaseExpiresAt: new Date(lifecycleNow.getTime() + 3_600_000),
          capabilityContracts: {
            create: {
              capability: "ANALYTICS_INTEGRATIONS",
              supportedContractVersion: 1,
              installedRoles: ["PRODUCER"],
            },
          },
        },
      });
      await prisma.analyticsRuntimeLease.update({
        where: { id: runtimeLeaseId },
        data: {
          heartbeatAt: lifecycleNow,
          leaseExpiresAt: new Date(lifecycleNow.getTime() + 3_600_000),
        },
      });
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "ANALYTICS_INTEGRATIONS" },
        data: {
          backend: "DORIS",
          deploymentGeneration: 1n,
          generation: 12n,
          contractVersion: 1,
          minimumRuntimeContract: 1,
          status: "DARK",
          captureEnabled: true,
          captureStartedAt: lifecycleNow,
          captureExpiresAt: new Date(
            lifecycleNow.getTime() + 24 * 60 * 60 * 1000,
          ),
          captureRowBudget: 1_000_000,
          captureRows: 0n,
          captureRequired: false,
          rescanRequired: false,
          cutoffState: Prisma.DbNull,
          cutoffActivationGeneration: null,
          cutoffDigest: null,
          bootstrapCompletedGeneration: null,
          bootstrapEvidenceDigest: null,
          bootstrapCompletedAt: null,
          activatedAt: null,
          drainingAt: null,
          disabledAt: null,
        },
      });
      await prisma.$transaction((transaction) =>
        deliveryRepository.prepareDorisAnalyticsIntegrationDarkCapture({
          transaction,
          now: lifecycleNow,
        }),
      );
      const [sealed] =
        await deliveryRepository.sealDorisAnalyticsIntegrationBootstrapManifest(
          {
            client: prisma,
            admissionContext,
            projectId: lifecycleProjectId,
            integrationType: "POSTHOG",
            identities: [
              { deliveryKind: "TRACE", entityKey: "bootstrap-trace" },
            ],
            now: new Date(lifecycleNow.getTime() + 1_000),
          },
        );
      if (!sealed) throw new Error("Expected sealed DARK bootstrap execution");

      const darkPublish = vi.fn();
      await expect(
        deliveryRepository.publishAnalyticsIntegrationExecution({
          client: prisma,
          admissionContext,
          envelope: sealed,
          queueJobId: sealed.executionId,
          publish: darkPublish,
          now: new Date(lifecycleNow.getTime() + 2_000),
        }),
      ).rejects.toThrow();
      expect(darkPublish).not.toHaveBeenCalled();
      await expect(
        prisma.posthogIntegration.findUniqueOrThrow({
          where: { projectId: lifecycleProjectId },
          select: { lastSyncAt: true },
        }),
      ).resolves.toEqual({ lastSyncAt: null });
      await expect(
        prisma.analyticsIntegrationState.findUniqueOrThrow({
          where: {
            projectId_integrationType: {
              projectId: lifecycleProjectId,
              integrationType: "POSTHOG",
            },
          },
          select: { status: true },
        }),
      ).resolves.toEqual({ status: "BOOTSTRAPPING_DARK" });

      const completedBootstrap =
        await activationRepository.completeAnalyticsCapabilityBootstrap({
          client: prisma,
          capability: "analyticsIntegrations",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: 12n,
          verifyDurableBootstrap: (transaction) =>
            deliveryRepository.verifyDorisAnalyticsIntegrationBootstrap({
              transaction,
            }),
          now: new Date(lifecycleNow.getTime() + 3_000),
        });
      const evidence = completedBootstrap.bootstrapEvidenceDigest;
      if (!evidence) throw new Error("Expected bootstrap evidence");
      await activationRepository.activateAnalyticsCapability({
        client: prisma,
        capability: "analyticsIntegrations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 12n,
        expectedRuntimeInstanceIds: [runtimeLeaseId, webRuntimeLeaseId],
        expectedBootstrapEvidenceDigest: evidence,
        now: new Date(lifecycleNow.getTime() + 4_000),
      });

      const activePublish = vi.fn(async () => undefined);
      await expect(
        deliveryRepository.publishAnalyticsIntegrationExecution({
          client: prisma,
          admissionContext,
          envelope: sealed,
          queueJobId: sealed.executionId,
          publish: activePublish,
          now: new Date(lifecycleNow.getTime() + 5_000),
        }),
      ).resolves.toBe(true);
      expect(activePublish).toHaveBeenCalledOnce();
      await expect(
        deliveryRepository.claimAnalyticsIntegrationExecution({
          client: prisma,
          admissionContext,
          envelope: sealed,
          workerId: "bootstrap-worker",
          now: new Date(lifecycleNow.getTime() + 6_000),
        }),
      ).resolves.not.toBeNull();
      const completedAt = new Date(lifecycleNow.getTime() + 7_000);
      await expect(
        deliveryRepository.completeAnalyticsIntegrationExecution({
          client: prisma,
          executionId: sealed.executionId,
          workerId: "bootstrap-worker",
          lastSyncAt: completedAt,
          now: completedAt,
        }),
      ).resolves.toBe(true);
      await expect(
        prisma.analyticsIntegrationState.findUniqueOrThrow({
          where: {
            projectId_integrationType: {
              projectId: lifecycleProjectId,
              integrationType: "POSTHOG",
            },
          },
          select: { status: true },
        }),
      ).resolves.toEqual({ status: "ACTIVE" });
      await expect(
        prisma.posthogIntegration.findUniqueOrThrow({
          where: { projectId: lifecycleProjectId },
          select: { lastSyncAt: true },
        }),
      ).resolves.toEqual({ lastSyncAt: completedAt });

      await activationRepository.beginAnalyticsCapabilityDrain({
        client: prisma,
        capability: "analyticsIntegrations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 12n,
        now: new Date(lifecycleNow.getTime() + 8_000),
      });
      const drainingOperation = await createCompletableOperation(
        "draining",
        lifecycleProjectId,
      );
      await loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId: drainingOperation.operationId,
        projectId: lifecycleProjectId,
        now: new Date(lifecycleNow.getTime() + 9_000),
        integrationCapture: {
          admissionContext,
          targets: targetsFor({
            ...drainingOperation,
            label: "draining",
          }),
        },
      });
      await expect(
        prisma.analyticsIntegrationPendingDelivery.count({
          where: {
            projectId: lifecycleProjectId,
            integrationGeneration: 1n,
            status: "SUSPENDED",
          },
        }),
      ).resolves.toBe(4);

      const disabled = await activationRepository.disableAnalyticsCapability({
        client: prisma,
        capability: "analyticsIntegrations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 12n,
        captureRequired: true,
        rescanRequired: true,
        verifyDurableDrain: (transaction, provenance) =>
          deliveryRepository.verifyDorisAnalyticsIntegrationDrain(
            transaction,
            provenance,
          ),
        sealReplayCutoff: (transaction) =>
          deliveryRepository.sealDorisAnalyticsIntegrationReplayCutoff(
            transaction,
          ),
        now: new Date(lifecycleNow.getTime() + 10_000),
      });
      expect(disabled).toMatchObject({
        status: "DISABLED",
        captureRequired: true,
        rescanRequired: true,
        cutoffActivationGeneration: 12n,
      });
      await activationRepository.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "analyticsIntegrations",
        expectedGeneration: 12n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now: new Date(lifecycleNow.getTime() + 11_000),
      });
      await activationRepository.enableAnalyticsCapabilityDarkCapture({
        client: prisma,
        capability: "analyticsIntegrations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 13n,
        expectedRuntimeInstanceIds: [runtimeLeaseId, webRuntimeLeaseId],
        now: new Date(lifecycleNow.getTime() + 12_000),
      });
      await expect(
        deliveryRepository.replayDorisAnalyticsIntegrationDrainCapture({
          client: prisma,
          admissionContext,
          projectId: lifecycleProjectId,
          integrationType: "POSTHOG",
          now: new Date(lifecycleNow.getTime() + 13_000),
        }),
      ).resolves.toBe(4);
      await expect(
        prisma.analyticsIntegrationPendingDelivery.groupBy({
          by: ["integrationGeneration", "status"],
          where: {
            projectId: lifecycleProjectId,
            operationId: drainingOperation.operationId,
          },
          _count: true,
          orderBy: { integrationGeneration: "asc" },
        }),
      ).resolves.toEqual([
        {
          integrationGeneration: 1n,
          status: "QUARANTINED",
          _count: 4,
        },
        {
          integrationGeneration: 2n,
          status: "SUSPENDED",
          _count: 4,
        },
      ]);
      await expect(
        prisma.analyticsIntegrationPendingDelivery.findMany({
          where: {
            projectId: lifecycleProjectId,
            integrationGeneration: 2n,
          },
          select: {
            capabilityActivationGeneration: true,
            analyticsBackend: true,
          },
        }),
      ).resolves.toEqual(
        Array.from({ length: 4 }, () => ({
          capabilityActivationGeneration: 13n,
          analyticsBackend: "DORIS",
        })),
      );
    });
  },
);
