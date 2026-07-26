import { Prisma, PrismaClient } from "@prisma/client";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import type { AnalyticsRuntimeAdmissionContext } from "../analytics-persistence/analyticsBackendAdmission";
import type { AnalyticsEvaluationDispatchEventType } from "../queues";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;

describe.skipIf(!controlDatabaseUrl)(
  "Doris evaluation visible-operation capture",
  () => {
    const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const organizationId = `evaluation-dispatch-org-${suffix}`;
    const projectId = `evaluation-dispatch-project-${suffix}`;
    const runtimeLeaseId = `evaluation-capture-worker-${suffix}`;
    const webRuntimeLeaseId = `evaluation-producer-web-${suffix}`;
    const epoch = "e".repeat(64);
    const queueNamespace = "q".repeat(64);
    const future = new Date(Date.now() + 3_600_000);
    const admissionContext: AnalyticsRuntimeAdmissionContext = {
      runtimeLeaseId,
      backend: "doris",
      deploymentGeneration: 1n,
    };
    let loadRepository: typeof import("./analyticsLoadBatches.js");

    beforeAll(async () => {
      loadRepository = await import("./analyticsLoadBatches.js");
      await prisma.organization.create({
        data: { id: organizationId, name: "Evaluation dispatch test" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          orgId: organizationId,
          name: "Evaluation dispatch test",
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
        where: { capability: "EVALUATIONS" },
        data: {
          backend: "DORIS",
          deploymentGeneration: 1n,
          generation: 7n,
          contractVersion: 1,
          minimumRuntimeContract: 1,
          status: "DARK",
          captureEnabled: false,
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
          leaseExpiresAt: future,
          capabilityContracts: {
            create: {
              capability: "EVALUATIONS",
              supportedContractVersion: 1,
              installedRoles: ["CAPTURE", "CONSUMER", "RECOVERY"],
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
          heartbeatAt: new Date(),
          leaseExpiresAt: future,
          capabilityContracts: {
            create: {
              capability: "EVALUATIONS",
              supportedContractVersion: 1,
              installedRoles: ["PRODUCER"],
            },
          },
        },
      });
    }, 90_000);

    afterAll(async () => {
      await prisma.organization.deleteMany({ where: { id: organizationId } });
      await prisma.analyticsRuntimeLease.deleteMany({
        where: { id: { in: [runtimeLeaseId, webRuntimeLeaseId] } },
      });
      await prisma.analyticsBackendDeploymentState.deleteMany();
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "EVALUATIONS" },
        data: {
          deploymentGeneration: 0n,
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

    const createCompletableOperation = async (label: string) => {
      const operationId = `evaluation-operation-${label}-${suffix}`;
      const loadBatchId = `evaluation-load-${label}-${suffix}`;
      const candidateOne = `event-one-${label}`;
      const candidateTwo = `event-two-${label}`;
      const entityKeyOne = `event-identity-${label}-0`;
      await prisma.analyticsIngestionOperation.create({
        data: {
          id: operationId,
          projectId,
          sourceOperationId: `source-${label}-${suffix}`,
          sourceChecksum: "a".repeat(64),
          rawObjectKey: `raw/${operationId}.json`,
          acceptedAt: new Date("2026-07-23T00:00:00.000Z"),
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
          recoverableUntil: new Date("2026-07-30T00:00:00.000Z"),
          statusExpiresAt: new Date("2026-08-30T00:00:00.000Z"),
          candidates: {
            create: [candidateOne, candidateTwo].map((candidateKey, index) => ({
              candidateKey,
              entityType: "EVENT",
              entityKey: `event-identity-${label}-${index}`,
              owningTraceId: "trace-1",
              partitionDate: new Date("2026-07-23T00:00:00.000Z"),
              sourceVersion: BigInt(index + 1),
              canonicalPayloadHash: String(index + 1).repeat(64),
              disposition: "LOAD_REQUIRED",
              loadBatchId,
            })),
          },
          loadBatches: {
            create: {
              id: loadBatchId,
              databaseName: "langfuse_test",
              targetTable: "events_current",
              logicalBatchId: `events-${label}`,
              attempt: 0,
              fenceGeneration: 1n,
              label: `evaluation_${label}_${suffix}`.replaceAll("-", "_"),
              payloadHash: "b".repeat(64),
              canonicalObjectKey: `canonical/${operationId}.json`,
              partitionDate: new Date("2026-07-23T00:00:00.000Z"),
              status: "VISIBLE",
              totalRows: 2,
              filteredRows: 0,
              visibleAt: new Date("2026-07-23T00:00:01.000Z"),
            },
          },
        },
      });
      return { operationId, candidateOne, candidateTwo, entityKeyOne };
    };

    it("does not capture while DARK capture is disabled", async () => {
      const operation = await createCompletableOperation("disabled");
      await expect(
        loadRepository.completeAnalyticsIngestionOperation({
          client: prisma,
          operationId: operation.operationId,
          projectId,
          now: new Date("2026-07-23T00:00:02.000Z"),
          evaluationCapture: {
            admissionContext,
            targets: [
              {
                candidateKey: operation.candidateOne,
                targetType: "TRACE_UPSERT",
                targetId: "trace-1",
                traceId: "trace-1",
                observationId: null,
                datasetItemId: null,
                targetTimestamp: new Date("2026-07-23T00:00:00.000Z"),
                traceEnvironment: "production",
              },
            ],
          },
        }),
      ).resolves.toMatchObject({ outcome: "completed", status: "VISIBLE" });
      await expect(
        prisma.analyticsEvaluationDispatch.count({
          where: { operationId: operation.operationId },
        }),
      ).resolves.toBe(0);
    });

    it("captures one suspended trace effect in DARK and a pending effect in ACTIVE", async () => {
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "EVALUATIONS" },
        data: {
          captureEnabled: true,
          captureStartedAt: new Date("2026-07-23T00:00:00.000Z"),
          captureExpiresAt: new Date("2026-07-24T00:00:00.000Z"),
          captureRowBudget: 100,
          captureRows: 0n,
        },
      });
      const dark = await createCompletableOperation("dark");
      await loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId: dark.operationId,
        projectId,
        now: new Date("2026-07-23T00:00:03.000Z"),
        evaluationCapture: {
          admissionContext,
          targets: [dark.candidateOne, dark.candidateTwo].map(
            (candidateKey) => ({
              candidateKey,
              targetType: "TRACE_UPSERT" as const,
              targetId: "trace-1",
              traceId: "trace-1",
              observationId: null,
              datasetItemId: null,
              targetTimestamp: new Date("2026-07-23T00:00:00.000Z"),
              traceEnvironment: "production",
            }),
          ),
        },
      });
      await expect(
        prisma.analyticsEvaluationDispatch.findMany({
          where: { operationId: dark.operationId },
        }),
      ).resolves.toMatchObject([
        {
          projectId,
          operationId: dark.operationId,
          targetType: "TRACE_UPSERT",
          targetId: "trace-1",
          status: "SUSPENDED",
          analyticsBackend: "DORIS",
          deploymentGeneration: 1n,
          capabilityActivationGeneration: 7n,
          capabilityContractVersion: 1,
        },
      ]);

      const activations = await import("./analyticsCapabilityActivations.js");
      const bootstrapEvidenceDigest = "d".repeat(64);
      await activations.completeAnalyticsCapabilityBootstrap({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 7n,
        verifyDurableBootstrap: async () => ({ bootstrapEvidenceDigest }),
      });
      await activations.activateAnalyticsCapability({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 7n,
        expectedRuntimeInstanceIds: [runtimeLeaseId, webRuntimeLeaseId],
        expectedBootstrapEvidenceDigest: bootstrapEvidenceDigest,
      });
      await expect(
        prisma.analyticsEvaluationDispatch.findFirstOrThrow({
          where: { operationId: dark.operationId },
        }),
      ).resolves.toMatchObject({
        status: "PENDING",
        failureCode: null,
      });

      const active = await createCompletableOperation("active");
      await loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId: active.operationId,
        projectId,
        now: new Date("2026-07-23T00:00:04.000Z"),
        evaluationCapture: {
          admissionContext,
          targets: [
            {
              candidateKey: active.candidateOne,
              targetType: "OBSERVATION_UPSERT",
              targetId: "span-1",
              traceId: "trace-1",
              observationId: "span-1",
              datasetItemId: null,
              targetTimestamp: new Date("2026-07-23T00:00:00.000Z"),
              traceEnvironment: "production",
            },
          ],
        },
      });
      await expect(
        prisma.analyticsEvaluationDispatch.findFirstOrThrow({
          where: { operationId: active.operationId },
        }),
      ).resolves.toMatchObject({
        status: "PENDING",
        targetType: "OBSERVATION_UPSERT",
        targetId: "span-1",
      });

      const dispatch =
        await prisma.analyticsEvaluationDispatch.findFirstOrThrow({
          where: { operationId: active.operationId },
        });
      await expect(
        (
          await import("./analyticsEvaluationDispatches.js")
        ).findPendingAnalyticsEvaluationDispatches({
          client: prisma,
          now: new Date("2026-07-23T00:00:05.000Z"),
          limit: 10,
        }),
      ).resolves.toContainEqual({
        id: dispatch.id,
        dispatchGeneration: 1,
      });

      let envelope: AnalyticsEvaluationDispatchEventType | undefined;
      const dispatchRepository =
        await import("./analyticsEvaluationDispatches.js");
      await expect(
        dispatchRepository.publishAnalyticsEvaluationDispatch({
          client: prisma,
          admissionContext,
          dispatchId: dispatch.id,
          expectedGeneration: 1,
          publish: async (value) => {
            envelope = value;
          },
        }),
      ).resolves.toBe(true);
      expect(envelope).toMatchObject({
        dispatchId: dispatch.id,
        dispatchGeneration: 1,
        projectId,
        operationId: active.operationId,
        targetType: "OBSERVATION_UPSERT",
        targetId: "span-1",
        analyticsBackend: "DORIS",
        deploymentGeneration: "1",
        capabilityActivationGeneration: "7",
      });

      await expect(
        dispatchRepository.claimAnalyticsEvaluationDispatch({
          client: prisma,
          admissionContext,
          envelope: { ...envelope!, projectId: "other-project" },
          leaseOwner: "consumer-a",
          leaseMs: 60_000,
          now: new Date("2026-07-23T00:00:06.000Z"),
        }),
      ).rejects.toThrow("envelope does not match");
      await expect(
        dispatchRepository.claimAnalyticsEvaluationDispatch({
          client: prisma,
          admissionContext,
          envelope: envelope!,
          leaseOwner: "consumer-a",
          leaseMs: 60_000,
          now: new Date("2026-07-23T00:00:06.000Z"),
        }),
      ).resolves.toMatchObject({
        id: dispatch.id,
        status: "PROCESSING",
        processingLeaseOwner: "consumer-a",
      });
      await expect(
        dispatchRepository.requeueAnalyticsEvaluationDispatch({
          client: prisma,
          dispatchId: dispatch.id,
          expectedGeneration: 1,
          leaseOwner: "consumer-a",
          now: new Date("2026-07-23T00:00:07.000Z"),
        }),
      ).resolves.toBe(true);
      await expect(
        dispatchRepository.quarantineAnalyticsEvaluationDispatch({
          client: prisma,
          dispatchId: dispatch.id,
          expectedGeneration: 1,
          failureCode: "STALE_CONSUMER",
        }),
      ).resolves.toBe(false);
      await expect(
        prisma.analyticsEvaluationDispatch.findUniqueOrThrow({
          where: { id: dispatch.id },
        }),
      ).resolves.toMatchObject({
        status: "PENDING",
        dispatchGeneration: 2,
        failureCode: null,
      });
    });

    it("creates idempotent historical dispatches only for current visible targets", async () => {
      const operation = await createCompletableOperation("historical");
      await loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId: operation.operationId,
        projectId,
        now: new Date("2026-07-23T00:10:00.000Z"),
      });
      await prisma.analyticsEntityHead.create({
        data: {
          projectId,
          entityType: "EVENT",
          entityKey: operation.entityKeyOne,
          lookupId: "historical-span",
          owningTraceId: "historical-trace",
          sourceVersion: 1n,
          canonicalPayloadHash: "c".repeat(64),
          partitionDate: new Date("2026-07-23T00:00:00.000Z"),
          canonicalizerVersion: "2",
          fenceGeneration: 1n,
          operationId: operation.operationId,
        },
      });
      const template = await prisma.evalTemplate.create({
        data: {
          id: `historical-template-${suffix}`,
          projectId,
          name: `Historical template ${suffix}`,
          version: 1,
          type: "LLM_AS_JUDGE",
        },
      });
      const configuration = await prisma.jobConfiguration.create({
        data: {
          id: `historical-config-${suffix}`,
          projectId,
          jobType: "EVAL",
          evalTemplateId: template.id,
          scoreName: "historical-score",
          filter: [],
          targetObject: "EVENT",
          variableMapping: [],
          sampling: new Prisma.Decimal(1),
          delay: 0,
        },
      });
      const dispatchRepository =
        await import("./analyticsEvaluationDispatches.js");
      const target = {
        requestId: `historical-request-${suffix}`,
        jobConfigurationId: configuration.id,
        targetId: "historical-span",
        traceId: "historical-trace",
        observationId: "historical-span",
        datasetItemId: null,
        datasetRunItemId: null,
        targetTimestamp: new Date("2026-07-23T00:00:00.000Z"),
        traceEnvironment: "production",
      } as const;

      await expect(
        dispatchRepository.createHistoricalAnalyticsEvaluationDispatches({
          client: prisma,
          admissionContext,
          projectId,
          targets: [target],
          now: new Date("2026-07-23T00:10:01.000Z"),
        }),
      ).resolves.toEqual({ created: 1, missing: 0 });
      await expect(
        dispatchRepository.createHistoricalAnalyticsEvaluationDispatches({
          client: prisma,
          admissionContext,
          projectId,
          targets: [target],
          now: new Date("2026-07-23T00:10:02.000Z"),
        }),
      ).resolves.toEqual({ created: 0, missing: 0 });
      await expect(
        prisma.analyticsEvaluationDispatch.findUniqueOrThrow({
          where: {
            requestId_targetType_targetId: {
              requestId: target.requestId,
              targetType: "HISTORICAL",
              targetId: target.targetId,
            },
          },
        }),
      ).resolves.toMatchObject({
        operationId: operation.operationId,
        sourceCandidateKey: operation.candidateOne,
        requestId: target.requestId,
        jobConfigurationId: configuration.id,
        status: "PENDING",
        analyticsBackend: "DORIS",
        deploymentGeneration: 1n,
        capabilityActivationGeneration: 7n,
      });

      await expect(
        dispatchRepository.createHistoricalAnalyticsEvaluationDispatches({
          client: prisma,
          admissionContext,
          projectId,
          targets: [
            {
              ...target,
              requestId: `missing-request-${suffix}`,
              targetId: "missing-span",
              observationId: "missing-span",
            },
          ],
          now: new Date("2026-07-23T00:10:03.000Z"),
        }),
      ).resolves.toEqual({ created: 0, missing: 1 });
    });

    it("captures same-generation suspended work while DRAINING", async () => {
      const activations = await import("./analyticsCapabilityActivations.js");
      await activations.beginAnalyticsCapabilityDrain({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 7n,
      });
      const operation = await createCompletableOperation("draining");
      await loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId: operation.operationId,
        projectId,
        now: new Date("2026-07-23T00:20:00.000Z"),
        evaluationCapture: {
          admissionContext,
          targets: [
            {
              candidateKey: operation.candidateOne,
              targetType: "TRACE_UPSERT",
              targetId: "trace-draining",
              traceId: "trace-draining",
              observationId: null,
              datasetItemId: null,
              targetTimestamp: new Date("2026-07-23T00:19:00.000Z"),
              traceEnvironment: "production",
            },
          ],
        },
      });
      const dispatch =
        await prisma.analyticsEvaluationDispatch.findFirstOrThrow({
          where: { operationId: operation.operationId },
        });
      expect(dispatch).toMatchObject({
        status: "SUSPENDED",
        capabilityActivationGeneration: 7n,
      });
      const dispatchRepository =
        await import("./analyticsEvaluationDispatches.js");
      await expect(
        dispatchRepository.findPendingAnalyticsEvaluationDispatches({
          client: prisma,
          now: new Date("2026-07-23T00:20:01.000Z"),
          limit: 100,
        }),
      ).resolves.not.toContainEqual({
        id: dispatch.id,
        dispatchGeneration: dispatch.dispatchGeneration,
      });
    });

    it("seals a configuration-stable cutoff and transfers suspended work to the next generation", async () => {
      const activations = await import("./analyticsCapabilityActivations.js");
      const evaluationCapability =
        await import("./analyticsEvaluationCapability.js");
      await prisma.analyticsEvaluationDispatch.updateMany({
        where: {
          capabilityActivationGeneration: 7n,
          status: { in: ["PENDING", "PUBLISHED", "PROCESSING"] },
        },
        data: {
          status: "COMPLETED",
          completedAt: new Date("2026-07-23T00:21:00.000Z"),
        },
      });

      const disabled = await activations.disableAnalyticsCapability({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 7n,
        captureRequired: true,
        rescanRequired: true,
        verifyDurableDrain: (transaction, provenance) =>
          evaluationCapability.verifyDurableAnalyticsEvaluationDrain(
            transaction,
            provenance,
          ),
        sealReplayCutoff: (transaction) =>
          evaluationCapability.sealAnalyticsEvaluationReplayCutoff({
            transaction,
            deploymentGeneration: 1n,
            activationGeneration: 7n,
            now: new Date("2026-07-23T00:22:00.000Z"),
          }),
        now: new Date("2026-07-23T00:22:00.000Z"),
      });
      const sealed = evaluationCapability.parseAnalyticsEvaluationReplayCutoff(
        disabled.cutoffState,
      );
      expect(sealed).toMatchObject({
        kind: "evaluation_operation_cutoff",
        deploymentGeneration: "1",
        sourceActivationGeneration: "7",
        configurationCount: expect.any(Number),
        lowerAcceptanceSequence: expect.stringMatching(/^[1-9][0-9]*$/),
      });
      expect(sealed.configurationCount).toBeGreaterThan(0);

      const nextDark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "evaluations",
        expectedGeneration: 7n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now: new Date("2026-07-23T00:23:00.000Z"),
      });
      const capture = await activations.enableAnalyticsCapabilityDarkCapture({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: nextDark.generation,
        expectedRuntimeInstanceIds: [runtimeLeaseId, webRuntimeLeaseId],
        now: new Date("2026-07-23T00:24:00.000Z"),
      });
      const handoff = evaluationCapability.parseAnalyticsEvaluationReplayCutoff(
        capture.cutoffState,
      );
      expect(handoff.captureHandoffAt).toBe("2026-07-23T00:24:00.000Z");
      expect(handoff.captureHandoffAcceptanceSequence).toEqual(
        expect.stringMatching(/^[1-9][0-9]*$/),
      );
      expect(capture.cutoffDigest).toBe(
        evaluationCapability.digestAnalyticsEvaluationReplayState(handoff),
      );

      await expect(
        evaluationCapability.transferSuspendedAnalyticsEvaluationDispatches({
          client: prisma,
          admissionContext,
          expectedCutoffDigest: capture.cutoffDigest!,
          now: new Date("2026-07-23T00:25:00.000Z"),
        }),
      ).resolves.toBe(1);
      await expect(
        prisma.analyticsEvaluationDispatch.findFirstOrThrow({
          where: {
            targetId: "trace-draining",
            targetType: "TRACE_UPSERT",
          },
        }),
      ).resolves.toMatchObject({
        status: "SUSPENDED",
        capabilityActivationGeneration: 8n,
        capabilityContractVersion: 1,
        captureRuntimeLeaseId: runtimeLeaseId,
      });

      const completed = await activations.completeAnalyticsCapabilityBootstrap({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 8n,
        expectedCutoffDigest: capture.cutoffDigest!,
        verifyDurableBootstrap: (transaction) =>
          evaluationCapability.verifyDurableAnalyticsEvaluationBootstrap(
            transaction,
            {
              deploymentGeneration: 1n,
              activationGeneration: 8n,
              expectedCutoffDigest: capture.cutoffDigest!,
            },
          ),
        now: new Date("2026-07-23T00:26:00.000Z"),
      });
      expect(completed.bootstrapEvidenceDigest).toMatch(/^[a-f0-9]{64}$/);
      await activations.activateAnalyticsCapability({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 8n,
        expectedRuntimeInstanceIds: [runtimeLeaseId, webRuntimeLeaseId],
        expectedBootstrapEvidenceDigest: completed.bootstrapEvidenceDigest!,
        now: new Date("2026-07-23T00:27:00.000Z"),
      });
      await expect(
        prisma.analyticsEvaluationDispatch.findFirstOrThrow({
          where: {
            targetId: "trace-draining",
            targetType: "TRACE_UPSERT",
          },
        }),
      ).resolves.toMatchObject({
        status: "PENDING",
        capabilityActivationGeneration: 8n,
      });
    });

    it("closes an exhausted DARK capture window without losing suspended work", async () => {
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "EVALUATIONS" },
        data: {
          generation: 9n,
          status: "DARK",
          captureEnabled: true,
          captureStartedAt: new Date("2026-07-23T01:00:00.000Z"),
          captureExpiresAt: new Date("2026-07-24T01:00:00.000Z"),
          captureRowBudget: 1,
          captureRows: 0n,
          captureRequired: false,
          rescanRequired: false,
          cutoffState: Prisma.DbNull,
          cutoffActivationGeneration: null,
          cutoffDigest: null,
        },
      });
      const first = await createCompletableOperation("budget-one");
      await loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId: first.operationId,
        projectId,
        now: new Date("2026-07-23T01:00:01.000Z"),
        evaluationCapture: {
          admissionContext,
          targets: [
            {
              candidateKey: first.candidateOne,
              targetType: "TRACE_UPSERT",
              targetId: "trace-budget-one",
              traceId: "trace-budget-one",
              observationId: null,
              datasetItemId: null,
              targetTimestamp: new Date("2026-07-23T01:00:00.000Z"),
              traceEnvironment: "production",
            },
          ],
        },
      });
      const second = await createCompletableOperation("budget-two");
      await loadRepository.completeAnalyticsIngestionOperation({
        client: prisma,
        operationId: second.operationId,
        projectId,
        now: new Date("2026-07-23T01:00:02.000Z"),
        evaluationCapture: {
          admissionContext,
          targets: [
            {
              candidateKey: second.candidateOne,
              targetType: "TRACE_UPSERT",
              targetId: "trace-budget-two",
              traceId: "trace-budget-two",
              observationId: null,
              datasetItemId: null,
              targetTimestamp: new Date("2026-07-23T01:00:00.000Z"),
              traceEnvironment: "production",
            },
          ],
        },
      });

      await expect(
        prisma.analyticsEvaluationDispatch.findFirstOrThrow({
          where: { operationId: first.operationId },
        }),
      ).resolves.toMatchObject({
        status: "SUSPENDED",
        failureCode: "EVALUATION_CAPTURE_BUDGET_EXCEEDED",
      });
      await expect(
        prisma.analyticsEvaluationDispatch.count({
          where: { operationId: second.operationId },
        }),
      ).resolves.toBe(0);
      await expect(
        prisma.analyticsCapabilityActivation.findUniqueOrThrow({
          where: { capability: "EVALUATIONS" },
        }),
      ).resolves.toMatchObject({
        status: "DISABLED",
        captureEnabled: false,
        captureRows: 1n,
        captureRequired: true,
        rescanRequired: true,
        cutoffActivationGeneration: 9n,
      });
    });
  },
);
