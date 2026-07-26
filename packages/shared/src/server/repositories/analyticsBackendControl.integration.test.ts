import { Prisma, PrismaClient } from "@prisma/client";
import { afterAll, beforeEach, describe, expect, it } from "vitest";

import { ANALYTICS_CAPABILITY_NAMES } from "../analytics-persistence/analyticsCapabilities";
import { toPrismaAnalyticsCapability } from "../analytics-persistence/analyticsBackendMapping";
import { createEmptyAnalyticsQueueDrainEvidence } from "../redis/analyticsQueueDrain.test-helper";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;
if (process.env.DORIS_POC_ENABLED === "1" && !controlDatabaseUrl) {
  throw new Error("Doris harness must provide an isolated control database");
}
const digest = (value: string) => value.repeat(64);

describe.skipIf(!controlDatabaseUrl)("analytics backend control plane", () => {
  const prisma = new PrismaClient({ datasourceUrl: controlDatabaseUrl });
  let deployment: typeof import("./analyticsBackendDeployment.js");
  let leases: typeof import("./analyticsRuntimeLeases.js");
  let activations: typeof import("./analyticsCapabilityActivations.js");
  let admission: typeof import("../analytics-persistence/analyticsBackendAdmission.js");
  let runtimeControl: typeof import("../analytics-persistence/AnalyticsRuntimeController.js");
  const queueNamespaceFingerprint = digest("f");
  const emptyQueueDrain = (scope: {
    backend: "clickhouse" | "doris";
    deploymentGeneration: bigint;
    workloadEpochFingerprint: string;
  }) =>
    createEmptyAnalyticsQueueDrainEvidence({
      scope,
      queueNamespaceFingerprint,
    });

  beforeEach(async () => {
    deployment = await import("./analyticsBackendDeployment.js");
    leases = await import("./analyticsRuntimeLeases.js");
    activations = await import("./analyticsCapabilityActivations.js");
    admission =
      await import("../analytics-persistence/analyticsBackendAdmission.js");
    runtimeControl =
      await import("../analytics-persistence/AnalyticsRuntimeController.js");

    await prisma.analyticsIngestionOperation.deleteMany();
    await prisma.analyticsDeletionOperation.deleteMany();
    await prisma.analyticsCheckpointGeneration.deleteMany();
    await prisma.analyticsRetentionRun.deleteMany();
    await prisma.batchAction.deleteMany();
    await prisma.batchExport.deleteMany();
    await prisma.jobExecution.deleteMany();
    await prisma.jobConfiguration.deleteMany();
    await prisma.analyticsBackendClaimLease.deleteMany();
    await prisma.analyticsRuntimeCapabilityContract.deleteMany();
    await prisma.analyticsRuntimeLease.deleteMany();
    await prisma.analyticsBackendDeploymentTransition.deleteMany();
    await prisma.analyticsBackendDeploymentState.deleteMany();
    await prisma.analyticsCapabilityActivation.updateMany({
      data: {
        backend: "DORIS",
        deploymentGeneration: 0n,
        generation: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        status: "DISABLED",
        captureEnabled: false,
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
  });

  afterAll(async () => {
    await prisma.$disconnect();
  });

  const freshInput = () => ({
    client: prisma,
    backend: "doris" as const,
    workloadEpochFingerprint:
      deployment.fingerprintAnalyticsWorkloadEpoch("fresh-epoch"),
    queueNamespaceFingerprint,
    foundationContractVersion: 1,
    allowFreshInitialization: true,
    freshDeploymentEvidence: {
      selectedBackendEmpty: true,
      evidenceDigest: digest("a"),
    },
    now: new Date("2026-07-21T10:00:00.000Z"),
  });

  type PendingRegistryKind =
    | "load batch"
    | "deletion"
    | "checkpoint"
    | "retention"
    | "batch action"
    | "batch export"
    | "job execution";

  type PendingRegistryFixture = {
    readonly assertUnchanged: () => Promise<void>;
    readonly terminalize: () => Promise<void>;
    readonly assertTerminal: () => Promise<void>;
    readonly cleanup: () => Promise<void>;
  };

  async function createPendingRegistryFixture(input: {
    kind: PendingRegistryKind;
    organizationId: string;
    projectId: string;
    suffix: string;
    now: Date;
  }): Promise<PendingRegistryFixture> {
    const noCleanup = async () => undefined;

    switch (input.kind) {
      case "load batch": {
        const operation = await prisma.analyticsIngestionOperation.create({
          data: {
            id: `registry-operation-${input.suffix}`,
            projectId: input.projectId,
            sourceOperationId: `registry-source-${input.suffix}`,
            sourceChecksum: digest("a"),
            rawObjectKey: `raw/${input.suffix}`,
            acceptedAt: input.now,
            acceptedAtNanos: BigInt(input.now.getTime()) * 1_000_000n,
            canonicalizerVersion: "1",
            schemaVersion: 3,
            recoverableUntil: new Date(input.now.getTime() + 86_400_000),
            statusExpiresAt: new Date(input.now.getTime() + 172_800_000),
            status: "VISIBLE",
            visibleAt: input.now,
            terminalAt: input.now,
          },
        });
        const row = await prisma.analyticsLoadBatch.create({
          data: {
            id: `registry-load-${input.suffix}`,
            operationId: operation.id,
            projectId: input.projectId,
            databaseName: "legacy_analytics",
            targetTable: "events",
            logicalBatchId: `registry-${input.suffix}`,
            fenceGeneration: 1n,
            label: `registry_load_${input.suffix}`,
            payloadHash: digest("b"),
            canonicalObjectKey: `canonical/${input.suffix}`,
            status: "PENDING",
          },
        });
        return {
          assertUnchanged: async () => {
            await expect(
              prisma.analyticsLoadBatch.findUniqueOrThrow({
                where: { id: row.id },
              }),
            ).resolves.toEqual(row);
          },
          terminalize: async () => {
            await prisma.analyticsLoadBatch.update({
              where: { id: row.id },
              data: { status: "VISIBLE", visibleAt: input.now },
            });
          },
          assertTerminal: async () => {
            await expect(
              prisma.analyticsLoadBatch.findUniqueOrThrow({
                where: { id: row.id },
              }),
            ).resolves.toMatchObject({ status: "VISIBLE" });
          },
          cleanup: noCleanup,
        };
      }
      case "deletion": {
        const row = await prisma.analyticsDeletionOperation.create({
          data: {
            id: `registry-deletion-${input.suffix}`,
            scope: "PROJECT",
            organizationId: input.organizationId,
            projectId: input.projectId,
            generation: 1n,
            requesterPrincipalType: "USER",
            requesterPrincipalId: `registry-user-${input.suffix}`,
            statusExpiresAt: new Date(input.now.getTime() + 172_800_000),
            status: "SCHEDULED",
          },
        });
        return {
          assertUnchanged: async () => {
            await expect(
              prisma.analyticsDeletionOperation.findUniqueOrThrow({
                where: { id: row.id },
              }),
            ).resolves.toEqual(row);
          },
          terminalize: async () => {
            await prisma.analyticsDeletionOperation.update({
              where: { id: row.id },
              data: { status: "COMPLETED", completedAt: input.now },
            });
          },
          assertTerminal: async () => {
            await expect(
              prisma.analyticsDeletionOperation.findUniqueOrThrow({
                where: { id: row.id },
              }),
            ).resolves.toMatchObject({ status: "COMPLETED" });
          },
          cleanup: async () => {
            await prisma.analyticsDeletionOperation.deleteMany({
              where: { id: row.id },
            });
          },
        };
      }
      case "checkpoint": {
        const row = await prisma.analyticsCheckpointGeneration.create({
          data: {
            generation: 9_001n,
            leaseOwner: `registry-checkpoint-${input.suffix}`,
            leaseExpiresAt: new Date(input.now.getTime() + 60_000),
            operationHighWatermarkAcceptedAt: input.now,
            operationHighWatermarkAcceptedAtNanos:
              BigInt(input.now.getTime()) * 1_000_000n,
            loadHighWatermarkCreatedAt: input.now,
            deletionHighWatermarkCreatedAt: input.now,
            status: "PREPARING",
          },
        });
        return {
          assertUnchanged: async () => {
            await expect(
              prisma.analyticsCheckpointGeneration.findUniqueOrThrow({
                where: { generation: row.generation },
              }),
            ).resolves.toEqual(row);
          },
          terminalize: async () => {
            await prisma.analyticsCheckpointGeneration.update({
              where: { generation: row.generation },
              data: { status: "SEALED", sealedAt: input.now },
            });
          },
          assertTerminal: async () => {
            await expect(
              prisma.analyticsCheckpointGeneration.findUniqueOrThrow({
                where: { generation: row.generation },
              }),
            ).resolves.toMatchObject({ status: "SEALED" });
          },
          cleanup: async () => {
            await prisma.analyticsCheckpointGeneration.deleteMany({
              where: { generation: row.generation },
            });
          },
        };
      }
      case "retention": {
        const row = await prisma.analyticsRetentionRun.create({
          data: {
            id: `registry-retention-${input.suffix}`,
            cutoffDate: new Date("2026-06-01T00:00:00.000Z"),
            status: "RUNNING",
          },
        });
        return {
          assertUnchanged: async () => {
            await expect(
              prisma.analyticsRetentionRun.findUniqueOrThrow({
                where: { id: row.id },
              }),
            ).resolves.toEqual(row);
          },
          terminalize: async () => {
            await prisma.analyticsRetentionRun.update({
              where: { id: row.id },
              data: { status: "COMPLETED", completedAt: input.now },
            });
          },
          assertTerminal: async () => {
            await expect(
              prisma.analyticsRetentionRun.findUniqueOrThrow({
                where: { id: row.id },
              }),
            ).resolves.toMatchObject({ status: "COMPLETED" });
          },
          cleanup: async () => {
            await prisma.analyticsRetentionRun.deleteMany({
              where: { id: row.id },
            });
          },
        };
      }
      case "batch action": {
        const row = await prisma.batchAction.create({
          data: {
            id: `registry-batch-action-${input.suffix}`,
            projectId: input.projectId,
            userId: `registry-user-${input.suffix}`,
            actionType: "trace-delete",
            tableName: "traces",
            status: "QUEUED",
            query: {},
          },
        });
        return {
          assertUnchanged: async () => {
            await expect(
              prisma.batchAction.findUniqueOrThrow({ where: { id: row.id } }),
            ).resolves.toEqual(row);
          },
          terminalize: async () => {
            await prisma.batchAction.update({
              where: { id: row.id },
              data: { status: "COMPLETED", finishedAt: input.now },
            });
          },
          assertTerminal: async () => {
            await expect(
              prisma.batchAction.findUniqueOrThrow({ where: { id: row.id } }),
            ).resolves.toMatchObject({ status: "COMPLETED" });
          },
          cleanup: noCleanup,
        };
      }
      case "batch export": {
        const row = await prisma.batchExport.create({
          data: {
            id: `registry-batch-export-${input.suffix}`,
            projectId: input.projectId,
            userId: `registry-user-${input.suffix}`,
            name: "Backend adoption registry test",
            status: "QUEUED",
            query: {},
            format: "JSON",
          },
        });
        return {
          assertUnchanged: async () => {
            await expect(
              prisma.batchExport.findUniqueOrThrow({ where: { id: row.id } }),
            ).resolves.toEqual(row);
          },
          terminalize: async () => {
            await prisma.batchExport.update({
              where: { id: row.id },
              data: { status: "COMPLETED", finishedAt: input.now },
            });
          },
          assertTerminal: async () => {
            await expect(
              prisma.batchExport.findUniqueOrThrow({ where: { id: row.id } }),
            ).resolves.toMatchObject({ status: "COMPLETED" });
          },
          cleanup: noCleanup,
        };
      }
      case "job execution": {
        const configuration = await prisma.jobConfiguration.create({
          data: {
            id: `registry-job-config-${input.suffix}`,
            projectId: input.projectId,
            jobType: "EVAL",
            scoreName: "registry-score",
            filter: [],
            targetObject: "trace",
            variableMapping: [],
            sampling: new Prisma.Decimal(1),
            delay: 0,
          },
        });
        const row = await prisma.jobExecution.create({
          data: {
            id: `registry-job-execution-${input.suffix}`,
            projectId: input.projectId,
            jobConfigurationId: configuration.id,
            status: "PENDING",
          },
        });
        return {
          assertUnchanged: async () => {
            await expect(
              prisma.jobExecution.findUniqueOrThrow({ where: { id: row.id } }),
            ).resolves.toEqual(row);
          },
          terminalize: async () => {
            await prisma.jobExecution.update({
              where: { id: row.id },
              data: { status: "COMPLETED", endTime: input.now },
            });
          },
          assertTerminal: async () => {
            await expect(
              prisma.jobExecution.findUniqueOrThrow({ where: { id: row.id } }),
            ).resolves.toMatchObject({ status: "COMPLETED" });
          },
          cleanup: noCleanup,
        };
      }
    }
  }

  async function prepareQuiescedAdoption(input: {
    backend: "clickhouse" | "doris";
    suffix: string;
    now: Date;
    webBackend?: "clickhouse" | "doris";
    workerBackend?: "clickhouse" | "doris";
    webEpochFingerprint?: string;
    workerEpochFingerprint?: string;
    expectedInventoryWorkerId?: string;
  }) {
    const workloadEpochFingerprint =
      deployment.fingerprintAnalyticsWorkloadEpoch(
        `registry-adoption-${input.suffix}`,
      );
    const webInstanceId = `web-registry-${input.suffix}`;
    const workerInstanceId = `worker-registry-${input.suffix}`;
    const leaseInput = {
      client: prisma,
      deploymentGeneration: 0n,
      queueNamespaceFingerprint,
      buildId: "foundation-f0",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 3 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 60_000,
      now: input.now,
    };
    const web = await leases.registerAnalyticsRuntimeLease({
      ...leaseInput,
      component: "web",
      instanceId: webInstanceId,
      backend: input.webBackend ?? input.backend,
      workloadEpochFingerprint:
        input.webEpochFingerprint ?? workloadEpochFingerprint,
    });
    const worker = await leases.registerAnalyticsRuntimeLease({
      ...leaseInput,
      component: "worker",
      instanceId: workerInstanceId,
      backend: input.workerBackend ?? input.backend,
      workloadEpochFingerprint:
        input.workerEpochFingerprint ?? workloadEpochFingerprint,
    });
    await leases.markAnalyticsRuntimeQuiesced({
      client: prisma,
      runtimeLeaseId: web.lease.id,
      now: new Date(input.now.getTime() + 1_000),
    });
    await leases.markAnalyticsRuntimeQuiesced({
      client: prisma,
      runtimeLeaseId: worker.lease.id,
      now: new Date(input.now.getTime() + 1_000),
    });
    const expectedInventory = [
      { instanceId: webInstanceId, component: "web" as const },
      {
        instanceId: input.expectedInventoryWorkerId ?? workerInstanceId,
        component: "worker" as const,
      },
    ];
    let queueDrainCalls = 0;
    const adopt = () =>
      deployment.adoptExistingAnalyticsBackend({
        client: prisma,
        expectedBackend: input.backend,
        workloadEpochFingerprint,
        foundationContractVersion: 1,
        expectedInventory,
        expectedInventoryDigest:
          deployment.digestAnalyticsRuntimeInventory(expectedInventory),
        verifyScoreDeletionQueuesEmpty: async (scope) => {
          queueDrainCalls += 1;
          return emptyQueueDrain(scope);
        },
        drainAttestationDigest: digest("1"),
        denyProbeAttestationDigest: digest("2"),
        now: new Date(input.now.getTime() + 2_000),
      });

    return {
      adopt,
      queueDrainCalls: () => queueDrainCalls,
      leaseIds: [web.lease.id, worker.lease.id],
    };
  }

  it("does not infer a fresh install from an empty brownfield backend", async () => {
    await expect(
      deployment.resolveAnalyticsBackendStartup({
        ...freshInput(),
        allowFreshInitialization: false,
      }),
    ).resolves.toEqual({ mode: "ADOPTION_REQUIRED" });
    await expect(prisma.analyticsBackendDeploymentState.count()).resolves.toBe(
      0,
    );
  });

  it("initializes a fresh deployment exactly once and binds six disabled rows", async () => {
    const [first, second] = await Promise.all([
      deployment.resolveAnalyticsBackendStartup(freshInput()),
      deployment.resolveAnalyticsBackendStartup(freshInput()),
    ]);

    expect([first.mode, second.mode]).toEqual(["READY", "READY"]);
    expect(
      [first, second].filter(
        (result) => result.mode === "READY" && result.initialized,
      ),
    ).toHaveLength(1);
    await expect(prisma.analyticsBackendDeploymentState.count()).resolves.toBe(
      1,
    );
    await expect(
      prisma.analyticsBackendDeploymentTransition.count({
        where: { kind: "INITIALIZE" },
      }),
    ).resolves.toBe(1);

    const rows = await prisma.analyticsCapabilityActivation.findMany({
      orderBy: { capability: "asc" },
    });
    expect(rows).toHaveLength(ANALYTICS_CAPABILITY_NAMES.length);
    expect(rows).toEqual(
      expect.arrayContaining(
        ANALYTICS_CAPABILITY_NAMES.map((capability) =>
          expect.objectContaining({
            capability: toPrismaAnalyticsCapability(capability),
            backend: "DORIS",
            deploymentGeneration: 1n,
            status: "DISABLED",
          }),
        ),
      ),
    );
  });

  it("uses adoption-required mode for brownfield and never overwrites mismatch", async () => {
    const epoch = deployment.fingerprintAnalyticsWorkloadEpoch("brownfield");
    await expect(
      deployment.resolveAnalyticsBackendStartup({
        client: prisma,
        backend: "clickhouse",
        workloadEpochFingerprint: epoch,
        queueNamespaceFingerprint,
        foundationContractVersion: 1,
        allowFreshInitialization: false,
        freshDeploymentEvidence: {
          selectedBackendEmpty: false,
          evidenceDigest: digest("b"),
        },
      }),
    ).resolves.toEqual({ mode: "ADOPTION_REQUIRED" });

    const ready = await deployment.resolveAnalyticsBackendStartup(freshInput());
    expect(ready.mode).toBe("READY");
    await expect(
      deployment.resolveAnalyticsBackendStartup({
        ...freshInput(),
        backend: "clickhouse",
      }),
    ).resolves.toMatchObject({
      mode: "MISMATCH",
      reasonCode: "BACKEND_MISMATCH",
    });
    await expect(
      deployment.resolveAnalyticsBackendStartup({
        ...freshInput(),
        workloadEpochFingerprint:
          deployment.fingerprintAnalyticsWorkloadEpoch("wrong-epoch"),
      }),
    ).resolves.toMatchObject({
      mode: "MISMATCH",
      reasonCode: "WORKLOAD_EPOCH_MISMATCH",
    });
    await expect(
      deployment.resolveAnalyticsBackendStartup({
        ...freshInput(),
        queueNamespaceFingerprint: digest("e"),
      }),
    ).resolves.toMatchObject({
      mode: "MISMATCH",
      reasonCode: "QUEUE_NAMESPACE_MISMATCH",
    });
    await expect(
      prisma.analyticsBackendDeploymentState.findUniqueOrThrow({
        where: { id: "global" },
      }),
    ).resolves.toMatchObject({ backend: "DORIS", generation: 1n });
  });

  it("expires leases without resurrection and requires quiescence for adoption", async () => {
    const now = new Date("2026-07-21T11:00:00.000Z");
    const epoch = deployment.fingerprintAnalyticsWorkloadEpoch("adopt-epoch");
    const base = {
      client: prisma,
      backend: "doris" as const,
      deploymentGeneration: 0n,
      workloadEpochFingerprint: epoch,
      queueNamespaceFingerprint,
      buildId: "foundation-f0",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 3 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 10_000,
      now,
    };
    const web = await leases.registerAnalyticsRuntimeLease({
      ...base,
      component: "web",
      instanceId: "web-adopt",
    });
    const worker = await leases.registerAnalyticsRuntimeLease({
      ...base,
      component: "worker",
      instanceId: "worker-adopt",
    });
    expect(web.mode).toBe("ADOPTION_REQUIRED");
    expect(worker.mode).toBe("ADOPTION_REQUIRED");

    await expect(
      leases.renewAnalyticsRuntimeLease({
        client: prisma,
        runtimeLeaseId: web.lease.id,
        leaseMs: 10_000,
        now: new Date(now.getTime() + 10_000),
      }),
    ).resolves.toBe(false);
    await expect(
      deployment.adoptExistingAnalyticsBackend({
        client: prisma,
        expectedBackend: "doris",
        workloadEpochFingerprint: epoch,
        foundationContractVersion: 1,
        expectedInventory: [
          { instanceId: "web-adopt", component: "web" },
          { instanceId: "worker-adopt", component: "worker" },
        ],
        expectedInventoryDigest: deployment.digestAnalyticsRuntimeInventory([
          { instanceId: "web-adopt", component: "web" },
          { instanceId: "worker-adopt", component: "worker" },
        ]),
        verifyScoreDeletionQueuesEmpty: emptyQueueDrain,
        drainAttestationDigest: digest("d"),
        denyProbeAttestationDigest: digest("e"),
        now: new Date(now.getTime() + 1_000),
      }),
    ).rejects.toThrow(/quiesced/i);

    await expect(
      leases.markAnalyticsRuntimeQuiesced({
        client: prisma,
        runtimeLeaseId: web.lease.id,
        now: new Date(now.getTime() + 2_000),
      }),
    ).resolves.toBe(true);
    await expect(
      leases.markAnalyticsRuntimeQuiesced({
        client: prisma,
        runtimeLeaseId: worker.lease.id,
        now: new Date(now.getTime() + 2_000),
      }),
    ).resolves.toBe(true);

    await expect(
      deployment.adoptExistingAnalyticsBackend({
        client: prisma,
        expectedBackend: "doris",
        workloadEpochFingerprint: epoch,
        foundationContractVersion: 1,
        expectedInventory: [
          { instanceId: "web-adopt", component: "web" },
          { instanceId: "worker-adopt", component: "worker" },
        ],
        expectedInventoryDigest: deployment.digestAnalyticsRuntimeInventory([
          { instanceId: "web-adopt", component: "web" },
          { instanceId: "worker-adopt", component: "worker" },
        ]),
        verifyScoreDeletionQueuesEmpty: emptyQueueDrain,
        drainAttestationDigest: digest("d"),
        denyProbeAttestationDigest: digest("e"),
        now: new Date(now.getTime() + 3_000),
      }),
    ).resolves.toMatchObject({ backend: "DORIS", generation: 1n });
    await expect(
      leases.renewAnalyticsRuntimeLease({
        client: prisma,
        runtimeLeaseId: worker.lease.id,
        leaseMs: 10_000,
        now: new Date(now.getTime() + 4_000),
      }),
    ).resolves.toBe(false);
  });

  it("refuses adoption until unstamped durable work drains and preserves evaluator configuration", async () => {
    const now = new Date("2026-07-21T11:30:00.000Z");
    const epoch = deployment.fingerprintAnalyticsWorkloadEpoch(
      "pending-adopt-epoch",
    );
    const inventory = [
      { instanceId: "web-pending-adopt", component: "web" as const },
      { instanceId: "worker-pending-adopt", component: "worker" as const },
    ];
    const leaseInput = {
      client: prisma,
      backend: "doris" as const,
      deploymentGeneration: 0n,
      workloadEpochFingerprint: epoch,
      queueNamespaceFingerprint,
      buildId: "foundation-f0",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 3 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 60_000,
      now,
    };
    const web = await leases.registerAnalyticsRuntimeLease({
      ...leaseInput,
      component: "web",
      instanceId: inventory[0].instanceId,
    });
    const worker = await leases.registerAnalyticsRuntimeLease({
      ...leaseInput,
      component: "worker",
      instanceId: inventory[1].instanceId,
    });
    await leases.markAnalyticsRuntimeQuiesced({
      client: prisma,
      runtimeLeaseId: web.lease.id,
      now: new Date(now.getTime() + 1_000),
    });
    await leases.markAnalyticsRuntimeQuiesced({
      client: prisma,
      runtimeLeaseId: worker.lease.id,
      now: new Date(now.getTime() + 1_000),
    });

    const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
    const organizationId = `adopt-org-${suffix}`;
    const projectId = `adopt-project-${suffix}`;
    const operationId = `adopt-operation-${suffix}`;
    await prisma.organization.create({
      data: { id: organizationId, name: "Backend adoption test" },
    });
    await prisma.project.create({
      data: {
        id: projectId,
        name: "Backend adoption test",
        orgId: organizationId,
      },
    });
    await prisma.analyticsIngestionOperation.create({
      data: {
        id: operationId,
        projectId,
        sourceOperationId: `source-${suffix}`,
        sourceChecksum: digest("f"),
        rawObjectKey: `raw/${suffix}`,
        acceptedAt: now,
        acceptedAtNanos: BigInt(now.getTime()) * 1_000_000n,
        canonicalizerVersion: "1",
        schemaVersion: 3,
        recoverableUntil: new Date(now.getTime() + 86_400_000),
        statusExpiresAt: new Date(now.getTime() + 172_800_000),
      },
    });

    const adoptInput = {
      client: prisma,
      expectedBackend: "doris" as const,
      workloadEpochFingerprint: epoch,
      foundationContractVersion: 1,
      expectedInventory: inventory,
      expectedInventoryDigest:
        deployment.digestAnalyticsRuntimeInventory(inventory),
      verifyScoreDeletionQueuesEmpty: emptyQueueDrain,
      drainAttestationDigest: digest("1"),
      denyProbeAttestationDigest: digest("2"),
      now: new Date(now.getTime() + 2_000),
    };
    await expect(
      deployment.adoptExistingAnalyticsBackend(adoptInput),
    ).rejects.toThrow(/pending work/i);

    await prisma.analyticsIngestionOperation.update({
      where: { id: operationId },
      data: { status: "VISIBLE", visibleAt: new Date(now.getTime() + 3_000) },
    });
    const jobConfigurationId = `adopt-job-config-${suffix}`;
    await prisma.jobConfiguration.create({
      data: {
        id: jobConfigurationId,
        projectId,
        jobType: "EVAL",
        scoreName: "adoption-test",
        filter: [],
        targetObject: "trace",
        variableMapping: [],
        sampling: new Prisma.Decimal(1),
        delay: 0,
      },
    });
    await expect(
      deployment.adoptExistingAnalyticsBackend({
        ...adoptInput,
        now: new Date(now.getTime() + 4_000),
      }),
    ).resolves.toMatchObject({ backend: "DORIS", generation: 1n });
    await expect(
      prisma.jobConfiguration.findUniqueOrThrow({
        where: { id: jobConfigurationId },
      }),
    ).resolves.toMatchObject({
      id: jobConfigurationId,
      projectId,
      scoreName: "adoption-test",
    });
    await expect(
      prisma.analyticsIngestionOperation.findUniqueOrThrow({
        where: { id: operationId },
      }),
    ).resolves.toMatchObject({
      status: "VISIBLE",
      analyticsBackend: null,
    });
    await prisma.organization.delete({ where: { id: organizationId } });
  });

  it.each<PendingRegistryKind>([
    "load batch",
    "deletion",
    "checkpoint",
    "retention",
    "batch action",
    "batch export",
    "job execution",
  ])(
    "refuses adoption for a pending %s without mutation and allows its terminal history",
    async (kind) => {
      const now = new Date("2026-07-21T11:40:00.000Z");
      const suffix = `${kind.replaceAll(" ", "-")}-${Date.now()}-${Math.random()
        .toString(16)
        .slice(2)}`;
      const organizationId = `registry-org-${suffix}`;
      const projectId = `registry-project-${suffix}`;
      await prisma.organization.create({
        data: { id: organizationId, name: "Backend registry test" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          name: "Backend registry test",
          orgId: organizationId,
        },
      });
      let fixture: PendingRegistryFixture | undefined;

      try {
        fixture = await createPendingRegistryFixture({
          kind,
          organizationId,
          projectId,
          suffix,
          now,
        });
        const fleet = await prepareQuiescedAdoption({
          backend: "clickhouse",
          suffix,
          now,
        });
        const leaseSnapshot = await prisma.analyticsRuntimeLease.findMany({
          where: { id: { in: fleet.leaseIds } },
          orderBy: { id: "asc" },
        });

        await expect(fleet.adopt()).rejects.toThrow(/unstamped pending work/i);
        expect(fleet.queueDrainCalls()).toBe(0);
        await fixture.assertUnchanged();
        await expect(
          prisma.analyticsBackendDeploymentState.count(),
        ).resolves.toBe(0);
        await expect(
          prisma.analyticsBackendDeploymentTransition.count(),
        ).resolves.toBe(0);
        await expect(
          prisma.analyticsRuntimeLease.findMany({
            where: { id: { in: fleet.leaseIds } },
            orderBy: { id: "asc" },
          }),
        ).resolves.toEqual(leaseSnapshot);

        await fixture.terminalize();
        await expect(fleet.adopt()).resolves.toMatchObject({
          backend: "CLICKHOUSE",
          generation: 1n,
        });
        expect(fleet.queueDrainCalls()).toBe(1);
        await fixture.assertTerminal();
      } finally {
        await fixture?.cleanup();
        await prisma.organization.deleteMany({
          where: { id: organizationId },
        });
      }
    },
  );

  it.each([
    {
      name: "mixed backend leases",
      workerBackend: "clickhouse" as const,
      workerEpoch: undefined,
      expectedInventoryWorkerId: undefined,
      error: /compatible and quiesced/i,
    },
    {
      name: "mixed workload-epoch leases",
      workerBackend: undefined,
      workerEpoch: "foreign-workload-epoch",
      expectedInventoryWorkerId: undefined,
      error: /compatible and quiesced/i,
    },
    {
      name: "declared inventory differing from live leases",
      workerBackend: undefined,
      workerEpoch: undefined,
      expectedInventoryWorkerId: "worker-declared-but-not-live",
      error: /inventory census/i,
    },
  ])(
    "refuses adoption for $name without creating control state",
    async ({
      name,
      workerBackend,
      workerEpoch,
      expectedInventoryWorkerId,
      error,
    }) => {
      const now = new Date("2026-07-21T11:50:00.000Z");
      const suffix = name.replaceAll(/[^a-z]+/g, "-");
      const fleet = await prepareQuiescedAdoption({
        backend: "doris",
        suffix,
        now,
        workerBackend,
        workerEpochFingerprint: workerEpoch
          ? deployment.fingerprintAnalyticsWorkloadEpoch(workerEpoch)
          : undefined,
        expectedInventoryWorkerId,
      });
      const leaseSnapshot = await prisma.analyticsRuntimeLease.findMany({
        where: { id: { in: fleet.leaseIds } },
        orderBy: { id: "asc" },
      });

      await expect(fleet.adopt()).rejects.toThrow(error);
      expect(fleet.queueDrainCalls()).toBe(0);
      await expect(
        prisma.analyticsBackendDeploymentState.count(),
      ).resolves.toBe(0);
      await expect(
        prisma.analyticsBackendDeploymentTransition.count(),
      ).resolves.toBe(0);
      await expect(
        prisma.analyticsRuntimeLease.findMany({
          where: { id: { in: fleet.leaseIds } },
          orderBy: { id: "asc" },
        }),
      ).resolves.toEqual(leaseSnapshot);
    },
  );

  it("activates only after an exact compatible census and admits atomically", async () => {
    await deployment.resolveAnalyticsBackendStartup(freshInput());
    const now = new Date("2026-07-21T12:00:00.000Z");
    const epoch = freshInput().workloadEpochFingerprint;

    const dark = await activations.beginAnalyticsCapabilityDark({
      client: prisma,
      capability: "coreBatchExports",
      expectedGeneration: 1n,
      contractVersion: 1,
      minimumRuntimeContract: 1,
      now,
    });
    expect(dark).toMatchObject({ status: "DARK", generation: 2n });
    await activations.completeAnalyticsCapabilityBootstrap({
      client: prisma,
      capability: "coreBatchExports",
      expectedDeploymentGeneration: 1n,
      expectedActivationGeneration: dark.generation,
      verifyDurableBootstrap: async () => ({
        bootstrapEvidenceDigest: digest("9"),
      }),
      now: new Date(now.getTime() + 1_000),
    });

    const common = {
      client: prisma,
      backend: "doris" as const,
      deploymentGeneration: 1n,
      workloadEpochFingerprint: epoch,
      queueNamespaceFingerprint,
      buildId: "parity-u0",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 3 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      leaseMs: 120_000,
      now,
    };
    const web = await leases.registerAnalyticsRuntimeLease({
      ...common,
      component: "web",
      instanceId: "web-current",
      capabilityContracts: [
        {
          capability: "coreBatchExports",
          supportedContractVersion: 1,
          installedRoles: ["producer"],
        },
      ],
    });
    const oldWorker = await leases.registerAnalyticsRuntimeLease({
      ...common,
      component: "worker",
      instanceId: "worker-old",
      capabilityContracts: [],
      leaseMs: 5_000,
    });

    await expect(
      prisma.$transaction((transaction) =>
        admission.lockAnalyticsAdmission({
          transaction,
          runtimeLeaseId: web.lease.id,
          expectedBackend: "doris",
          expectedDeploymentGeneration: 1n,
          capability: "coreBatchExports",
          action: "externalProducer",
          now: new Date(now.getTime() + 1_000),
        }),
      ),
    ).rejects.toThrow(/not active/i);

    await expect(
      activations.activateAnalyticsCapability({
        client: prisma,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: dark.generation,
        expectedRuntimeInstanceIds: ["web-current"],
        expectedBootstrapEvidenceDigest: digest("9"),
        now: new Date(now.getTime() + 6_000),
      }),
    ).rejects.toThrow(/census/i);

    const worker = await leases.registerAnalyticsRuntimeLease({
      ...common,
      component: "worker",
      instanceId: "worker-current",
      capabilityContracts: [
        {
          capability: "coreBatchExports",
          supportedContractVersion: 1,
          installedRoles: ["consumer", "recovery"],
        },
      ],
      now: new Date(now.getTime() + 66_000),
    });
    const checkpointRuntime = await leases.registerAnalyticsRuntimeLease({
      ...common,
      component: "checkpoint",
      instanceId: "checkpoint-one-shot",
      capabilityContracts: [],
      now: new Date(now.getTime() + 66_000),
    });
    const activationInput = {
      client: prisma,
      capability: "coreBatchExports" as const,
      expectedDeploymentGeneration: 1n,
      expectedActivationGeneration: dark.generation,
      expectedRuntimeInstanceIds: ["web-current", "worker-current"],
      expectedBootstrapEvidenceDigest: digest("9"),
      now: new Date(now.getTime() + 66_000),
    };
    const activationRace = await Promise.allSettled([
      activations.activateAnalyticsCapability(activationInput),
      activations.activateAnalyticsCapability(activationInput),
    ]);
    expect(
      activationRace.filter(({ status }) => status === "fulfilled"),
    ).toHaveLength(1);
    expect(
      activationRace.filter(({ status }) => status === "rejected"),
    ).toHaveLength(1);
    const active = await prisma.analyticsCapabilityActivation.findUniqueOrThrow(
      {
        where: { capability: "CORE_BATCH_EXPORTS" },
      },
    );
    expect(active.status).toBe("ACTIVE");
    await expect(
      leases.renewAnalyticsRuntimeLease({
        client: prisma,
        runtimeLeaseId: checkpointRuntime.lease.id,
        leaseMs: 60_000,
        now: new Date(now.getTime() + 67_000),
      }),
    ).resolves.toBe(true);

    await expect(
      leases.renewAnalyticsRuntimeLease({
        client: prisma,
        runtimeLeaseId: oldWorker.lease.id,
        leaseMs: 60_000,
        now: new Date(now.getTime() + 66_000),
      }),
    ).resolves.toBe(false);
    await expect(
      prisma.$transaction((transaction) =>
        admission.lockAnalyticsAdmission({
          transaction,
          runtimeLeaseId: web.lease.id,
          expectedBackend: "doris",
          expectedDeploymentGeneration: 1n,
          capability: "coreBatchExports",
          action: "externalProducer",
          now: new Date(now.getTime() + 67_000),
        }),
      ),
    ).resolves.toMatchObject({
      analyticsBackend: "DORIS",
      deploymentGeneration: 1n,
      workloadEpochFingerprint: epoch,
      runtimeContractVersion: 1,
      admittingRuntimeLeaseId: web.lease.id,
      capabilityActivationGeneration: 2n,
      capabilityContractVersion: 1,
    });
    await expect(
      activations.beginAnalyticsCapabilityDrain({
        client: prisma,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: active.generation,
        now: new Date(now.getTime() + 68_000),
      }),
    ).resolves.toMatchObject({ status: "DRAINING", generation: 2n });
    await expect(
      prisma.$transaction((transaction) =>
        admission.lockAnalyticsAdmission({
          transaction,
          runtimeLeaseId: web.lease.id,
          expectedBackend: "doris",
          expectedDeploymentGeneration: 1n,
          capability: "coreBatchExports",
          action: "externalProducer",
          now: new Date(now.getTime() + 69_000),
        }),
      ),
    ).rejects.toThrow(/not active/i);
    await expect(
      prisma.$transaction((transaction) =>
        admission.lockAnalyticsAdmission({
          transaction,
          runtimeLeaseId: worker.lease.id,
          expectedBackend: "doris",
          expectedDeploymentGeneration: 1n,
          capability: "coreBatchExports",
          action: "claimExisting",
          expectedCapabilityActivationGeneration: 2n,
          expectedCapabilityContractVersion: 1,
          now: new Date(now.getTime() + 69_000),
        }),
      ),
    ).resolves.toMatchObject({ capabilityActivationGeneration: 2n });
    await expect(
      activations.disableAnalyticsCapability({
        client: prisma,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 2n,
        captureRequired: false,
        rescanRequired: false,
        verifyDurableDrain: async () => {
          throw new Error("pending export execution");
        },
        now: new Date(now.getTime() + 70_000),
      }),
    ).rejects.toThrow("pending export execution");
    await expect(
      prisma.analyticsCapabilityActivation.findUniqueOrThrow({
        where: { capability: "CORE_BATCH_EXPORTS" },
      }),
    ).resolves.toMatchObject({ status: "DRAINING", generation: 2n });
    await expect(
      activations.disableAnalyticsCapability({
        client: prisma,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: 2n,
        captureRequired: false,
        rescanRequired: false,
        verifyDurableDrain: async () => undefined,
        now: new Date(now.getTime() + 70_000),
      }),
    ).resolves.toMatchObject({ status: "DISABLED", generation: 2n });
    await expect(
      activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "coreBatchExports",
        expectedGeneration: 2n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now: new Date(now.getTime() + 71_000),
      }),
    ).resolves.toMatchObject({ status: "DARK", generation: 3n });
    expect(worker.lease.id).not.toBe(web.lease.id);
  });

  it("switches only a fully quiesced empty generation and fences stale leases", async () => {
    const ready = await deployment.resolveAnalyticsBackendStartup(freshInput());
    if (ready.mode !== "READY") throw new Error("Fresh deployment is required");
    const now = new Date("2026-07-21T13:00:00.000Z");
    const inventory = [
      { instanceId: "web-switch", component: "web" as const },
      { instanceId: "worker-switch", component: "worker" as const },
    ];
    const common = {
      client: prisma,
      backend: "doris" as const,
      deploymentGeneration: ready.marker.generation,
      workloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
      queueNamespaceFingerprint,
      buildId: "parity-u0",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 3 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 60_000,
      now,
    };
    const web = await leases.registerAnalyticsRuntimeLease({
      ...common,
      component: "web",
      instanceId: inventory[0].instanceId,
    });
    const worker = await leases.registerAnalyticsRuntimeLease({
      ...common,
      component: "worker",
      instanceId: inventory[1].instanceId,
    });

    const switchInput = {
      client: prisma,
      expectedBackend: "doris" as const,
      expectedGeneration: ready.marker.generation,
      expectedWorkloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
      targetBackend: "clickhouse" as const,
      targetWorkloadEpochFingerprint:
        deployment.fingerprintAnalyticsWorkloadEpoch("clickhouse-epoch"),
      targetFoundationContractVersion: 1,
      expectedQuiescedInventory: inventory,
      verifyBackendEmptiness: async () => ({
        source: {
          backend: "doris" as const,
          empty: true,
          evidenceDigest: digest("7"),
        },
        target: {
          backend: "clickhouse" as const,
          empty: true,
          evidenceDigest: digest("8"),
        },
      }),
      verifyScoreDeletionQueuesEmpty: emptyQueueDrain,
      externalDrainAttestationDigest: digest("3"),
      denyProbeAttestationDigest: digest("4"),
      now: new Date(now.getTime() + 2_000),
    };
    await expect(
      deployment.switchAnalyticsBackend(switchInput),
    ).rejects.toThrow(/quiesced/i);

    await leases.markAnalyticsRuntimeQuiesced({
      client: prisma,
      runtimeLeaseId: web.lease.id,
      now: new Date(now.getTime() + 1_000),
    });
    await leases.markAnalyticsRuntimeQuiesced({
      client: prisma,
      runtimeLeaseId: worker.lease.id,
      now: new Date(now.getTime() + 1_000),
    });
    await expect(
      deployment.switchAnalyticsBackend(switchInput),
    ).resolves.toMatchObject({ backend: "CLICKHOUSE", generation: 2n });
    await expect(
      prisma.analyticsBackendDeploymentTransition.count({
        where: { kind: "SWITCH", toGeneration: 2n },
      }),
    ).resolves.toBe(1);
    await expect(
      admission.lockAnalyticsAdmission({
        transaction: prisma,
        runtimeLeaseId: web.lease.id,
        expectedBackend: "doris",
        expectedDeploymentGeneration: 1n,
        action: "foundation",
        now: new Date(now.getTime() + 3_000),
      }),
    ).rejects.toThrow(/generation changed/i);
  });

  it("blocks runtime quiescence while an admitted claim lease is live", async () => {
    const ready = await deployment.resolveAnalyticsBackendStartup(freshInput());
    if (ready.mode !== "READY") throw new Error("Fresh deployment is required");
    const now = new Date("2026-07-21T14:00:00.000Z");
    const worker = await leases.registerAnalyticsRuntimeLease({
      client: prisma,
      component: "worker",
      instanceId: "worker-claim",
      backend: "doris",
      deploymentGeneration: ready.marker.generation,
      workloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
      queueNamespaceFingerprint,
      buildId: "parity-u0",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 3 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 60_000,
      now,
    });
    const claim = await leases.createAnalyticsBackendClaimLease({
      client: prisma,
      runtimeLeaseId: worker.lease.id,
      expectedBackend: "doris",
      expectedDeploymentGeneration: ready.marker.generation,
      expectedWorkloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
      expectedRuntimeContractVersion: ready.marker.foundationContractVersion,
      action: "foundation",
      claimKind: "integration-test",
      resourceIdentity: "resource-1",
      leaseMs: 30_000,
      now: new Date(now.getTime() + 1_000),
    });
    expect(claim).not.toBeNull();
    await expect(
      leases.markAnalyticsRuntimeQuiesced({
        client: prisma,
        runtimeLeaseId: worker.lease.id,
        now: new Date(now.getTime() + 2_000),
      }),
    ).resolves.toBe(false);
    await expect(
      leases.releaseAnalyticsBackendClaimLease({
        client: prisma,
        claimLeaseId: claim!.id,
        runtimeLeaseId: worker.lease.id,
        now: new Date(now.getTime() + 3_000),
      }),
    ).resolves.toBe(true);
    await expect(
      leases.markAnalyticsRuntimeQuiesced({
        client: prisma,
        runtimeLeaseId: worker.lease.id,
        now: new Date(now.getTime() + 4_000),
      }),
    ).resolves.toBe(true);
  });

  it("keeps marker-absent legacy compatibility fenced after managed adoption", async () => {
    const now = new Date("2026-07-21T15:00:00.000Z");
    const common = {
      client: prisma,
      component: "web" as const,
      backend: "doris" as const,
      buildId: "parity-u0",
      foundationContractVersion: 1,
      acceptedSchemaVersion: { min: 1, max: 3 },
      acceptedCanonicalVersion: { min: 1, max: 1 },
      capabilityContracts: [],
      leaseMs: 10_000,
      queueNamespaceFingerprint,
    };
    const legacy = new runtimeControl.AnalyticsRuntimeController({
      ...common,
      instanceId: "web-legacy",
      workloadEpoch: undefined,
    });
    await expect(
      legacy.initialize({
        selectedBackendEmpty: false,
        evidenceDigest: digest("5"),
        now,
      }),
    ).resolves.toEqual({ mode: "LEGACY_COMPATIBILITY" });
    await expect(
      legacy.checkReadiness({ now: new Date(now.getTime() + 1_000) }),
    ).resolves.toBe(true);

    const managed = new runtimeControl.AnalyticsRuntimeController({
      ...common,
      instanceId: "web-managed",
      workloadEpoch: "managed-epoch",
      allowFreshInitialization: true,
    });
    await expect(
      managed.initialize({
        selectedBackendEmpty: true,
        evidenceDigest: digest("6"),
        now: new Date(now.getTime() + 2_000),
      }),
    ).resolves.toMatchObject({ mode: "READY", deploymentGeneration: 1n });
    expect(managed.getRuntimeLeaseId()).toBeTruthy();
    await expect(
      legacy.checkReadiness({ now: new Date(now.getTime() + 3_000) }),
    ).resolves.toBe(false);
    await expect(
      managed.renew({ now: new Date(now.getTime() + 4_000) }),
    ).resolves.toBe(true);
    await expect(
      managed.quiesce({ now: new Date(now.getTime() + 5_000) }),
    ).resolves.toBe(true);
    await expect(
      managed.checkReadiness({ now: new Date(now.getTime() + 6_000) }),
    ).resolves.toBe(false);
  });
});
