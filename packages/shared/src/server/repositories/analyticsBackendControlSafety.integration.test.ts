import { Prisma, PrismaClient } from "@prisma/client";
import { afterAll, beforeEach, describe, expect, it } from "vitest";

import { resetAnalyticsRuntimeIoFenceForTests } from "../analytics-persistence/analyticsRuntimeIoFence";
import { createEmptyAnalyticsQueueDrainEvidence } from "../redis/analyticsQueueDrain.test-helper";

const controlDatabaseUrl = process.env.DORIS_CONTROL_TEST_DATABASE_URL;
if (process.env.DORIS_POC_ENABLED === "1" && !controlDatabaseUrl) {
  throw new Error("Doris harness must provide an isolated control database");
}
const digest = (value: string) => value.repeat(64);

describe.skipIf(!controlDatabaseUrl)(
  "analytics backend control-plane safety",
  () => {
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
      resetAnalyticsRuntimeIoFenceForTests();
      deployment = await import("./analyticsBackendDeployment.js");
      leases = await import("./analyticsRuntimeLeases.js");
      activations = await import("./analyticsCapabilityActivations.js");
      admission =
        await import("../analytics-persistence/analyticsBackendAdmission.js");
      runtimeControl =
        await import("../analytics-persistence/AnalyticsRuntimeController.js");

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

    function freshInput(backend: "doris" | "clickhouse" = "doris") {
      return {
        client: prisma,
        backend,
        workloadEpochFingerprint: deployment.fingerprintAnalyticsWorkloadEpoch(
          `${backend}-safety-epoch`,
        ),
        queueNamespaceFingerprint,
        foundationContractVersion: 1,
        allowFreshInitialization: true,
        freshDeploymentEvidence: {
          selectedBackendEmpty: true,
          evidenceDigest: digest(backend === "doris" ? "a" : "b"),
        },
        now: new Date("2026-07-21T16:00:00.000Z"),
      };
    }

    function runtimeInput(input: {
      backend?: "doris" | "clickhouse";
      component: "web" | "worker";
      instanceId: string;
      now: Date;
      leaseMs?: number;
      capabilityContracts?: Parameters<
        typeof leases.registerAnalyticsRuntimeLease
      >[0]["capabilityContracts"];
    }) {
      const backend = input.backend ?? "doris";
      return {
        client: prisma,
        component: input.component,
        instanceId: input.instanceId,
        backend,
        deploymentGeneration: 1n,
        workloadEpochFingerprint: deployment.fingerprintAnalyticsWorkloadEpoch(
          `${backend}-safety-epoch`,
        ),
        queueNamespaceFingerprint,
        buildId: "u0-safety",
        foundationContractVersion: 1,
        acceptedSchemaVersion: { min: 1, max: 3 },
        acceptedCanonicalVersion: { min: 1, max: 1 },
        capabilityContracts: input.capabilityContracts ?? [],
        leaseMs: input.leaseMs ?? 60_000,
        now: input.now,
      };
    }

    async function prepareQuiescedSwitch(input: { now: Date; suffix: string }) {
      const ready =
        await deployment.resolveAnalyticsBackendStartup(freshInput());
      if (ready.mode !== "READY") throw new Error("Fresh marker is required");
      const inventory = [
        {
          instanceId: `web-switch-${input.suffix}`,
          component: "web" as const,
        },
        {
          instanceId: `worker-switch-${input.suffix}`,
          component: "worker" as const,
        },
      ];
      const web = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "web",
          instanceId: inventory[0].instanceId,
          now: input.now,
        }),
      );
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: inventory[1].instanceId,
          now: input.now,
        }),
      );
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
      return { inventory, marker: ready.marker };
    }

    function switchInput(input: {
      marker: Awaited<ReturnType<typeof prepareQuiescedSwitch>>["marker"];
      inventory: Awaited<ReturnType<typeof prepareQuiescedSwitch>>["inventory"];
      now: Date;
      verifyBackendEmptiness: Parameters<
        typeof deployment.switchAnalyticsBackend
      >[0]["verifyBackendEmptiness"];
      verifyScoreDeletionQueuesEmpty?: Parameters<
        typeof deployment.switchAnalyticsBackend
      >[0]["verifyScoreDeletionQueuesEmpty"];
    }) {
      return {
        client: prisma,
        expectedBackend: "doris" as const,
        expectedGeneration: input.marker.generation,
        expectedWorkloadEpochFingerprint: input.marker.workloadEpochFingerprint,
        targetBackend: "clickhouse" as const,
        targetWorkloadEpochFingerprint:
          deployment.fingerprintAnalyticsWorkloadEpoch(
            "clickhouse-safety-epoch",
          ),
        targetFoundationContractVersion: 1,
        expectedQuiescedInventory: input.inventory,
        verifyBackendEmptiness: input.verifyBackendEmptiness,
        verifyScoreDeletionQueuesEmpty:
          input.verifyScoreDeletionQueuesEmpty ?? emptyQueueDrain,
        externalDrainAttestationDigest: digest("9"),
        denyProbeAttestationDigest: digest("8"),
        now: new Date(input.now.getTime() + 2_000),
      };
    }

    it("requires adoption inventory to include both web and worker", async () => {
      const epoch = deployment.fingerprintAnalyticsWorkloadEpoch(
        "incomplete-adoption",
      );
      const webOnly = [{ instanceId: "web-only", component: "web" as const }];

      await expect(
        deployment.adoptExistingAnalyticsBackend({
          client: prisma,
          expectedBackend: "doris",
          workloadEpochFingerprint: epoch,
          foundationContractVersion: 1,
          expectedInventory: [],
          expectedInventoryDigest: deployment.digestAnalyticsRuntimeInventory(
            [],
          ),
          verifyScoreDeletionQueuesEmpty: emptyQueueDrain,
          drainAttestationDigest: digest("c"),
          denyProbeAttestationDigest: digest("d"),
        }),
      ).rejects.toThrow(/web.*worker|inventory/i);
      await expect(
        deployment.adoptExistingAnalyticsBackend({
          client: prisma,
          expectedBackend: "doris",
          workloadEpochFingerprint: epoch,
          foundationContractVersion: 1,
          expectedInventory: webOnly,
          expectedInventoryDigest:
            deployment.digestAnalyticsRuntimeInventory(webOnly),
          verifyScoreDeletionQueuesEmpty: emptyQueueDrain,
          drainAttestationDigest: digest("e"),
          denyProbeAttestationDigest: digest("f"),
        }),
      ).rejects.toThrow(/web.*worker|inventory/i);
    });

    it("rejects malformed durable provenance at the database boundary", async () => {
      await expect(
        prisma.analyticsRetentionRun.create({
          data: {
            id: "invalid-provenance",
            cutoffDate: new Date("2026-07-01T00:00:00.000Z"),
            analyticsBackend: "DORIS",
            deploymentGeneration: 0n,
            workloadEpochFingerprint: digest("a"),
            runtimeContractVersion: 1,
            producerRuntimeLeaseId: "producer",
          },
        }),
      ).rejects.toThrow();
      await expect(
        prisma.analyticsRetentionRun.create({
          data: {
            id: "partial-provenance",
            cutoffDate: new Date("2026-07-01T00:00:00.000Z"),
            analyticsBackend: "DORIS",
          },
        }),
      ).rejects.toThrow();
    });

    it("rotates lease identity on expired instance takeover", async () => {
      await deployment.resolveAnalyticsBackendStartup(freshInput());
      const now = new Date("2026-07-21T16:10:00.000Z");
      const first = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-takeover",
          now,
          leaseMs: 1_000,
        }),
      );
      const firstClaim = await leases.createAnalyticsBackendClaimLease({
        client: prisma,
        runtimeLeaseId: first.lease.id,
        expectedBackend: "doris",
        expectedDeploymentGeneration: 1n,
        expectedWorkloadEpochFingerprint:
          deployment.fingerprintAnalyticsWorkloadEpoch("doris-safety-epoch"),
        expectedRuntimeContractVersion: 1,
        action: "foundation",
        claimKind: "takeover-safety",
        resourceIdentity: "old-incarnation",
        leaseMs: 30_000,
        now: new Date(now.getTime() + 500),
      });
      if (!firstClaim) throw new Error("Initial claim is required");
      const second = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-takeover",
          now: new Date(now.getTime() + 1_001),
          leaseMs: 10_000,
        }),
      );

      expect(second.lease.id).not.toBe(first.lease.id);
      await expect(
        leases.renewAnalyticsRuntimeLease({
          client: prisma,
          runtimeLeaseId: first.lease.id,
          leaseMs: 10_000,
          now: new Date(now.getTime() + 1_002),
        }),
      ).resolves.toBe(false);
      await expect(
        prisma.$transaction((transaction) =>
          admission.lockAnalyticsAdmission({
            transaction,
            runtimeLeaseId: first.lease.id,
            expectedBackend: "doris",
            expectedDeploymentGeneration: 1n,
            action: "foundation",
            now: new Date(now.getTime() + 1_002),
          }),
        ),
      ).rejects.toThrow(/not admitted/i);
      await expect(
        prisma.$transaction((transaction) =>
          admission.lockAnalyticsAdmission({
            transaction,
            runtimeLeaseId: second.lease.id,
            expectedBackend: "doris",
            expectedDeploymentGeneration: 1n,
            action: "foundation",
            now: new Date(now.getTime() + 1_002),
          }),
        ),
      ).resolves.toMatchObject({ admittingRuntimeLeaseId: second.lease.id });
      await expect(
        prisma.analyticsBackendClaimLease.findUniqueOrThrow({
          where: { id: firstClaim.id },
        }),
      ).resolves.toMatchObject({ runtimeLeaseId: first.lease.id });
    });

    it("keeps registration retries idempotent and serializes incarnations", async () => {
      await deployment.resolveAnalyticsBackendStartup(freshInput());
      const now = new Date("2026-07-21T16:15:00.000Z");
      const idempotentInput = {
        ...runtimeInput({
          component: "worker" as const,
          instanceId: "worker-idempotent",
          now,
        }),
        runtimeLeaseId: "lease-idempotent",
      };
      const first = await leases.registerAnalyticsRuntimeLease(idempotentInput);
      const retry = await leases.registerAnalyticsRuntimeLease(idempotentInput);
      expect(retry.lease.id).toBe(first.lease.id);

      const concurrentInput = runtimeInput({
        component: "worker",
        instanceId: "worker-concurrent-incarnation",
        now,
      });
      const results = await Promise.allSettled([
        leases.registerAnalyticsRuntimeLease({
          ...concurrentInput,
          runtimeLeaseId: "lease-concurrent-a",
        }),
        leases.registerAnalyticsRuntimeLease({
          ...concurrentInput,
          runtimeLeaseId: "lease-concurrent-b",
        }),
      ]);
      expect(
        results.filter(({ status }) => status === "fulfilled"),
      ).toHaveLength(1);
      expect(
        results.filter(({ status }) => status === "rejected"),
      ).toHaveLength(1);
      await expect(
        prisma.analyticsRuntimeLease.count({
          where: {
            instanceId: "worker-concurrent-incarnation",
            supersededAt: null,
          },
        }),
      ).resolves.toBe(1);
    });

    it("never moves heartbeat or expiry backwards", async () => {
      await deployment.resolveAnalyticsBackendStartup(freshInput());
      const now = new Date("2026-07-21T16:20:00.000Z");
      const registered = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-monotonic",
          now,
        }),
      );

      await expect(
        leases.renewAnalyticsRuntimeLease({
          client: prisma,
          runtimeLeaseId: registered.lease.id,
          leaseMs: 60_000,
          now: new Date(now.getTime() + 20_000),
        }),
      ).resolves.toBe(true);
      await expect(
        leases.renewAnalyticsRuntimeLease({
          client: prisma,
          runtimeLeaseId: registered.lease.id,
          leaseMs: 60_000,
          now: new Date(now.getTime() + 10_000),
        }),
      ).resolves.toBe(true);

      await expect(
        prisma.analyticsRuntimeLease.findUniqueOrThrow({
          where: { id: registered.lease.id },
        }),
      ).resolves.toMatchObject({
        heartbeatAt: new Date(now.getTime() + 20_000),
        leaseExpiresAt: new Date(now.getTime() + 80_000),
      });
    });

    it("keeps brownfield compatibility ready only until adoption", async () => {
      const now = new Date("2026-07-21T16:30:00.000Z");
      const epoch = "brownfield-controller-epoch";
      const epochFingerprint =
        deployment.fingerprintAnalyticsWorkloadEpoch(epoch);
      const controller = new runtimeControl.AnalyticsRuntimeController({
        client: prisma,
        component: "web",
        instanceId: "web-brownfield",
        backend: "doris",
        workloadEpoch: epoch,
        queueNamespaceFingerprint,
        buildId: "u0-safety",
        foundationContractVersion: 1,
        acceptedSchemaVersion: { min: 1, max: 3 },
        acceptedCanonicalVersion: { min: 1, max: 1 },
        capabilityContracts: [],
        leaseMs: 60_000,
      });
      await expect(
        controller.initialize({
          selectedBackendEmpty: false,
          evidenceDigest: digest("1"),
          now,
        }),
      ).resolves.toMatchObject({ mode: "ADOPTION_REQUIRED" });
      await expect(
        controller.checkReadiness({ now: new Date(now.getTime() + 1_000) }),
      ).resolves.toBe(true);

      const worker = await leases.registerAnalyticsRuntimeLease({
        ...runtimeInput({
          component: "worker",
          instanceId: "worker-brownfield",
          now,
        }),
        deploymentGeneration: 0n,
        workloadEpochFingerprint: epochFingerprint,
      });
      await controller.quiesce({ now: new Date(now.getTime() + 2_000) });
      await leases.markAnalyticsRuntimeQuiesced({
        client: prisma,
        runtimeLeaseId: worker.lease.id,
        now: new Date(now.getTime() + 2_000),
      });
      const inventory = [
        { instanceId: "web-brownfield", component: "web" as const },
        { instanceId: "worker-brownfield", component: "worker" as const },
      ];
      await deployment.adoptExistingAnalyticsBackend({
        client: prisma,
        expectedBackend: "doris",
        workloadEpochFingerprint: epochFingerprint,
        foundationContractVersion: 1,
        expectedInventory: inventory,
        expectedInventoryDigest:
          deployment.digestAnalyticsRuntimeInventory(inventory),
        verifyScoreDeletionQueuesEmpty: emptyQueueDrain,
        drainAttestationDigest: digest("2"),
        denyProbeAttestationDigest: digest("3"),
        now: new Date(now.getTime() + 3_000),
      });
      await expect(
        controller.checkReadiness({ now: new Date(now.getTime() + 4_000) }),
      ).resolves.toBe(false);
    });

    it("drops readiness immediately when capability renewal is rejected", async () => {
      const now = new Date("2026-07-21T16:40:00.000Z");
      const controller = new runtimeControl.AnalyticsRuntimeController({
        client: prisma,
        component: "worker",
        instanceId: "worker-readiness",
        backend: "doris",
        workloadEpoch: "doris-safety-epoch",
        queueNamespaceFingerprint,
        allowFreshInitialization: true,
        buildId: "u0-safety",
        foundationContractVersion: 1,
        acceptedSchemaVersion: { min: 1, max: 3 },
        acceptedCanonicalVersion: { min: 1, max: 1 },
        capabilityContracts: [],
        leaseMs: 60_000,
      });
      await controller.initialize({
        selectedBackendEmpty: true,
        evidenceDigest: digest("4"),
        now,
      });
      await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "coreBatchExports",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now: new Date(now.getTime() + 1_000),
      });
      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "CORE_BATCH_EXPORTS" },
        data: { captureEnabled: true },
      });

      await expect(
        controller.renew({ now: new Date(now.getTime() + 2_000) }),
      ).resolves.toBe(false);
      await expect(
        controller.checkReadiness({ now: new Date(now.getTime() + 3_000) }),
      ).resolves.toBe(false);
    });

    it("allows renewal while a capability is plain DARK without capture", async () => {
      await deployment.resolveAnalyticsBackendStartup(freshInput());
      const now = new Date("2026-07-21T16:45:00.000Z");
      const registered = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-plain-dark",
          now,
        }),
      );
      await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "coreBatchExports",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now: new Date(now.getTime() + 1_000),
      });

      await expect(
        leases.renewAnalyticsRuntimeLease({
          client: prisma,
          runtimeLeaseId: registered.lease.id,
          leaseMs: 60_000,
          now: new Date(now.getTime() + 2_000),
        }),
      ).resolves.toBe(true);
    });

    it("requires live leases, not recently expired leases, for activation", async () => {
      await deployment.resolveAnalyticsBackendStartup(freshInput());
      const now = new Date("2026-07-21T16:50:00.000Z");
      const dark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "coreBatchExports",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now,
      });
      await activations.completeAnalyticsCapabilityBootstrap({
        client: prisma,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: dark.generation,
        verifyDurableBootstrap: async () => ({
          bootstrapEvidenceDigest: digest("5"),
        }),
        now: new Date(now.getTime() + 1_000),
      });
      const capabilityContracts = [
        {
          capability: "coreBatchExports" as const,
          supportedContractVersion: 1,
          installedRoles: ["producer" as const],
        },
      ];
      await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "web",
          instanceId: "web-live-census",
          now,
          capabilityContracts,
        }),
      );
      await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-expired-census",
          now,
          leaseMs: 5_000,
          capabilityContracts: [
            {
              capability: "coreBatchExports",
              supportedContractVersion: 1,
              installedRoles: ["producer", "consumer", "recovery"],
            },
          ],
        }),
      );

      await expect(
        activations.activateAnalyticsCapability({
          client: prisma,
          capability: "coreBatchExports",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: dark.generation,
          expectedRuntimeInstanceIds: [
            "web-live-census",
            "worker-expired-census",
          ],
          expectedBootstrapEvidenceDigest: digest("5"),
          now: new Date(now.getTime() + 6_000),
        }),
      ).rejects.toThrow(/expired|grace|census/i);
    });

    it("preserves replay cutoff until durable bootstrap completion", async () => {
      await deployment.resolveAnalyticsBackendStartup(freshInput());
      const now = new Date("2026-07-21T17:00:00.000Z");
      const firstDark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "evaluations",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now,
      });
      await activations.completeAnalyticsCapabilityBootstrap({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: firstDark.generation,
        verifyDurableBootstrap: async () => ({
          bootstrapEvidenceDigest: digest("6"),
        }),
        now: new Date(now.getTime() + 1_000),
      });
      const web = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "web",
          instanceId: "web-replay",
          now,
          capabilityContracts: [
            {
              capability: "evaluations",
              supportedContractVersion: 1,
              installedRoles: ["producer"],
            },
          ],
        }),
      );
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-replay",
          now,
          capabilityContracts: [
            {
              capability: "evaluations",
              supportedContractVersion: 1,
              installedRoles: ["capture", "consumer", "recovery"],
            },
          ],
        }),
      );
      await expect(
        activations.activateAnalyticsCapability({
          client: prisma,
          capability: "evaluations",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: firstDark.generation,
          expectedRuntimeInstanceIds: [
            web.lease.instanceId,
            worker.lease.instanceId,
          ],
          expectedBootstrapEvidenceDigest: digest("6"),
          now: new Date(now.getTime() + 1_250),
        }),
      ).rejects.toThrow(/capture/i);
      await activations.enableAnalyticsCapabilityDarkCapture({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: firstDark.generation,
        expectedRuntimeInstanceIds: [
          web.lease.instanceId,
          worker.lease.instanceId,
        ],
        now: new Date(now.getTime() + 1_500),
      });
      await activations.activateAnalyticsCapability({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: firstDark.generation,
        expectedRuntimeInstanceIds: [
          web.lease.instanceId,
          worker.lease.instanceId,
        ],
        expectedBootstrapEvidenceDigest: digest("6"),
        now: new Date(now.getTime() + 2_000),
      });
      await activations.beginAnalyticsCapabilityDrain({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: firstDark.generation,
        now: new Date(now.getTime() + 3_000),
      });
      await activations.disableAnalyticsCapability({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: firstDark.generation,
        captureRequired: true,
        rescanRequired: true,
        verifyDurableDrain: async () => undefined,
        sealReplayCutoff: async () => ({
          cutoffState: { operationId: "op-cutoff" },
          cutoffDigest: digest("a"),
        }),
        now: new Date(now.getTime() + 4_000),
      });

      const nextDark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "evaluations",
        expectedGeneration: firstDark.generation,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now: new Date(now.getTime() + 5_000),
      });
      expect(nextDark).toMatchObject({
        status: "DARK",
        rescanRequired: true,
        captureRequired: true,
        cutoffState: { operationId: "op-cutoff" },
        cutoffDigest: digest("a"),
        bootstrapEvidenceDigest: null,
      });
      await expect(
        activations.activateAnalyticsCapability({
          client: prisma,
          capability: "evaluations",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: nextDark.generation,
          expectedRuntimeInstanceIds: [
            web.lease.instanceId,
            worker.lease.instanceId,
          ],
          expectedBootstrapEvidenceDigest: digest("7"),
          now: new Date(now.getTime() + 6_000),
        }),
      ).rejects.toThrow(/bootstrap|replay/i);
      await activations.enableAnalyticsCapabilityDarkCapture({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: nextDark.generation,
        expectedRuntimeInstanceIds: [
          web.lease.instanceId,
          worker.lease.instanceId,
        ],
        now: new Date(now.getTime() + 7_000),
      });
      await activations.completeAnalyticsCapabilityBootstrap({
        client: prisma,
        capability: "evaluations",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: nextDark.generation,
        expectedCutoffDigest: digest("a"),
        verifyDurableBootstrap: async () => ({
          bootstrapEvidenceDigest: digest("7"),
        }),
        now: new Date(now.getTime() + 8_000),
      });
      await expect(
        activations.activateAnalyticsCapability({
          client: prisma,
          capability: "evaluations",
          expectedDeploymentGeneration: 1n,
          expectedActivationGeneration: nextDark.generation,
          expectedRuntimeInstanceIds: [
            web.lease.instanceId,
            worker.lease.instanceId,
          ],
          expectedBootstrapEvidenceDigest: digest("7"),
          now: new Date(now.getTime() + 9_000),
        }),
      ).resolves.toMatchObject({
        status: "ACTIVE",
        rescanRequired: false,
        captureRequired: false,
        cutoffState: null,
        bootstrapEvidenceDigest: digest("7"),
      });
    });

    it("fences capability claims and resolves same-resource races", async () => {
      await deployment.resolveAnalyticsBackendStartup(freshInput());
      const now = new Date("2026-07-21T17:10:00.000Z");
      const dark = await activations.beginAnalyticsCapabilityDark({
        client: prisma,
        capability: "coreBatchExports",
        expectedGeneration: 1n,
        contractVersion: 1,
        minimumRuntimeContract: 1,
        now,
      });
      await activations.completeAnalyticsCapabilityBootstrap({
        client: prisma,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: dark.generation,
        verifyDurableBootstrap: async () => ({
          bootstrapEvidenceDigest: digest("8"),
        }),
        now: new Date(now.getTime() + 1_000),
      });
      const web = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "web",
          instanceId: "web-claim-census",
          now,
          capabilityContracts: [
            {
              capability: "coreBatchExports",
              supportedContractVersion: 1,
              installedRoles: ["producer"],
            },
          ],
        }),
      );
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-claim-census",
          now,
          capabilityContracts: [
            {
              capability: "coreBatchExports",
              supportedContractVersion: 1,
              installedRoles: ["producer", "consumer", "recovery"],
            },
          ],
        }),
      );
      await activations.activateAnalyticsCapability({
        client: prisma,
        capability: "coreBatchExports",
        expectedDeploymentGeneration: 1n,
        expectedActivationGeneration: dark.generation,
        expectedRuntimeInstanceIds: [
          web.lease.instanceId,
          worker.lease.instanceId,
        ],
        expectedBootstrapEvidenceDigest: digest("8"),
        now: new Date(now.getTime() + 2_000),
      });

      const commonClaim = {
        client: prisma,
        runtimeLeaseId: worker.lease.id,
        expectedBackend: "doris" as const,
        expectedDeploymentGeneration: 1n,
        expectedWorkloadEpochFingerprint:
          deployment.fingerprintAnalyticsWorkloadEpoch("doris-safety-epoch"),
        expectedRuntimeContractVersion: 1,
        capability: "coreBatchExports" as const,
        action: "claimExisting" as const,
        expectedCapabilityActivationGeneration: dark.generation,
        expectedCapabilityContractVersion: 1,
        claimKind: "core-export",
        resourceIdentity: "export-1",
        leaseMs: 30_000,
        now: new Date(now.getTime() + 3_000),
      };
      await expect(
        leases.createAnalyticsBackendClaimLease({
          ...commonClaim,
          expectedCapabilityActivationGeneration: 1n,
        }),
      ).rejects.toThrow(/generation|provenance/i);

      const results = await Promise.allSettled([
        leases.createAnalyticsBackendClaimLease(commonClaim),
        leases.createAnalyticsBackendClaimLease(commonClaim),
      ]);
      expect(results.every(({ status }) => status === "fulfilled")).toBe(true);
      expect(
        results.filter(
          (result) => result.status === "fulfilled" && result.value !== null,
        ),
      ).toHaveLength(1);
      expect(
        results.filter(
          (result) => result.status === "fulfilled" && result.value === null,
        ),
      ).toHaveLength(1);
    });

    it("keeps expired claim takeover and release on one lock order", async () => {
      const ready =
        await deployment.resolveAnalyticsBackendStartup(freshInput());
      if (ready.mode !== "READY") throw new Error("Fresh marker is required");
      const now = new Date("2026-07-21T17:12:00.000Z");
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-claim-release-race",
          now,
        }),
      );
      const fence = {
        runtimeLeaseId: worker.lease.id,
        expectedBackend: "doris" as const,
        expectedDeploymentGeneration: ready.marker.generation,
        expectedWorkloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
        expectedRuntimeContractVersion: ready.marker.foundationContractVersion,
        action: "foundation" as const,
      };
      const claimInput = {
        client: prisma,
        ...fence,
        claimKind: "release-race",
        resourceIdentity: "resource-1",
        leaseMs: 1_000,
        now: new Date(now.getTime() + 1_000),
      };
      const expiredClaim =
        await leases.createAnalyticsBackendClaimLease(claimInput);
      if (!expiredClaim) throw new Error("Initial claim is required");

      let resumeRelease!: () => void;
      const releaseGate = new Promise<void>((resolve) => {
        resumeRelease = resolve;
      });
      let markReleasePaused!: () => void;
      const releasePaused = new Promise<void>((resolve) => {
        markReleasePaused = resolve;
      });
      const releaseClient = prisma.$extends({
        query: {
          analyticsBackendClaimLease: {
            async updateMany({ args, query }) {
              markReleasePaused();
              await releaseGate;
              return query(args);
            },
          },
        },
      });
      const release = leases.releaseAnalyticsBackendClaimLease({
        client: releaseClient as unknown as PrismaClient,
        claimLeaseId: expiredClaim.id,
        runtimeLeaseId: worker.lease.id,
        now: new Date(now.getTime() + 3_000),
      });
      await releasePaused;

      const takeover = leases.createAnalyticsBackendClaimLease({
        ...claimInput,
        leaseMs: 30_000,
        now: new Date(now.getTime() + 3_000),
      });
      try {
        await expect
          .poll(
            async () => {
              const [row] = await prisma.$queryRaw<
                readonly { waiting: bigint }[]
              >(Prisma.sql`
                SELECT count(*)::bigint AS waiting
                FROM pg_stat_activity
                WHERE datname = current_database()
                  AND wait_event_type = 'Lock'
                  AND query LIKE '%analytics_runtime_leases%'
                  AND query LIKE '%FOR SHARE%'
              `);
              return Number(row?.waiting ?? 0n);
            },
            { timeout: 2_000, interval: 20 },
          )
          .toBeGreaterThan(0);
      } finally {
        resumeRelease();
      }

      const [released, replacement] = await Promise.all([release, takeover]);
      expect(released).toBe(true);
      expect(replacement?.id).not.toBe(expiredClaim.id);
      await expect(
        prisma.analyticsBackendClaimLease.findUniqueOrThrow({
          where: { id: expiredClaim.id },
        }),
      ).resolves.toMatchObject({
        releasedAt: new Date(now.getTime() + 3_000),
      });
    });

    it("rejects durable work admission when the runtime queue namespace diverges", async () => {
      const ready =
        await deployment.resolveAnalyticsBackendStartup(freshInput());
      if (ready.mode !== "READY") throw new Error("Fresh marker is required");
      const now = new Date("2026-07-21T17:13:00.000Z");
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-queue-namespace-mismatch",
          now,
        }),
      );

      await prisma.analyticsRuntimeLease.update({
        where: { id: worker.lease.id },
        data: { queueNamespaceFingerprint: digest("e") },
      });

      await expect(
        leases.createAnalyticsBackendClaimLease({
          client: prisma,
          runtimeLeaseId: worker.lease.id,
          expectedBackend: "doris",
          expectedDeploymentGeneration: ready.marker.generation,
          expectedWorkloadEpochFingerprint:
            ready.marker.workloadEpochFingerprint,
          expectedRuntimeContractVersion:
            ready.marker.foundationContractVersion,
          action: "foundation",
          claimKind: "queue-namespace-mismatch",
          resourceIdentity: "resource-1",
          leaseMs: 30_000,
          now: new Date(now.getTime() + 1_000),
        }),
      ).rejects.toThrow(/runtime lease is not admitted/i);
      await expect(
        prisma.analyticsBackendClaimLease.count({
          where: { runtimeLeaseId: worker.lease.id },
        }),
      ).resolves.toBe(0);
    });

    it("revalidates and monotonically renews a claim before analytics IO", async () => {
      const ready =
        await deployment.resolveAnalyticsBackendStartup(freshInput());
      if (ready.mode !== "READY") throw new Error("Fresh marker is required");
      const now = new Date("2026-07-21T17:15:00.000Z");
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-claim-io",
          now,
        }),
      );
      const fence = {
        runtimeLeaseId: worker.lease.id,
        expectedBackend: "doris" as const,
        expectedDeploymentGeneration: ready.marker.generation,
        expectedWorkloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
        expectedRuntimeContractVersion: ready.marker.foundationContractVersion,
        action: "foundation" as const,
      };
      const claim = await leases.createAnalyticsBackendClaimLease({
        client: prisma,
        ...fence,
        claimKind: "page-io",
        resourceIdentity: "stream-1",
        leaseMs: 5_000,
        now: new Date(now.getTime() + 1_000),
      });
      if (!claim) throw new Error("Claim is required");

      const renewed = await leases.renewAnalyticsBackendClaimLease({
        client: prisma,
        claimLeaseId: claim.id,
        fence,
        leaseMs: 20_000,
        now: new Date(now.getTime() + 2_000),
      });
      expect(renewed.leaseExpiresAt).toEqual(new Date(now.getTime() + 22_000));
      await expect(
        leases.renewAnalyticsBackendClaimLease({
          client: prisma,
          claimLeaseId: claim.id,
          fence,
          leaseMs: 1_000,
          now: new Date(now.getTime() + 1_500),
        }),
      ).resolves.toMatchObject({
        leaseExpiresAt: new Date(now.getTime() + 22_000),
      });
      await expect(
        prisma.$transaction((transaction) =>
          leases.lockAnalyticsBackendClaimLeaseForIo({
            transaction,
            claimLeaseId: claim.id,
            fence,
            now: new Date(now.getTime() + 3_000),
          }),
        ),
      ).resolves.toMatchObject({ id: claim.id });

      await leases.releaseAnalyticsBackendClaimLease({
        client: prisma,
        claimLeaseId: claim.id,
        runtimeLeaseId: worker.lease.id,
        now: new Date(now.getTime() + 4_000),
      });
      let ioStarted = false;
      await expect(
        prisma.$transaction(async (transaction) => {
          await leases.lockAnalyticsBackendClaimLeaseForIo({
            transaction,
            claimLeaseId: claim.id,
            fence,
            now: new Date(now.getTime() + 5_000),
          });
          ioStarted = true;
        }),
      ).rejects.toThrow(/no longer admitted/i);
      expect(ioStarted).toBe(false);
    });

    it("allows the runtime heartbeat to renew while a claim fences analytics IO", async () => {
      const ready =
        await deployment.resolveAnalyticsBackendStartup(freshInput());
      if (ready.mode !== "READY") throw new Error("Fresh marker is required");
      const now = new Date("2026-07-21T17:17:00.000Z");
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-long-io",
          now,
        }),
      );
      const fence = {
        runtimeLeaseId: worker.lease.id,
        expectedBackend: "doris" as const,
        expectedDeploymentGeneration: ready.marker.generation,
        expectedWorkloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
        expectedRuntimeContractVersion: ready.marker.foundationContractVersion,
        action: "foundation" as const,
      };
      const claim = await leases.createAnalyticsBackendClaimLease({
        client: prisma,
        ...fence,
        claimKind: "long-page-io",
        resourceIdentity: "stream-heartbeat",
        leaseMs: 30_000,
        now: new Date(now.getTime() + 1_000),
      });
      if (!claim) throw new Error("Claim is required");

      let markIoLocked!: () => void;
      const ioLocked = new Promise<void>((resolve) => {
        markIoLocked = resolve;
      });
      let releaseIo!: () => void;
      const ioRelease = new Promise<void>((resolve) => {
        releaseIo = resolve;
      });
      const ioTransaction = prisma.$transaction(
        async (transaction) => {
          await leases.lockAnalyticsBackendClaimLeaseForIo({
            transaction,
            claimLeaseId: claim.id,
            fence,
            now: new Date(now.getTime() + 2_000),
          });
          markIoLocked();
          await ioRelease;
        },
        { timeout: 10_000 },
      );
      await ioLocked;

      const renewal = leases.renewAnalyticsRuntimeLease({
        client: prisma,
        runtimeLeaseId: worker.lease.id,
        leaseMs: 60_000,
        now: new Date(now.getTime() + 3_000),
      });
      const renewedBeforeIoRelease = await Promise.race([
        renewal.then((result) => result),
        new Promise<false>((resolve) => setTimeout(() => resolve(false), 250)),
      ]);
      releaseIo();
      await ioTransaction;
      await renewal;

      expect(renewedBeforeIoRelease).toBe(true);
    });

    it("serializes claim creation with runtime quiescence", async () => {
      const ready =
        await deployment.resolveAnalyticsBackendStartup(freshInput());
      if (ready.mode !== "READY") throw new Error("Fresh marker is required");
      const now = new Date("2026-07-21T17:18:00.000Z");
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-claim-quiesce-race",
          now,
        }),
      );
      const fence = {
        runtimeLeaseId: worker.lease.id,
        expectedBackend: "doris" as const,
        expectedDeploymentGeneration: ready.marker.generation,
        expectedWorkloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
        expectedRuntimeContractVersion: ready.marker.foundationContractVersion,
        action: "foundation" as const,
      };
      const claimKind = "quiesce-race";
      const resourceIdentity = "resource-1";
      const resourceKey = [
        "DORIS",
        ready.marker.generation.toString(),
        claimKind,
        resourceIdentity,
      ].join(":");

      let releaseResourceLock!: () => void;
      const resourceLockRelease = new Promise<void>((resolve) => {
        releaseResourceLock = resolve;
      });
      let markResourceLocked!: () => void;
      const resourceLocked = new Promise<void>((resolve) => {
        markResourceLocked = resolve;
      });
      const blocker = prisma.$transaction(
        async (transaction) => {
          await transaction.$queryRaw(
            Prisma.sql`SELECT pg_advisory_xact_lock(hashtextextended(${resourceKey}, ${8_216_401_974n}))::text AS locked`,
          );
          markResourceLocked();
          await resourceLockRelease;
        },
        { timeout: 10_000 },
      );
      await resourceLocked;

      const claim = leases.createAnalyticsBackendClaimLease({
        client: prisma,
        ...fence,
        claimKind,
        resourceIdentity,
        leaseMs: 30_000,
        now: new Date(now.getTime() + 1_000),
      });
      await expect
        .poll(
          async () => {
            const [row] = await prisma.$queryRaw<
              readonly { waiting: bigint }[]
            >(Prisma.sql`
              SELECT count(*)::bigint AS waiting
              FROM pg_stat_activity
              WHERE datname = current_database()
                AND wait_event_type = 'Lock'
                AND wait_event = 'advisory'
            `);
            return Number(row?.waiting ?? 0n);
          },
          { timeout: 2_000, interval: 20 },
        )
        .toBeGreaterThan(0);

      const quiesce = leases.markAnalyticsRuntimeQuiesced({
        client: prisma,
        runtimeLeaseId: worker.lease.id,
        now: new Date(now.getTime() + 2_000),
      });
      await expect(quiesce).resolves.toBe(true);

      releaseResourceLock();
      await blocker;
      await expect(claim).rejects.toThrow(/runtime lease is no longer active/i);
      await expect(
        prisma.analyticsBackendClaimLease.count({
          where: { runtimeLeaseId: worker.lease.id, releasedAt: null },
        }),
      ).resolves.toBe(0);
    });

    it("refreshes the database clock after waiting for a claim resource lock", async () => {
      const ready =
        await deployment.resolveAnalyticsBackendStartup(freshInput());
      if (ready.mode !== "READY") throw new Error("Fresh marker is required");
      const [clock] = await prisma.$queryRaw<readonly { now: Date }[]>(
        Prisma.sql`SELECT clock_timestamp() AS now`,
      );
      if (!clock) throw new Error("Database clock is required");
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          component: "worker",
          instanceId: "worker-claim-expiry-race",
          now: clock.now,
        }),
      );
      const fence = {
        runtimeLeaseId: worker.lease.id,
        expectedBackend: "doris" as const,
        expectedDeploymentGeneration: ready.marker.generation,
        expectedWorkloadEpochFingerprint: ready.marker.workloadEpochFingerprint,
        expectedRuntimeContractVersion: ready.marker.foundationContractVersion,
        action: "foundation" as const,
      };
      const claimKind = "expiry-race";
      const resourceIdentity = "resource-1";
      const resourceKey = [
        "DORIS",
        ready.marker.generation.toString(),
        claimKind,
        resourceIdentity,
      ].join(":");

      let releaseResourceLock!: () => void;
      const resourceLockRelease = new Promise<void>((resolve) => {
        releaseResourceLock = resolve;
      });
      let markResourceLocked!: () => void;
      const resourceLocked = new Promise<void>((resolve) => {
        markResourceLocked = resolve;
      });
      const blocker = prisma.$transaction(
        async (transaction) => {
          await transaction.$queryRaw(
            Prisma.sql`SELECT pg_advisory_xact_lock(hashtextextended(${resourceKey}, ${8_216_401_974n}))::text AS locked`,
          );
          markResourceLocked();
          await resourceLockRelease;
        },
        { timeout: 10_000 },
      );
      await resourceLocked;

      const claim = leases.createAnalyticsBackendClaimLease({
        client: prisma,
        ...fence,
        claimKind,
        resourceIdentity,
        leaseMs: 30_000,
      });
      await expect
        .poll(
          async () => {
            const [row] = await prisma.$queryRaw<
              readonly { waiting: bigint }[]
            >(Prisma.sql`
              SELECT count(*)::bigint AS waiting
              FROM pg_stat_activity
              WHERE datname = current_database()
                AND wait_event_type = 'Lock'
                AND wait_event = 'advisory'
            `);
            return Number(row?.waiting ?? 0n);
          },
          { timeout: 2_000, interval: 20 },
        )
        .toBeGreaterThan(0);

      await prisma.$executeRaw(
        Prisma.sql`
          UPDATE analytics_runtime_leases
          SET lease_expires_at = clock_timestamp()
          WHERE id = ${worker.lease.id}
        `,
      );
      releaseResourceLock();
      await blocker;

      await expect(claim).rejects.toThrow(/runtime lease is no longer active/i);
      await expect(
        prisma.analyticsBackendClaimLease.count({
          where: { runtimeLeaseId: worker.lease.id, releasedAt: null },
        }),
      ).resolves.toBe(0);
    });

    it("starts a controller lease from the database clock after deployment lock wait", async () => {
      let releaseDeploymentLock!: () => void;
      const deploymentLockRelease = new Promise<void>((resolve) => {
        releaseDeploymentLock = resolve;
      });
      let markDeploymentLocked!: () => void;
      const deploymentLocked = new Promise<void>((resolve) => {
        markDeploymentLocked = resolve;
      });
      const blocker = prisma.$transaction(
        async (transaction) => {
          await transaction.$queryRaw(
            Prisma.sql`SELECT pg_advisory_xact_lock(${7_681_221_833_480_517n})::text AS locked`,
          );
          markDeploymentLocked();
          await deploymentLockRelease;
        },
        { timeout: 10_000 },
      );
      await deploymentLocked;

      const controller = new runtimeControl.AnalyticsRuntimeController({
        client: prisma,
        component: "worker",
        instanceId: "worker-lock-wait-clock",
        backend: "doris",
        workloadEpoch: "doris-safety-epoch",
        queueNamespaceFingerprint,
        buildId: "u0-safety",
        foundationContractVersion: 1,
        acceptedSchemaVersion: { min: 1, max: 3 },
        acceptedCanonicalVersion: { min: 1, max: 1 },
        capabilityContracts: [],
        leaseMs: 1_000,
        allowFreshInitialization: true,
      });
      const initialization = controller.initialize({
        selectedBackendEmpty: true,
        evidenceDigest: digest("a"),
      });
      await expect
        .poll(
          async () => {
            const [row] = await prisma.$queryRaw<
              readonly { waiting: bigint }[]
            >(Prisma.sql`
              SELECT count(*)::bigint AS waiting
              FROM pg_stat_activity
              WHERE datname = current_database()
                AND wait_event_type = 'Lock'
                AND wait_event = 'advisory'
            `);
            return Number(row?.waiting ?? 0n);
          },
          { timeout: 2_000, interval: 20 },
        )
        .toBeGreaterThan(0);

      await new Promise((resolve) => setTimeout(resolve, 1_200));
      releaseDeploymentLock();
      await blocker;
      await expect(initialization).resolves.toMatchObject({ mode: "READY" });
      await expect(controller.checkReadiness()).resolves.toBe(true);
    });

    it("supports static ClickHouse capability claims without Doris stamps", async () => {
      const ready = await deployment.resolveAnalyticsBackendStartup(
        freshInput("clickhouse"),
      );
      if (ready.mode !== "READY") throw new Error("Fresh marker is required");
      const now = new Date("2026-07-21T17:20:00.000Z");
      const worker = await leases.registerAnalyticsRuntimeLease(
        runtimeInput({
          backend: "clickhouse",
          component: "worker",
          instanceId: "worker-clickhouse-claim",
          now,
          capabilityContracts: [
            {
              capability: "coreBatchExports",
              supportedContractVersion: 1,
              installedRoles: ["consumer", "recovery"],
            },
          ],
        }),
      );

      await expect(
        leases.createAnalyticsBackendClaimLease({
          client: prisma,
          runtimeLeaseId: worker.lease.id,
          expectedBackend: "clickhouse",
          expectedDeploymentGeneration: ready.marker.generation,
          expectedWorkloadEpochFingerprint:
            ready.marker.workloadEpochFingerprint,
          expectedRuntimeContractVersion:
            ready.marker.foundationContractVersion,
          capability: "coreBatchExports",
          action: "claimExisting",
          claimKind: "clickhouse-export",
          resourceIdentity: "export-clickhouse",
          leaseMs: 30_000,
          now: new Date(now.getTime() + 1_000),
        }),
      ).resolves.toMatchObject({
        backend: "CLICKHOUSE",
        capability: "CORE_BATCH_EXPORTS",
        capabilityActivationGeneration: null,
        capabilityContractVersion: null,
      });
    });

    it("rejects switch when either analytics backend is non-empty", async () => {
      const now = new Date("2026-07-21T17:30:00.000Z");
      const prepared = await prepareQuiescedSwitch({
        now,
        suffix: "emptiness",
      });

      await expect(
        deployment.switchAnalyticsBackend(
          switchInput({
            ...prepared,
            now,
            verifyBackendEmptiness: async () => ({
              source: {
                backend: "doris",
                empty: false,
                evidenceDigest: digest("1"),
              },
              target: {
                backend: "clickhouse",
                empty: true,
                evidenceDigest: digest("2"),
              },
            }),
          }),
        ),
      ).rejects.toThrow(/empty source and target/i);
      await expect(
        deployment.switchAnalyticsBackend(
          switchInput({
            ...prepared,
            now,
            verifyBackendEmptiness: async () => ({
              source: {
                backend: "doris",
                empty: true,
                evidenceDigest: digest("3"),
              },
              target: {
                backend: "clickhouse",
                empty: false,
                evidenceDigest: digest("4"),
              },
            }),
          }),
        ),
      ).rejects.toThrow(/empty source and target/i);
    });

    it("rejects queue drain evidence from another Redis namespace", async () => {
      const now = new Date("2026-07-21T17:35:00.000Z");
      const prepared = await prepareQuiescedSwitch({
        now,
        suffix: "queue-namespace",
      });

      await expect(
        deployment.switchAnalyticsBackend(
          switchInput({
            ...prepared,
            now,
            verifyBackendEmptiness: async () => ({
              source: {
                backend: "doris",
                empty: true,
                evidenceDigest: digest("1"),
              },
              target: {
                backend: "clickhouse",
                empty: true,
                evidenceDigest: digest("2"),
              },
            }),
            verifyScoreDeletionQueuesEmpty: async (scope) => ({
              ...(await emptyQueueDrain(scope)),
              queueNamespaceFingerprint: digest("e"),
            }),
          }),
        ),
      ).rejects.toThrow(/queue namespace/i);
      await expect(
        prisma.analyticsBackendDeploymentState.findUniqueOrThrow({
          where: { id: "global" },
        }),
      ).resolves.toMatchObject({
        backend: "DORIS",
        generation: prepared.marker.generation,
      });
    });

    it("rejects switch with historical control state or a replay fence", async () => {
      const now = new Date("2026-07-21T17:40:00.000Z");
      const prepared = await prepareQuiescedSwitch({
        now,
        suffix: "history",
      });
      const suffix = `${Date.now()}-${Math.random().toString(16).slice(2)}`;
      const organizationId = `switch-org-${suffix}`;
      const projectId = `switch-project-${suffix}`;
      const operationId = `switch-operation-${suffix}`;
      const emptyEvidence = async () => ({
        source: {
          backend: "doris" as const,
          empty: true,
          evidenceDigest: digest("5"),
        },
        target: {
          backend: "clickhouse" as const,
          empty: true,
          evidenceDigest: digest("6"),
        },
      });

      await prisma.organization.create({
        data: { id: organizationId, name: "Switch history safety" },
      });
      await prisma.project.create({
        data: {
          id: projectId,
          name: "Switch history safety",
          orgId: organizationId,
        },
      });
      await prisma.analyticsIngestionOperation.create({
        data: {
          id: operationId,
          projectId,
          sourceOperationId: `source-${suffix}`,
          sourceChecksum: digest("7"),
          rawObjectKey: `raw/${suffix}`,
          acceptedAt: now,
          acceptedAtNanos: BigInt(now.getTime()) * 1_000_000n,
          canonicalizerVersion: "u0-safety",
          schemaVersion: 3,
          status: "VISIBLE",
          recoverableUntil: new Date(now.getTime() + 86_400_000),
          statusExpiresAt: new Date(now.getTime() + 172_800_000),
          terminalAt: now,
          visibleAt: now,
        },
      });
      await expect(
        deployment.switchAnalyticsBackend(
          switchInput({
            ...prepared,
            now,
            verifyBackendEmptiness: emptyEvidence,
          }),
        ),
      ).rejects.toThrow(/historical analytics data/i);
      await prisma.analyticsIngestionOperation.delete({
        where: { id: operationId },
      });
      await prisma.project.delete({ where: { id: projectId } });
      await prisma.organization.delete({ where: { id: organizationId } });

      await prisma.analyticsCapabilityActivation.update({
        where: { capability: "EVALUATIONS" },
        data: {
          rescanRequired: true,
          cutoffState: { operationId: "replay-before-switch" },
          cutoffActivationGeneration: 1n,
          cutoffDigest: digest("a"),
        },
      });
      await expect(
        deployment.switchAnalyticsBackend(
          switchInput({
            ...prepared,
            now,
            verifyBackendEmptiness: emptyEvidence,
          }),
        ),
      ).rejects.toThrow(/capabilities must be disabled/i);
    });
  },
);
