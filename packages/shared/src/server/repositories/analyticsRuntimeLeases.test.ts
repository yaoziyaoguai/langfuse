import type {
  AnalyticsBackendClaimLease,
  AnalyticsRuntimeLease,
  PrismaClient,
} from "@prisma/client";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  lockAnalyticsAdmission: vi.fn(),
}));

vi.mock("../../db", () => ({ prisma: {} }));
vi.mock("../analytics-persistence/analyticsBackendAdmission", () => ({
  lockAnalyticsAdmission: mocks.lockAnalyticsAdmission,
}));
vi.mock("./analyticsBackendDeployment", () => ({
  acquireAnalyticsDeploymentSharedLock: vi.fn(),
  lockAnalyticsBackendDeploymentState: vi.fn(),
}));

import {
  createAnalyticsBackendClaimLease,
  evaluateAnalyticsContractRollout,
  type AnalyticsRuntimeCompatibilityEvidence,
} from "./analyticsRuntimeLeases";

describe("analytics runtime lease claim locking", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("locks the runtime row before taking over an expired claim row", async () => {
    const now = new Date("2026-07-21T17:12:03.000Z");
    const fingerprint = "a".repeat(64);
    const runtimeLeaseId = "runtime-lock-order";
    const order: string[] = [];
    const expiredClaim = {
      id: "expired-claim",
      leaseExpiresAt: new Date(now.getTime() - 1),
    } as AnalyticsBackendClaimLease;
    const runtimeLease = {
      id: runtimeLeaseId,
      state: "ACTIVE",
      supersededAt: null,
      leaseExpiresAt: new Date(now.getTime() + 60_000),
      backend: "DORIS",
      deploymentGeneration: 1n,
      workloadEpochFingerprint: fingerprint,
      foundationContractVersion: 1,
    } as AnalyticsRuntimeLease;
    const replacement = {
      id: "replacement-claim",
    } as AnalyticsBackendClaimLease;
    let rawLockCount = 0;
    const transaction = {
      $queryRaw: vi.fn(async () => {
        rawLockCount += 1;
        order.push(rawLockCount === 1 ? "resource-lock" : "runtime-lock");
        return [];
      }),
      analyticsRuntimeLease: {
        findUnique: vi.fn(async () => {
          order.push("runtime-find");
          return runtimeLease;
        }),
      },
      analyticsBackendClaimLease: {
        findFirst: vi.fn(async () => {
          order.push("claim-find");
          return expiredClaim;
        }),
        update: vi.fn(async () => {
          order.push("claim-update");
          return expiredClaim;
        }),
        create: vi.fn(async () => {
          order.push("claim-create");
          return replacement;
        }),
      },
    };
    const client = {
      $transaction: vi.fn(
        async (callback: (tx: typeof transaction) => Promise<unknown>) =>
          callback(transaction),
      ),
    } as unknown as PrismaClient;
    mocks.lockAnalyticsAdmission.mockResolvedValue({
      analyticsBackend: "DORIS",
      deploymentGeneration: 1n,
      workloadEpochFingerprint: fingerprint,
      runtimeContractVersion: 1,
      admittingRuntimeLeaseId: runtimeLeaseId,
      admittedAt: now,
    });

    await expect(
      createAnalyticsBackendClaimLease({
        client,
        runtimeLeaseId,
        expectedBackend: "doris",
        expectedDeploymentGeneration: 1n,
        expectedWorkloadEpochFingerprint: fingerprint,
        expectedRuntimeContractVersion: 1,
        action: "foundation",
        claimKind: "lock-order",
        resourceIdentity: "resource-1",
        leaseMs: 30_000,
        now,
      }),
    ).resolves.toBe(replacement);
    expect(order).toEqual([
      "resource-lock",
      "runtime-lock",
      "runtime-find",
      "claim-find",
      "claim-update",
      "claim-create",
    ]);
  });
});

describe("analytics serving runtime census", () => {
  it("does not treat checkpoint auxiliaries as serving fleet members", () => {
    const now = new Date("2026-07-21T17:12:03.000Z");
    const lease = (input: {
      component: "WEB" | "WORKER" | "CHECKPOINT";
      instanceId: string;
      state: "ACTIVE" | "QUIESCED";
      buildId: string;
    }) =>
      ({
        ...input,
        acceptedSchemaVersionMin: 1,
        acceptedSchemaVersionMax: 1,
        acceptedCanonicalVersionMin: 1,
        acceptedCanonicalVersionMax: 1,
        leaseExpiresAt: new Date(now.getTime() + 60_000),
        supersededAt: null,
        quiescedAt: input.state === "QUIESCED" ? now : null,
      }) as AnalyticsRuntimeCompatibilityEvidence;

    expect(
      evaluateAnalyticsContractRollout({
        leases: [
          lease({
            component: "WEB",
            instanceId: "web-current",
            state: "ACTIVE",
            buildId: "current",
          }),
          lease({
            component: "WORKER",
            instanceId: "worker-current",
            state: "ACTIVE",
            buildId: "current",
          }),
          lease({
            component: "CHECKPOINT",
            instanceId: "checkpoint-one-shot",
            state: "ACTIVE",
            buildId: "current",
          }),
          lease({
            component: "WEB",
            instanceId: "web-rollback",
            state: "QUIESCED",
            buildId: "rollback",
          }),
          lease({
            component: "WORKER",
            instanceId: "worker-rollback",
            state: "QUIESCED",
            buildId: "rollback",
          }),
        ],
        now,
        expectedRuntimeInstanceIds: ["web-current", "worker-current"],
        requiredSchemaVersions: [1],
        requiredCanonicalVersions: [1],
        rollbackBuildId: "rollback",
      }),
    ).toEqual({ ready: true, reasonCode: null });
  });
});
