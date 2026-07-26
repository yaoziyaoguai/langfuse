import type { AnalyticsDeletionOperation, PrismaClient } from "@prisma/client";
import { UnrecoverableError } from "bullmq";
import { describe, expect, it, vi } from "vitest";

import {
  withAnalyticsDeletionWorkFence,
  withAnalyticsDurableWorkFence,
} from "./analyticsDeletionWorkFence";

const provenance = {
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "7",
  workloadEpochFingerprint: "a".repeat(64),
  runtimeContractVersion: 3,
  producerRuntimeLeaseId: "runtime-original",
};

function operation(
  overrides: Partial<AnalyticsDeletionOperation> = {},
): AnalyticsDeletionOperation {
  const now = new Date("2026-07-18T00:00:00.000Z");
  return {
    id: "operation-1",
    scope: "TRACE",
    organizationId: "org-1",
    projectId: "project-1",
    traceId: "trace-1",
    generation: 1n,
    checkpointGeneration: 0n,
    analyticsBackend: "DORIS",
    deploymentGeneration: 7n,
    workloadEpochFingerprint: "a".repeat(64),
    runtimeContractVersion: 3,
    producerRuntimeLeaseId: "runtime-original",
    workerFence: 0n,
    leaseOwner: null,
    leaseExpiresAt: null,
    requesterPrincipalType: "system",
    requesterPrincipalId: "test",
    status: "RETRYING",
    phase: "visibility_barrier",
    logicallyInvisible: false,
    cancellationReasonCode: null,
    statusExpiresAt: new Date("2026-08-18T00:00:00.000Z"),
    completedAt: null,
    createdAt: now,
    updatedAt: now,
    ...overrides,
  };
}

function harness() {
  const transaction = {};
  const client = {
    $transaction: vi.fn((callback) => callback(transaction)),
  } as unknown as PrismaClient;
  const dependencies = {
    createClaim: vi.fn().mockResolvedValue({ id: "claim-1" }),
    lockClaimForIo: vi.fn().mockResolvedValue({ id: "claim-1" }),
    lockAdmission: vi.fn().mockResolvedValue({}),
    lockLegacyAdmission: vi.fn().mockResolvedValue(undefined),
    releaseClaim: vi.fn().mockResolvedValue(true),
  };
  return { client, dependencies, transaction };
}

describe("withAnalyticsDeletionWorkFence", () => {
  it("rejects tampered queue provenance before a claim or analytics IO", async () => {
    const { client, dependencies } = harness();
    const run = vi.fn();

    await expect(
      withAnalyticsDeletionWorkFence({
        client,
        operation: operation(),
        serializedProvenance: {
          ...provenance,
          deploymentGeneration: "8",
        },
        admissionContext: {
          runtimeLeaseId: "runtime-current",
          backend: "doris",
          deploymentGeneration: 7n,
        },
        selectedBackend: "doris",
        claimKind: "analytics-deletion-process",
        run,
        dependencies,
      }),
    ).rejects.toBeInstanceOf(UnrecoverableError);
    expect(dependencies.createClaim).not.toHaveBeenCalled();
    expect(run).not.toHaveBeenCalled();
  });

  it("revalidates a managed claim under the deployment lock before IO", async () => {
    const { client, dependencies, transaction } = harness();
    const run = vi.fn().mockResolvedValue(undefined);

    await withAnalyticsDeletionWorkFence({
      client,
      operation: operation(),
      serializedProvenance: provenance,
      admissionContext: {
        runtimeLeaseId: "runtime-current",
        backend: "doris",
        deploymentGeneration: 7n,
      },
      selectedBackend: "doris",
      claimKind: "analytics-deletion-process",
      run,
      dependencies,
    });

    expect(dependencies.createClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        runtimeLeaseId: "runtime-current",
        expectedBackend: "doris",
        expectedDeploymentGeneration: 7n,
        expectedWorkloadEpochFingerprint: "a".repeat(64),
        expectedRuntimeContractVersion: 3,
        resourceIdentity: "operation-1",
      }),
    );
    expect(dependencies.lockClaimForIo).toHaveBeenCalledWith(
      expect.objectContaining({ transaction, claimLeaseId: "claim-1" }),
    );
    expect(run).toHaveBeenCalledOnce();
    expect(dependencies.releaseClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        claimLeaseId: "claim-1",
        runtimeLeaseId: "runtime-current",
      }),
    );
  });

  it("uses the same managed claim fence for ClickHouse operations", async () => {
    const { client, dependencies } = harness();
    const run = vi.fn().mockResolvedValue(undefined);

    await withAnalyticsDeletionWorkFence({
      client,
      operation: operation({ analyticsBackend: "CLICKHOUSE" }),
      serializedProvenance: {
        ...provenance,
        analyticsBackend: "CLICKHOUSE",
      },
      admissionContext: {
        runtimeLeaseId: "runtime-current",
        backend: "clickhouse",
        deploymentGeneration: 7n,
      },
      selectedBackend: "clickhouse",
      claimKind: "analytics-deletion-operation",
      run,
      dependencies,
    });

    expect(dependencies.createClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedBackend: "clickhouse",
        expectedDeploymentGeneration: 7n,
        resourceIdentity: "operation-1",
      }),
    );
    expect(run).toHaveBeenCalledOnce();
  });

  it("allows an unstamped legacy ClickHouse job only while the marker is absent", async () => {
    const { client, dependencies, transaction } = harness();
    const run = vi.fn().mockResolvedValue(undefined);

    await withAnalyticsDeletionWorkFence({
      client,
      operation: null,
      serializedProvenance: undefined,
      admissionContext: null,
      selectedBackend: "clickhouse",
      claimKind: "analytics-deletion-process",
      run,
      dependencies,
    });

    expect(dependencies.lockLegacyAdmission).toHaveBeenCalledWith(transaction);
    expect(run).toHaveBeenCalledOnce();
    expect(dependencies.createClaim).not.toHaveBeenCalled();
  });

  it("runs an unstamped legacy ClickHouse job under the managed ClickHouse admission lock", async () => {
    const { client, dependencies, transaction } = harness();
    const run = vi.fn().mockResolvedValue(undefined);

    await withAnalyticsDeletionWorkFence({
      client,
      operation: null,
      serializedProvenance: undefined,
      admissionContext: {
        runtimeLeaseId: "runtime-current",
        backend: "clickhouse",
        deploymentGeneration: 7n,
      },
      selectedBackend: "clickhouse",
      claimKind: "analytics-deletion-process",
      run,
      dependencies,
    });

    expect(dependencies.lockAdmission).toHaveBeenCalledWith({
      transaction,
      runtimeLeaseId: "runtime-current",
      expectedBackend: "clickhouse",
      expectedDeploymentGeneration: 7n,
      action: "foundation",
    });
    expect(dependencies.lockLegacyAdmission).not.toHaveBeenCalled();
    expect(run).toHaveBeenCalledOnce();
  });

  it("rejects an unstamped Doris job before analytics IO", async () => {
    const { client, dependencies } = harness();
    const run = vi.fn();

    await expect(
      withAnalyticsDeletionWorkFence({
        client,
        operation: operation({
          analyticsBackend: null,
          deploymentGeneration: null,
          workloadEpochFingerprint: null,
          runtimeContractVersion: null,
          producerRuntimeLeaseId: null,
        }),
        serializedProvenance: undefined,
        admissionContext: null,
        selectedBackend: "doris",
        claimKind: "analytics-deletion-recovery",
        run,
        dependencies,
      }),
    ).rejects.toBeInstanceOf(UnrecoverableError);
    expect(dependencies.lockLegacyAdmission).not.toHaveBeenCalled();
    expect(run).not.toHaveBeenCalled();
  });
});

describe("withAnalyticsDurableWorkFence", () => {
  it("fails closed before Doris work when queue provenance is missing", async () => {
    const { client, dependencies } = harness();
    const run = vi.fn();

    await expect(
      withAnalyticsDurableWorkFence({
        client,
        serializedProvenance: undefined,
        admissionContext: {
          runtimeLeaseId: "runtime-current",
          backend: "doris",
          deploymentGeneration: 7n,
        },
        selectedBackend: "doris",
        claimKind: "score-delete",
        resourceIdentity: "job-1",
        run,
        dependencies,
      }),
    ).rejects.toBeInstanceOf(UnrecoverableError);
    expect(dependencies.createClaim).not.toHaveBeenCalled();
    expect(run).not.toHaveBeenCalled();
  });

  it("rejects stale score work before creating a claim", async () => {
    const { client, dependencies } = harness();
    const run = vi.fn();

    await expect(
      withAnalyticsDurableWorkFence({
        client,
        serializedProvenance: provenance,
        admissionContext: {
          runtimeLeaseId: "runtime-current",
          backend: "doris",
          deploymentGeneration: 8n,
        },
        selectedBackend: "doris",
        claimKind: "score-delete",
        resourceIdentity: "job-1",
        run,
        dependencies,
      }),
    ).rejects.toBeInstanceOf(UnrecoverableError);
    expect(dependencies.createClaim).not.toHaveBeenCalled();
    expect(run).not.toHaveBeenCalled();
  });

  it("rejects a score job stamped for the other backend", async () => {
    const { client, dependencies } = harness();
    const run = vi.fn();

    await expect(
      withAnalyticsDurableWorkFence({
        client,
        serializedProvenance: provenance,
        admissionContext: {
          runtimeLeaseId: "runtime-current",
          backend: "clickhouse",
          deploymentGeneration: 7n,
        },
        selectedBackend: "clickhouse",
        claimKind: "score-delete",
        resourceIdentity: "job-1",
        run,
        dependencies,
      }),
    ).rejects.toBeInstanceOf(UnrecoverableError);
    expect(dependencies.createClaim).not.toHaveBeenCalled();
    expect(run).not.toHaveBeenCalled();
  });

  it("does not start analytics IO after the claim loses admission", async () => {
    const { client, dependencies } = harness();
    dependencies.lockClaimForIo.mockRejectedValueOnce(
      new Error("claim lease expired"),
    );
    const run = vi.fn();

    await expect(
      withAnalyticsDurableWorkFence({
        client,
        serializedProvenance: provenance,
        admissionContext: {
          runtimeLeaseId: "runtime-current",
          backend: "doris",
          deploymentGeneration: 7n,
        },
        selectedBackend: "doris",
        claimKind: "score-delete",
        resourceIdentity: "job-1",
        run,
        dependencies,
      }),
    ).rejects.toThrow("claim lease expired");
    expect(run).not.toHaveBeenCalled();
    expect(dependencies.releaseClaim).toHaveBeenCalledOnce();
  });

  it("keeps unstamped legacy ClickHouse work behind the marker-absent lock", async () => {
    const { client, dependencies, transaction } = harness();
    const run = vi.fn().mockResolvedValue(undefined);

    await withAnalyticsDurableWorkFence({
      client,
      serializedProvenance: undefined,
      admissionContext: null,
      selectedBackend: "clickhouse",
      claimKind: "score-delete",
      resourceIdentity: "legacy-job-1",
      run,
      dependencies,
    });

    expect(dependencies.lockLegacyAdmission).toHaveBeenCalledWith(transaction);
    expect(dependencies.createClaim).not.toHaveBeenCalled();
    expect(run).toHaveBeenCalledOnce();
  });

  it("claims managed ClickHouse score work with its durable provenance", async () => {
    const { client, dependencies } = harness();
    const run = vi.fn().mockResolvedValue(undefined);

    await withAnalyticsDurableWorkFence({
      client,
      serializedProvenance: {
        ...provenance,
        analyticsBackend: "CLICKHOUSE",
      },
      admissionContext: {
        runtimeLeaseId: "runtime-current",
        backend: "clickhouse",
        deploymentGeneration: 7n,
      },
      selectedBackend: "clickhouse",
      claimKind: "score-delete",
      resourceIdentity: "job-1",
      run,
      dependencies,
    });

    expect(dependencies.createClaim).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedBackend: "clickhouse",
        expectedDeploymentGeneration: 7n,
        expectedWorkloadEpochFingerprint: "a".repeat(64),
        expectedRuntimeContractVersion: 3,
      }),
    );
    expect(run).toHaveBeenCalledOnce();
  });

  it("holds the deployment transaction fence across all claimed work", async () => {
    const order: string[] = [];
    const transaction = {};
    const client = {
      $transaction: vi.fn(async (callback) => {
        order.push("transaction-enter");
        const result = await callback(transaction);
        order.push("transaction-exit");
        return result;
      }),
    } as unknown as PrismaClient;
    const dependencies = {
      createClaim: vi.fn(async () => {
        order.push("claim");
        return { id: "claim-1" } as never;
      }),
      lockClaimForIo: vi.fn(async () => {
        order.push("lock");
        return { id: "claim-1" } as never;
      }),
      lockAdmission: vi.fn(),
      lockLegacyAdmission: vi.fn(),
      releaseClaim: vi.fn(async () => {
        order.push("release");
        return true;
      }),
    };

    await withAnalyticsDurableWorkFence({
      client,
      serializedProvenance: provenance,
      admissionContext: {
        runtimeLeaseId: "runtime-current",
        backend: "doris",
        deploymentGeneration: 7n,
      },
      selectedBackend: "doris",
      claimKind: "score-delete",
      resourceIdentity: "job-1",
      run: async () => {
        order.push("entity-head-read");
        order.push("stream-load-and-reconcile");
      },
      dependencies,
    });

    expect(order).toEqual([
      "claim",
      "transaction-enter",
      "lock",
      "entity-head-read",
      "stream-load-and-reconcile",
      "transaction-exit",
      "release",
    ]);
  });
});
