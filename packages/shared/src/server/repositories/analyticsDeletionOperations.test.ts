import type { PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  AnalyticsProjectDeletionInProgressError,
  finalizeProjectDeletionOperation,
  findRecoverableDeletionOperations,
  hasPreBarrierIngestionWork,
  scheduleProjectDeletionOperation,
  scheduleTraceDeletionOperations,
} from "./analyticsDeletionOperations";

const NOW = new Date("2026-07-18T12:00:00.000Z");
const EPOCH = "a".repeat(64);

function managedDeletionTransaction(input?: {
  readonly existing?: Record<string, unknown> | null;
}) {
  const created = {
    id: "operation-created",
    scope: "TRACE",
    organizationId: "org-1",
    projectId: "project-1",
    traceId: "trace-1",
    generation: 1n,
  };
  const transaction = {
    $queryRaw: vi.fn().mockResolvedValue([
      {
        now: NOW,
        id: "project-1",
        sequence: 4_611_686_018_427_387_904n,
        locked: "",
      },
    ]),
    analyticsBackendDeploymentState: {
      findUnique: vi.fn().mockResolvedValue({
        id: "global",
        backend: "DORIS",
        generation: 7n,
        workloadEpochFingerprint: EPOCH,
        foundationContractVersion: 3,
      }),
    },
    analyticsRuntimeLease: {
      findUnique: vi.fn().mockResolvedValue({
        id: "runtime-current",
        state: "ACTIVE",
        leaseExpiresAt: new Date("2026-07-18T12:05:00.000Z"),
        supersededAt: null,
        backend: "DORIS",
        deploymentGeneration: 7n,
        workloadEpochFingerprint: EPOCH,
        foundationContractVersion: 3,
        capabilityContracts: [],
      }),
    },
    analyticsCheckpointGeneration: {
      findFirst: vi.fn().mockResolvedValue(null),
    },
    analyticsProjectDeletionGeneration: {
      findUnique: vi
        .fn()
        .mockResolvedValue(input?.existing ? { generation: 1n } : null),
      create: vi.fn().mockResolvedValue({ generation: 1n }),
    },
    project: {
      findFirstOrThrow: vi.fn().mockResolvedValue({ id: "project-1" }),
      updateMany: vi.fn().mockResolvedValue({ count: 1 }),
    },
    analyticsDeletionTombstone: {
      findUnique: vi.fn().mockResolvedValue(null),
      create: vi.fn().mockResolvedValue({ generation: 1n }),
    },
    analyticsDeletionOperation: {
      findFirst: vi.fn().mockResolvedValue(input?.existing ?? null),
      create: vi
        .fn()
        .mockImplementation(({ data }) => ({ ...created, ...data })),
    },
  };
  const client = {
    $transaction: vi.fn((callback) => callback(transaction)),
  } as unknown as PrismaClient;
  return { client, transaction };
}

const admissionContext = {
  runtimeLeaseId: "runtime-current",
  backend: "doris" as const,
  deploymentGeneration: 7n,
};

describe("findRecoverableDeletionOperations", () => {
  it("keeps the timestamp barrier for deletion operations created before acceptance sequences", async () => {
    const count = vi.fn().mockResolvedValue(1);
    const client = {
      analyticsIngestionOperation: { count },
    } as unknown as PrismaClient;

    await expect(
      hasPreBarrierIngestionWork({
        client,
        projectId: "project-1",
        barrierCreatedAt: NOW,
        barrierAcceptanceSequence: null,
      }),
    ).resolves.toBe(true);

    expect(count).toHaveBeenCalledWith({
      where: {
        projectId: "project-1",
        status: {
          in: ["ACCEPTED", "QUEUED", "PERSISTED", "RETRYING"],
        },
        createdAt: { lte: NOW },
      },
    });
  });

  it("uses the sequence barrier while conservatively draining every legacy NULL receipt", async () => {
    const count = vi.fn().mockResolvedValue(0);
    const client = {
      analyticsIngestionOperation: { count },
    } as unknown as PrismaClient;

    await expect(
      hasPreBarrierIngestionWork({
        client,
        projectId: "project-1",
        barrierCreatedAt: NOW,
        barrierAcceptanceSequence: 42n,
      }),
    ).resolves.toBe(false);

    expect(count).toHaveBeenCalledWith({
      where: {
        projectId: "project-1",
        status: {
          in: ["ACCEPTED", "QUEUED", "PERSISTED", "RETRYING"],
        },
        OR: [{ acceptanceSequence: { lt: 42n } }, { acceptanceSequence: null }],
      },
    });
  });

  it("selects only stale, unleased retryable operations in stable order", async () => {
    const findMany = vi.fn().mockResolvedValue([]);
    const client = {
      analyticsDeletionOperation: { findMany },
    } as unknown as PrismaClient;
    const updatedBefore = new Date("2026-07-18T11:59:30.000Z");
    const leaseExpiredBefore = new Date("2026-07-18T12:00:00.000Z");

    await findRecoverableDeletionOperations({
      client,
      scopes: ["TRACE"],
      updatedBefore,
      leaseExpiredBefore,
      limit: 100,
    });

    expect(findMany).toHaveBeenCalledWith({
      where: {
        scope: { in: ["TRACE"] },
        status: { in: ["RETRYING", "SCHEDULED"] },
        completedAt: null,
        updatedAt: { lte: updatedBefore },
        OR: [
          { leaseOwner: null },
          { leaseExpiresAt: null },
          { leaseExpiresAt: { lte: leaseExpiredBefore } },
        ],
      },
      orderBy: [{ updatedAt: "asc" }, { id: "asc" }],
      take: 100,
    });
  });

  it("rejects an empty recovery scope", () => {
    expect(() =>
      findRecoverableDeletionOperations({
        client: {} as PrismaClient,
        scopes: [],
        updatedBefore: new Date(),
        leaseExpiredBefore: new Date(),
        limit: 100,
      }),
    ).toThrow("Invalid deletion recovery query");
  });

  it("does not create trace deletion state after a project deletion fence exists", async () => {
    const projectFind = vi.fn();
    const transaction = {
      $queryRaw: vi.fn().mockResolvedValue([
        {
          now: NOW,
          id: "project-1",
          sequence: 4_611_686_018_427_387_904n,
          locked: "",
        },
      ]),
      analyticsBackendDeploymentState: {
        findUnique: vi.fn().mockResolvedValue(null),
      },
      analyticsCheckpointGeneration: {
        findFirst: vi.fn().mockResolvedValue(null),
      },
      analyticsProjectDeletionGeneration: {
        findUnique: vi.fn().mockResolvedValue({ generation: 1n }),
      },
      project: { findFirstOrThrow: projectFind },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      scheduleTraceDeletionOperations({
        client,
        projectId: "project-1",
        organizationId: "org-1",
        traceIds: ["trace-1"],
        requester: { principalType: "system", principalId: "test" },
      }),
    ).rejects.toBeInstanceOf(AnalyticsProjectDeletionInProgressError);
    expect(projectFind).not.toHaveBeenCalled();
  });

  it("stamps a new managed trace deletion from admission in its create transaction", async () => {
    const { client, transaction } = managedDeletionTransaction();

    const [scheduled] = await scheduleTraceDeletionOperations({
      client,
      projectId: "project-1",
      organizationId: "org-1",
      traceIds: ["trace-1"],
      requester: { principalType: "system", principalId: "test" },
      analyticsAdmissionContext: admissionContext,
      now: NOW,
    });

    expect(scheduled?.operation).toMatchObject({
      analyticsBackend: "DORIS",
      deploymentGeneration: 7n,
      workloadEpochFingerprint: EPOCH,
      runtimeContractVersion: 3,
      producerRuntimeLeaseId: "runtime-current",
    });
    expect(
      transaction.analyticsDeletionOperation.create,
    ).toHaveBeenCalledOnce();
  });

  it("rejects an existing managed deletion whose durable provenance was tampered", async () => {
    const { client } = managedDeletionTransaction({
      existing: {
        id: "operation-existing",
        scope: "PROJECT",
        organizationId: "org-1",
        projectId: "project-1",
        traceId: null,
        generation: 1n,
        analyticsBackend: "DORIS",
        deploymentGeneration: 7n,
        workloadEpochFingerprint: "b".repeat(64),
        runtimeContractVersion: 3,
        producerRuntimeLeaseId: "runtime-original",
      },
    });

    await expect(
      scheduleProjectDeletionOperation({
        client,
        projectId: "project-1",
        organizationId: "org-1",
        requester: { principalType: "system", principalId: "test" },
        analyticsAdmissionContext: admissionContext,
        now: NOW,
      }),
    ).rejects.toThrow("Analytics deletion durable provenance changed");
  });

  it("does not restamp the original producer lease on a repeated managed deletion", async () => {
    const existing = {
      id: "operation-existing",
      scope: "PROJECT",
      organizationId: "org-1",
      projectId: "project-1",
      traceId: null,
      generation: 1n,
      analyticsBackend: "DORIS",
      deploymentGeneration: 7n,
      workloadEpochFingerprint: EPOCH,
      runtimeContractVersion: 3,
      producerRuntimeLeaseId: "runtime-original",
    };
    const { client, transaction } = managedDeletionTransaction({ existing });

    await expect(
      scheduleProjectDeletionOperation({
        client,
        projectId: "project-1",
        organizationId: "org-1",
        requester: { principalType: "system", principalId: "test" },
        analyticsAdmissionContext: admissionContext,
        now: NOW,
      }),
    ).resolves.toBe(existing);
    expect(
      transaction.analyticsDeletionOperation.create,
    ).not.toHaveBeenCalled();
    expect(transaction.project.updateMany).toHaveBeenCalledWith({
      where: { id: "project-1", orgId: "org-1" },
      data: { deletedAt: NOW },
    });
  });

  it("creates the Project intent and soft-delete in the same transaction", async () => {
    const { client, transaction } = managedDeletionTransaction();

    await scheduleProjectDeletionOperation({
      client,
      projectId: "project-1",
      organizationId: "org-1",
      requester: { principalType: "system", principalId: "test" },
      analyticsAdmissionContext: admissionContext,
      now: NOW,
    });

    expect(client.$transaction).toHaveBeenCalledOnce();
    expect(
      transaction.analyticsDeletionOperation.create,
    ).toHaveBeenCalledOnce();
    expect(transaction.project.updateMany).toHaveBeenCalledWith({
      where: { id: "project-1", orgId: "org-1" },
      data: { deletedAt: NOW },
    });
  });

  it("locks the Project and atomically finalizes children, Project, and operation", async () => {
    const order: string[] = [];
    const deletionUpdateMany = vi.fn(async ({ where }) => {
      order.push(where.scope === "TRACE" ? "children" : "operation");
      return { count: 1 };
    });
    const transaction = {
      $queryRaw: vi.fn(async () => {
        order.push("project-lock");
        return [{ id: "project-1" }];
      }),
      analyticsDeletionOperation: {
        findFirst: vi.fn(async () => {
          order.push("validate-operation");
          return {
            id: "project-operation",
            createdAt: NOW,
            ingestionBarrierSequence: 42n,
          };
        }),
        updateMany: deletionUpdateMany,
      },
      analyticsDeletionTombstone: {
        updateMany: vi.fn(async () => {
          order.push("tombstones");
          return { count: 1 };
        }),
      },
      analyticsIngestionOperation: {
        count: vi.fn(async () => {
          order.push("ingestion-drain-check");
          return 0;
        }),
      },
      project: {
        deleteMany: vi.fn(async () => {
          order.push("project-delete");
          return { count: 1 };
        }),
      },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      finalizeProjectDeletionOperation({
        client,
        projectOperationId: "project-operation",
        projectId: "project-1",
        organizationId: "org-1",
        projectGeneration: 1n,
        lease: { owner: "worker-1", fence: 2n },
        now: NOW,
      }),
    ).resolves.toBe(true);

    expect(order).toEqual([
      "project-lock",
      "validate-operation",
      "ingestion-drain-check",
      "children",
      "tombstones",
      "project-delete",
      "operation",
    ]);
    expect(client.$transaction).toHaveBeenCalledOnce();
  });

  it("keeps the Project when pre-barrier ingestion revives before finalization", async () => {
    const deleteProject = vi.fn();
    const updateOperations = vi.fn();
    const transaction = {
      $queryRaw: vi.fn().mockResolvedValue([{ id: "project-1" }]),
      analyticsDeletionOperation: {
        findFirst: vi.fn().mockResolvedValue({
          id: "project-operation",
          createdAt: NOW,
          ingestionBarrierSequence: 42n,
        }),
        updateMany: updateOperations,
      },
      analyticsIngestionOperation: {
        count: vi.fn().mockResolvedValue(1),
      },
      analyticsDeletionTombstone: { updateMany: vi.fn() },
      project: { deleteMany: deleteProject },
    };
    const client = {
      $transaction: vi.fn((callback) => callback(transaction)),
    } as unknown as PrismaClient;

    await expect(
      finalizeProjectDeletionOperation({
        client,
        projectOperationId: "project-operation",
        projectId: "project-1",
        organizationId: "org-1",
        projectGeneration: 1n,
        lease: { owner: "worker-1", fence: 2n },
        now: NOW,
      }),
    ).resolves.toBe(false);

    expect(updateOperations).not.toHaveBeenCalled();
    expect(deleteProject).not.toHaveBeenCalled();
  });
});
