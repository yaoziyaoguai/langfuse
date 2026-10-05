import type { NextApiRequest, NextApiResponse } from "next";
import type { ApiAccessScope } from "@langfuse/shared/src/server";
import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  backend: "clickhouse" as "clickhouse" | "doris",
}));

const mocks = vi.hoisted(() => ({
  admissionContext: null as {
    runtimeLeaseId: string;
    backend: "clickhouse" | "doris";
    deploymentGeneration: bigint;
  } | null,
  analyticsDurableProvenanceFromRecord: vi.fn(),
  apiKeyDeleteMany: vi.fn(),
  auditLog: vi.fn(),
  getProjectDeleteQueue: vi.fn(),
  getWebAnalyticsAdmissionContext: vi.fn(),
  invalidateCachedProjectApiKeys: vi.fn(),
  isDorisAnalyticsBackend: vi.fn(),
  projectFind: vi.fn(),
  projectUpdate: vi.fn(),
  queueAdd: vi.fn(),
  scheduleProjectDeletionOperation: vi.fn(),
  serializeAnalyticsDurableProvenance: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    apiKey: { deleteMany: mocks.apiKeyDeleteMany },
    project: {
      findUniqueOrThrow: mocks.projectFind,
      update: mocks.projectUpdate,
    },
  },
}));

vi.mock("@langfuse/shared/src/server", () => ({
  analyticsDurableProvenanceFromRecord:
    mocks.analyticsDurableProvenanceFromRecord,
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
  isDorisAnalyticsBackend: mocks.isDorisAnalyticsBackend,
  logger: { debug: vi.fn(), error: vi.fn() },
  ProjectDeleteQueue: { getInstance: mocks.getProjectDeleteQueue },
  QueueJobs: { ProjectDelete: "project-delete" },
  redis: { status: "end" },
  scheduleProjectDeletionOperation: mocks.scheduleProjectDeletionOperation,
  serializeAnalyticsDurableProvenance:
    mocks.serializeAnalyticsDurableProvenance,
}));

vi.mock("@/src/server/analyticsRuntime", () => ({
  getWebAnalyticsAdmissionContext: mocks.getWebAnalyticsAdmissionContext,
}));

vi.mock("@/src/features/public-api/server/apiAuth", () => ({
  ApiAuthService: class {
    invalidateCachedProjectApiKeys = mocks.invalidateCachedProjectApiKeys;
  },
}));

vi.mock("@/src/features/audit-logs/auditLog", () => ({
  auditLog: mocks.auditLog,
}));

vi.mock("@/src/features/entitlements/server/hasEntitlement", () => ({
  hasEntitlement: vi.fn(() => true),
  hasEntitlementBasedOnPlan: vi.fn(() => true),
}));

vi.mock("@/src/features/auth/lib/projectNameSchema", () => ({
  projectNameSchema: { parse: vi.fn() },
}));

vi.mock("@/src/features/auth/lib/projectRetentionSchema", () => ({
  projectRetentionSchema: { parse: vi.fn() },
}));

import { handleDeleteProject } from "@/src/features/admin-api/server/projects";

const projectId = "project-1";
const orgId = "org-1";
const apiKeyId = "org-api-key-1";
const workloadEpochFingerprint = "a".repeat(64);
const scope = { orgId, apiKeyId } as unknown as ApiAccessScope;

function operation(analyticsBackend: "CLICKHOUSE" | "DORIS" | null) {
  return {
    id: "deletion-operation-1",
    generation: 13n,
    status: "PENDING",
    phase: "scheduled",
    logicallyInvisible: false,
    analyticsBackend,
    deploymentGeneration: analyticsBackend ? 7n : null,
    workloadEpochFingerprint: analyticsBackend
      ? workloadEpochFingerprint
      : null,
    runtimeContractVersion: analyticsBackend ? 1 : null,
    producerRuntimeLeaseId: analyticsBackend ? "web-runtime-1" : null,
  };
}

async function invokeDelete() {
  const json = vi.fn();
  const status = vi.fn(() => ({ json }));
  const res = { status } as unknown as NextApiResponse;

  await handleDeleteProject(
    { body: {} } as NextApiRequest,
    res,
    projectId,
    scope,
  );

  return { json, status };
}

describe("admin API project deletion producer", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    state.backend = "clickhouse";
    mocks.admissionContext = null;
    mocks.getProjectDeleteQueue.mockReturnValue({ add: mocks.queueAdd });
    mocks.getWebAnalyticsAdmissionContext.mockImplementation(
      () => mocks.admissionContext,
    );
    mocks.isDorisAnalyticsBackend.mockImplementation(
      () => state.backend === "doris",
    );
    mocks.analyticsDurableProvenanceFromRecord.mockImplementation((record) =>
      record.analyticsBackend
        ? {
            analyticsBackend: record.analyticsBackend,
            deploymentGeneration: record.deploymentGeneration,
            workloadEpochFingerprint: record.workloadEpochFingerprint,
            runtimeContractVersion: record.runtimeContractVersion,
            producerRuntimeLeaseId: record.producerRuntimeLeaseId,
          }
        : null,
    );
    mocks.serializeAnalyticsDurableProvenance.mockImplementation(
      (provenance) => ({
        ...provenance,
        deploymentGeneration: provenance.deploymentGeneration.toString(),
      }),
    );
    mocks.scheduleProjectDeletionOperation.mockResolvedValue(operation(null));
    mocks.invalidateCachedProjectApiKeys.mockResolvedValue(undefined);
    mocks.apiKeyDeleteMany.mockResolvedValue({ count: 1 });
    mocks.projectUpdate.mockResolvedValue({ id: projectId, orgId });
    mocks.projectFind.mockResolvedValue({ id: projectId, orgId });
    mocks.auditLog.mockResolvedValue(undefined);
    mocks.queueAdd.mockResolvedValue(undefined);
  });

  it("stamps managed Doris deletion jobs with durable provenance", async () => {
    state.backend = "doris";
    mocks.admissionContext = {
      runtimeLeaseId: "web-runtime-1",
      backend: "doris",
      deploymentGeneration: 7n,
    };
    mocks.scheduleProjectDeletionOperation.mockResolvedValue(
      operation("DORIS"),
    );

    const { json, status } = await invokeDelete();

    expect(status).toHaveBeenCalledWith(202);
    expect(json).toHaveBeenCalledWith({
      success: true,
      message:
        "Project deletion has been initiated and is being processed asynchronously",
    });
    expect(mocks.scheduleProjectDeletionOperation).toHaveBeenCalledWith({
      projectId,
      organizationId: orgId,
      requester: {
        principalType: "api_key",
        principalId: apiKeyId,
      },
      analyticsAdmissionContext: mocks.admissionContext,
    });
    expect(mocks.queueAdd).toHaveBeenCalledWith("project-delete", {
      timestamp: expect.any(Date),
      id: expect.any(String),
      payload: {
        projectId,
        orgId,
        deletionOperationId: "deletion-operation-1",
        deletionGeneration: "13",
        analyticsProvenance: {
          analyticsBackend: "DORIS",
          deploymentGeneration: "7",
          workloadEpochFingerprint,
          runtimeContractVersion: 1,
          producerRuntimeLeaseId: "web-runtime-1",
        },
      },
      name: "project-delete",
    });
    expect(
      mocks.scheduleProjectDeletionOperation.mock.invocationCallOrder[0],
    ).toBeLessThan(
      mocks.invalidateCachedProjectApiKeys.mock.invocationCallOrder[0] ?? 0,
    );
    expect(mocks.projectFind).toHaveBeenCalledOnce();
    expect(mocks.projectUpdate).not.toHaveBeenCalled();
  });

  it("keeps managed ClickHouse on the legacy queue contract", async () => {
    state.backend = "clickhouse";
    mocks.admissionContext = {
      runtimeLeaseId: "web-runtime-1",
      backend: "clickhouse",
      deploymentGeneration: 7n,
    };

    const { status } = await invokeDelete();

    expect(status).toHaveBeenCalledWith(202);
    expect(mocks.scheduleProjectDeletionOperation).not.toHaveBeenCalled();
    expect(mocks.queueAdd.mock.calls[0]?.[1].payload).toEqual({
      projectId,
      orgId,
    });
    expect(mocks.projectUpdate).toHaveBeenCalledOnce();
  });

  it("keeps legacy ClickHouse scheduling without adding provenance", async () => {
    const { status } = await invokeDelete();

    expect(status).toHaveBeenCalledWith(202);
    expect(mocks.scheduleProjectDeletionOperation).not.toHaveBeenCalled();
    const queuedJob = mocks.queueAdd.mock.calls[0]?.[1];
    expect(queuedJob).toMatchObject({
      payload: {
        projectId,
        orgId,
      },
    });
    expect(queuedJob?.payload).not.toHaveProperty("analyticsProvenance");
  });

  it("does not mutate the project when durable scheduling fails", async () => {
    state.backend = "doris";
    mocks.admissionContext = {
      runtimeLeaseId: "web-runtime-1",
      backend: "doris",
      deploymentGeneration: 7n,
    };
    mocks.scheduleProjectDeletionOperation.mockRejectedValue(
      new Error("schedule failed"),
    );

    const { status } = await invokeDelete();

    expect(status).toHaveBeenCalledWith(500);
    expect(mocks.invalidateCachedProjectApiKeys).not.toHaveBeenCalled();
    expect(mocks.apiKeyDeleteMany).not.toHaveBeenCalled();
    expect(mocks.projectUpdate).not.toHaveBeenCalled();
    expect(mocks.auditLog).not.toHaveBeenCalled();
    expect(mocks.queueAdd).not.toHaveBeenCalled();
  });

  it("does not schedule or mutate when the project deletion queue is unavailable", async () => {
    mocks.getProjectDeleteQueue.mockReturnValue(null);

    const { status } = await invokeDelete();

    expect(status).toHaveBeenCalledWith(500);
    expect(mocks.scheduleProjectDeletionOperation).not.toHaveBeenCalled();
    expect(mocks.invalidateCachedProjectApiKeys).not.toHaveBeenCalled();
    expect(mocks.apiKeyDeleteMany).not.toHaveBeenCalled();
    expect(mocks.projectUpdate).not.toHaveBeenCalled();
  });

  it("requires managed admission before scheduling a Doris deletion", async () => {
    state.backend = "doris";

    const { status } = await invokeDelete();

    expect(status).toHaveBeenCalledWith(500);
    expect(mocks.scheduleProjectDeletionOperation).not.toHaveBeenCalled();
    expect(mocks.invalidateCachedProjectApiKeys).not.toHaveBeenCalled();
    expect(mocks.apiKeyDeleteMany).not.toHaveBeenCalled();
    expect(mocks.projectUpdate).not.toHaveBeenCalled();
  });
});
