import type { Session } from "next-auth";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type * as EnvModule from "@/src/env.mjs";
import type * as CommunityCapabilityRuntimeModule from "@/src/server/communityCapabilityRuntime";

const mocks = vi.hoisted(() => ({
  auditLog: vi.fn(),
  createManaged: vi.fn(),
  dispatchManaged: vi.fn(),
  internalCapabilityActive: vi.fn(),
  legacyCreate: vi.fn(),
}));

vi.mock("@/src/env.mjs", async (importOriginal) => {
  const actual = await importOriginal<typeof EnvModule>();
  return {
    ...actual,
    env: { ...actual.env, LANGFUSE_ANALYTICS_BACKEND: "doris" },
  };
});

vi.mock("@/src/features/audit-logs/auditLog", () => ({
  auditLog: mocks.auditLog,
}));

vi.mock("@/src/features/batch-exports/server/dorisBatchExport", () => ({
  createAdmittedDorisBatchExport: mocks.createManaged,
  dispatchDorisBatchExport: mocks.dispatchManaged,
}));

vi.mock("@/src/server/analyticsRuntime", () => ({
  getWebAnalyticsAdmissionContext: () => ({
    runtimeLeaseId: "web-runtime",
    backend: "doris",
    deploymentGeneration: 7n,
  }),
}));

vi.mock("@/src/server/communityCapabilityRuntime", async (importOriginal) => ({
  ...(await importOriginal<typeof CommunityCapabilityRuntimeModule>()),
  isInternalDorisCapabilityActive: mocks.internalCapabilityActive,
}));

import { CommunityCapabilityUnavailableError } from "@/src/features/capabilities/communityAvailability";
import { appRouter } from "@/src/server/api/root";
import { createInnerTRPCContext } from "@/src/server/api/trpc";
import { BatchExportFileFormat, BatchTableNames } from "@langfuse/shared";

const projectId = "project-1";
const session: Session = {
  expires: "1",
  user: {
    id: "user-1",
    name: "Test User",
    canCreateOrganizations: true,
    v4BetaEnabled: false,
    organizations: [
      {
        id: "org-1",
        name: "Test Org",
        role: "OWNER",
        plan: "oss",
        cloudConfig: undefined,
        metadata: {},
        aiFeaturesEnabled: false,
        aiTelemetryEnabled: false,
        projects: [
          {
            id: projectId,
            name: "Test Project",
            role: "OWNER",
            retentionDays: null,
            deletedAt: null,
            hasTraces: false,
            metadata: {},
            createdAt: new Date().toISOString(),
          },
        ],
      },
    ],
    featureFlags: {
      excludeClickhouseRead: false,
      templateFlag: false,
      searchBar: false,
      v4BetaToggleVisible: false,
      observationEvals: false,
      experimentsV4Enabled: false,
    },
    admin: false,
  },
  environment: {
    enableExperimentalFeatures: false,
    selfHostedInstancePlan: "oss",
  },
};

const input = {
  projectId,
  name: "Doris trace export",
  query: {
    tableName: BatchTableNames.Traces,
    filter: null,
    orderBy: null,
  },
  format: BatchExportFileFormat.JSONL,
};

function createCaller() {
  return appRouter.createCaller({
    ...createInnerTRPCContext({ session, headers: {} }),
    prisma: {
      batchExport: { create: mocks.legacyCreate },
    } as never,
  });
}

describe("Doris batch export router", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.internalCapabilityActive.mockResolvedValue(false);
  });

  it("returns NOT_IMPLEMENTED before any mutation when activation is unavailable", async () => {
    mocks.createManaged.mockRejectedValue(
      new CommunityCapabilityUnavailableError("batchExports"),
    );

    await expect(
      createCaller().batchExport.create(input),
    ).rejects.toMatchObject({ code: "NOT_IMPLEMENTED" });
    expect(mocks.legacyCreate).not.toHaveBeenCalled();
    expect(mocks.auditLog).not.toHaveBeenCalled();
    expect(mocks.dispatchManaged).not.toHaveBeenCalled();
  });

  it("audits and dispatches only after the durable managed intent exists", async () => {
    const managed = {
      id: "export-1",
      projectId,
      userId: "user-1",
      name: input.name,
      format: input.format,
      query: input.query,
      status: "QUEUED",
      dispatchOutbox: { generation: 1 },
    };
    mocks.createManaged.mockImplementation(async (args) => {
      await args.audit({} as never, managed);
      return managed;
    });

    await expect(
      createCaller().batchExport.create(input),
    ).resolves.toBeUndefined();

    expect(mocks.createManaged).toHaveBeenCalledOnce();
    expect(mocks.createManaged).toHaveBeenCalledWith(
      expect.objectContaining({ audit: expect.any(Function) }),
    );
    expect(mocks.legacyCreate).not.toHaveBeenCalled();
    expect(mocks.auditLog).toHaveBeenCalledWith(
      expect.objectContaining({
        resourceId: "export-1",
        action: "create",
        after: managed,
      }),
      expect.anything(),
    );
    expect(mocks.auditLog.mock.invocationCallOrder[0]).toBeLessThan(
      mocks.dispatchManaged.mock.invocationCallOrder[0]!,
    );
    expect(mocks.dispatchManaged).toHaveBeenCalledWith(
      expect.objectContaining({ batchExport: managed }),
    );
  });

  it("rejects dataset-run exports before mutation without their activation", async () => {
    await expect(
      createCaller().batchExport.create({
        ...input,
        query: {
          ...input.query,
          tableName: BatchTableNames.DatasetRunItems,
        },
      }),
    ).rejects.toMatchObject({ code: "NOT_IMPLEMENTED" });

    expect(mocks.internalCapabilityActive).toHaveBeenCalledWith(
      "datasetRunExports",
    );
    expect(mocks.createManaged).not.toHaveBeenCalled();
    expect(mocks.auditLog).not.toHaveBeenCalled();
    expect(mocks.dispatchManaged).not.toHaveBeenCalled();
  });

  it("creates a dataset-run export only after its independent activation", async () => {
    mocks.internalCapabilityActive.mockResolvedValue(true);
    const managed = {
      id: "dataset-export-1",
      projectId,
      userId: "user-1",
      name: input.name,
      format: input.format,
      query: {
        ...input.query,
        tableName: BatchTableNames.DatasetRunItems,
      },
      status: "QUEUED",
      dispatchOutbox: { generation: 1 },
    };
    mocks.createManaged.mockImplementation(async (args) => {
      await args.audit({} as never, managed);
      return managed;
    });

    await expect(
      createCaller().batchExport.create({
        ...input,
        query: {
          ...input.query,
          tableName: BatchTableNames.DatasetRunItems,
        },
      }),
    ).resolves.toBeUndefined();

    expect(mocks.createManaged).toHaveBeenCalledWith(
      expect.objectContaining({
        query: expect.objectContaining({
          tableName: BatchTableNames.DatasetRunItems,
        }),
      }),
    );
    expect(mocks.dispatchManaged).toHaveBeenCalledOnce();
  });
});
