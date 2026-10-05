import type { Session } from "next-auth";
import { describe, expect, it, vi } from "vitest";
import type * as EnvModule from "@/src/env.mjs";

vi.mock("@/src/env.mjs", async (importOriginal) => {
  const actual = await importOriginal<typeof EnvModule>();
  return {
    ...actual,
    env: {
      ...actual.env,
      LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED: "true",
    },
  };
});

import { auditLogsRouter } from "@/src/server/api/routers/auditLogs";
import { createInnerTRPCContext } from "@/src/server/api/trpc";

const projectId = "project-1";
const session: Session = {
  expires: "1",
  user: {
    id: "user-1",
    canCreateOrganizations: true,
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
    selfHostedInstancePlan: null,
    communityExtensionEnabled: true,
  },
};

describe("Community Extensions audit log router", () => {
  it("allows an OSS project owner to query the independently implemented capability", async () => {
    const findMany = vi.fn().mockResolvedValue([]);
    const count = vi.fn().mockResolvedValue(0);
    const caller = auditLogsRouter.createCaller({
      ...createInnerTRPCContext({ session, headers: {} }),
      prisma: {
        auditLog: { findMany, count },
        user: { findMany: vi.fn().mockResolvedValue([]) },
        apiKey: { findMany: vi.fn().mockResolvedValue([]) },
      } as never,
    });

    await expect(
      caller.all({ projectId, page: 0, limit: 20 }),
    ).resolves.toEqual({ data: [], totalCount: 0 });
    expect(findMany).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { projectId },
        skip: 0,
        take: 20,
      }),
    );
  });

  it("keeps project authorization mandatory when the capability is enabled", async () => {
    const viewerSession = structuredClone(session);
    viewerSession.user!.organizations[0]!.role = "MEMBER";
    viewerSession.user!.organizations[0]!.projects[0]!.role = "VIEWER";
    const findMany = vi.fn().mockResolvedValue([]);
    const caller = auditLogsRouter.createCaller({
      ...createInnerTRPCContext({ session: viewerSession, headers: {} }),
      prisma: {
        auditLog: { findMany, count: vi.fn().mockResolvedValue(0) },
        user: { findMany: vi.fn().mockResolvedValue([]) },
        apiKey: { findMany: vi.fn().mockResolvedValue([]) },
      } as never,
    });

    await expect(
      caller.all({ projectId, page: 0, limit: 20 }),
    ).rejects.toMatchObject({ code: "FORBIDDEN" });
    expect(findMany).not.toHaveBeenCalled();
  });
});
