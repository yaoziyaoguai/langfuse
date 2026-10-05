import type { Session } from "next-auth";
import { describe, expect, it, vi } from "vitest";
import type * as EnvModule from "@/src/env.mjs";

const mocks = vi.hoisted(() => ({
  auditLog: vi.fn(),
}));

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

vi.mock("@/src/features/audit-logs/auditLog", () => ({
  auditLog: mocks.auditLog,
}));

import { promptRouter } from "@/src/features/prompts/server/routers/promptRouter";
import { createInnerTRPCContext } from "@/src/server/api/trpc";

const projectId = "project-1";

function session(role: "OWNER" | "VIEWER"): Session {
  return {
    expires: "1",
    user: {
      id: "user-1",
      canCreateOrganizations: true,
      organizations: [
        {
          id: "org-1",
          name: "Test Org",
          role,
          plan: "oss",
          cloudConfig: undefined,
          metadata: {},
          aiFeaturesEnabled: false,
          aiTelemetryEnabled: false,
          projects: [
            {
              id: projectId,
              name: "Test Project",
              role,
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
}

describe("Community Extensions protected prompt labels", () => {
  it("allows an OSS project owner to read and add protected labels", async () => {
    const findMany = vi.fn(async () => [{ label: "production" }]);
    const upsert = vi.fn(async () => ({
      id: "protected-label-1",
      projectId,
      label: "staging",
    }));
    const caller = promptRouter.createCaller({
      ...createInnerTRPCContext({
        session: session("OWNER"),
        headers: {},
      }),
      prisma: {
        promptProtectedLabels: { findMany, upsert },
      } as never,
    });

    await expect(caller.getProtectedLabels({ projectId })).resolves.toEqual([
      "production",
    ]);
    await expect(
      caller.addProtectedLabel({ projectId, label: "staging" }),
    ).resolves.toMatchObject({ label: "staging" });
    expect(upsert).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          projectId_label: { projectId, label: "staging" },
        },
      }),
    );
  });

  it("keeps project authorization mandatory", async () => {
    const upsert = vi.fn();
    const caller = promptRouter.createCaller({
      ...createInnerTRPCContext({
        session: session("VIEWER"),
        headers: {},
      }),
      prisma: {
        promptProtectedLabels: { upsert },
      } as never,
    });

    await expect(
      caller.addProtectedLabel({ projectId, label: "staging" }),
    ).rejects.toMatchObject({ code: "FORBIDDEN" });
    expect(upsert).not.toHaveBeenCalled();
  });
});
