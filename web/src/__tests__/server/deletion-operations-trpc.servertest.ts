import { randomUUID } from "node:crypto";

import { prisma } from "@langfuse/shared/src/db";
import type { Session } from "next-auth";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { appRouter } from "@/src/server/api/root";
import { createInnerTRPCContext } from "@/src/server/api/trpc";

type SessionUser = NonNullable<Session["user"]>;
type SessionOrganizations = SessionUser["organizations"];
type SessionFeatureFlags = SessionUser["featureFlags"];

const suffix = randomUUID();
const organizationId = `deletion-status-org-${suffix}`;
const otherOrganizationId = `deletion-status-other-org-${suffix}`;
const projectId = `deletion-status-project-${suffix}`;
const operationId = `deletion-status-operation-${suffix}`;

function session(input: {
  organizationId: string;
  role: "OWNER" | "MEMBER";
}): Session {
  return {
    expires: "1",
    user: {
      id: `deletion-status-user-${input.role.toLowerCase()}`,
      name: "Deletion Status Test User",
      canCreateOrganizations: true,
      organizations: [
        {
          id: input.organizationId,
          name: "Deletion Status Test Organization",
          role: input.role,
          plan: "cloud:hobby",
          cloudConfig: undefined,
          metadata: {},
          aiFeaturesEnabled: false,
          aiTelemetryEnabled: false,
          projects: [],
        },
      ] as SessionOrganizations,
      featureFlags: {
        excludeClickhouseRead: false,
        templateFlag: true,
      } as SessionFeatureFlags,
      admin: false,
    },
    environment: {} as Session["environment"],
  };
}

function caller(input: Parameters<typeof session>[0]) {
  const ctx = createInnerTRPCContext({
    session: session(input),
    headers: {},
  });
  return appRouter.createCaller({ ...ctx, prisma });
}

describe("deletionOperations.projectStatus", () => {
  beforeAll(async () => {
    await prisma.organization.createMany({
      data: [
        { id: organizationId, name: "Deletion Status Test Organization" },
        {
          id: otherOrganizationId,
          name: "Other Deletion Status Test Organization",
        },
      ],
    });
    await prisma.project.create({
      data: {
        id: projectId,
        orgId: organizationId,
        name: "Deleted project",
      },
    });
    await prisma.analyticsDeletionOperation.create({
      data: {
        id: operationId,
        scope: "PROJECT",
        organizationId,
        projectId,
        generation: 1n,
        requesterPrincipalType: "user",
        requesterPrincipalId: "owner-1",
        status: "SCHEDULED",
        phase: "materialized_cleanup",
        logicallyInvisible: true,
        statusExpiresAt: new Date("2099-01-01T00:00:00.000Z"),
      },
    });
    await prisma.project.delete({ where: { id: projectId } });
  });

  afterAll(async () => {
    await prisma.analyticsDeletionOperation.deleteMany({
      where: { id: operationId },
    });
    await prisma.organization.deleteMany({
      where: { id: { in: [organizationId, otherOrganizationId] } },
    });
  });

  it("remains readable by an organization owner after the project row is gone", async () => {
    await expect(
      caller({
        organizationId,
        role: "OWNER",
      }).deletionOperations.projectStatus({
        orgId: organizationId,
        deletionOperationId: operationId,
      }),
    ).resolves.toMatchObject({
      deletionOperationId: operationId,
      projectId,
      status: "scheduled",
      phase: "materialized_cleanup",
      logicallyInvisible: true,
    });
  });

  it("hides operation existence from members without project-delete access", async () => {
    await expect(
      caller({
        organizationId,
        role: "MEMBER",
      }).deletionOperations.projectStatus({
        orgId: organizationId,
        deletionOperationId: operationId,
      }),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
  });

  it("hides operation existence from another organization", async () => {
    await expect(
      caller({
        organizationId: otherOrganizationId,
        role: "OWNER",
      }).deletionOperations.projectStatus({
        orgId: otherOrganizationId,
        deletionOperationId: operationId,
      }),
    ).rejects.toMatchObject({ code: "NOT_FOUND" });
  });
});
