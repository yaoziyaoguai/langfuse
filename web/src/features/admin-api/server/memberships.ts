import { auditLog } from "@/src/features/audit-logs/auditLog";
import { getOptionalSfdcService } from "@/src/features/sfdc-sync/server/getOptionalSfdcService";
import { Role } from "@langfuse/shared";
import { Prisma, prisma } from "@langfuse/shared/src/db";
import { type NextApiRequest, type NextApiResponse } from "next";
import { z } from "zod";

const membershipSchema = z.object({
  userId: z.string(),
  role: z.enum(Role),
});
const deleteMembershipSchema = z.object({ userId: z.string() });

const sendLastOwnerResponse = (res: NextApiResponse) =>
  res.status(403).json({
    error:
      "Cannot remove the last owner of an organization. Assign new owner or delete organization.",
  });

const sendConcurrentUpdateResponse = (res: NextApiResponse) =>
  res.status(409).json({
    error: "Concurrent membership update conflict. Please retry.",
  });

const isSerializationConflict = (error: unknown) =>
  error instanceof Prisma.PrismaClientKnownRequestError &&
  error.code === "P2034";

export async function handleGetOrganizationMemberships(
  _req: NextApiRequest,
  res: NextApiResponse,
  orgId: string,
) {
  const memberships = await prisma.organizationMembership.findMany({
    where: { orgId },
    include: {
      user: { select: { id: true, email: true, name: true } },
    },
  });
  return res.status(200).json({
    memberships: memberships.map(({ userId, role, user }) => ({
      userId,
      role,
      email: user.email,
      name: user.name,
    })),
  });
}

export async function handleUpdateOrganizationMembership(
  req: NextApiRequest,
  res: NextApiResponse,
  orgId: string,
  apiKeyId = "ORG_KEY",
) {
  const bodyResult = membershipSchema.safeParse(req.body);
  if (!bodyResult.success) {
    return res.status(400).json({
      error: "Invalid request body",
      details: bodyResult.error.issues,
    });
  }

  const user = await prisma.user.findUnique({
    where: { id: bodyResult.data.userId },
  });
  if (!user) return res.status(404).json({ error: "User not found" });

  try {
    const result = await prisma.$transaction(
      async (tx) => {
        const before = await tx.organizationMembership.findUnique({
          where: { orgId_userId: { orgId, userId: user.id } },
        });
        if (
          before?.role === Role.OWNER &&
          bodyResult.data.role !== Role.OWNER
        ) {
          const ownerCount = await tx.organizationMembership.count({
            where: { orgId, role: Role.OWNER },
          });
          if (ownerCount <= 1) return { kind: "last-owner" as const };
        }

        const membership = await tx.organizationMembership.upsert({
          where: { orgId_userId: { orgId, userId: user.id } },
          update: { role: bodyResult.data.role },
          create: { orgId, userId: user.id, role: bodyResult.data.role },
        });
        await auditLog(
          {
            resourceType: "orgMembership",
            resourceId: membership.id,
            action: before ? "update" : "create",
            before: before ?? undefined,
            after: membership,
            apiKeyId,
            orgId,
          },
          tx,
        );
        return { kind: "updated" as const, before, membership };
      },
      { isolationLevel: Prisma.TransactionIsolationLevel.Serializable },
    );

    if (result.kind === "last-owner") return sendLastOwnerResponse(res);

    await (
      await getOptionalSfdcService()
    )?.setUserRole({
      orgId,
      userId: result.membership.userId,
      email: user.email,
      role: result.membership.role,
    });
    return res.status(200).json({
      userId: result.membership.userId,
      role: result.membership.role,
      email: user.email,
      name: user.name,
    });
  } catch (error) {
    if (isSerializationConflict(error)) {
      return sendConcurrentUpdateResponse(res);
    }
    throw error;
  }
}

export async function handleDeleteOrganizationMembership(
  req: NextApiRequest,
  res: NextApiResponse,
  orgId: string,
  apiKeyId = "ORG_KEY",
) {
  const bodyResult = deleteMembershipSchema.safeParse(req.body);
  if (!bodyResult.success) {
    return res.status(400).json({
      error: "Invalid request body",
      details: bodyResult.error.issues,
    });
  }

  try {
    const result = await prisma.$transaction(
      async (tx) => {
        const before = await tx.organizationMembership.findUnique({
          where: {
            orgId_userId: { orgId, userId: bodyResult.data.userId },
          },
        });
        if (!before) return { kind: "missing" as const };

        if (before.role === Role.OWNER) {
          const ownerCount = await tx.organizationMembership.count({
            where: { orgId, role: Role.OWNER },
          });
          if (ownerCount <= 1) return { kind: "last-owner" as const };
        }
        await tx.organizationMembership.delete({
          where: {
            orgId_userId: { orgId, userId: bodyResult.data.userId },
          },
        });
        await auditLog(
          {
            resourceType: "orgMembership",
            resourceId: before.id,
            action: "delete",
            before,
            apiKeyId,
            orgId,
          },
          tx,
        );
        return { kind: "deleted" as const, before };
      },
      { isolationLevel: Prisma.TransactionIsolationLevel.Serializable },
    );

    if (result.kind === "last-owner") return sendLastOwnerResponse(res);

    if (result.kind === "deleted") {
      const user = await prisma.user.findUnique({
        where: { id: bodyResult.data.userId },
        select: { email: true },
      });
      await (
        await getOptionalSfdcService()
      )?.removeUser({
        orgId,
        userId: bodyResult.data.userId,
        email: user?.email,
      });
    }
    return res.status(200).json({
      message: "Membership deleted successfully",
      userId: bodyResult.data.userId,
    });
  } catch (error) {
    if (isSerializationConflict(error)) {
      return sendConcurrentUpdateResponse(res);
    }
    throw error;
  }
}

export async function handleGetProjectMemberships(
  _req: NextApiRequest,
  res: NextApiResponse,
  projectId: string,
  orgId: string,
) {
  const memberships = await prisma.projectMembership.findMany({
    where: { projectId, organizationMembership: { orgId } },
    include: {
      user: { select: { id: true, email: true, name: true } },
    },
  });
  return res.status(200).json({
    memberships: memberships.map(({ userId, role, user }) => ({
      userId,
      role,
      email: user.email,
      name: user.name,
    })),
  });
}

export async function handleUpdateProjectMembership(
  req: NextApiRequest,
  res: NextApiResponse,
  projectId: string,
  orgId: string,
  apiKeyId = "ORG_KEY",
) {
  const bodyResult = membershipSchema.safeParse(req.body);
  if (!bodyResult.success) {
    return res.status(400).json({
      error: "Invalid request body",
      details: bodyResult.error.issues,
    });
  }

  const result = await prisma.$transaction(async (tx) => {
    const orgMembership = await tx.organizationMembership.findUnique({
      where: {
        orgId_userId: { orgId, userId: bodyResult.data.userId },
      },
      include: { user: { select: { email: true, name: true } } },
    });
    if (!orgMembership) return null;

    const before = await tx.projectMembership.findUnique({
      where: {
        projectId_userId: { projectId, userId: bodyResult.data.userId },
      },
    });
    const membership = await tx.projectMembership.upsert({
      where: {
        projectId_userId: { projectId, userId: bodyResult.data.userId },
      },
      update: { role: bodyResult.data.role },
      create: {
        projectId,
        userId: bodyResult.data.userId,
        role: bodyResult.data.role,
        orgMembershipId: orgMembership.id,
      },
    });
    await auditLog(
      {
        resourceType: "projectMembership",
        resourceId: `${projectId}--${bodyResult.data.userId}`,
        action: before ? "update" : "create",
        before: before ?? undefined,
        after: membership,
        apiKeyId,
        orgId,
        projectId,
      },
      tx,
    );
    return { membership, orgMembership };
  });
  if (!result) {
    return res
      .status(404)
      .json({ error: "User is not a member of this organization" });
  }
  return res.status(200).json({
    userId: result.membership.userId,
    role: result.membership.role,
    email: result.orgMembership.user.email,
    name: result.orgMembership.user.name,
  });
}

export async function handleDeleteProjectMembership(
  req: NextApiRequest,
  res: NextApiResponse,
  projectId: string,
  orgId: string,
  apiKeyId = "ORG_KEY",
) {
  const bodyResult = deleteMembershipSchema.safeParse(req.body);
  if (!bodyResult.success) {
    return res.status(400).json({
      error: "Invalid request body",
      details: bodyResult.error.issues,
    });
  }

  const result = await prisma.$transaction(async (tx) => {
    const membership = await tx.projectMembership.findUnique({
      where: {
        projectId_userId: { projectId, userId: bodyResult.data.userId },
      },
      include: {
        organizationMembership: { select: { orgId: true } },
      },
    });
    if (!membership) return { kind: "missing" as const };
    if (membership.organizationMembership.orgId !== orgId) {
      return { kind: "wrong-org" as const };
    }

    await tx.projectMembership.delete({
      where: {
        projectId_userId: { projectId, userId: bodyResult.data.userId },
      },
    });
    await auditLog(
      {
        resourceType: "projectMembership",
        resourceId: `${projectId}--${bodyResult.data.userId}`,
        action: "delete",
        before: membership,
        apiKeyId,
        orgId,
        projectId,
      },
      tx,
    );
    return { kind: "deleted" as const };
  });
  if (result.kind === "missing") {
    return res.status(404).json({ error: "Project membership not found" });
  }
  if (result.kind === "wrong-org") {
    return res.status(403).json({
      error: "Project membership does not belong to this organization",
    });
  }
  return res.status(200).json({
    message: "Project membership deleted successfully",
    userId: bodyResult.data.userId,
  });
}
