import { type NextApiRequest, type NextApiResponse } from "next";
import { z } from "zod";

import { auditLog } from "@/src/features/audit-logs/auditLog";
import { organizationNameSchema } from "@/src/features/organizations/utils/organizationNameSchema";
import { type Prisma, prisma } from "@langfuse/shared/src/db";
import { logger } from "@langfuse/shared/src/server";

const organizationIdSchema = z.object({ organizationId: z.string().min(1) });

const selectOrganization = {
  id: true,
  name: true,
  createdAt: true,
  metadata: true,
  projects: {
    select: {
      id: true,
      name: true,
      metadata: true,
      createdAt: true,
      updatedAt: true,
    },
    where: { deletedAt: null },
  },
} as const;

const normalizeOrganization = <T extends { metadata: unknown }>(
  organization: T,
) => ({
  ...organization,
  metadata: organization.metadata ?? {},
});

const parseMetadata = (
  metadata: unknown,
):
  | { success: true; value: Prisma.InputJsonValue | undefined }
  | { success: false; error: string } => {
  if (metadata === undefined) return { success: true, value: undefined };

  let value = metadata;
  if (typeof value === "string") {
    try {
      value = JSON.parse(value);
    } catch (error) {
      return {
        success: false,
        error: `Invalid metadata. Should be a valid JSON object: ${error}`,
      };
    }
  }

  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return {
      success: false,
      error: "Invalid metadata. Should be a valid JSON object.",
    };
  }
  return {
    success: true,
    value: value as Prisma.InputJsonValue,
  };
};

export const validateOrganizationId = (query: unknown): string | null => {
  const result = organizationIdSchema.safeParse(query);
  return result.success ? result.data.organizationId : null;
};

export async function handleGetOrganizations(
  _req: NextApiRequest,
  res: NextApiResponse,
) {
  const organizations = await prisma.organization.findMany({
    select: selectOrganization,
  });
  return res.status(200).json({
    organizations: organizations.map(normalizeOrganization),
  });
}

export async function handleCreateOrganization(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const nameResult = organizationNameSchema.safeParse(req.body);
  if (!nameResult.success) {
    return res.status(400).json({
      error: "Invalid request body",
      details: z.formatError(nameResult.error),
    });
  }

  const metadataResult = parseMetadata(req.body?.metadata);
  if (!metadataResult.success) {
    return res.status(400).json({ message: metadataResult.error });
  }

  const organization = await prisma.$transaction(async (tx) => {
    const created = await tx.organization.create({
      data: {
        name: nameResult.data.name,
        metadata: metadataResult.value,
      },
      select: selectOrganization,
    });
    await auditLog(
      {
        resourceType: "organization",
        resourceId: created.id,
        action: "create",
        orgId: created.id,
        orgRole: "ADMIN",
        after: created,
        apiKeyId: "ADMIN_KEY",
      },
      tx,
    );
    return created;
  });
  logger.info(`Created organization ${organization.id} via admin API`);

  return res.status(201).json(normalizeOrganization(organization));
}

export async function handleGetOrganizationById(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const organizationId = validateOrganizationId(req.query);
  if (!organizationId) {
    return res.status(400).json({ error: "Invalid organization ID" });
  }

  const organization = await prisma.organization.findUnique({
    where: { id: organizationId },
    select: selectOrganization,
  });
  if (!organization) {
    return res.status(404).json({ error: "Organization not found" });
  }
  return res.status(200).json(normalizeOrganization(organization));
}

export async function handleUpdateOrganization(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const organizationId = validateOrganizationId(req.query);
  if (!organizationId) {
    return res.status(400).json({ error: "Invalid organization ID" });
  }

  const nameResult = organizationNameSchema.safeParse(req.body);
  if (!nameResult.success) {
    return res.status(400).json({
      error: "Invalid request body",
      details: z.formatError(nameResult.error),
    });
  }
  const metadataResult = parseMetadata(req.body?.metadata);
  if (!metadataResult.success) {
    return res.status(400).json({ message: metadataResult.error });
  }

  const organization = await prisma.$transaction(async (tx) => {
    const before = await tx.organization.findUnique({
      where: { id: organizationId },
      select: selectOrganization,
    });
    if (!before) return null;

    const updated = await tx.organization.update({
      where: { id: organizationId },
      data: {
        name: nameResult.data.name,
        metadata: metadataResult.value,
      },
      select: selectOrganization,
    });
    await auditLog(
      {
        resourceType: "organization",
        resourceId: organizationId,
        action: "update",
        orgId: organizationId,
        orgRole: "ADMIN",
        before,
        after: updated,
        apiKeyId: "ADMIN_KEY",
      },
      tx,
    );
    return updated;
  });
  if (!organization) {
    return res.status(404).json({ error: "Organization not found" });
  }
  logger.info(`Updated organization ${organizationId} via admin API`);

  return res.status(200).json(normalizeOrganization(organization));
}

export async function handleDeleteOrganization(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  const organizationId = validateOrganizationId(req.query);
  if (!organizationId) {
    return res.status(400).json({ error: "Invalid organization ID" });
  }

  const result = await prisma.$transaction(async (tx) => {
    const organization = await tx.organization.findUnique({
      where: { id: organizationId },
      select: selectOrganization,
    });
    if (!organization) return { kind: "missing" as const };

    const [activeProjectCount, totalProjectCount] = await Promise.all([
      tx.project.count({
        where: { orgId: organizationId, deletedAt: null },
      }),
      tx.project.count({ where: { orgId: organizationId } }),
    ]);
    if (activeProjectCount > 0) {
      return { kind: "active-projects" as const };
    }
    if (totalProjectCount > 0) {
      return { kind: "deleting-projects" as const };
    }

    await tx.organization.delete({ where: { id: organizationId } });
    await auditLog(
      {
        resourceType: "organization",
        resourceId: organizationId,
        action: "delete",
        orgId: organizationId,
        orgRole: "ADMIN",
        before: organization,
        apiKeyId: "ADMIN_KEY",
      },
      tx,
    );
    return { kind: "deleted" as const };
  });
  if (result.kind === "missing") {
    return res.status(404).json({ error: "Organization not found" });
  }
  if (result.kind === "active-projects") {
    return res.status(400).json({
      error: "Cannot delete organization with existing projects",
      message:
        "Please delete or transfer all projects before deleting the organization.",
    });
  }
  if (result.kind === "deleting-projects") {
    return res.status(400).json({
      error: "Cannot delete organization with existing projects",
      message:
        "Deletion of your projects is still being processed, please try deleting the organization later",
    });
  }

  logger.info(`Deleted organization ${organizationId} via admin API`);

  return res.status(200).json({ success: true });
}
