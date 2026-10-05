import { type NextApiRequest, type NextApiResponse } from "next";
import { z } from "zod";

import { auditLog } from "@/src/features/audit-logs/auditLog";
import { ApiAuthService } from "@/src/features/public-api/server/apiAuth";
import { prisma } from "@langfuse/shared/src/db";
import {
  createAndAddApiKeysToDb,
  logger,
  redis,
} from "@langfuse/shared/src/server";

const publicApiKeyFields = {
  id: true,
  createdAt: true,
  expiresAt: true,
  lastUsedAt: true,
  note: true,
  publicKey: true,
  displaySecretKey: true,
} as const;

export const validateProjectId = (query: unknown): string | null => {
  const result = z.object({ projectId: z.string().min(1) }).safeParse(query);
  return result.success ? result.data.projectId : null;
};

export const validateProjectApiKeyParams = (
  query: unknown,
): { projectId: string; apiKeyId: string } | null => {
  const result = z
    .object({ projectId: z.string().min(1), apiKeyId: z.string().min(1) })
    .safeParse(query);
  return result.success ? result.data : null;
};

export const validateOrganizationApiKeyParams = (
  query: unknown,
): { organizationId: string; apiKeyId: string } | null => {
  const result = z
    .object({ organizationId: z.string().min(1), apiKeyId: z.string().min(1) })
    .safeParse(query);
  return result.success ? result.data : null;
};

export async function handleGetProjectApiKeys(
  _req: NextApiRequest,
  res: NextApiResponse,
  projectId: string,
) {
  const apiKeys = await prisma.apiKey.findMany({
    where: { projectId, scope: "PROJECT", isInAppAgentKey: false },
    select: publicApiKeyFields,
    orderBy: { createdAt: "asc" },
  });
  return res.status(200).json({ apiKeys });
}

export async function handleCreateProjectApiKey(
  req: NextApiRequest,
  res: NextApiResponse,
  projectId: string,
  orgId: string,
  createdByApiKeyId?: string,
) {
  const bodyResult = z
    .object({
      note: z.string().optional(),
      publicKey: z.string().optional(),
      secretKey: z.string().optional(),
    })
    .safeParse(req.body);
  if (!bodyResult.success) {
    return res.status(400).json({
      message: "Invalid request body",
      details: z.formatError(bodyResult.error),
    });
  }

  const { note, publicKey, secretKey } = bodyResult.data;
  if ((publicKey && !secretKey) || (secretKey && !publicKey)) {
    return res.status(400).json({
      message:
        "Both publicKey and secretKey must be provided together when specifying predefined keys",
    });
  }
  if (publicKey && !publicKey.startsWith("pk-lf-")) {
    return res.status(400).json({
      message: "publicKey must start with 'pk-lf-'",
    });
  }
  if (secretKey && !secretKey.startsWith("sk-lf-")) {
    return res.status(400).json({
      message: "secretKey must start with 'sk-lf-'",
    });
  }

  try {
    const key = await prisma.$transaction(async (tx) => {
      const created = await createAndAddApiKeysToDb({
        prisma: tx,
        entityId: projectId,
        note,
        scope: "PROJECT",
        createdByApiKeyId,
        predefinedKeys:
          publicKey && secretKey ? { publicKey, secretKey } : undefined,
      });
      await auditLog(
        {
          resourceType: "apiKey",
          resourceId: created.id,
          action: "create",
          orgId,
          projectId,
          orgRole: "ADMIN",
          apiKeyId: createdByApiKeyId ?? "ORG_KEY",
        },
        tx,
      );
      return created;
    });
    logger.info(
      `Created API key ${key.id} for project ${projectId} via public API`,
    );
    return res.status(201).json(key);
  } catch (error) {
    if (
      error instanceof Error &&
      error.message.toLowerCase().includes("unique constraint")
    ) {
      return res.status(409).json({
        message:
          "API key with the provided publicKey or secretKey already exists",
      });
    }
    throw error;
  }
}

export async function handleDeleteProjectApiKey(
  _req: NextApiRequest,
  res: NextApiResponse,
  projectId: string,
  apiKeyId: string,
  orgId: string,
  deletedByApiKeyId = "ORG_KEY",
) {
  const key = await prisma.apiKey.findFirst({
    where: {
      id: apiKeyId,
      projectId,
      scope: "PROJECT",
      isInAppAgentKey: false,
    },
  });
  if (!key) return res.status(404).json({ message: "API key not found" });

  const authService = new ApiAuthService(prisma, redis);
  await authService.invalidateCachedApiKeys([key], `key ${apiKeyId}`);
  await prisma.$transaction(async (tx) => {
    await tx.apiKey.delete({
      where: {
        id: apiKeyId,
        projectId,
        scope: "PROJECT",
        isInAppAgentKey: false,
      },
    });
    await auditLog(
      {
        resourceType: "apiKey",
        resourceId: apiKeyId,
        action: "delete",
        orgId,
        projectId,
        orgRole: "ADMIN",
        apiKeyId: deletedByApiKeyId,
      },
      tx,
    );
  });
  // Close the small window in which a concurrent request could repopulate the
  // cache after the pre-delete invalidation but before the transaction commits.
  await authService.invalidateCachedApiKeys([key], `key ${apiKeyId}`);
  logger.info(
    `Deleted API key ${apiKeyId} for project ${projectId} via public API`,
  );
  return res.status(200).json({ success: true });
}

export async function handleGetOrganizationApiKeys(
  _req: NextApiRequest,
  res: NextApiResponse,
  organizationId: string,
) {
  const apiKeys = await prisma.apiKey.findMany({
    where: {
      orgId: organizationId,
      scope: "ORGANIZATION",
      isInAppAgentKey: false,
    },
    select: publicApiKeyFields,
    orderBy: { createdAt: "asc" },
  });
  return res.status(200).json({ apiKeys });
}

export async function handleCreateOrganizationApiKey(
  req: NextApiRequest,
  res: NextApiResponse,
  organizationId: string,
) {
  const bodyResult = z
    .object({ note: z.string().optional() })
    .safeParse(req.body);
  if (!bodyResult.success) {
    return res.status(400).json({
      error: "Invalid request body",
      details: z.formatError(bodyResult.error),
    });
  }

  const key = await prisma.$transaction(async (tx) => {
    const created = await createAndAddApiKeysToDb({
      prisma: tx,
      entityId: organizationId,
      note: bodyResult.data.note,
      scope: "ORGANIZATION",
    });
    await auditLog(
      {
        resourceType: "apiKey",
        resourceId: created.id,
        action: "create",
        orgId: organizationId,
        orgRole: "ADMIN",
        apiKeyId: "ADMIN_KEY",
      },
      tx,
    );
    return created;
  });
  logger.info(
    `Created API key ${key.id} for organization ${organizationId} via admin API`,
  );
  return res.status(201).json(key);
}

export async function handleDeleteOrganizationApiKey(
  _req: NextApiRequest,
  res: NextApiResponse,
  organizationId: string,
  apiKeyId: string,
) {
  const key = await prisma.apiKey.findFirst({
    where: {
      id: apiKeyId,
      orgId: organizationId,
      scope: "ORGANIZATION",
      isInAppAgentKey: false,
    },
  });
  if (!key) return res.status(404).json({ error: "API key not found" });

  const authService = new ApiAuthService(prisma, redis);
  await authService.invalidateCachedApiKeys([key], `key ${apiKeyId}`);
  await prisma.$transaction(async (tx) => {
    await tx.apiKey.delete({
      where: {
        id: apiKeyId,
        orgId: organizationId,
        scope: "ORGANIZATION",
        isInAppAgentKey: false,
      },
    });
    await auditLog(
      {
        resourceType: "apiKey",
        resourceId: apiKeyId,
        action: "delete",
        orgId: organizationId,
        orgRole: "ADMIN",
        apiKeyId: "ADMIN_KEY",
      },
      tx,
    );
  });
  await authService.invalidateCachedApiKeys([key], `key ${apiKeyId}`);
  logger.info(
    `Deleted API key ${apiKeyId} for organization ${organizationId} via admin API`,
  );
  return res.status(200).json({ success: true });
}
