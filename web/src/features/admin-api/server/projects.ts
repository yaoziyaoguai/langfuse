import { randomUUID } from "node:crypto";
import { type NextApiRequest, type NextApiResponse } from "next";

import { auditLog } from "@/src/features/audit-logs/auditLog";
import { projectNameSchema } from "@/src/features/auth/lib/projectNameSchema";
import { projectRetentionSchema } from "@/src/features/auth/lib/projectRetentionSchema";
import { hasPlanEntitlementOrCommunityCapability } from "@/src/features/community-extensions/server/access";
import { ApiAuthService } from "@/src/features/public-api/server/apiAuth";
import { getWebAnalyticsAdmissionContext } from "@/src/server/analyticsRuntime";
import { type Prisma, prisma } from "@langfuse/shared/src/db";
import {
  analyticsDurableProvenanceFromRecord,
  isDorisAnalyticsBackend,
  logger,
  ProjectDeleteQueue,
  QueueJobs,
  redis,
  scheduleProjectDeletionOperation,
  serializeAnalyticsDurableProvenance,
  type ApiAccessScope,
} from "@langfuse/shared/src/server";

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
  return { success: true, value: value as Prisma.InputJsonValue };
};

const validateRetention = (
  retention: unknown,
  scope: ApiAccessScope,
):
  | { success: true }
  | { success: false; status: 400 | 403; message: string } => {
  if (retention === undefined) return { success: true };

  const result = projectRetentionSchema.safeParse({ retention });
  if (!result.success) {
    return {
      success: false,
      status: 400,
      message: "Invalid retention value. Must be 0 or at least 3 days.",
    };
  }
  if (
    result.data.retention > 0 &&
    !hasPlanEntitlementOrCommunityCapability({
      plan: scope.plan,
      entitlement: "data-retention",
      capability: "data-retention",
    })
  ) {
    return {
      success: false,
      status: 403,
      message:
        "The data-retention entitlement is required to set a non-zero retention period.",
    };
  }
  return { success: true };
};

const projectResponse = (project: {
  id: string;
  name: string;
  metadata: unknown;
  retentionDays: number | null;
}) => ({
  id: project.id,
  name: project.name,
  metadata: project.metadata ?? {},
  ...(project.retentionDays ? { retentionDays: project.retentionDays } : {}),
});

const auditableProjectFields = {
  id: true,
  name: true,
  metadata: true,
  retentionDays: true,
} as const;

const acceptedProjectDeletion = {
  success: true,
  message:
    "Project deletion has been initiated and is being processed asynchronously",
} as const;
const projectDeletionFailure = { message: "Internal server error" } as const;

export async function handleGetProjects(
  _req: NextApiRequest,
  res: NextApiResponse,
  orgId: string,
) {
  const projects = await prisma.project.findMany({
    where: { orgId, deletedAt: null },
    select: {
      id: true,
      name: true,
      metadata: true,
      createdAt: true,
      updatedAt: true,
    },
  });
  return res.status(200).json({ projects });
}

export async function handleCreateProject(
  req: NextApiRequest,
  res: NextApiResponse,
  scope: ApiAccessScope,
) {
  const { name, retention, metadata } = req.body ?? {};
  if (!projectNameSchema.safeParse({ name }).success) {
    return res.status(400).json({
      message: "Invalid project name. Should be between 3 and 60 characters.",
    });
  }

  const metadataResult = parseMetadata(metadata);
  if (!metadataResult.success) {
    return res.status(400).json({ message: metadataResult.error });
  }
  const retentionResult = validateRetention(retention, scope);
  if (!retentionResult.success) {
    return res
      .status(retentionResult.status)
      .json({ message: retentionResult.message });
  }

  const duplicate = await prisma.project.findFirst({
    where: { name, orgId: scope.orgId, deletedAt: null },
    select: { id: true },
  });
  if (duplicate) {
    return res.status(409).json({
      message: "A project with this name already exists in your organization",
    });
  }

  const project = await prisma.$transaction(async (tx) => {
    const created = await tx.project.create({
      data: {
        name,
        orgId: scope.orgId,
        retentionDays: retention,
        metadata: metadataResult.value,
      },
      select: auditableProjectFields,
    });
    await auditLog(
      {
        apiKeyId: scope.apiKeyId,
        orgId: scope.orgId,
        projectId: created.id,
        resourceType: "project",
        resourceId: created.id,
        action: "create",
        after: created,
      },
      tx,
    );
    return created;
  });
  return res.status(201).json(projectResponse(project));
}

export async function handleUpdateProject(
  req: NextApiRequest,
  res: NextApiResponse,
  projectId: string,
  scope: ApiAccessScope,
) {
  const { name, retention, metadata } = req.body ?? {};
  if (!projectNameSchema.safeParse({ name }).success) {
    return res.status(400).json({
      message: "Invalid project name. Should be between 3 and 60 characters.",
    });
  }

  const metadataResult = parseMetadata(metadata);
  if (!metadataResult.success) {
    return res.status(400).json({ message: metadataResult.error });
  }
  const retentionResult = validateRetention(retention, scope);
  if (!retentionResult.success) {
    return res
      .status(retentionResult.status)
      .json({ message: retentionResult.message });
  }

  const project = await prisma.$transaction(async (tx) => {
    const before = await tx.project.findUniqueOrThrow({
      where: { id: projectId, orgId: scope.orgId },
      select: auditableProjectFields,
    });
    const updated = await tx.project.update({
      where: { id: projectId, orgId: scope.orgId },
      data: {
        name,
        ...(retention !== undefined ? { retentionDays: retention } : {}),
        ...(metadata !== undefined ? { metadata: metadataResult.value } : {}),
      },
      select: auditableProjectFields,
    });
    await auditLog(
      {
        apiKeyId: scope.apiKeyId,
        orgId: scope.orgId,
        projectId,
        resourceType: "project",
        resourceId: projectId,
        action: "update",
        before,
        after: updated,
      },
      tx,
    );
    return updated;
  });
  return res.status(200).json(projectResponse(project));
}

export async function handleDeleteProject(
  _req: NextApiRequest,
  res: NextApiResponse,
  projectId: string,
  scope: ApiAccessScope,
) {
  try {
    const queue = ProjectDeleteQueue.getInstance();
    if (!queue) {
      logger.error("ProjectDeleteQueue is not available");
      return res.status(500).json({ message: "Internal server error" });
    }

    const usesDoris = isDorisAnalyticsBackend();
    const admission = usesDoris ? getWebAnalyticsAdmissionContext() : null;
    if (usesDoris && !admission) {
      throw new Error(
        "Doris analytics deletion requires managed runtime admission",
      );
    }

    const deletionOperation = usesDoris
      ? await scheduleProjectDeletionOperation({
          projectId,
          organizationId: scope.orgId,
          requester: {
            principalType: "api_key",
            principalId: scope.apiKeyId,
          },
          analyticsAdmissionContext: admission,
        })
      : null;
    const provenance = deletionOperation
      ? analyticsDurableProvenanceFromRecord(deletionOperation)
      : null;

    await new ApiAuthService(prisma, redis).invalidateCachedProjectApiKeys(
      projectId,
    );
    await prisma.apiKey.deleteMany({
      where: { projectId, scope: "PROJECT" },
    });

    const project = deletionOperation
      ? await prisma.project.findUniqueOrThrow({
          where: { id: projectId, orgId: scope.orgId },
        })
      : await prisma.project.update({
          where: { id: projectId, orgId: scope.orgId },
          data: { deletedAt: new Date() },
        });
    await auditLog({
      apiKeyId: scope.apiKeyId,
      orgId: scope.orgId,
      projectId,
      resourceType: "project",
      resourceId: projectId,
      before: project,
      action: "delete",
    });

    await queue.add(QueueJobs.ProjectDelete, {
      timestamp: new Date(),
      id: randomUUID(),
      payload: {
        projectId,
        orgId: scope.orgId,
        ...(deletionOperation
          ? {
              deletionOperationId: deletionOperation.id,
              deletionGeneration: deletionOperation.generation.toString(),
              ...(provenance
                ? {
                    analyticsProvenance:
                      serializeAnalyticsDurableProvenance(provenance),
                  }
                : {}),
            }
          : {}),
      },
      name: QueueJobs.ProjectDelete,
    });
    return res.status(202).json(acceptedProjectDeletion);
  } catch (error) {
    logger.error("Failed to delete project", error);
    return res.status(500).json(projectDeletionFailure);
  }
}
