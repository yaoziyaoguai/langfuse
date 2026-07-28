import {
  prisma as _prisma,
  type Prisma,
  type Role,
  AuditLogRecordType,
} from "@langfuse/shared/src/db";

export type AuditableResource =
  | "annotationQueue"
  | "annotationQueueItem"
  | "annotationQueueAssignment"
  | "organization"
  | "orgMembership"
  | "projectMembership"
  | "membershipInvitation"
  | "comment"
  | "datasetItem"
  | "dataset"
  | "datasetRun"
  | "datasetRunItem"
  | "trace"
  | "project"
  | "observation"
  | "score"
  | "scoreConfig"
  | "model"
  | "notificationPreference"
  | "prompt"
  | "promptProtectedLabel"
  | "session"
  | "apiKey"
  | "evalTemplate"
  | "job"
  | "blobStorageIntegration"
  | "posthogIntegration"
  | "mixpanelIntegration"
  | "webCalloutEndpoint"
  | "llmApiKey"
  | "llmTool"
  | "llmSchema"
  | "batchExport"
  | "stripeCheckoutSession"
  | "batchAction"
  | "automation"
  | "action"
  | "dashboardWidget"
  | "dashboard"
  | "slackIntegration"
  | "cloudSpendAlert"
  | "verifiedDomain"
  | "ssoConfig"
  // legacy resources
  | "membership";

type AuditLog = {
  resourceType: AuditableResource;
  resourceId: string;
  action: string;
  before?: unknown;
  after?: unknown;
} & (
  | {
      userId: string;
      orgId: string;
      orgRole?: Role;
      projectId?: string;
      projectRole?: Role;
    }
  | {
      session: {
        user: {
          id: string;
        };
        orgId: string;
        orgRole?: Role;
        projectId?: string;
        projectRole?: Role;
      };
    }
  | {
      apiKeyId: string;
      orgId: string;
      projectId?: string;
    }
);

type AuditLogTransaction = Pick<
  Prisma.TransactionClient,
  "apiKey" | "auditLog"
>;

function isPrismaClient(
  db: typeof _prisma | Prisma.TransactionClient,
): db is typeof _prisma {
  return (
    "$transaction" in db &&
    typeof (db as { $transaction?: unknown }).$transaction === "function"
  );
}

function serializeAuditValue(value: unknown): string {
  return JSON.stringify(value, (_key, item: unknown) =>
    typeof item === "bigint" ? item.toString() : item,
  );
}

export async function auditLog(
  log: AuditLog,
  prisma?: typeof _prisma | Prisma.TransactionClient,
) {
  const db = prisma ?? _prisma;
  const shared = {
    resourceType: log.resourceType,
    resourceId: log.resourceId,
    action: log.action,
    before: log.before ? serializeAuditValue(log.before) : undefined,
    after: log.after ? serializeAuditValue(log.after) : undefined,
  };

  if ("apiKeyId" in log) {
    const createApiKeyAudit = async (tx: AuditLogTransaction) => {
      const apiKey = await tx.apiKey.findUnique({
        where: { id: log.apiKeyId },
        select: {
          isInAppAgentKey: true,
          createdByUserId: true,
        },
      });

      await tx.auditLog.create({
        data: {
          apiKeyId: log.apiKeyId,
          userId:
            apiKey?.isInAppAgentKey === true
              ? (apiKey.createdByUserId ?? undefined)
              : undefined,
          orgId: log.orgId,
          projectId: log.projectId,
          type: AuditLogRecordType.API_KEY,
          ...shared,
        },
      });
    };

    if (isPrismaClient(db)) {
      await db.$transaction(createApiKeyAudit);
    } else {
      await createApiKeyAudit(db);
    }

    return;
  }

  if ("session" in log) {
    await db.auditLog.create({
      data: {
        userId: log.session.user.id,
        orgId: log.session.orgId,
        userOrgRole: log.session.orgRole,
        projectId: log.session.projectId,
        userProjectRole: log.session.projectRole,
        type: AuditLogRecordType.USER,
        ...shared,
      },
    });

    return;
  }

  if ("userId" in log) {
    await db.auditLog.create({
      data: {
        userId: log.userId,
        orgId: log.orgId,
        userOrgRole: log.orgRole,
        projectId: log.projectId,
        userProjectRole: log.projectRole,
        type: AuditLogRecordType.USER,
        ...shared,
      },
    });

    return;
  }
}
