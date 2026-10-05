import { ApiAuthService } from "@/src/features/public-api/server/apiAuth";
import { cors, runMiddleware } from "@/src/features/public-api/server/cors";
import { auditLog } from "@/src/features/audit-logs/auditLog";
import { Prisma, prisma, type User, type Role } from "@langfuse/shared/src/db";
import { logger, redis } from "@langfuse/shared/src/server";
import { z } from "zod";
import { type NextApiRequest, type NextApiResponse } from "next";
import { getOptionalSfdcService } from "@/src/features/sfdc-sync/server/getOptionalSfdcService";
import { hasPlanEntitlementOrCommunityCapability } from "@/src/features/community-extensions/server/access";

// Parse the first role from a SCIM `roles` array. An absent or empty attribute
// means "no explicit role requested"; malformed or unsupported values are
// rejected before provisioning mutates membership state.
const scimRoleSchema = z.enum(["OWNER", "ADMIN", "MEMBER", "VIEWER", "NONE"]);

function parseScimRole(
  roles: unknown,
):
  | { success: true; role: Role | undefined }
  | { success: false; error: string } {
  if (!Array.isArray(roles) || roles.length === 0) {
    return roles === undefined || (Array.isArray(roles) && roles.length === 0)
      ? { success: true, role: undefined }
      : { success: false, error: "roles must be an array" };
  }
  const parsed = z.array(scimRoleSchema).safeParse(roles);
  return parsed.success
    ? { success: true, role: parsed.data[0] }
    : {
        success: false,
        error: "roles must contain only supported organization roles",
      };
}

type ProvisionOutcome =
  | { kind: "created"; role: Role }
  | { kind: "updated"; role: Role }
  | { kind: "unchanged"; role: Role };

// Provision a user into an organization in response to a SCIM `active: true`.
//
// Behaviour:
// - role provided  → set the membership to that role (create if missing,
//   update if it differs). An explicit role is always honoured.
// - role omitted    → create the membership with the default NONE role when it
//   is missing, but leave an existing membership untouched. This is what keeps
//   a periodic IdP full-sync (which omits roles) from resetting a member's role
//   back to NONE.
//
// Writes an audit log entry and fires the SFDC membership sync only when state
// actually changes (create/update), never for a no-op — so a periodic IdP
// full-sync that re-PUTs every member does not ping SFDC on every cycle.
// Returns the provisioning outcome, or null when the request was rejected and
// the response has already been written (e.g. last-OWNER demotion → 403).
async function provisionMembership({
  res,
  userId,
  orgId,
  apiKeyId,
  email,
  role,
}: {
  res: NextApiResponse;
  userId: string;
  orgId: string;
  apiKeyId: string;
  email: string | null;
  role?: Role;
}): Promise<ProvisionOutcome | null> {
  // Apply an explicit role to an existing membership. The membership is
  // re-read INSIDE the Serializable transaction so the no-op check, the
  // last-OWNER guard, and the update all act on the same snapshot — a read
  // taken outside the transaction could race a concurrent promotion or
  // deprovision past the guard (Postgres SSI only protects reads made within
  // the transaction). Returns "missing" when the membership disappeared before
  // the transaction started, so the caller can fall through to the create
  // path; returns null when the request was rejected and the response written.
  const applyRoleToExisting = async (
    targetRole: Role,
  ): Promise<ProvisionOutcome | "missing" | null> => {
    try {
      const result = await prisma.$transaction(
        async (tx) => {
          const current = await tx.organizationMembership.findUnique({
            where: { orgId_userId: { orgId, userId } },
          });
          if (!current) {
            return { t: "missing" as const };
          }
          if (current.role === targetRole) {
            return { t: "unchanged" as const, role: current.role };
          }
          // Enforce the org-wide "at least one OWNER" invariant, mirroring
          // deprovisionOrReject and the tRPC updateOrgMembership path, so
          // demoting the last OWNER cannot orphan the org and two concurrent
          // demotions cannot both pass the check.
          if (current.role === "OWNER" && targetRole !== "OWNER") {
            const ownerCount = await tx.organizationMembership.count({
              where: { orgId, role: "OWNER" },
            });
            if (ownerCount <= 1) {
              return { t: "lastOwner" as const };
            }
          }
          const updated = await tx.organizationMembership.update({
            where: { orgId_userId: { orgId, userId } },
            data: { role: targetRole },
          });
          await auditLog(
            {
              resourceType: "orgMembership",
              resourceId: updated.id,
              action: "update",
              before: current,
              after: updated,
              apiKeyId,
              orgId,
            },
            tx,
          );
          return { t: "updated" as const, before: current, after: updated };
        },
        { isolationLevel: Prisma.TransactionIsolationLevel.Serializable },
      );

      if (result.t === "missing") {
        return "missing";
      }
      if (result.t === "unchanged") {
        return { kind: "unchanged", role: result.role };
      }
      if (result.t === "lastOwner") {
        logger.warn(
          `[SCIM] Refused to demote last OWNER ${userId} in org ${orgId}`,
        );
        res.status(403).json({
          schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
          detail:
            "Cannot remove the last owner of an organization. Assign new owner or delete organization.",
          status: 403,
        });
        return null;
      }

      await (
        await getOptionalSfdcService()
      )?.setUserRole({
        orgId,
        userId,
        email,
        role: targetRole,
      });
      return { kind: "updated", role: targetRole };
    } catch (error) {
      // Serialization failure (P2034) from a concurrent membership change;
      // surface as 409 so the SCIM client retries.
      if (
        error instanceof Prisma.PrismaClientKnownRequestError &&
        error.code === "P2034"
      ) {
        logger.warn(
          `[SCIM] Concurrent role update conflict for user ${userId} in org ${orgId}`,
        );
        res.status(409).json({
          schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
          detail: "Concurrent update conflict for this user. Please retry.",
          status: 409,
        });
        return null;
      }
      throw error;
    }
  };

  const existing = await prisma.organizationMembership.findUnique({
    where: { orgId_userId: { orgId, userId } },
  });
  if (existing) {
    // No explicit role requested → leave the membership untouched.
    if (role === undefined) {
      return { kind: "unchanged", role: existing.role };
    }
    const applied = await applyRoleToExisting(role);
    if (applied !== "missing") {
      return applied;
    }
    // Membership vanished between the read and the transaction (concurrent
    // deprovision) — fall through to the create path below.
  }

  // Missing → create with the explicit role when provided, otherwise NONE.
  // Creating a membership never removes an existing OWNER, so no guard needed.
  const roleToCreate: Role = role ?? "NONE";
  try {
    await prisma.$transaction(async (tx) => {
      const membership = await tx.organizationMembership.create({
        data: { userId, orgId, role: roleToCreate },
      });
      await auditLog(
        {
          resourceType: "orgMembership",
          resourceId: membership.id,
          action: "create",
          after: membership,
          apiKeyId,
          orgId,
        },
        tx,
      );
      return membership;
    });
    await (
      await getOptionalSfdcService()
    )?.setUserRole({
      orgId,
      userId,
      email,
      role: roleToCreate,
    });
    return { kind: "created", role: roleToCreate };
  } catch (error) {
    // A concurrent provisioning request created the row first. Apply the
    // requested role onto it (if any), keeping the periodic sync idempotent.
    if (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === "P2002"
    ) {
      if (role === undefined) {
        return { kind: "unchanged", role: roleToCreate };
      }
      const applied = await applyRoleToExisting(role);
      // "missing" here means the row vanished again right after the conflict;
      // don't loop — report the request as applied with no changes, like the
      // pre-existing fallback did. The next sync converges.
      return applied === "missing"
        ? { kind: "unchanged", role: roleToCreate }
        : applied;
    }
    throw error;
  }
}

// Single place for the SCIM provisioning log line so PUT and PATCH stay
// consistent.
function logScimProvision(
  outcome: ProvisionOutcome,
  userId: string,
  orgId: string,
  via: "PUT" | "PATCH",
) {
  switch (outcome.kind) {
    case "created":
      logger.info(
        `[SCIM] Provisioned user ${userId} in org ${orgId} with role ${outcome.role} via ${via}`,
      );
      break;
    case "updated":
      logger.info(
        `[SCIM] Updated role for user ${userId} in org ${orgId} to ${outcome.role} via ${via}`,
      );
      break;
    case "unchanged":
      logger.info(
        `[SCIM] User ${userId} already a member of org ${orgId}; no changes via ${via}`,
      );
      break;
  }
}

// Mirrors the tRPC `deleteMembership` invariant. Wraps the owner-count check
// and the membership delete in a single Serializable transaction so two
// concurrent SCIM deprovision requests cannot both pass the guard and orphan
// the org.
// Audits and syncs the removal to SFDC only when a membership was
// actually deleted, never for a no-op (already absent). Returns false (with
// the response already written) when the caller must stop; returns true after
// the membership has been removed.
async function deprovisionOrReject(
  res: NextApiResponse,
  userId: string,
  orgId: string,
  apiKeyId: string,
  email: string | null,
): Promise<boolean> {
  try {
    const outcome = await prisma.$transaction(
      async (tx) => {
        const membership = await tx.organizationMembership.findUnique({
          where: { orgId_userId: { orgId, userId } },
          // Capture the project memberships that Postgres cascade-deletes with
          // this row (ProjectMembership.organizationMembership is onDelete:
          // Cascade) so the audit `before` preserves which projects the user
          // could access. They are unrecoverable once the delete commits, and
          // this keeps parity with the tRPC deleteMembership audit entry.
          include: { ProjectMemberships: true },
        });
        // Already absent: nothing to delete. Idempotent success, no audit.
        if (!membership) {
          return { result: "noop" as const };
        }
        if (membership.role === "OWNER") {
          const ownerCount = await tx.organizationMembership.count({
            where: { orgId, role: "OWNER" },
          });
          if (ownerCount <= 1) {
            return { result: "lastOwner" as const };
          }
        }
        await tx.organizationMembership.delete({
          where: { orgId_userId: { orgId, userId } },
        });
        await auditLog(
          {
            resourceType: "orgMembership",
            resourceId: membership.id,
            action: "delete",
            before: membership,
            apiKeyId,
            orgId,
          },
          tx,
        );
        return { result: "deleted" as const, membership };
      },
      { isolationLevel: Prisma.TransactionIsolationLevel.Serializable },
    );

    if (outcome.result === "lastOwner") {
      logger.warn(
        `[SCIM] Refused to remove last OWNER ${userId} from org ${orgId}`,
      );
      res.status(403).json({
        schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
        detail:
          "Cannot remove the last owner of an organization. Assign new owner or delete organization.",
        status: 403,
      });
      return false;
    }

    // Only sync when a membership was actually removed. The audit record was
    // committed atomically with the deletion inside the transaction above.
    if (outcome.result === "deleted") {
      await (
        await getOptionalSfdcService()
      )?.removeUser({
        orgId,
        userId,
        email,
      });
    }
    return true;
  } catch (error) {
    // Postgres maps a serialization failure to Prisma error code P2034 ("could
    // not serialize access due to concurrent update"). Surface as 409 so the
    // SCIM client retries; on retry the guard will see the updated owner count.
    if (
      error instanceof Prisma.PrismaClientKnownRequestError &&
      error.code === "P2034"
    ) {
      logger.warn(
        `[SCIM] Concurrent deprovision conflict for user ${userId} in org ${orgId}`,
      );
      res.status(409).json({
        schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
        detail: "Concurrent deprovision conflict for this user. Please retry.",
        status: 409,
      });
      return false;
    }
    throw error;
  }
}

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  await runMiddleware(req, res, cors);

  if (!["GET", "DELETE", "PATCH", "PUT"].includes(req.method || "")) {
    logger.error(
      `[SCIM] Method not allowed for ${req.method} on /api/public/scim/Users/[id]`,
    );
    return res.status(405).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail: "Method not allowed",
      status: 405,
    });
  }

  // CHECK AUTH
  const authCheck = await new ApiAuthService(
    prisma,
    redis,
  ).verifyAuthHeaderAndReturnScope(req.headers.authorization);
  if (!authCheck.validKey) {
    return res.status(401).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail: authCheck.error,
      status: 401,
    });
  }
  // END CHECK AUTH

  // Check if using an organization API key
  if (
    authCheck.scope.accessLevel !== "organization" ||
    !authCheck.scope.orgId
  ) {
    return res.status(403).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail:
        "Invalid API key. Organization-scoped API key required for this operation.",
      status: 403,
    });
  }

  // Gate SCIM provisioning behind the `admin-api` entitlement, matching the
  // sibling organization admin endpoints (memberships, projects, apiKeys).
  // Without this, any org-scoped key could mutate memberships and roles on
  // plans that do not include the feature.
  if (
    !hasPlanEntitlementOrCommunityCapability({
      plan: authCheck.scope.plan,
      entitlement: "admin-api",
      capability: "scim",
    })
  ) {
    return res.status(403).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail: "This feature is not available on your current plan.",
      status: 403,
    });
  }

  logger.info(
    `[SCIM] Received request for /api/public/scim/Users/[id] with method ${req.method} for orgId ${authCheck.scope.orgId} and userId ${req.query.id}`,
  );

  // First, check if the user exists in the system at all
  const user = await prisma.user.findUnique({
    where: {
      id: req.query.id as string,
    },
  });

  if (!user) {
    return res.status(404).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail: "User not found",
      status: 404,
    });
  }

  // Route to the appropriate handler based on HTTP method
  try {
    switch (req.method) {
      case "PATCH":
        return handlePatch(
          req,
          res,
          user,
          authCheck.scope.orgId,
          authCheck.scope.apiKeyId,
        );
      case "PUT":
        return handlePut(
          req,
          res,
          user,
          authCheck.scope.orgId,
          authCheck.scope.apiKeyId,
        );
      case "GET":
        return handleGet(req, res, user, authCheck.scope.orgId);
      case "DELETE":
        return handleDelete(
          req,
          res,
          user,
          authCheck.scope.orgId,
          authCheck.scope.apiKeyId,
        );
      default:
        // This should never happen due to the check at the beginning
        return res.status(405).json({
          schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
          detail: "Method not allowed",
          status: 405,
        });
    }
  } catch (error) {
    logger.error(
      `[SCIM] Error handling user ${req.query.id} for ${req.method}`,
      error,
    );
    return res.status(500).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail: "Internal server error",
      status: 500,
    });
  }
}

// GET - Retrieve a specific user
async function handleGet(
  req: NextApiRequest,
  res: NextApiResponse,
  user: User,
  orgId: string,
) {
  // For GET operations, verify the user is a member of the organization
  const orgMembership = await prisma.organizationMembership.findFirst({
    where: {
      orgId: orgId,
      userId: user.id,
    },
  });

  if (!orgMembership) {
    return res.status(404).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail: "User not found in organization",
      status: 404,
    });
  }

  // Transform to SCIM format
  // With NextJS 15, we can't return NextApiResponse objects anymore
  res.status(200).json({
    schemas: ["urn:ietf:params:scim:schemas:core:2.0:User"],
    id: user.id,
    userName: user.email,
    name: {
      formatted: user.name,
    },
    emails: [
      {
        primary: true,
        value: user.email,
        type: "work",
      },
    ],
    meta: {
      resourceType: "User",
      created: user.createdAt?.toISOString(),
      lastModified: user.updatedAt?.toISOString(),
    },
  });
}

// PATCH - Update user details (Use only for deprovisioning for now)
// Payload is a string like: "{\"schemas\":[\"urn:ietf:params:scim:api:messages:2.0:PatchOp\"],\"Operations\":[{\"op\":\"replace\",\"value\":{\"active\":false}}]}"
async function handlePatch(
  req: NextApiRequest,
  res: NextApiResponse,
  user: User,
  orgId: string,
  apiKeyId: string,
) {
  let body = req.body;

  // Check if body is a string and parse it
  if (typeof body === "string") {
    try {
      body = JSON.parse(body);
    } catch (error) {
      logger.warn("[SCIM] Failed to parse JSON body", error);
      return res.status(400).json({
        schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
        detail: "Invalid JSON body",
        status: 400,
      });
    }
  }

  // Validate the request body
  if (
    body === null ||
    typeof body !== "object" ||
    Array.isArray(body) ||
    !body.schemas ||
    !Array.isArray(body.schemas) ||
    !body.schemas.includes("urn:ietf:params:scim:api:messages:2.0:PatchOp")
  ) {
    logger.warn(
      "[SCIM] Invalid request body. Must include 'schemas' with 'urn:ietf:params:scim:api:messages:2.0:PatchOp'.",
    );
    return res.status(400).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail:
        "Invalid request body. Must include 'schemas' with 'urn:ietf:params:scim:api:messages:2.0:PatchOp'.",
      status: 400,
    });
  }

  // Check for operations
  if (!body.Operations || !Array.isArray(body.Operations)) {
    logger.warn(
      "[SCIM] Invalid request body. Must include 'Operations' array with at least one operation.",
    );
    return res.status(400).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail:
        "Invalid request body. Must include 'Operations' array with at least one operation.",
      status: 400,
    });
  }

  const operationsResult = z
    .array(
      z.object({
        op: z.literal("replace"),
        value: z.object({ active: z.boolean() }),
      }),
    )
    .length(1)
    .safeParse(body.Operations);
  if (!operationsResult.success) {
    logger.warn(
      "[SCIM] Unsupported operation or invalid value in request body.",
    );
    return res.status(400).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail:
        "Unsupported operation or invalid value in request body. Exactly one 'replace' operation with an 'active' field is supported.",
      status: 400,
    });
  }

  const [operation] = operationsResult.data;
  if (operation.value.active) {
    // PATCH PatchOp values only carry `active` (no roles), so this always
    // creates a NONE membership when missing and is a no-op otherwise.
    const outcome = await provisionMembership({
      res,
      userId: user.id,
      orgId,
      apiKeyId,
      email: user.email,
    });
    if (!outcome) {
      return;
    }
    logScimProvision(outcome, user.id, orgId, "PATCH");
    // With NextJS 15, we can't return NextApiResponse objects anymore
    return handleGet(req, res, user, orgId);
  }

  // Deprovision atomically: check + delete in one Serializable txn.
  if (!(await deprovisionOrReject(res, user.id, orgId, apiKeyId, user.email))) {
    return;
  }
  logger.info(
    `[SCIM] Deprovisioned user ${user.id} from org ${orgId} via PATCH`,
  );
  return res.status(204).end();
}

// PUT - Update user details
async function handlePut(
  req: NextApiRequest,
  res: NextApiResponse,
  user: User,
  orgId: string,
  apiKeyId: string,
) {
  let body = req.body;

  // Check if body is a string and parse it
  if (typeof body === "string") {
    try {
      body = JSON.parse(body);
    } catch (error) {
      logger.warn("[SCIM] Failed to parse JSON body", error);
      return res.status(400).json({
        schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
        detail: "Invalid JSON body",
        status: 400,
      });
    }
  }

  // Validate that it's a SCIM user object
  if (
    body === null ||
    typeof body !== "object" ||
    Array.isArray(body) ||
    !body.schemas ||
    !Array.isArray(body.schemas) ||
    !body.schemas.includes("urn:ietf:params:scim:schemas:core:2.0:User")
  ) {
    logger.warn(
      "[SCIM] Invalid request body. Must include 'schemas' with 'urn:ietf:params:scim:schemas:core:2.0:User'.",
    );
    return res.status(400).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail:
        "Invalid request body. Must include 'schemas' with 'urn:ietf:params:scim:schemas:core:2.0:User'.",
      status: 400,
    });
  }

  // Handle active status for provisioning/deprovisioning.
  //
  // `active: true` ensures the user is provisioned. An explicit `roles` value is
  // honoured (the membership is created or updated to that role). When `roles`
  // is omitted we create a default NONE membership if one is missing but leave
  // an existing membership untouched — so a periodic IdP full-sync that omits
  // roles cannot reset an existing member's role back to NONE.
  const roleResult = parseScimRole(body.roles);
  if (!roleResult.success) {
    return res.status(400).json({
      schemas: ["urn:ietf:params:scim:api:messages:2.0:Error"],
      detail: `Invalid roles: ${roleResult.error}.`,
      status: 400,
    });
  }
  if (typeof body.active === "boolean") {
    if (body.active) {
      const outcome = await provisionMembership({
        res,
        userId: user.id,
        orgId,
        apiKeyId,
        email: user.email,
        role: roleResult.role,
      });
      // null → request rejected (e.g. last-OWNER demotion); response written.
      if (!outcome) {
        return;
      }
      logScimProvision(outcome, user.id, orgId, "PUT");
    } else {
      // Deprovision atomically: check + delete in one Serializable txn.
      if (
        !(await deprovisionOrReject(res, user.id, orgId, apiKeyId, user.email))
      ) {
        return;
      }
      logger.info(
        `[SCIM] Deprovisioned user ${user.id} from org ${orgId} via PUT`,
      );
    }
  }

  // For PUT operations, we could also update other user attributes like name
  // if they are provided in the request body. For now, matching the existing
  // feature set which only handles active status changes.

  // Return SCIM formatted user (abbreviated)
  // With NextJS 15, we can't return NextApiResponse objects anymore
  res.status(200).json({
    schemas: ["urn:ietf:params:scim:schemas:core:2.0:User"],
    id: user.id,
    userName: user.email,
    meta: {
      resourceType: "User",
      created: user.createdAt?.toISOString(),
      lastModified: user.updatedAt?.toISOString(),
    },
  });
}

// DELETE - Remove user from organization
async function handleDelete(
  req: NextApiRequest,
  res: NextApiResponse,
  user: User,
  orgId: string,
  apiKeyId: string,
) {
  // Deprovision atomically: check + delete in one Serializable txn.
  if (!(await deprovisionOrReject(res, user.id, orgId, apiKeyId, user.email))) {
    return;
  }
  logger.info(`[SCIM] Removed user ${user.id} from org ${orgId} via DELETE`);

  // Return empty response with 204 No Content.
  // With NextJS 15, we can't return NextApiResponse objects anymore
  res.status(204).end();
}
