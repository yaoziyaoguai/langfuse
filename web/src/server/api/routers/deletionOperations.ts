import { projectRoleAccessRights } from "@/src/features/rbac/constants/projectAccessRights";
import {
  createTRPCRouter,
  protectedOrganizationProcedure,
  protectedProjectProcedure,
} from "@/src/server/api/trpc";
import { throwIfNoProjectAccess } from "@/src/features/rbac/utils/checkProjectAccess";
import { TRPCError } from "@trpc/server";
import * as z from "zod";

export const deletionOperationsRouter = createTRPCRouter({
  traceStatus: protectedProjectProcedure
    .input(
      z.object({
        projectId: z.string(),
        deletionOperationId: z.string().min(1),
      }),
    )
    .query(async ({ input, ctx }) => {
      throwIfNoProjectAccess({
        session: ctx.session,
        projectId: input.projectId,
        scope: "traces:delete",
      });
      const operation = await ctx.prisma.analyticsDeletionOperation.findFirst({
        where: {
          id: input.deletionOperationId,
          projectId: input.projectId,
          scope: "TRACE",
        },
      });
      if (!operation) {
        throw new TRPCError({ code: "NOT_FOUND", message: "Not found" });
      }
      return {
        deletionOperationId: operation.id,
        traceId: operation.traceId,
        status: operation.status.toLowerCase() as
          | "scheduled"
          | "retrying"
          | "needs_attention"
          | "completed",
        phase: operation.phase,
        logicallyInvisible: operation.logicallyInvisible,
        createdAt: operation.createdAt,
        completedAt: operation.completedAt,
        statusExpiresAt: operation.statusExpiresAt,
      };
    }),
  projectStatus: protectedOrganizationProcedure
    .input(
      z.object({
        orgId: z.string(),
        deletionOperationId: z.string().min(1),
      }),
    )
    .query(async ({ input, ctx }) => {
      if (
        !projectRoleAccessRights[ctx.session.orgRole].includes("project:delete")
      ) {
        throw new TRPCError({ code: "NOT_FOUND", message: "Not found" });
      }

      const operation = await ctx.prisma.analyticsDeletionOperation.findFirst({
        where: {
          id: input.deletionOperationId,
          organizationId: input.orgId,
          scope: "PROJECT",
        },
      });
      if (!operation) {
        throw new TRPCError({ code: "NOT_FOUND", message: "Not found" });
      }

      return {
        deletionOperationId: operation.id,
        projectId: operation.projectId,
        status: operation.status.toLowerCase() as
          | "scheduled"
          | "retrying"
          | "needs_attention"
          | "completed",
        phase: operation.phase,
        logicallyInvisible: operation.logicallyInvisible,
        createdAt: operation.createdAt,
        completedAt: operation.completedAt,
        statusExpiresAt: operation.statusExpiresAt,
      };
    }),
});
