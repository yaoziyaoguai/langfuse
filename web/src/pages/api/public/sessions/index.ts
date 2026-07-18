import { prisma } from "@langfuse/shared/src/db";
import {
  GetSessionsV1Query,
  GetSessionsV1Response,
} from "@/src/features/public-api/types/sessions";
import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import { legacyPublicApiRateLimitUpgradePaths } from "@/src/features/public-api/server/rateLimitUpgradePaths";
import {
  getSessionsTableCountFromEvents,
  getSessionsTableFromEvents,
} from "@langfuse/shared/src/server";
import type { FilterState } from "@langfuse/shared";
import { env } from "@/src/env.mjs";

export default withMiddlewares({
  GET: createAuthedProjectAPIRoute({
    name: "Get Sessions",
    rateLimitResource: "public-api-legacy",
    querySchema: GetSessionsV1Query,
    responseSchema: GetSessionsV1Response,
    rateLimitUpgradePath: legacyPublicApiRateLimitUpgradePaths.sessionsList,
    rejectInEventsOnlyMode: false,
    fn: async ({ query, auth }) => {
      const { fromTimestamp, toTimestamp, limit, page, environment } = query;

      if (
        env.LANGFUSE_ANALYTICS_BACKEND === "doris" ||
        env.LANGFUSE_MIGRATION_V4_WRITE_MODE === "events_only"
      ) {
        const filter: FilterState = [
          ...(fromTimestamp
            ? [
                {
                  type: "datetime" as const,
                  column: "createdAt",
                  operator: ">=" as const,
                  value: new Date(fromTimestamp),
                },
              ]
            : []),
          ...(toTimestamp
            ? [
                {
                  type: "datetime" as const,
                  column: "createdAt",
                  operator: "<" as const,
                  value: new Date(toTimestamp),
                },
              ]
            : []),
          ...(environment
            ? [
                {
                  type: "stringOptions" as const,
                  column: "environment",
                  operator: "any of" as const,
                  value: Array.isArray(environment)
                    ? environment
                    : [environment],
                },
              ]
            : []),
        ];
        const [sessions, totalItems] = await Promise.all([
          getSessionsTableFromEvents({
            projectId: auth.scope.projectId,
            filter,
            orderBy: { column: "createdAt", order: "DESC" },
            limit,
            page: page - 1,
          }),
          getSessionsTableCountFromEvents({
            projectId: auth.scope.projectId,
            filter,
          }),
        ]);

        return {
          data: sessions.map((session) => ({
            id: session.session_id,
            createdAt: new Date(session.min_timestamp),
            projectId: auth.scope.projectId,
            environment: session.environment ?? "default",
          })),
          meta: {
            totalItems,
            totalPages: Math.ceil(totalItems / limit),
            page,
            limit,
          },
        };
      }

      const where = {
        projectId: auth.scope.projectId,
        createdAt: {
          ...(fromTimestamp && { gte: new Date(fromTimestamp) }),
          ...(toTimestamp && { lt: new Date(toTimestamp) }),
        },
        environment: environment
          ? Array.isArray(environment)
            ? { in: environment }
            : environment
          : undefined,
      };

      const [sessions, totalItems] = await Promise.all([
        prisma.traceSession.findMany({
          select: {
            id: true,
            createdAt: true,
            projectId: true,
            environment: true,
          },
          where,
          orderBy: { createdAt: "desc" },
          take: limit,
          skip: (page - 1) * limit,
        }),
        prisma.traceSession.count({ where }),
      ]);

      return {
        data: sessions,
        meta: {
          totalItems,
          totalPages: Math.ceil(totalItems / limit),
          page,
          limit,
        },
      };
    },
  }),
});
