import { prisma } from "@langfuse/shared/src/db";
import {
  GetSessionV1Query,
  GetSessionV1Response,
} from "@/src/features/public-api/types/sessions";
import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import { LangfuseNotFoundError } from "@langfuse/shared";
import {
  getSessionTracesFromEvents,
  getSessionsTableFromEvents,
  getTraceByIdFromEventsTable,
  getTracesBySessionId,
} from "@langfuse/shared/src/server";
import { legacyPublicApiRateLimitUpgradePaths } from "@/src/features/public-api/server/rateLimitUpgradePaths";
import { env } from "@/src/env.mjs";

export default withMiddlewares({
  GET: createAuthedProjectAPIRoute({
    name: "Get Session",
    rateLimitResource: "public-api-legacy",
    querySchema: GetSessionV1Query,
    responseSchema: GetSessionV1Response,
    rateLimitUpgradePath: legacyPublicApiRateLimitUpgradePaths.sessionGet,
    rejectInEventsOnlyMode: false,
    fn: async ({ query, auth }) => {
      const { sessionId } = query;
      if (
        env.LANGFUSE_ANALYTICS_BACKEND === "doris" ||
        env.LANGFUSE_MIGRATION_V4_WRITE_MODE === "events_only"
      ) {
        const filter = [
          {
            type: "stringOptions" as const,
            column: "id",
            operator: "any of" as const,
            value: [sessionId],
          },
        ];
        const sessions = await getSessionsTableFromEvents({
          projectId: auth.scope.projectId,
          filter,
          limit: 1,
          page: 0,
        });
        const session = sessions[0];
        if (!session) {
          throw new LangfuseNotFoundError(
            "Session not found within authorized project",
          );
        }
        const traces =
          env.LANGFUSE_ANALYTICS_BACKEND === "doris"
            ? await getTracesBySessionId(
                auth.scope.projectId,
                [sessionId],
                new Date(session.min_timestamp),
              )
            : (
                await Promise.all(
                  (
                    await getSessionTracesFromEvents({
                      projectId: auth.scope.projectId,
                      sessionId,
                    })
                  ).map((trace) =>
                    getTraceByIdFromEventsTable({
                      projectId: auth.scope.projectId,
                      traceId: trace.id,
                      timestamp: trace.timestamp,
                    }),
                  ),
                )
              ).filter((trace) => trace !== undefined);
        return {
          id: session.session_id,
          createdAt: new Date(session.min_timestamp),
          projectId: auth.scope.projectId,
          environment: session.environment ?? "default",
          traces: traces.map((trace) => ({
            ...trace,
            externalId: null,
          })),
        };
      }

      const session = await prisma.traceSession.findUnique({
        where: {
          id_projectId: {
            id: sessionId,
            projectId: auth.scope.projectId,
          },
        },
        select: {
          id: true,
          createdAt: true,
          projectId: true,
          environment: true,
        },
      });

      if (!session) {
        throw new LangfuseNotFoundError(
          "Session not found within authorized project",
        );
      }

      const traces = await getTracesBySessionId(
        auth.scope.projectId,
        [sessionId],
        session.createdAt,
      );

      return {
        ...session,
        traces: traces.map((trace) => ({
          ...trace,
          externalId: null,
        })),
      };
    },
  }),
});
