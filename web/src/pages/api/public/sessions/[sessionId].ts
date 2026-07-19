import {
  GetSessionV1Query,
  GetSessionV1Response,
} from "@/src/features/public-api/types/sessions";
import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import { LangfuseNotFoundError } from "@langfuse/shared";
import {
  getSessionsTableFromEvents,
  getTracesBySessionId,
} from "@langfuse/shared/src/server";
import { legacyPublicApiRateLimitUpgradePaths } from "@/src/features/public-api/server/rateLimitUpgradePaths";

export default withMiddlewares({
  GET: createAuthedProjectAPIRoute({
    name: "Get Session",
    rateLimitResource: "public-api-legacy",
    querySchema: GetSessionV1Query,
    responseSchema: GetSessionV1Response,
    rateLimitUpgradePath: legacyPublicApiRateLimitUpgradePaths.sessionGet,
    fn: async ({ query, auth }) => {
      const { sessionId } = query;
      const sessions = await getSessionsTableFromEvents({
        projectId: auth.scope.projectId,
        filter: [
          {
            type: "stringOptions",
            column: "id",
            operator: "any of",
            value: [sessionId],
          },
        ],
        limit: 1,
        page: 0,
      });
      const session = sessions[0];
      if (!session) {
        throw new LangfuseNotFoundError(
          "Session not found within authorized project",
        );
      }

      const traces = await getTracesBySessionId(
        auth.scope.projectId,
        [sessionId],
        new Date(session.min_timestamp),
      );

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
    },
  }),
});
