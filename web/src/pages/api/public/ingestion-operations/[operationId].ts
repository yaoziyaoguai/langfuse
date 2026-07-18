import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import {
  AnalyticsIngestionOperationQuery,
  AnalyticsIngestionOperationResponse,
} from "@/src/features/public-api/types/analyticsIngestion";
import { withMiddlewares } from "@/src/features/public-api/server/withMiddlewares";
import { serializeAnalyticsIngestionStatus } from "@/src/features/public-api/server/analyticsIngestionStatus";
import { prisma } from "@langfuse/shared/src/db";
import { getAnalyticsIngestionStatusForProject } from "@langfuse/shared/src/server";

export default withMiddlewares({
  GET: createAuthedProjectAPIRoute({
    name: "Get analytics ingestion operation",
    querySchema: AnalyticsIngestionOperationQuery,
    responseSchema: AnalyticsIngestionOperationResponse,
    fn: async ({ query, auth, res }) => {
      const operation = await getAnalyticsIngestionStatusForProject({
        client: prisma,
        operationId: query.operationId,
        projectId: auth.scope.projectId,
      });
      if (!operation) {
        res.status(404);
        return { message: "Ingestion operation not found" as const };
      }
      return serializeAnalyticsIngestionStatus(operation);
    },
  }),
});
