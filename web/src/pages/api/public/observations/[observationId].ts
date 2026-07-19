import {
  GetObservationV1Query,
  GetObservationV1Response,
  transformDbToApiObservation,
} from "@/src/features/public-api/types/observations";
import {
  LEGACY_PUBLIC_API_OBSERVATIONS_ANALYTICS_RESOURCE_ERROR_MESSAGE,
  withMiddlewares,
} from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";
import { LangfuseNotFoundError } from "@langfuse/shared";
import { getObservationsFromEventsTableForPublicApi } from "@langfuse/shared/src/server";
import { legacyPublicApiRateLimitUpgradePaths } from "@/src/features/public-api/server/rateLimitUpgradePaths";

export default withMiddlewares(
  {
    GET: createAuthedProjectAPIRoute({
      name: "Get Observation",
      allowInAppAgentKey: true,
      rateLimitResource: "public-api-legacy",
      querySchema: GetObservationV1Query,
      responseSchema: GetObservationV1Response,
      rateLimitUpgradePath: legacyPublicApiRateLimitUpgradePaths.observationGet,
      fn: async ({ query, auth }) => {
        const [observation] = await getObservationsFromEventsTableForPublicApi({
          projectId: auth.scope.projectId,
          page: 0,
          limit: 1,
          advancedFilters: [
            {
              type: "stringOptions",
              column: "id",
              operator: "any of",
              value: [query.observationId],
            },
          ],
        });

        if (!observation) {
          throw new LangfuseNotFoundError(
            "Observation not found within authorized project",
          );
        }
        return transformDbToApiObservation(observation);
      },
    }),
  },
  {
    analyticsResourceErrorMessage:
      LEGACY_PUBLIC_API_OBSERVATIONS_ANALYTICS_RESOURCE_ERROR_MESSAGE,
  },
);
