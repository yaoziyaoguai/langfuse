import {
  getObservationsFromEventsTableForPublicApi,
  getObservationsCountFromEventsTableForPublicApi,
} from "@langfuse/shared/src/server";

import {
  LEGACY_PUBLIC_API_OBSERVATIONS_ANALYTICS_RESOURCE_ERROR_MESSAGE,
  withMiddlewares,
} from "@/src/features/public-api/server/withMiddlewares";
import { createAuthedProjectAPIRoute } from "@/src/features/public-api/server/createAuthedProjectAPIRoute";

import {
  GetObservationsV1Query,
  GetObservationsV1Response,
  transformDbToApiObservation,
} from "@/src/features/public-api/types/observations";
import { legacyPublicApiRateLimitUpgradePaths } from "@/src/features/public-api/server/rateLimitUpgradePaths";

export default withMiddlewares(
  {
    GET: createAuthedProjectAPIRoute({
      name: "Get Observations",
      allowInAppAgentKey: true,
      rateLimitResource: "public-api-legacy",
      querySchema: GetObservationsV1Query,
      responseSchema: GetObservationsV1Response,
      rateLimitUpgradePath:
        legacyPublicApiRateLimitUpgradePaths.observationsList,
      fn: async ({ query, auth }) => {
        const filterProps = {
          projectId: auth.scope.projectId,
          page: query.page,
          limit: query.limit,
          traceId: query.traceId ?? undefined,
          userId: query.userId ?? undefined,
          level: query.level ?? undefined,
          name: query.name ?? undefined,
          type: query.type ?? undefined,
          environment: query.environment ?? undefined,
          parentObservationId: query.parentObservationId ?? undefined,
          fromStartTime: query.fromStartTime ?? undefined,
          toStartTime: query.toStartTime ?? undefined,
          version: query.version ?? undefined,
          advancedFilters: query.filter,
        };

        const [items, count] = await Promise.all([
          getObservationsFromEventsTableForPublicApi(filterProps),
          getObservationsCountFromEventsTableForPublicApi(filterProps),
        ]);

        return {
          data: items.map(transformDbToApiObservation),
          meta: {
            page: query.page,
            limit: query.limit,
            totalItems: count,
            totalPages: Math.ceil(count / query.limit),
          },
        };
      },
    }),
  },
  {
    analyticsResourceErrorMessage:
      LEGACY_PUBLIC_API_OBSERVATIONS_ANALYTICS_RESOURCE_ERROR_MESSAGE,
  },
);
