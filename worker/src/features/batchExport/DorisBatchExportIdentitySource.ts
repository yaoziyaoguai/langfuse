import { BatchExportTableName, type FilterCondition } from "@langfuse/shared";
import { prisma } from "@langfuse/shared/src/db";
import {
  buildDorisBatchExportScoreQuery,
  buildDorisDerivedQuery,
  buildDorisLegacyObservationQuery,
  buildDorisTraceReadQuery,
  getDatasetItems,
  getDorisTelemetryRepositories,
} from "@langfuse/shared/src/server";

import type { AnalyticsExportRequest } from "./AnalyticsExportSource";
import { AnalyticsExportUnsupportedError } from "./AnalyticsExportSource";
import type { BatchExportIdentity } from "./BatchExportIdentityManifest";

type IdentityRepositories = Pick<
  ReturnType<typeof getDorisTelemetryRepositories>,
  "traces" | "observations" | "scores" | "sessions" | "datasetRunItems"
>;

type IdentitySourceDependencies = {
  readonly repositories: IdentityRepositories;
  readonly datasetItemIds: (input: {
    readonly projectId: string;
    readonly filter: FilterCondition[];
    readonly cutoffCreatedAt: Date;
    readonly limit: number;
  }) => Promise<readonly string[]>;
  readonly auditLogIds: (input: {
    readonly projectId: string;
    readonly cutoffCreatedAt: Date;
    readonly limit: number;
  }) => Promise<readonly string[]>;
};

function cutoffFilter(column: string, cutoffCreatedAt: Date): FilterCondition {
  return {
    column,
    operator: "<",
    value: cutoffCreatedAt,
    type: "datetime",
  };
}

function requestedLimit(
  request: AnalyticsExportRequest,
  hardLimit: number,
): number {
  const limit = Math.min(request.limit ?? hardLimit, hardLimit);
  if (!Number.isSafeInteger(limit) || limit < 1) {
    throw new RangeError("Invalid Doris batch export row limit");
  }
  return limit;
}

export class DorisBatchExportIdentitySource {
  constructor(private readonly dependencies: IdentitySourceDependencies) {}

  async *scan(
    request: AnalyticsExportRequest,
    hardLimit: number,
    signal?: AbortSignal,
  ): AsyncIterable<BatchExportIdentity> {
    const limit = requestedLimit(request, hardLimit);
    const search = request.searchQuery
      ? { query: request.searchQuery, searchType: request.searchType }
      : undefined;

    switch (request.tableName) {
      case BatchExportTableName.Traces: {
        const query = await buildDorisTraceReadQuery(
          request.projectId,
          request.filter.concat(
            cutoffFilter("timestamp", request.cutoffCreatedAt),
          ),
        );
        if (query.impossible) return;
        yield* this.dependencies.repositories.traces.scanIdentities({
          projectId: request.projectId,
          range: query.range,
          filters: query.filters,
          search,
          limit,
          signal,
        });
        return;
      }
      case BatchExportTableName.Observations:
      case BatchExportTableName.Events: {
        const query = buildDorisLegacyObservationQuery(
          request.filter.concat(
            cutoffFilter("Start Time", request.cutoffCreatedAt),
          ),
        );
        yield* this.dependencies.repositories.observations.scanIdentities({
          projectId: request.projectId,
          range: query.range,
          filters: query.filters,
          search,
          limit,
          signal,
        });
        return;
      }
      case BatchExportTableName.Scores: {
        const query = buildDorisBatchExportScoreQuery(
          request.filter.concat(
            cutoffFilter("timestamp", request.cutoffCreatedAt),
          ),
        );
        yield* this.dependencies.repositories.scores.scanIdentities({
          projectId: request.projectId,
          range: query.range,
          filters: query.filters,
          limit,
          signal,
        });
        return;
      }
      case BatchExportTableName.Sessions: {
        const query = buildDorisDerivedQuery(
          request.filter.concat(
            cutoffFilter("createdAt", request.cutoffCreatedAt),
          ),
          "session",
          request.cutoffCreatedAt,
        );
        yield* this.dependencies.repositories.sessions.scanIdentities({
          projectId: request.projectId,
          range: query.range,
          filters: [],
          sessionFilters: query.sessionFilters,
          search,
          limit,
          signal,
        });
        return;
      }
      case BatchExportTableName.DatasetItems: {
        const ids = await this.dependencies.datasetItemIds({
          projectId: request.projectId,
          filter: request.filter,
          cutoffCreatedAt: request.cutoffCreatedAt,
          limit,
        });
        for (const id of ids.slice().sort()) yield { id };
        return;
      }
      case BatchExportTableName.DatasetRunItems: {
        const items = await this.dependencies.repositories.datasetRunItems.list(
          {
            projectId: request.projectId,
            filters: request.filter.concat(
              cutoffFilter("createdAt", request.cutoffCreatedAt),
            ),
            orderBy: { column: "id", order: "ASC" },
            limit,
            includeIO: false,
          },
        );
        for (const item of items) yield { id: item.id };
        return;
      }
      case BatchExportTableName.AuditLogs: {
        const ids = await this.dependencies.auditLogIds({
          projectId: request.projectId,
          cutoffCreatedAt: request.cutoffCreatedAt,
          limit,
        });
        for (const id of ids) yield { id };
        return;
      }
      default:
        throw new AnalyticsExportUnsupportedError(request.tableName);
    }
  }
}

export function createDorisBatchExportIdentitySource(): DorisBatchExportIdentitySource {
  return new DorisBatchExportIdentitySource({
    repositories: getDorisTelemetryRepositories(),
    datasetItemIds: async (input) =>
      (
        await getDatasetItems<true, true>({
          projectId: input.projectId,
          filterState: input.filter.concat(
            cutoffFilter("createdAt", input.cutoffCreatedAt),
          ),
          includeIO: true,
          includeDatasetName: true,
          limit: input.limit,
          page: 0,
        })
      ).map(({ id }) => id),
    auditLogIds: async (input) =>
      (
        await prisma.auditLog.findMany({
          where: {
            projectId: input.projectId,
            createdAt: { lt: input.cutoffCreatedAt },
          },
          select: { id: true },
          orderBy: { id: "asc" },
          take: input.limit,
        })
      ).map(({ id }) => id),
  });
}
