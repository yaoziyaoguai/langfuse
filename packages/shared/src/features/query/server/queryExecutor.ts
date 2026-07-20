import type { QueryType, ViewVersion } from "../types";
import type { AnalyticsQueryStreamEvent } from "./AnalyticsQueryEngine";
import { getAnalyticsQueryEngine } from "./analyticsQueryRuntime";

export async function executeQuery(
  projectId: string,
  query: QueryType,
  version: ViewVersion = "v1",
  enableSingleLevelOptimization = false,
): Promise<Array<Record<string, unknown>>> {
  return getAnalyticsQueryEngine().execute({
    projectId,
    query,
    version,
    enableSingleLevelOptimization,
  });
}

export function streamAnalyticsQuery(input: {
  readonly projectId: string;
  readonly query: QueryType;
  readonly version?: ViewVersion;
  readonly enableSingleLevelOptimization?: boolean;
  readonly signal?: AbortSignal;
}): AsyncIterable<AnalyticsQueryStreamEvent> {
  return getAnalyticsQueryEngine().stream({
    projectId: input.projectId,
    query: input.query,
    version: input.version ?? "v1",
    enableSingleLevelOptimization: input.enableSingleLevelOptimization,
    signal: input.signal,
  });
}
