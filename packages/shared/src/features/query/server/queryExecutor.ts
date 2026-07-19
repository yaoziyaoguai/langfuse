import { type QueryType, type ViewVersion } from "../types";
import { getDorisQueryExecutor } from "../../../server/repositories/telemetry/doris/runtime";
import { executeDorisAnalyticsQuery } from "./adapters/doris/DorisAnalyticsQueryEngine";

export async function executeQuery(
  projectId: string,
  query: QueryType,
  version: ViewVersion = "v1",
  _enableSingleLevelOptimization = false,
): Promise<Array<Record<string, unknown>>> {
  return executeDorisAnalyticsQuery({
    executor: getDorisQueryExecutor(),
    projectId,
    query,
    version,
  });
}
