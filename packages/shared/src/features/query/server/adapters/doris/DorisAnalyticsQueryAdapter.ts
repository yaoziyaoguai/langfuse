import type { DorisQueryExecutor } from "../../../../../server/doris/client";
import type {
  AnalyticsQueryEngine,
  AnalyticsQueryRequest,
} from "../../AnalyticsQueryEngine";
import { executeDorisAnalyticsQuery } from "./DorisAnalyticsQueryEngine";

export function createDorisAnalyticsQueryEngine(
  executor: DorisQueryExecutor,
): AnalyticsQueryEngine {
  const execute = (
    request: AnalyticsQueryRequest,
  ): Promise<Array<Record<string, unknown>>> =>
    executeDorisAnalyticsQuery({
      executor,
      projectId: request.projectId,
      query: request.query,
      version: request.version,
      ...(request.signal ? { signal: request.signal } : {}),
    });

  return {
    execute,

    async *stream(request) {
      if (request.signal?.aborted) return;
      const rows = await execute(request);
      if (request.signal?.aborted) return;
      for (const row of rows) {
        if (request.signal?.aborted) return;
        yield { type: "row", row };
      }
    },
  };
}
