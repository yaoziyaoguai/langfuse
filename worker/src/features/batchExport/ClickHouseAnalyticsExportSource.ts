import { BatchExportTableName } from "@langfuse/shared";

import type { getEventsStream } from "../database-read-stream/event-stream";
import type { getDatabaseReadStreamPaginated } from "../database-read-stream/getDatabaseReadStream";
import type { getObservationStream } from "../database-read-stream/observation-stream";
import type { getTraceStream } from "../database-read-stream/trace-stream";
import type {
  AnalyticsExportRequest,
  AnalyticsExportSource,
} from "./AnalyticsExportSource";

type ClickHouseAnalyticsExportDependencies = {
  readonly observations: typeof getObservationStream;
  readonly traces: typeof getTraceStream;
  readonly events: typeof getEventsStream;
  readonly paginated: typeof getDatabaseReadStreamPaginated;
};

export class ClickHouseAnalyticsExportSource implements AnalyticsExportSource {
  constructor(
    private readonly dependencies: ClickHouseAnalyticsExportDependencies,
  ) {}

  async open(request: AnalyticsExportRequest) {
    const { fileFormat, ...query } = request;
    switch (request.tableName) {
      case BatchExportTableName.Observations:
        return this.dependencies.observations({ ...query, fileFormat });
      case BatchExportTableName.Traces:
        return this.dependencies.traces(query);
      case BatchExportTableName.Events:
        return this.dependencies.events(query);
      default:
        return this.dependencies.paginated(query);
    }
  }
}
