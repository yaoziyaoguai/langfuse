import { BatchExportTableName } from "@langfuse/shared";

import type { getDatabaseReadStreamPaginated } from "../database-read-stream/getDatabaseReadStream";
import {
  AnalyticsExportUnsupportedError,
  type AnalyticsExportRequest,
  type AnalyticsExportSource,
} from "./AnalyticsExportSource";

type DorisAnalyticsExportDependencies = {
  readonly paginated: typeof getDatabaseReadStreamPaginated;
};

export class DorisAnalyticsExportSource implements AnalyticsExportSource {
  constructor(
    private readonly dependencies: DorisAnalyticsExportDependencies,
  ) {}

  async open(request: AnalyticsExportRequest) {
    if (request.tableName === BatchExportTableName.DatasetRunItems) {
      throw new AnalyticsExportUnsupportedError(request.tableName);
    }
    const { fileFormat: _fileFormat, ...query } = request;
    return this.dependencies.paginated(query);
  }
}
