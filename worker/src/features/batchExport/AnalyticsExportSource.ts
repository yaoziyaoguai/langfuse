import type {
  BatchExportFileFormat,
  BatchExportQueryType,
  FilterCondition,
} from "@langfuse/shared";
import type { Readable } from "stream";

export type AnalyticsExportRequest = Omit<BatchExportQueryType, "filter"> & {
  readonly projectId: string;
  readonly cutoffCreatedAt: Date;
  readonly filter: FilterCondition[];
  readonly fileFormat: BatchExportFileFormat;
};

export interface AnalyticsExportSource {
  open(request: AnalyticsExportRequest): Promise<Readable>;
}

export class AnalyticsExportUnsupportedError extends Error {
  readonly code = "ANALYTICS_EXPORT_UNSUPPORTED";

  constructor(readonly tableName: string) {
    super(`Analytics export is unsupported for table: ${tableName}`);
    this.name = "AnalyticsExportUnsupportedError";
  }
}
