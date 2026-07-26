import type {
  BatchExportFileFormat,
  BatchExportQueryType,
  FilterCondition,
} from "@langfuse/shared";
import type { Readable } from "stream";
import type { BatchExportIdentity } from "./BatchExportIdentityManifest";

export type AnalyticsExportRequest = Omit<BatchExportQueryType, "filter"> & {
  readonly projectId: string;
  readonly cutoffCreatedAt: Date;
  readonly filter: FilterCondition[];
  readonly fileFormat: BatchExportFileFormat;
};

export type AnalyticsExportOpenOptions = {
  readonly identities?: AsyncIterable<BatchExportIdentity>;
  readonly signal?: AbortSignal;
  readonly revalidate?: () => Promise<void>;
};

export interface AnalyticsExportSource {
  open(
    request: AnalyticsExportRequest,
    options?: AnalyticsExportOpenOptions,
  ): Promise<Readable>;
  scanIdentities?(
    request: AnalyticsExportRequest,
    hardLimit: number,
    signal?: AbortSignal,
  ): AsyncIterable<BatchExportIdentity>;
}

export class AnalyticsExportUnsupportedError extends Error {
  readonly code = "ANALYTICS_EXPORT_UNSUPPORTED";

  constructor(readonly tableName: string) {
    super(`Analytics export is unsupported for table: ${tableName}`);
    this.name = "AnalyticsExportUnsupportedError";
  }
}
