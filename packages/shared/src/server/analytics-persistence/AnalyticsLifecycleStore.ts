export interface TraceTombstoneRequest {
  readonly operationId: string;
  readonly projectId: string;
  readonly traceId: string;
  readonly generation: bigint;
}

export interface TraceTombstoneResult {
  readonly projectId: string;
  readonly traceId: string;
  readonly generation: bigint;
  readonly visible: boolean;
}

export interface AnalyticsDeletionProgress {
  readonly operationId: string;
  readonly projectId: string;
  readonly phase: string;
  readonly logicallyInvisible: boolean;
}

export interface AnalyticsLifecycleStore {
  publishTraceTombstone(
    request: TraceTombstoneRequest,
  ): Promise<TraceTombstoneResult>;
  getDeletionProgress(input: {
    readonly operationId: string;
    readonly projectId: string;
  }): Promise<AnalyticsDeletionProgress | null>;
}
