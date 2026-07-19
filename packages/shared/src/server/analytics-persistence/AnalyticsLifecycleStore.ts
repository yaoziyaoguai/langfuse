export interface TraceTombstoneRequest {
  readonly operationId: string;
  readonly projectId: string;
  readonly traceId: string;
  readonly generation: bigint;
  readonly createdAt: Date;
}

export interface TraceTombstoneResult {
  readonly projectId: string;
  readonly traceId: string;
  readonly generation: bigint;
  readonly visible: boolean;
  readonly barrierLabel: string;
}

export interface AnalyticsDeletionProgress {
  readonly operationId: string;
  readonly projectId: string;
  readonly phase: string;
  readonly logicallyInvisible: boolean;
}

export interface ProjectTombstoneRequest {
  readonly operationId: string;
  readonly projectId: string;
  readonly generation: bigint;
  readonly createdAt: Date;
}

export interface ProjectTombstoneResult {
  readonly projectId: string;
  readonly generation: bigint;
  readonly visible: boolean;
  readonly barrierLabel: string;
}

export interface AnalyticsLifecycleStore {
  publishTraceTombstone(
    request: TraceTombstoneRequest,
  ): Promise<TraceTombstoneResult>;
  publishProjectTombstone(
    request: ProjectTombstoneRequest,
  ): Promise<ProjectTombstoneResult>;
  getDeletionProgress(input: {
    readonly operationId: string;
    readonly projectId: string;
  }): Promise<AnalyticsDeletionProgress | null>;
}
