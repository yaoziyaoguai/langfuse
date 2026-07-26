import type {
  FilterState,
  Observation,
  ObservationForEval,
  TraceDomain,
} from "@langfuse/shared";

export type EvaluationTraceRequest = {
  readonly projectId: string;
  readonly traceId: string;
  readonly timestamp?: Date;
  readonly excludeInputOutput?: boolean;
  readonly excludeMetadata?: boolean;
};

export type EvaluationObservationsByNameRequest = {
  readonly projectId: string;
  readonly traceId: string;
  readonly name: string;
  readonly timestamp?: Date;
  readonly fetchWithInputOutput?: boolean;
};

export type EvaluationTraceExistenceRequest = {
  readonly projectId: string;
  readonly traceId: string;
  readonly timestamp: Date;
  readonly filter: FilterState;
  readonly maxTimeStamp?: Date;
  readonly exactTimestamp?: Date;
};

export interface AnalyticsEvaluationTargetSource {
  getTrace(request: EvaluationTraceRequest): Promise<TraceDomain | undefined>;
  getObservationsByName(
    request: EvaluationObservationsByNameRequest,
  ): Promise<Observation[]>;
  checkTraceExists(
    request: EvaluationTraceExistenceRequest,
  ): Promise<{ exists: boolean; timestamp?: Date }>;
  checkObservationExists(request: {
    readonly projectId: string;
    readonly observationId: string;
  }): Promise<boolean>;
  getDatasetItemsByTraceId(request: {
    readonly projectId: string;
    readonly traceId: string;
  }): Promise<
    readonly {
      readonly id: string;
      readonly datasetId: string;
      readonly observationId: string | null;
    }[]
  >;
  getObservationForEvaluation(request: {
    readonly projectId: string;
    readonly traceId: string;
    readonly observationId: string;
  }): Promise<ObservationForEval | undefined>;
}
