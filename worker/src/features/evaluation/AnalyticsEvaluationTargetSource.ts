import type { Observation, TraceDomain } from "@langfuse/shared";

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

export interface AnalyticsEvaluationTargetSource {
  getTrace(request: EvaluationTraceRequest): Promise<TraceDomain | undefined>;
  getObservationsByName(
    request: EvaluationObservationsByNameRequest,
  ): Promise<Observation[]>;
}
