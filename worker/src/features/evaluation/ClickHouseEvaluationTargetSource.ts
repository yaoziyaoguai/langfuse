import type {
  AnalyticsEvaluationTargetSource,
  EvaluationObservationsByNameRequest,
  EvaluationTraceRequest,
} from "./AnalyticsEvaluationTargetSource";

type ClickHouseEvaluationTargetDependencies = {
  readonly getTrace: AnalyticsEvaluationTargetSource["getTrace"];
  readonly getObservationsByName: AnalyticsEvaluationTargetSource["getObservationsByName"];
};

export class ClickHouseEvaluationTargetSource implements AnalyticsEvaluationTargetSource {
  constructor(
    private readonly dependencies: ClickHouseEvaluationTargetDependencies,
  ) {}

  getTrace(request: EvaluationTraceRequest) {
    return this.dependencies.getTrace(request);
  }

  getObservationsByName(request: EvaluationObservationsByNameRequest) {
    return this.dependencies.getObservationsByName(request);
  }
}
