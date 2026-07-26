import type {
  AnalyticsEvaluationTargetSource,
  EvaluationObservationsByNameRequest,
  EvaluationTraceRequest,
} from "./AnalyticsEvaluationTargetSource";

type ClickHouseEvaluationTargetDependencies = {
  readonly getTrace: AnalyticsEvaluationTargetSource["getTrace"];
  readonly getObservationsByName: AnalyticsEvaluationTargetSource["getObservationsByName"];
  readonly checkTraceExists: AnalyticsEvaluationTargetSource["checkTraceExists"];
  readonly checkObservationExists: AnalyticsEvaluationTargetSource["checkObservationExists"];
  readonly getDatasetItemsByTraceId: AnalyticsEvaluationTargetSource["getDatasetItemsByTraceId"];
  readonly getObservationForEvaluation: AnalyticsEvaluationTargetSource["getObservationForEvaluation"];
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

  checkTraceExists(
    request: Parameters<AnalyticsEvaluationTargetSource["checkTraceExists"]>[0],
  ) {
    return this.dependencies.checkTraceExists(request);
  }

  checkObservationExists(
    request: Parameters<
      AnalyticsEvaluationTargetSource["checkObservationExists"]
    >[0],
  ) {
    return this.dependencies.checkObservationExists(request);
  }

  getDatasetItemsByTraceId(
    request: Parameters<
      AnalyticsEvaluationTargetSource["getDatasetItemsByTraceId"]
    >[0],
  ) {
    return this.dependencies.getDatasetItemsByTraceId(request);
  }

  getObservationForEvaluation(
    request: Parameters<
      AnalyticsEvaluationTargetSource["getObservationForEvaluation"]
    >[0],
  ) {
    return this.dependencies.getObservationForEvaluation(request);
  }
}
