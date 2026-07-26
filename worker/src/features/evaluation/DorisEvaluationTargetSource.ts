import type { Observation, TraceDomain } from "@langfuse/shared";
import type {
  DorisObservation,
  DorisTrace,
  getDorisTelemetryRepositories,
} from "@langfuse/shared/src/server";

import type {
  AnalyticsEvaluationTargetSource,
  EvaluationObservationsByNameRequest,
  EvaluationTraceRequest,
} from "./AnalyticsEvaluationTargetSource";

type DorisEvaluationTargetDependencies = {
  readonly repositories: typeof getDorisTelemetryRepositories;
  readonly toTraceDomain: (
    trace: DorisTrace,
    control: { readonly bookmarked: boolean; readonly public: boolean },
  ) => TraceDomain;
  readonly toObservationDomain: (observation: DorisObservation) => Observation;
  readonly getTraceControl: (request: {
    readonly projectId: string;
    readonly traceId: string;
  }) => Promise<{
    readonly bookmarked: boolean;
    readonly public: boolean;
  } | null>;
  readonly checkTraceExists: AnalyticsEvaluationTargetSource["checkTraceExists"];
  readonly checkObservationExists: AnalyticsEvaluationTargetSource["checkObservationExists"];
  readonly getDatasetItemsByTraceId: AnalyticsEvaluationTargetSource["getDatasetItemsByTraceId"];
  readonly getObservationForEvaluation: AnalyticsEvaluationTargetSource["getObservationForEvaluation"];
};

export class DorisEvaluationTargetSource implements AnalyticsEvaluationTargetSource {
  constructor(
    private readonly dependencies: DorisEvaluationTargetDependencies,
  ) {}

  async getTrace(request: EvaluationTraceRequest) {
    const trace = await this.dependencies.repositories().traces.get(request);
    if (!trace) return undefined;
    if (
      request.timestamp &&
      trace.timestamp.toISOString().slice(0, 10) !==
        request.timestamp.toISOString().slice(0, 10)
    ) {
      return undefined;
    }
    const control = await this.dependencies.getTraceControl(request);
    const domain = this.dependencies.toTraceDomain(trace, {
      bookmarked: control?.bookmarked ?? false,
      public: control?.public ?? false,
    });
    return {
      ...domain,
      input: request.excludeInputOutput ? null : domain.input,
      output: request.excludeInputOutput ? null : domain.output,
      metadata: request.excludeMetadata ? {} : domain.metadata,
    };
  }

  async getObservationsByName(request: EvaluationObservationsByNameRequest) {
    const page = await this.dependencies
      .repositories()
      .observations.listForTrace({
        projectId: request.projectId,
        traceId: request.traceId,
        filters: [
          {
            type: "string",
            column: "name",
            operator: "=",
            value: request.name,
          },
        ],
        includeFullContent: request.fetchWithInputOutput,
        limit: 999,
      });
    return page.items.map(this.dependencies.toObservationDomain);
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
