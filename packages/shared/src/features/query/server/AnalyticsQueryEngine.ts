import type { QueryType, ViewVersion } from "../types";

export type AnalyticsQueryRequest = {
  readonly projectId: string;
  readonly query: QueryType;
  readonly version: ViewVersion;
  readonly enableSingleLevelOptimization?: boolean;
  readonly signal?: AbortSignal;
};

export type AnalyticsQueryStreamEvent =
  | { readonly type: "progress"; readonly progress: object }
  | { readonly type: "row"; readonly row: Record<string, unknown> };

export type AnalyticsQueryErrorCode = "RESOURCE_EXHAUSTED";

export class AnalyticsQueryError extends Error {
  readonly name = "AnalyticsQueryError";

  constructor(
    readonly code: AnalyticsQueryErrorCode,
    message: string,
    options?: ErrorOptions,
  ) {
    super(message, options);
  }
}

export interface AnalyticsQueryEngine {
  execute(
    request: AnalyticsQueryRequest,
  ): Promise<Array<Record<string, unknown>>>;

  stream(
    request: AnalyticsQueryRequest,
  ): AsyncIterable<AnalyticsQueryStreamEvent>;
}
