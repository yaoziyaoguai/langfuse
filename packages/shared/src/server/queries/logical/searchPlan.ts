import type { TracingSearchType } from "../../../interfaces/search";

const MAX_FULL_CONTENT_RANGE_MS = 30 * 24 * 60 * 60 * 1_000;

export class AnalyticsQueryValidationError extends Error {
  readonly name = "AnalyticsQueryValidationError";
  readonly code = "InvalidTimeRange";
  readonly maxDays = 30;

  constructor(
    readonly acceptedRange: {
      readonly from: Date;
      readonly to: Date;
    } | null,
  ) {
    super("Analytics query requires a valid bounded UTC time range");
  }
}

export type AnalyticsTimeRange = {
  readonly from: Date;
  readonly to: Date;
};

export type LogicalSearchPlan = {
  readonly query: string;
  readonly searchType: readonly TracingSearchType[];
  readonly requiresFullContent: boolean;
};

export function assertAnalyticsTimeRange(
  range: AnalyticsTimeRange | null,
): asserts range is AnalyticsTimeRange {
  if (
    !range ||
    !Number.isFinite(range.from.getTime()) ||
    !Number.isFinite(range.to.getTime()) ||
    range.from >= range.to
  ) {
    throw new AnalyticsQueryValidationError(range);
  }
}

export function buildSearchPlan(input: {
  readonly range: AnalyticsTimeRange | null;
  readonly search?: {
    readonly query: string;
    readonly searchType?: readonly TracingSearchType[];
  };
  readonly filtersRequireFullContent: boolean;
}): LogicalSearchPlan | null {
  assertAnalyticsTimeRange(input.range);
  const query = input.search?.query.trim() ?? "";
  const searchType = input.search?.searchType?.length
    ? input.search.searchType
    : (["id", "content"] as const);
  const requiresFullContent =
    input.filtersRequireFullContent ||
    (query.length > 0 &&
      searchType.some(
        (type) => type === "content" || type === "input" || type === "output",
      ));
  if (
    requiresFullContent &&
    input.range.to.getTime() - input.range.from.getTime() >
      MAX_FULL_CONTENT_RANGE_MS
  ) {
    throw new AnalyticsQueryValidationError(input.range);
  }
  return query.length > 0 ? { query, searchType, requiresFullContent } : null;
}
