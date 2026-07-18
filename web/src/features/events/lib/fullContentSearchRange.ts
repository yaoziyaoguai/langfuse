import type { FilterState, TracingSearchType } from "@langfuse/shared";
import type { TableDateRange } from "@/src/utils/date-range-utils";

export const MAX_FULL_CONTENT_SEARCH_RANGE_MS = 30 * 24 * 60 * 60 * 1_000;

const FULL_CONTENT_COLUMNS = new Set(["input", "output", "metadata"]);
const FULL_CONTENT_SEARCH_TYPES = new Set<TracingSearchType>([
  "content",
  "input",
  "output",
]);

export function requiresFullContentSearch(input: {
  readonly filters: FilterState;
  readonly searchQuery: string | null;
  readonly searchType: readonly TracingSearchType[];
}): boolean {
  return (
    input.filters.some(({ column }) => FULL_CONTENT_COLUMNS.has(column)) ||
    ((input.searchQuery?.trim().length ?? 0) > 0 &&
      input.searchType.some((type) => FULL_CONTENT_SEARCH_TYPES.has(type)))
  );
}

export function validateFullContentSearchRange(input: {
  readonly filters: FilterState;
  readonly searchQuery: string | null;
  readonly searchType: readonly TracingSearchType[];
  readonly range: TableDateRange | undefined;
}): "valid" | "missing" | "too_wide" {
  if (!requiresFullContentSearch(input)) return "valid";
  const { from, to } = input.range ?? {};
  if (
    !(from instanceof Date) ||
    !(to instanceof Date) ||
    !Number.isFinite(from.getTime()) ||
    !Number.isFinite(to.getTime()) ||
    from >= to
  ) {
    return "missing";
  }
  return to.getTime() - from.getTime() <= MAX_FULL_CONTENT_SEARCH_RANGE_MS
    ? "valid"
    : "too_wide";
}
