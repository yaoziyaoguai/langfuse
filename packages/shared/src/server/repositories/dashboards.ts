import type { FilterState } from "../../types";
import { InvalidRequestError } from "../../errors";

export type DateTrunc = "month" | "week" | "day" | "hour" | "minute";

export const extractFromAndToTimestampsFromFilter = (filter?: FilterState) => {
  if (!filter) {
    throw new InvalidRequestError(
      "Time filter is required for time series queries",
    );
  }
  const from = filter.find(
    (item) =>
      item.type === "datetime" &&
      (item.operator === ">" || item.operator === ">="),
  );
  const to = filter.find(
    (item) =>
      item.type === "datetime" &&
      (item.operator === "<" || item.operator === "<="),
  );
  return [from, to] as const;
};

function unavailable(): never {
  throw new InvalidRequestError(
    "Legacy dashboard repository queries are unavailable in Doris R1A",
  );
}

export const getScoreAggregate = async (..._args: unknown[]) => unavailable();
export const getObservationCostByTypeByTime = async (..._args: unknown[]) =>
  unavailable();
export const getObservationUsageByTypeByTime = async (..._args: unknown[]) =>
  unavailable();

export const orderByTimeSeries = (
  _filter: FilterState,
  _column: string,
): never => unavailable();

export const selectTimeseriesColumn = (
  _bucketSizeInSeconds: number,
  _column: string,
  _alias: string,
): never => unavailable();
