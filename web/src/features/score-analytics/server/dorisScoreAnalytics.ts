import type { ScoreDomain } from "@langfuse/shared";
import {
  getDorisTelemetryRepositories,
  type DorisScoreAnalyticsIdentifier,
  type DorisScoreAnalyticsObjectType,
} from "@langfuse/shared/src/server";
import { InvalidRequestError } from "@langfuse/shared";
import type { IntervalConfig } from "@/src/utils/date-range-utils";

const SAMPLE_SIZE = 100_000;
const MATCH_LIMIT = 1_000_000;

type ComparisonInput = {
  projectId: string;
  score1: DorisScoreAnalyticsIdentifier;
  score2: DorisScoreAnalyticsIdentifier;
  fromTimestamp: Date;
  toTimestamp: Date;
  interval: IntervalConfig;
  nBins: number;
  objectType: DorisScoreAnalyticsObjectType;
  mode: "single" | "two";
};

type Pair = { first: ScoreDomain; second: ScoreDomain };

function assertInput(input: ComparisonInput) {
  if (input.fromTimestamp >= input.toTimestamp) {
    throw new InvalidRequestError("fromTimestamp must be before toTimestamp");
  }
}

function attachmentKey(score: ScoreDomain): string {
  return JSON.stringify([
    score.traceId ?? null,
    score.observationId ?? null,
    score.sessionId ?? null,
    score.datasetRunId ?? null,
  ]);
}

function scoreCategory(score: ScoreDomain): string {
  if (score.dataType === "BOOLEAN") return score.value === 1 ? "True" : "False";
  return score.stringValue ?? String(score.value);
}

function pairScores(
  first: readonly ScoreDomain[],
  second: readonly ScoreDomain[],
  identical: boolean,
): Pair[] {
  if (identical) return first.map((score) => ({ first: score, second: score }));
  const secondByAttachment = new Map<string, ScoreDomain[]>();
  for (const score of second) {
    const key = attachmentKey(score);
    const bucket = secondByAttachment.get(key) ?? [];
    bucket.push(score);
    secondByAttachment.set(key, bucket);
  }
  const pairs: Pair[] = [];
  for (const score of first) {
    for (const counterpart of secondByAttachment.get(attachmentKey(score)) ??
      []) {
      pairs.push({ first: score, second: counterpart });
      if (pairs.length >= MATCH_LIMIT) return pairs;
    }
  }
  return pairs;
}

function mean(values: readonly number[]): number | null {
  return values.length === 0
    ? null
    : values.reduce((sum, value) => sum + value, 0) / values.length;
}

function standardDeviation(values: readonly number[]): number | null {
  const average = mean(values);
  if (average === null) return null;
  return Math.sqrt(
    values.reduce((sum, value) => sum + (value - average) ** 2, 0) /
      values.length,
  );
}

function pearson(
  left: readonly number[],
  right: readonly number[],
): number | null {
  if (left.length < 2 || left.length !== right.length) return null;
  const leftMean = mean(left)!;
  const rightMean = mean(right)!;
  let numerator = 0;
  let leftVariance = 0;
  let rightVariance = 0;
  for (let index = 0; index < left.length; index++) {
    const leftDelta = left[index]! - leftMean;
    const rightDelta = right[index]! - rightMean;
    numerator += leftDelta * rightDelta;
    leftVariance += leftDelta ** 2;
    rightVariance += rightDelta ** 2;
  }
  const denominator = Math.sqrt(leftVariance * rightVariance);
  return denominator === 0 ? null : numerator / denominator;
}

function ranks(values: readonly number[]): number[] {
  const sorted = values
    .map((value, index) => ({ value, index }))
    .sort((a, b) => a.value - b.value || a.index - b.index);
  const result = Array<number>(values.length);
  for (let start = 0; start < sorted.length; ) {
    let end = start + 1;
    while (end < sorted.length && sorted[end]!.value === sorted[start]!.value) {
      end++;
    }
    const averageRank = (start + 1 + end) / 2;
    for (let index = start; index < end; index++) {
      result[sorted[index]!.index] = averageRank;
    }
    start = end;
  }
  return result;
}

function bucketDate(value: Date, interval: IntervalConfig): Date {
  const date = new Date(value);
  const count = interval.count;
  switch (interval.unit) {
    case "second":
      date.setUTCMilliseconds(0);
      date.setUTCSeconds(Math.floor(date.getUTCSeconds() / count) * count);
      return date;
    case "minute":
      date.setUTCSeconds(0, 0);
      date.setUTCMinutes(Math.floor(date.getUTCMinutes() / count) * count);
      return date;
    case "hour":
      date.setUTCMinutes(0, 0, 0);
      date.setUTCHours(Math.floor(date.getUTCHours() / count) * count);
      return date;
    case "day": {
      const epochDay = Math.floor(date.getTime() / 86_400_000);
      return new Date(Math.floor(epochDay / count) * count * 86_400_000);
    }
    case "month":
      return new Date(
        Date.UTC(
          date.getUTCFullYear(),
          Math.floor(date.getUTCMonth() / count) * count,
          1,
        ),
      );
    case "year":
      return new Date(
        Date.UTC(Math.floor(date.getUTCFullYear() / count) * count, 0, 1),
      );
  }
}

function numericTimeSeries(
  first: readonly ScoreDomain[],
  second: readonly ScoreDomain[],
  interval: IntervalConfig,
) {
  const buckets = new Map<
    number,
    { first: number[]; second: number[]; count: number }
  >();
  const add = (score: ScoreDomain, side: "first" | "second") => {
    const timestamp = bucketDate(score.timestamp, interval).getTime();
    const bucket = buckets.get(timestamp) ?? {
      first: [],
      second: [],
      count: 0,
    };
    bucket[side].push(score.value);
    bucket.count++;
    buckets.set(timestamp, bucket);
  };
  first.forEach((score) => add(score, "first"));
  second.forEach((score) => add(score, "second"));
  return [...buckets.entries()]
    .sort(([left], [right]) => left - right)
    .map(([timestamp, bucket]) => ({
      timestamp: new Date(timestamp),
      avg1: mean(bucket.first),
      avg2: mean(bucket.second),
      count: bucket.count,
    }));
}

function categoricalTimeSeries(
  scores: readonly ScoreDomain[],
  interval: IntervalConfig,
) {
  const counts = new Map<
    string,
    { timestamp: Date; category: string; count: number }
  >();
  for (const score of scores) {
    const timestamp = bucketDate(score.timestamp, interval);
    const category = scoreCategory(score);
    const key = JSON.stringify([timestamp.getTime(), category]);
    const row = counts.get(key) ?? { timestamp, category, count: 0 };
    row.count++;
    counts.set(key, row);
  }
  return [...counts.values()].sort(
    (left, right) =>
      left.timestamp.getTime() - right.timestamp.getTime() ||
      left.category.localeCompare(right.category),
  );
}

function binIndex(
  value: number,
  min: number,
  max: number,
  nBins: number,
): number {
  if (max <= min) return 0;
  return Math.min(
    nBins - 1,
    Math.max(0, Math.floor(((value - min) / (max - min)) * nBins)),
  );
}

function numericDistribution(
  values: readonly number[],
  min: number,
  max: number,
  nBins: number,
) {
  const counts = new Map<number, number>();
  for (const value of values) {
    const index = binIndex(value, min, max, nBins);
    counts.set(index, (counts.get(index) ?? 0) + 1);
  }
  return [...counts.entries()]
    .sort(([left], [right]) => left - right)
    .map(([index, count]) => ({ binIndex: index, count }));
}

function categoricalDistribution(scores: readonly ScoreDomain[]) {
  const categories = [...new Set(scores.map(scoreCategory))].sort();
  const index = new Map(
    categories.map((category, position) => [category, position]),
  );
  const counts = new Map<number, number>();
  for (const score of scores) {
    const position = index.get(scoreCategory(score))!;
    counts.set(position, (counts.get(position) ?? 0) + 1);
  }
  return [...counts.entries()].map(([binIndex, count]) => ({
    binIndex,
    count,
  }));
}

function groupedRows<T extends Record<string, string>>(
  rows: readonly T[],
  keys: readonly (keyof T)[],
) {
  const counts = new Map<string, T & { count: number }>();
  for (const row of rows) {
    const key = JSON.stringify(keys.map((field) => row[field]));
    const existing = counts.get(key) ?? { ...row, count: 0 };
    existing.count++;
    counts.set(key, existing);
  }
  return [...counts.values()];
}

export async function estimateDorisScoreComparison(
  input: Omit<ComparisonInput, "interval" | "nBins" | "mode">,
) {
  assertInput({
    ...input,
    interval: { count: 1, unit: "day" },
    nBins: 10,
    mode: "two",
  });
  return getDorisTelemetryRepositories().scores.comparisonCounts({
    projectId: input.projectId,
    range: { from: input.fromTimestamp, to: input.toTimestamp },
    score1: input.score1,
    score2: input.score2,
    objectType: input.objectType,
  });
}

export async function getDorisScoreComparisonAnalytics(input: ComparisonInput) {
  assertInput(input);
  const objectType = input.objectType;
  const repository = getDorisTelemetryRepositories().scores;
  const counts = await repository.comparisonCounts({
    projectId: input.projectId,
    range: { from: input.fromTimestamp, to: input.toTimestamp },
    score1: input.score1,
    score2: input.score2,
    objectType,
  });
  const identical =
    input.score1.name === input.score2.name &&
    input.score1.source === input.score2.source &&
    input.score1.dataType === input.score2.dataType;
  const [first, second] = await Promise.all([
    repository.analyticsRows({
      projectId: input.projectId,
      range: { from: input.fromTimestamp, to: input.toTimestamp },
      score: input.score1,
      objectType,
      limit: SAMPLE_SIZE,
    }),
    identical
      ? Promise.resolve(null)
      : repository.analyticsRows({
          projectId: input.projectId,
          range: { from: input.fromTimestamp, to: input.toTimestamp },
          score: input.score2,
          objectType,
          limit: SAMPLE_SIZE,
        }),
  ]);
  const secondRows = second ?? first;
  const pairs = pairScores(first, secondRows, identical);
  const isNumeric =
    input.score1.dataType === "NUMERIC" && input.score2.dataType === "NUMERIC";
  const firstValues = first.map((score) => score.value);
  const secondValues = secondRows.map((score) => score.value);
  const pairedFirst = pairs.map(({ first: score }) => score.value);
  const pairedSecond = pairs.map(({ second: score }) => score.value);
  const min1 = firstValues.reduce(
    (minimum, value) => Math.min(minimum, value),
    firstValues[0] ?? 0,
  );
  const max1 = firstValues.reduce(
    (maximum, value) => Math.max(maximum, value),
    firstValues[0] ?? 0,
  );
  const min2 = secondValues.reduce(
    (minimum, value) => Math.min(minimum, value),
    secondValues[0] ?? 0,
  );
  const max2 = secondValues.reduce(
    (maximum, value) => Math.max(maximum, value),
    secondValues[0] ?? 0,
  );
  const globalMin = Math.min(min1, min2);
  const globalMax = Math.max(max1, max2);

  const heatmap = isNumeric
    ? groupedRows(
        pairs.map(({ first: left, second: right }) => ({
          binX: String(binIndex(left.value, min1, max1, input.nBins)),
          binY: String(binIndex(right.value, min2, max2, input.nBins)),
        })),
        ["binX", "binY"],
      ).map((row) => ({
        binX: Number(row.binX),
        binY: Number(row.binY),
        count: row.count,
        min1,
        max1,
        min2,
        max2,
        globalMin,
        globalMax,
      }))
    : [];
  const confusionMatrix: Array<{
    rowCategory: string;
    colCategory: string;
    count: number;
  }> = isNumeric
    ? []
    : groupedRows(
        pairs.map(({ first: left, second: right }) => ({
          rowCategory: scoreCategory(left),
          colCategory: scoreCategory(right),
        })),
        ["rowCategory", "colCategory"],
      ).map(({ rowCategory, colCategory, count }) => ({
        rowCategory,
        colCategory,
        count,
      }));

  const secondByAttachment = new Map<string, ScoreDomain[]>();
  for (const score of secondRows) {
    const key = attachmentKey(score);
    const bucket = secondByAttachment.get(key) ?? [];
    bucket.push(score);
    secondByAttachment.set(key, bucket);
  }
  const stacked = first.flatMap((score) => {
    const matches = identical
      ? [score]
      : secondByAttachment.get(attachmentKey(score));
    return (matches?.length ? matches : [null]).map((counterpart) => ({
      score1Category: scoreCategory(score),
      score2Stack: counterpart ? scoreCategory(counterpart) : "__unmatched__",
    }));
  });
  const stackedDistribution = groupedRows(stacked, [
    "score1Category",
    "score2Stack",
  ]);
  const stackedDistributionMatched = groupedRows(
    pairs.map(({ first: left, second: right }) => ({
      score1Category: scoreCategory(left),
      score2Stack: scoreCategory(right),
    })),
    ["score1Category", "score2Stack"],
  );
  const pairedFirstScores = pairs.map(({ first: score }) => score);
  const pairedSecondScores = pairs.map(({ second: score }) => score);
  const sampled =
    counts.score1Count > SAMPLE_SIZE || counts.score2Count > SAMPLE_SIZE;
  const samplingRate = sampled
    ? Math.min(
        1,
        SAMPLE_SIZE / Math.max(counts.score1Count, counts.score2Count),
      )
    : 1;
  const differences = pairedFirst.map(
    (value, index) => value - pairedSecond[index]!,
  );

  return {
    counts: {
      score1Total: first.length,
      score2Total: secondRows.length,
      matchedCount: pairs.length,
    },
    heatmap,
    confusionMatrix,
    statistics: {
      matchedCount: pairs.length,
      mean1: isNumeric ? mean(firstValues) : null,
      mean2: isNumeric ? mean(secondValues) : null,
      std1: isNumeric ? standardDeviation(firstValues) : null,
      std2: isNumeric ? standardDeviation(secondValues) : null,
      pearsonCorrelation:
        isNumeric && !identical ? pearson(pairedFirst, pairedSecond) : null,
      spearmanCorrelation:
        isNumeric && !identical
          ? pearson(ranks(pairedFirst), ranks(pairedSecond))
          : null,
      mae: isNumeric ? mean(differences.map(Math.abs)) : null,
      rmse: isNumeric
        ? Math.sqrt(mean(differences.map((value) => value ** 2)) ?? 0)
        : null,
    },
    timeSeries: numericTimeSeries(
      first,
      identical ? [] : secondRows,
      input.interval,
    ),
    distribution1: isNumeric
      ? numericDistribution(firstValues, globalMin, globalMax, input.nBins)
      : categoricalDistribution(first),
    distribution2: isNumeric
      ? numericDistribution(secondValues, globalMin, globalMax, input.nBins)
      : categoricalDistribution(secondRows),
    stackedDistribution,
    stackedDistributionMatched,
    score2Categories: [...new Set(secondRows.map(scoreCategory))].sort(),
    timeSeriesMatched: numericTimeSeries(
      pairedFirstScores,
      identical ? [] : pairedSecondScores,
      input.interval,
    ),
    distribution1Matched: isNumeric
      ? numericDistribution(pairedFirst, globalMin, globalMax, input.nBins)
      : categoricalDistribution(pairedFirstScores),
    distribution2Matched: isNumeric
      ? numericDistribution(pairedSecond, globalMin, globalMax, input.nBins)
      : categoricalDistribution(pairedSecondScores),
    distribution1Individual: isNumeric
      ? numericDistribution(firstValues, min1, max1, input.nBins)
      : categoricalDistribution(first),
    distribution2Individual: isNumeric
      ? numericDistribution(secondValues, min2, max2, input.nBins)
      : categoricalDistribution(secondRows),
    timeSeriesCategorical1: categoricalTimeSeries(first, input.interval),
    timeSeriesCategorical2: identical
      ? []
      : categoricalTimeSeries(secondRows, input.interval),
    timeSeriesCategorical1Matched: categoricalTimeSeries(
      pairedFirstScores,
      input.interval,
    ),
    timeSeriesCategorical2Matched: identical
      ? []
      : categoricalTimeSeries(pairedSecondScores, input.interval),
    samplingMetadata: {
      isSampled: sampled,
      samplingMethod: sampled ? ("limit" as const) : ("none" as const),
      samplingRate,
      estimatedTotalMatches: counts.matchedCount,
      actualSampleSize: pairs.length,
      samplingExpression: null,
      preflightEstimates: {
        score1Count: counts.score1Count,
        score2Count: counts.score2Count,
        estimatedMatchedCount: counts.matchedCount,
      },
      adaptiveFinal: {
        usedFinal: true,
        reason:
          "Doris current-state tables already expose merged score versions",
      },
    },
    metadata: {
      mode: input.mode,
      isSameScore: identical,
      dataType: input.score1.dataType,
    },
  };
}
