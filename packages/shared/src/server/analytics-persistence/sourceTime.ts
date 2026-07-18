import { AnalyticsPersistenceError } from "./errors";
import {
  normalizeVersionToken,
  partitionDateFromVersionToken,
  type VersionTokenInput,
} from "./canonicalHash";
import type { AnalyticsSourceContract } from "./types";

export type OtlpNanoTimestamp =
  | bigint
  | string
  | { readonly high: number; readonly low: number }
  | null
  | undefined;

export interface CanonicalSourceTime {
  readonly sourceContract: Extract<AnalyticsSourceContract, "v4" | "otlp">;
  readonly sourceVersion: bigint;
  readonly startTime: bigint;
  readonly endTime: bigint | null;
  readonly partitionDate: string;
}

export interface CanonicalScoreSourceTime {
  readonly sourceContract: "score";
  readonly sourceVersion: bigint;
  readonly timestamp: bigint;
  readonly partitionDate: string;
}

export interface CanonicalFileReferenceSourceTime {
  readonly sourceContract: "file-reference";
  readonly sourceVersion: bigint;
  readonly partitionDate: string;
}

function validationError(): AnalyticsPersistenceError {
  return new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
}

function normalizeRequired(input: VersionTokenInput): bigint {
  try {
    return normalizeVersionToken(input);
  } catch {
    throw validationError();
  }
}

export function deriveV4SourceTime(input: {
  readonly envelopeTimestamp: VersionTokenInput;
  readonly bodyStartTime?: VersionTokenInput | null;
  readonly bodyEndTime?: VersionTokenInput | null;
}): CanonicalSourceTime {
  const sourceVersion = normalizeRequired(input.envelopeTimestamp);
  const startTime =
    input.bodyStartTime == null
      ? sourceVersion
      : normalizeRequired(input.bodyStartTime);
  const endTime =
    input.bodyEndTime == null ? null : normalizeRequired(input.bodyEndTime);
  if (endTime !== null && endTime < startTime) {
    throw new AnalyticsPersistenceError("ANALYTICS_INVALID_TIME_RANGE", false);
  }
  return {
    sourceContract: "v4",
    sourceVersion,
    startTime,
    endTime,
    partitionDate: partitionDateFromVersionToken(startTime),
  };
}

function normalizeOtlpTimestamp(
  input: Exclude<OtlpNanoTimestamp, null | undefined>,
): bigint {
  if (typeof input === "object") {
    if (
      !Number.isInteger(input.high) ||
      !Number.isInteger(input.low) ||
      input.high < 0 ||
      input.high > 0x7fffffff ||
      input.low < -0x80000000 ||
      input.low > 0xffffffff
    ) {
      throw validationError();
    }
    const token = (BigInt(input.high) << 32n) | BigInt(input.low >>> 0);
    return normalizeRequired(token);
  }
  return normalizeRequired(input);
}

export function deriveOtlpSourceTime(input: {
  readonly startTimeUnixNano: OtlpNanoTimestamp;
  readonly endTimeUnixNano: OtlpNanoTimestamp;
}): CanonicalSourceTime {
  const hasStart = input.startTimeUnixNano != null;
  const hasEnd = input.endTimeUnixNano != null;
  if (!hasStart && !hasEnd) throw validationError();

  const normalizedStart = hasStart
    ? normalizeOtlpTimestamp(
        input.startTimeUnixNano as Exclude<OtlpNanoTimestamp, null | undefined>,
      )
    : null;
  const normalizedEnd = hasEnd
    ? normalizeOtlpTimestamp(
        input.endTimeUnixNano as Exclude<OtlpNanoTimestamp, null | undefined>,
      )
    : null;
  const startTime = normalizedStart ?? normalizedEnd;
  const endTime = normalizedEnd ?? normalizedStart;
  if (startTime === null || endTime === null) throw validationError();
  if (endTime < startTime) {
    throw new AnalyticsPersistenceError("ANALYTICS_INVALID_TIME_RANGE", false);
  }
  return {
    sourceContract: "otlp",
    sourceVersion: endTime,
    startTime,
    endTime,
    partitionDate: partitionDateFromVersionToken(startTime),
  };
}

export function deriveScoreSourceTime(input: {
  readonly timestamp: VersionTokenInput | null | undefined;
  readonly updatedAt?: VersionTokenInput | null;
}): CanonicalScoreSourceTime {
  if (input.timestamp == null) throw validationError();
  const timestamp = normalizeRequired(input.timestamp);
  const sourceVersion =
    input.updatedAt == null ? timestamp : normalizeRequired(input.updatedAt);
  return {
    sourceContract: "score",
    sourceVersion,
    timestamp,
    partitionDate: partitionDateFromVersionToken(timestamp),
  };
}

function normalizePartitionDate(value: string): string {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) throw validationError();
  try {
    const token = normalizeVersionToken(`${value}T00:00:00Z`);
    if (partitionDateFromVersionToken(token) !== value) throw validationError();
    return value;
  } catch {
    throw validationError();
  }
}

export function deriveFileReferenceSourceTime(input: {
  readonly parentSourceVersion: VersionTokenInput;
  readonly parentPartitionDate: string;
}): CanonicalFileReferenceSourceTime {
  return {
    sourceContract: "file-reference",
    sourceVersion: normalizeRequired(input.parentSourceVersion),
    partitionDate: normalizePartitionDate(input.parentPartitionDate),
  };
}
