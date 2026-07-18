import { AnalyticsPersistenceError } from "./errors";
import {
  normalizeVersionToken,
  partitionDateFromVersionToken,
} from "./canonicalHash";
import type { CanonicalAnalyticsBatch } from "./types";

export const MAX_ANALYTICS_BATCH_CHILDREN = 10_000;

export interface AnalyticsBatchReceipt {
  readonly operationId: string;
  readonly status: "QUEUED" | "PERSISTED" | "VISIBLE";
}

export interface AnalyticsBatchSink {
  persist(batch: CanonicalAnalyticsBatch): Promise<AnalyticsBatchReceipt>;
}

function invalidBoundary(): never {
  throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
}

export function assertAnalyticsBatchBoundary(
  batch: CanonicalAnalyticsBatch,
): void {
  if (
    !batch.projectId ||
    !batch.operationId ||
    !batch.rawObjectKey ||
    !batch.canonicalizerVersion ||
    !Number.isSafeInteger(batch.schemaVersion) ||
    batch.schemaVersion <= 0 ||
    batch.acceptedAt < 0n ||
    batch.children.length === 0 ||
    batch.children.length > MAX_ANALYTICS_BATCH_CHILDREN
  ) {
    invalidBoundary();
  }

  try {
    normalizeVersionToken(batch.acceptedAt);
  } catch {
    invalidBoundary();
  }

  for (const child of batch.children) {
    if (
      child.entity.projectId !== batch.projectId ||
      child.entity.canonicalizerVersion !== batch.canonicalizerVersion ||
      child.entity.schemaVersion !== batch.schemaVersion ||
      child.entity.rawObjectKey !== batch.rawObjectKey ||
      child.entity.systemTimestamp !== batch.acceptedAt ||
      !/^[a-f0-9]{64}$/.test(child.entity.canonicalPayloadHash) ||
      (child.entity.kind === "event" &&
        child.entity.sourceContract !== "v4" &&
        child.entity.sourceContract !== "otlp") ||
      (child.entity.kind === "score" &&
        child.entity.sourceContract !== "score") ||
      (child.entity.kind === "fileReference" &&
        child.entity.sourceContract !== "file-reference") ||
      child.fenceGeneration <= 0n ||
      child.traceDeletionGeneration < 0n ||
      child.projectDeletionGeneration < 0n
    ) {
      invalidBoundary();
    }

    try {
      normalizeVersionToken(child.entity.sourceVersion);
      if (child.expectedSourceVersion !== null) {
        normalizeVersionToken(child.expectedSourceVersion);
      }
      if (
        child.entity.kind === "event" &&
        partitionDateFromVersionToken(child.entity.startTime) !==
          child.entity.partitionDate
      ) {
        invalidBoundary();
      }
      if (
        child.entity.kind === "score" &&
        partitionDateFromVersionToken(child.entity.timestamp) !==
          child.entity.partitionDate
      ) {
        invalidBoundary();
      }
    } catch {
      invalidBoundary();
    }
  }
}
