import { z } from "zod";

export const AnalyticsIngestionOperationQuery = z.object({
  operationId: z.string().min(1),
});

const nullableDate = z.iso.datetime().nullable();

export const AnalyticsIngestionOperationResponse = z.union([
  z.object({ message: z.literal("Ingestion operation not found") }),
  z.object({
    operationId: z.string(),
    status: z.enum([
      "ACCEPTED",
      "QUEUED",
      "PERSISTED",
      "VISIBLE",
      "RETRYING",
      "PARTIAL_FAILED",
      "QUARANTINED",
      "UNRECOVERABLE",
      "CANCELLED_BY_DELETION",
      "COMPLETED_WITH_CANCELLATIONS",
    ]),
    manifest: z.enum(["PENDING", "CANDIDATE_PUBLISHED", "FROZEN"]),
    outbox: z.enum(["PENDING", "PUBLISHED"]),
    acceptedAt: z.iso.datetime(),
    recoverableUntil: z.iso.datetime(),
    statusExpiresAt: z.iso.datetime(),
    visibleAt: nullableDate,
    terminalAt: nullableDate,
    reasonCode: z.string().nullable(),
    candidates: z.array(
      z.object({
        candidateKey: z.string(),
        entityType: z.enum(["EVENT", "SCORE", "FILE_REFERENCE"]),
        disposition: z.enum([
          "PENDING",
          "LOAD_REQUIRED",
          "NOOP",
          "QUARANTINED",
          "CANCELLED_BY_DELETION",
        ]),
        loadBatchId: z.string().nullable(),
        reasonCode: z.string().nullable(),
        entityLink: z.url().nullable(),
      }),
    ),
    loads: z.array(
      z.object({
        id: z.string(),
        entityType: z.enum(["EVENT", "SCORE", "FILE_REFERENCE"]),
        status: z.enum([
          "PENDING",
          "LOADING",
          "VISIBLE",
          "UNKNOWN",
          "FAILED",
          "CANCELLED_BY_DELETION",
        ]),
        totalRows: z.number().int().nullable(),
        filteredRows: z.number().int().nullable(),
        lastErrorCode: z.string().nullable(),
        visibleAt: nullableDate,
      }),
    ),
    guidance: z.string(),
  }),
]);
