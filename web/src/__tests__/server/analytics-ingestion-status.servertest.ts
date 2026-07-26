import {
  encodeDatasetRunItemIdentity,
  encodeEventIdentity,
  encodeScoreIdentity,
} from "@langfuse/shared/src/server";
import { describe, expect, it } from "vitest";

import { serializeAnalyticsIngestionStatus } from "@/src/features/public-api/server/analyticsIngestionStatus";
import { AnalyticsIngestionOperationResponse } from "@/src/features/public-api/types/analyticsIngestion";

describe("analytics ingestion status response", () => {
  it("exposes links only for visible candidates and omits internal identities", () => {
    const projectId = "project-status";
    const operation = {
      projectId,
      operationId: "operation-status",
      status: "VISIBLE" as const,
      manifest: "FROZEN" as const,
      outbox: "PUBLISHED" as const,
      acceptedAt: new Date("2026-07-18T13:00:00.000Z"),
      recoverableUntil: new Date("2026-07-25T13:00:00.000Z"),
      statusExpiresAt: new Date("2026-08-18T13:00:00.000Z"),
      visibleAt: new Date("2026-07-18T13:00:01.000Z"),
      terminalAt: new Date("2026-07-18T13:00:01.000Z"),
      reasonCode: null,
      candidates: [
        {
          candidateKey: "candidate-visible",
          entityType: "EVENT" as const,
          entityKey: encodeEventIdentity({
            projectId,
            traceId: "trace-visible",
            spanId: "span-visible",
          }),
          owningTraceId: "trace-visible",
          disposition: "LOAD_REQUIRED" as const,
          loadBatchId: "load-visible",
          reasonCode: null,
        },
        {
          candidateKey: "candidate-pending",
          entityType: "SCORE" as const,
          entityKey: encodeScoreIdentity({
            projectId,
            scoreId: "score-pending",
          }),
          owningTraceId: "trace-pending",
          disposition: "LOAD_REQUIRED" as const,
          loadBatchId: "load-pending",
          reasonCode: null,
        },
        {
          candidateKey: "candidate-run-item",
          entityType: "DATASET_RUN_ITEM" as const,
          entityKey: encodeDatasetRunItemIdentity({
            projectId,
            runItemId: "run-item-visible",
          }),
          owningTraceId: "trace-visible",
          disposition: "LOAD_REQUIRED" as const,
          loadBatchId: "load-run-item",
          reasonCode: null,
        },
      ],
      loads: [
        {
          id: "load-visible",
          targetTable: "events",
          status: "VISIBLE" as const,
          totalRows: 1,
          filteredRows: 0,
          lastErrorCode: null,
          visibleAt: new Date("2026-07-18T13:00:01.000Z"),
        },
        {
          id: "load-pending",
          targetTable: "scores",
          status: "PENDING" as const,
          totalRows: null,
          filteredRows: null,
          lastErrorCode: null,
          visibleAt: null,
        },
        {
          id: "load-run-item",
          targetTable: "dataset_run_items",
          status: "VISIBLE" as const,
          totalRows: 1,
          filteredRows: 0,
          lastErrorCode: null,
          visibleAt: new Date("2026-07-18T13:00:01.000Z"),
        },
      ],
    };

    const response = serializeAnalyticsIngestionStatus(operation);

    expect(AnalyticsIngestionOperationResponse.parse(response)).toEqual(
      response,
    );
    expect(response.candidates[0]?.entityLink).toContain(
      "/project/project-status/traces/trace-visible?observation=span-visible",
    );
    expect(response.candidates[1]?.entityLink).toBeNull();
    expect(response.candidates[2]).toMatchObject({
      entityType: "DATASET_RUN_ITEM",
      entityLink: null,
    });
    expect(JSON.stringify(response)).not.toContain(
      operation.candidates[0]!.entityKey,
    );
    expect(JSON.stringify(response)).not.toContain("score-pending");
  });
});
