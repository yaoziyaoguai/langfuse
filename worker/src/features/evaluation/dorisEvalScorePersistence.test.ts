import { describe, expect, it, vi } from "vitest";
import { ScoreDataTypeEnum } from "@langfuse/shared";

import type { EvalScoreWritePayload } from "./evalScoreEvent";
import {
  buildDorisEvalScoreOperationId,
  persistDorisEvalScoreBatch,
} from "./dorisEvalScorePersistence";

vi.mock("../../env", () => ({
  env: {
    LANGFUSE_S3_EVENT_UPLOAD_BUCKET: "test-bucket",
    LANGFUSE_S3_EVENT_UPLOAD_PREFIX: "test-prefix/",
  },
}));

vi.mock("../../analyticsRuntime", () => ({
  getWorkerAnalyticsAdmissionContext: vi.fn(() => null),
}));

const scorePayload = (timestamp: string): EvalScoreWritePayload => ({
  eventId: "f5cb87c2-4924-5108-bc61-67aa0ab3185e",
  scoreId: "f5cb87c2-4924-5108-bc61-67aa0ab3185e",
  event: {
    id: "f5cb87c2-4924-5108-bc61-67aa0ab3185e",
    timestamp,
    type: "score-create",
    body: {
      id: "f5cb87c2-4924-5108-bc61-67aa0ab3185e",
      traceId: "trace-1",
      name: "quality",
      source: "EVAL",
      value: 0.9,
      dataType: ScoreDataTypeEnum.NUMERIC,
    },
  },
});

describe("persistDorisEvalScoreBatch", () => {
  it("accepts one deterministic operation and returns only after VISIBLE", async () => {
    const accept = vi.fn().mockResolvedValue({
      operationId: buildDorisEvalScoreOperationId("job-1"),
      status: "ACCEPTED",
    });
    const getStatus = vi
      .fn()
      .mockResolvedValueOnce(undefined)
      .mockResolvedValueOnce({ status: "PERSISTED", reasonCode: null })
      .mockResolvedValueOnce({ status: "VISIBLE", reasonCode: null });
    const wait = vi.fn().mockResolvedValue(undefined);
    const admissionContext = {
      runtimeLeaseId: "worker-lease",
      backend: "doris" as const,
      deploymentGeneration: 1n,
    };
    const acceptedAt = new Date("2026-07-23T10:00:02.000Z");

    await expect(
      persistDorisEvalScoreBatch(
        {
          projectId: "project-1",
          jobExecutionId: "job-1",
          scoreWritePayloads: [scorePayload("2026-07-23T10:00:00.000Z")],
          maxWaitMs: 1_000,
          pollIntervalMs: 1,
        },
        {
          accept,
          getStatus,
          getAdmissionContext: vi.fn(() => admissionContext),
          getStorageService: vi.fn(() => ({ storage: true }) as never),
          now: vi.fn(() => acceptedAt),
          wait,
        },
      ),
    ).resolves.toEqual({
      operationId: buildDorisEvalScoreOperationId("job-1"),
    });

    expect(accept).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        operationId: buildDorisEvalScoreOperationId("job-1"),
        sourceOperationId: "evaluation:job-1",
        acceptedAt,
        admissionContext,
        envelope: expect.objectContaining({
          formatVersion: 1,
          source: "score",
          payload: [scorePayload("2026-07-23T10:00:00.000Z").event],
        }),
      }),
    );
    expect(wait).toHaveBeenCalledOnce();
    expect(getStatus).toHaveBeenCalledTimes(3);
  });

  it("reuses an existing durable operation without accepting a changed retry payload", async () => {
    const accept = vi.fn();
    const getStatus = vi
      .fn()
      .mockResolvedValueOnce({ status: "PERSISTED", reasonCode: null })
      .mockResolvedValueOnce({ status: "VISIBLE", reasonCode: null });

    await expect(
      persistDorisEvalScoreBatch(
        {
          projectId: "project-1",
          jobExecutionId: "job-1",
          scoreWritePayloads: [
            {
              ...scorePayload("2026-07-23T10:00:01.000Z"),
              event: {
                ...scorePayload("2026-07-23T10:00:01.000Z").event,
                body: {
                  ...scorePayload("2026-07-23T10:00:01.000Z").event.body,
                  value: 0.1,
                },
              },
            },
          ],
          maxWaitMs: 1_000,
          pollIntervalMs: 1,
        },
        {
          accept,
          getStatus,
          getAdmissionContext: vi.fn(() => ({
            runtimeLeaseId: "worker-lease",
            backend: "doris",
            deploymentGeneration: 1n,
          })),
          getStorageService: vi.fn(() => ({ storage: true }) as never),
          wait: vi.fn().mockResolvedValue(undefined),
        },
      ),
    ).resolves.toEqual({
      operationId: buildDorisEvalScoreOperationId("job-1"),
    });

    expect(accept).not.toHaveBeenCalled();
  });

  it("fails visibly when canonical score ingestion terminalizes", async () => {
    await expect(
      persistDorisEvalScoreBatch(
        {
          projectId: "project-1",
          jobExecutionId: "job-1",
          scoreWritePayloads: [scorePayload("2026-07-23T10:00:00.000Z")],
          maxWaitMs: 1_000,
          pollIntervalMs: 1,
        },
        {
          accept: vi.fn().mockResolvedValue({
            operationId: buildDorisEvalScoreOperationId("job-1"),
            status: "ACCEPTED",
          }),
          getStatus: vi.fn().mockResolvedValue({
            status: "QUARANTINED",
            reasonCode: "INVALID_SCORE",
          }),
          getAdmissionContext: vi.fn(() => ({
            runtimeLeaseId: "worker-lease",
            backend: "doris",
            deploymentGeneration: 1n,
          })),
          getStorageService: vi.fn(() => ({ storage: true }) as never),
          wait: vi.fn(),
        },
      ),
    ).rejects.toThrow("QUARANTINED");
  });

  it("rejects score persistence without an admitted Doris runtime", async () => {
    const accept = vi.fn();

    await expect(
      persistDorisEvalScoreBatch(
        {
          projectId: "project-1",
          jobExecutionId: "job-1",
          scoreWritePayloads: [scorePayload("2026-07-23T10:00:00.000Z")],
        },
        {
          accept,
          getStatus: vi.fn(),
          getAdmissionContext: vi.fn(() => null),
          getStorageService: vi.fn(),
          wait: vi.fn(),
        },
      ),
    ).rejects.toThrow("runtime is not admitted");

    expect(accept).not.toHaveBeenCalled();
  });
});
