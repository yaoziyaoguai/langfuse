import { describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({ backend: "clickhouse" }));
const queueNames = vi.hoisted(() => ({
  ProjectDelete: "project-delete",
  TraceDelete: "trace-delete",
  ScoreDelete: "score-delete",
  BatchActionQueue: "batch-action-queue",
  DataRetentionProcessingQueue: "data-retention-processing-queue",
}));

vi.mock("@langfuse/shared/src/server", () => ({
  QueueName: queueNames,
  getQueue: vi.fn(),
  logger: { info: vi.fn(), error: vi.fn() },
  recordHistogram: vi.fn(),
}));

vi.mock("../../env", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return state.backend;
    },
  },
}));

import { DlqRetryService } from "./dlqRetryService";

describe("DlqRetryService", () => {
  it("retries ClickHouse retention jobs only on the ClickHouse backend", () => {
    state.backend = "clickhouse";
    expect(DlqRetryService.getRetryQueues()).toContain(
      queueNames.DataRetentionProcessingQueue,
    );

    state.backend = "doris";
    expect(DlqRetryService.getRetryQueues()).not.toContain(
      queueNames.DataRetentionProcessingQueue,
    );
    expect(DlqRetryService.getRetryQueues()).toContain(queueNames.TraceDelete);
  });
});
