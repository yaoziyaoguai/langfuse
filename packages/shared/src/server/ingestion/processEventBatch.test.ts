import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  acceptAnalyticsIngestion: vi.fn(),
  storageService: {},
}));

vi.mock("../analytics-persistence", () => ({
  acceptAnalyticsIngestion: mocks.acceptAnalyticsIngestion,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION: "1",
  CURRENT_ANALYTICS_SCHEMA_VERSION: 1,
}));

vi.mock("../instrumentation", () => ({
  getCurrentSpan: vi.fn(() => undefined),
  recordDistribution: vi.fn(),
  recordIncrement: vi.fn(),
}));

vi.mock("../logger", () => ({
  logger: { info: vi.fn(), warn: vi.fn() },
}));

vi.mock("../s3", () => ({
  getS3EventStorageClient: vi.fn(() => mocks.storageService),
}));

vi.mock("../../env", () => ({
  env: {
    LANGFUSE_ANALYTICS_BACKEND: "doris",
    LANGFUSE_S3_EVENT_UPLOAD_BUCKET: "events",
    LANGFUSE_S3_EVENT_UPLOAD_PREFIX: "raw/",
  },
}));

import type { AuthHeaderValidVerificationResultIngestion } from "../auth/types";
import { processEventBatch } from "./processEventBatch";

const auth = {
  validKey: true,
  scope: { projectId: "project-1", accessLevel: "project" },
} satisfies AuthHeaderValidVerificationResultIngestion;

const options = {
  attribution: {
    ingestionApiKey: "pk-test",
    ingestionSdkName: "javascript",
    ingestionSdkVersion: "5.0.0",
  },
};

describe("processEventBatch", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.acceptAnalyticsIngestion.mockResolvedValue({
      operationId: "operation-1",
      status: "ACCEPTED",
    });
  });

  it("accepts scores through durable Doris analytics ingestion", async () => {
    const event = {
      id: "score-event-1",
      type: "score-create",
      timestamp: "2026-07-18T14:00:00.123Z",
      body: {
        id: "score-1",
        name: "quality",
        value: 1,
        traceId: "trace-1",
      },
    };

    await expect(processEventBatch([event], auth, options)).resolves.toEqual({
      successes: [{ id: event.id, status: 201 }],
      errors: [],
    });
    expect(mocks.acceptAnalyticsIngestion).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        envelope: expect.objectContaining({
          source: "score",
          payload: [expect.objectContaining({ id: event.id })],
        }),
      }),
    );
  });

  it("accepts legacy tracing through the durable Doris canonical pipeline", async () => {
    const event = {
      id: "trace-event-1",
      type: "trace-create",
      timestamp: "2026-07-18T14:00:00.123Z",
      body: { id: "trace-1", name: "legacy trace" },
    };

    await expect(processEventBatch([event], auth, options)).resolves.toEqual({
      successes: [{ id: event.id, status: 201 }],
      errors: [],
    });
    expect(mocks.acceptAnalyticsIngestion).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        operationId: expect.stringMatching(/^[a-f0-9]{64}$/),
        sourceOperationId: expect.stringMatching(
          /^legacy:[a-f0-9]{64}:[a-f0-9]{64}$/,
        ),
        envelope: expect.objectContaining({
          source: "legacy-event",
          payload: [expect.objectContaining({ id: event.id })],
        }),
      }),
    );
  });

  it("keeps unsupported dataset-run ingestion fail-closed in Doris mode", async () => {
    const event = {
      id: "dataset-event-1",
      type: "dataset-run-item-create",
      timestamp: "2026-07-18T14:00:00.123Z",
      body: {
        id: "dataset-run-item-1",
        traceId: "trace-1",
        datasetId: "dataset-1",
        runId: "run-1",
        datasetItemId: "item-1",
      },
    };

    await expect(
      processEventBatch([event], auth, {
        ...options,
        isLangfuseInternal: true,
      }),
    ).resolves.toMatchObject({
      successes: [],
      errors: [
        expect.objectContaining({
          id: event.id,
          status: 501,
          code: "R1B_EXPERIMENTS_UNAVAILABLE",
        }),
      ],
    });
    expect(mocks.acceptAnalyticsIngestion).not.toHaveBeenCalled();
  });
});
