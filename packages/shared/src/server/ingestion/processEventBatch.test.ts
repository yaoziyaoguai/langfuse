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

  it("rejects legacy tracing with the stable R1A capability response", async () => {
    const event = {
      id: "trace-event-1",
      type: "trace-create",
      timestamp: "2026-07-18T14:00:00.123Z",
      body: { id: "trace-1", name: "legacy trace" },
    };

    await expect(processEventBatch([event], auth, options)).resolves.toEqual({
      successes: [],
      errors: [
        {
          id: event.id,
          status: 501,
          error: "UnsupportedFeature",
          code: "R2_LEGACY_INGESTION_UNAVAILABLE",
          message:
            "This ingestion event type is not available in the Doris R1A release.",
          recovery:
            "Use the OTLP traces endpoint for tracing data. Dataset-run analytics requires a separately approved R1B adoption.",
        },
      ],
    });
    expect(mocks.acceptAnalyticsIngestion).not.toHaveBeenCalled();
  });
});
