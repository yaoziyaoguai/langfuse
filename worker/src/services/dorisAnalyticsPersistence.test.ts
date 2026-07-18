import { describe, expect, it, vi } from "vitest";

import { EventCanonicalizer } from "./EventCanonicalizer";
import { RawAnalyticsIngestionCanonicalizer } from "./RawAnalyticsIngestionCanonicalizer";
import { createDorisAnalyticsPersistence } from "./dorisAnalyticsPersistence";

describe("createDorisAnalyticsPersistence", () => {
  it("owns the raw canonicalizer and queue processor for the durable path", () => {
    const composition = createDorisAnalyticsPersistence({
      runtimeEnv: {
        LANGFUSE_S3_EVENT_UPLOAD_BUCKET: "test-bucket",
      },
      prismaClient: {} as never,
      storageService: {
        download: vi.fn(),
      } as never,
      streamLoadTransport: {
        load: vi.fn(),
        reconcile: vi.fn(),
      },
      databaseName: "langfuse_test",
      workerId: "worker-1",
      eventCanonicalizer: new EventCanonicalizer({
        warnOnUsageTotalMismatch: vi.fn(),
        resolvePrompt: vi.fn(async () => null),
        resolveGenerationUsage: vi.fn(async () => null),
      }),
    });

    expect(composition.canonicalizer).toBeInstanceOf(
      RawAnalyticsIngestionCanonicalizer,
    );
    expect(composition.processor).toEqual(expect.any(Function));
  });
});
