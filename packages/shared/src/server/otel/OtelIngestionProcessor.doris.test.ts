import { describe, expect, it, vi } from "vitest";

import { OtelIngestionProcessor } from "./OtelIngestionProcessor";

describe("OtelIngestionProcessor Doris acceptance", () => {
  it("persists the canonical raw envelope before returning an operation id", async () => {
    const acceptAnalytics = vi.fn().mockResolvedValue({
      operationId: "operation-1",
      status: "ACCEPTED",
    });
    const processor = new OtelIngestionProcessor({
      projectId: "project-1",
      publicKey: "pk-lf-test",
      sdkName: "python",
      sdkVersion: "4.0.0",
      acceptAnalytics,
      storageService: {} as never,
    });
    const resourceSpans = [{ scopeSpans: [] }];

    await expect(
      processor.publishToAnalyticsIngestion(resourceSpans),
    ).resolves.toEqual({ operationId: "operation-1", status: "ACCEPTED" });
    expect(acceptAnalytics).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        canonicalizerVersion: "1",
        schemaVersion: 1,
        envelope: {
          formatVersion: 1,
          source: "otlp",
          payload: resourceSpans,
          attribution: {
            ingestionApiKey: "pk-lf-test",
            ingestionSdkName: "python",
            ingestionSdkVersion: "4.0.0",
          },
        },
      }),
    );
  });

  it("dispatches each configured backend to exactly one ingestion path", async () => {
    const processor = new OtelIngestionProcessor({
      projectId: "project-1",
      publicKey: "pk-lf-test",
      sdkName: "python",
      sdkVersion: "4.0.0",
    });
    const resourceSpans = [{ scopeSpans: [] }];
    const publishDoris = vi
      .spyOn(processor, "publishToAnalyticsIngestion")
      .mockResolvedValue({ operationId: "operation-1", status: "ACCEPTED" });
    const publishClickHouse = vi
      .spyOn(processor, "publishToOtelIngestionQueue")
      .mockResolvedValue({} as never);

    await processor.publishToAnalyticsBackend(resourceSpans, "doris");
    expect(publishDoris).toHaveBeenCalledOnce();
    expect(publishClickHouse).not.toHaveBeenCalled();

    vi.clearAllMocks();
    await processor.publishToAnalyticsBackend(resourceSpans, "clickhouse");
    expect(publishClickHouse).toHaveBeenCalledOnce();
    expect(publishDoris).not.toHaveBeenCalled();
  });
});
