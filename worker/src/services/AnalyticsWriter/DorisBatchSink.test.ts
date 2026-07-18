import {
  canonicalPayloadHash,
  normalizeVersionToken,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsEntity,
} from "@langfuse/shared/analytics-persistence";
import { describe, expect, it, vi } from "vitest";

import {
  AnalyticsLoadAdmissionController,
  DorisBatchSink,
  prepareDorisLoadBatches,
} from "./DorisBatchSink";

const acceptedAt = normalizeVersionToken("2026-07-18T12:00:00.123456789Z");
const startTime = normalizeVersionToken("2026-07-17T10:00:00.123456789Z");
const sourceVersion = normalizeVersionToken("2026-07-17T10:02:00.987654321Z");

function entities(): readonly CanonicalAnalyticsEntity[] {
  const common = {
    projectId: "project-1",
    partitionDate: "2026-07-17",
    canonicalizerVersion: "1",
    schemaVersion: 3,
    systemTimestamp: acceptedAt,
    rawObjectKey: "events/project-1/raw/operation-1.json",
    resolvedEnrichmentIds: {},
  } as const;
  return [
    {
      ...common,
      kind: "event",
      sourceContract: "v4",
      sourceVersion,
      canonicalPayloadHash: canonicalPayloadHash({ kind: "event" }),
      traceId: "trace-1",
      spanId: "span-1",
      parentSpanId: "parent-1",
      type: "GENERATION",
      name: "generation",
      environment: "production",
      version: "version-1",
      release: "release-1",
      traceName: "trace-name",
      startTime,
      endTime: normalizeVersionToken("2026-07-17T10:01:00.999999999Z"),
      completionStartTime: normalizeVersionToken(
        "2026-07-17T10:00:30.000001999Z",
      ),
      userId: "user-1",
      sessionId: "session-1",
      level: "DEFAULT",
      statusMessage: "completed",
      isAppRoot: false,
      bookmarked: true,
      public: false,
      tags: ["alpha", "🚀"],
      input: { question: "价格" },
      output: "answer",
      metadata: { nested: { region: "eu" } },
      providedModelName: "gpt-test",
      internalModelId: "model-1",
      promptId: "prompt-1",
      promptName: "support",
      promptVersion: 3,
      modelParameters: { temperature: 0 },
      providedUsageDetails: { input: 9 },
      usageDetails: { input: 10, output: 2, total: 12 },
      providedCostDetails: { input: 0.4 },
      costDetails: { input: 0.5, output: 0.2, total: 0.7 },
      totalCost: 0.7,
      toolDefinitions: { search: "{}" },
      toolCalls: ["search"],
      toolCallNames: ["search"],
      source: "sdk",
      ingestionSdkName: "langfuse-js",
      ingestionSdkVersion: "4.0.0",
      serviceName: "api",
      telemetrySdkLanguage: "nodejs",
      eventBytes: 123,
    },
    {
      ...common,
      kind: "score",
      sourceContract: "score",
      sourceVersion: sourceVersion + 1n,
      canonicalPayloadHash: canonicalPayloadHash({ kind: "score" }),
      scoreId: "score-1",
      traceId: "trace-1",
      observationId: "span-1",
      sessionId: null,
      timestamp: startTime,
      name: "quality",
      source: "API",
      dataType: "NUMERIC",
      numericValue: 0.9,
      stringValue: null,
      longStringValue: null,
      booleanValue: null,
      comment: "good",
      authorUserId: "user-1",
      configId: null,
      queueId: null,
      environment: "production",
      metadata: { evaluator: "human" },
    },
    {
      ...common,
      kind: "fileReference",
      owningTraceId: null,
      sourceContract: "file-reference",
      sourceVersion: sourceVersion + 2n,
      canonicalPayloadHash: canonicalPayloadHash({ kind: "fileReference" }),
      entityType: "EVENT",
      entityId: "span-1",
      fileId: "file-1",
      eventId: "event-1",
      bucketName: "media",
      bucketPath: "project-1/file-1.bin",
    },
  ];
}

function batch(
  input: readonly CanonicalAnalyticsEntity[],
): CanonicalAnalyticsBatch {
  return {
    projectId: "project-1",
    operationId: "operation-1",
    canonicalizerVersion: "1",
    schemaVersion: 3,
    acceptedAt,
    rawObjectKey: "events/project-1/raw/operation-1.json",
    children: input.map((entity) => ({
      entity,
      expectedSourceVersion: null,
      fenceGeneration: 1n,
      traceDeletionGeneration: 0n,
      projectDeletionGeneration: 0n,
    })),
  };
}

describe("DorisBatchSink", () => {
  it("prepares deterministic exact rows for every R1A entity table", () => {
    const source = entities();
    const prepared = prepareDorisLoadBatches(batch(source));
    const reordered = prepareDorisLoadBatches(batch([...source].reverse()));
    expect(reordered).toEqual(prepared);
    expect(prepared).toHaveLength(3);

    const eventBatch = prepared.find(
      ({ targetTable }) => targetTable === "events_current",
    )!;
    const event = JSON.parse(eventBatch.ndjsonBody.trim()) as Record<
      string,
      unknown
    >;
    expect(event).toMatchObject({
      project_id: "project-1",
      partition_date: "2026-07-17",
      trace_id: "trace-1",
      span_id: "span-1",
      version_token: sourceVersion.toString(),
      status_message: "completed",
      release: "release-1",
      version: "version-1",
      trace_name: "trace-name",
      start_time: "2026-07-17 10:00:00.123456",
      end_time: "2026-07-17 10:01:00.999999",
      completion_start_time: "2026-07-17 10:00:30.000001",
      input: '{"question":"价格"}',
      output: "answer",
      total_input_tokens: "10",
      total_output_tokens: "2",
      total_cost: "0.7",
    });
    expect(eventBatch.payloadHash).toMatch(/^[a-f0-9]{64}$/);

    const score = JSON.parse(
      prepared
        .find(({ targetTable }) => targetTable === "scores_current")!
        .ndjsonBody.trim(),
    ) as Record<string, unknown>;
    expect(score).toMatchObject({
      score_id: "score-1",
      version_token: (sourceVersion + 1n).toString(),
      data_type: "NUMERIC",
      value: 0.9,
      timestamp: "2026-07-17 10:00:00.123456",
    });

    const file = JSON.parse(
      prepared
        .find(({ targetTable }) => targetTable === "blob_storage_file_log")!
        .ndjsonBody.trim(),
    ) as Record<string, unknown>;
    expect(file).toMatchObject({
      entity_type: "EVENT",
      entity_id: "span-1",
      file_id: "file-1",
      version_token: (sourceVersion + 2n).toString(),
      bucket_path: "project-1/file-1.bin",
    });
  });

  it("delegates a prepared batch to the hardened Stream Load client", async () => {
    const load = vi.fn().mockResolvedValue({
      status: "Success",
      label: "lf_test_label",
      numberTotalRows: 1,
      numberFilteredRows: 0,
      committed: true,
      requiresReconciliation: false,
    });
    const sink = new DorisBatchSink({ load, reconcile: vi.fn() });
    const prepared = prepareDorisLoadBatches(batch([entities()[0]!]))[0]!;

    await expect(sink.load(prepared, "lf_test_label")).resolves.toMatchObject({
      committed: true,
      numberFilteredRows: 0,
    });
    expect(load).toHaveBeenCalledWith({
      database: undefined,
      table: "events_current",
      label: "lf_test_label",
      ndjsonBody: prepared.ndjsonBody,
    });
  });

  it("bounds in-flight loads and rejects buffered-byte overflow", async () => {
    const controller = new AnalyticsLoadAdmissionController({
      maxBatchBytes: 8,
      maxInflightLoads: 1,
      globalBufferedByteCap: 10,
    });
    let releaseFirst!: () => void;
    const first = controller.run(
      6,
      () =>
        new Promise<string>((resolve) => {
          releaseFirst = () => resolve("first");
        }),
    );
    const secondTask = vi.fn(async () => "second");
    const second = controller.run(4, secondTask);

    await expect(
      controller.run(1, async () => "overflow"),
    ).rejects.toMatchObject({
      code: "ANALYTICS_RESOURCE_EXHAUSTED",
      retryable: true,
    });
    expect(secondTask).not.toHaveBeenCalled();
    releaseFirst();
    await expect(first).resolves.toBe("first");
    await expect(second).resolves.toBe("second");
    expect(controller.snapshot()).toEqual({
      bufferedBytes: 0,
      inflightLoads: 0,
      waitingLoads: 0,
    });
    await expect(
      controller.run(9, async () => "oversized"),
    ).rejects.toMatchObject({
      code: "ANALYTICS_RESOURCE_EXHAUSTED",
      retryable: false,
    });
  });
});
