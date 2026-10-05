import { createHash } from "node:crypto";

import {
  encodeRawAnalyticsIngestionEnvelope,
  type RawAnalyticsIngestionEnvelope,
} from "@langfuse/shared/analytics-persistence";
import { describe, expect, it, vi } from "vitest";

import { EventCanonicalizer } from "./EventCanonicalizer";
import { RawAnalyticsIngestionCanonicalizer } from "./RawAnalyticsIngestionCanonicalizer";

function bufferId(hex: string) {
  return { type: "Buffer" as const, data: [...Buffer.from(hex, "hex")] };
}

function nanoTimestamp(value: bigint) {
  return {
    high: Number(value >> 32n),
    low: Number(value & 0xffff_ffffn),
    unsigned: true,
  };
}

function operation(body: string) {
  return {
    id: "operation-1",
    projectId: "project-1",
    rawObjectKey: "analytics-ingestion/raw/project-1/operation-1.json",
    sourceChecksum: createHash("sha256").update(body).digest("hex"),
    acceptedAtNanos: 1_784_383_200_123_000_000n,
    canonicalizerVersion: "r1a-v1",
    schemaVersion: 3,
  };
}

function canonicalizer(
  body: string,
  overrides: Partial<
    ConstructorParameters<typeof RawAnalyticsIngestionCanonicalizer>[0]
  > = {},
) {
  return new RawAnalyticsIngestionCanonicalizer({
    storageService: {
      downloadIfExists: vi.fn(async () => body),
    } as never,
    eventCanonicalizer: new EventCanonicalizer({
      warnOnUsageTotalMismatch: vi.fn(),
      resolvePrompt: vi.fn(async () => null),
      resolveGenerationUsage: vi.fn(async () => null),
    }),
    getProjectDeletionGeneration: vi.fn(async () => 2n),
    getTraceDeletionGeneration: vi.fn(async () => 3n),
    ...overrides,
  });
}

describe("RawAnalyticsIngestionCanonicalizer", () => {
  it("preserves exact OTLP nanoseconds while using the existing enrichment path", async () => {
    const start = 1_714_488_530_686_000_001n;
    const end = 1_714_488_530_687_000_009n;
    const envelope: RawAnalyticsIngestionEnvelope = {
      formatVersion: 1,
      source: "otlp",
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "python",
        ingestionSdkVersion: "4.0.0",
      },
      payload: [
        {
          resource: { attributes: [] },
          scopeSpans: [
            {
              scope: { name: "langfuse-sdk", version: "4.0.0" },
              spans: [
                {
                  traceId: bufferId("aabbccdd11223344aabbccdd11223344"),
                  spanId: bufferId("1122334455667788"),
                  name: "exact-time",
                  kind: 1,
                  startTimeUnixNano: nanoTimestamp(start),
                  endTimeUnixNano: nanoTimestamp(end),
                  attributes: [],
                  status: {},
                },
              ],
            },
          ],
        },
      ],
    };
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);

    const batch = await canonicalizer(body).canonicalize(operation(body));

    expect(batch).toMatchObject({
      operationId: "operation-1",
      projectId: "project-1",
      acceptedAt: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    expect(batch.children).toHaveLength(1);
    expect(batch.children[0]).toMatchObject({
      expectedSourceVersion: null,
      fenceGeneration: 1n,
      traceDeletionGeneration: 3n,
      projectDeletionGeneration: 2n,
      entity: {
        kind: "event",
        sourceContract: "otlp",
        startTime: start,
        endTime: end,
        sourceVersion: end,
        systemTimestamp: 1_784_383_200_123_000_000n,
        rawObjectKey: "analytics-ingestion/raw/project-1/operation-1.json",
      },
    });
  });

  it("canonicalizes current score and raw file-reference children", async () => {
    const envelope: RawAnalyticsIngestionEnvelope = {
      formatVersion: 1,
      source: "score",
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "javascript",
        ingestionSdkVersion: "5.0.0",
      },
      payload: [
        {
          id: "score-event-1",
          type: "score-create",
          timestamp: "2026-07-18T14:00:00.123456789Z",
          body: {
            id: "score-1",
            name: "quality",
            value: "detailed result",
            dataType: "TEXT",
            datasetRunId: "run-1",
            executionTraceId: "evaluation-trace-1",
            environment: "production",
            metadata: { evaluator: "human" },
          },
        },
      ],
    };
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);

    const batch = await canonicalizer(body).canonicalize(operation(body));

    expect(batch.children).toHaveLength(2);
    expect(batch.children.map(({ entity }) => entity.kind)).toEqual([
      "score",
      "fileReference",
    ]);
    expect(batch.children[0]?.entity).toMatchObject({
      kind: "score",
      scoreId: "score-1",
      sourceContract: "score",
      dataType: "TEXT",
      stringValue: "detailed result",
      traceId: null,
      observationId: null,
      datasetRunId: "run-1",
      executionTraceId: "evaluation-trace-1",
      metadata: { evaluator: "human" },
    });
    expect(batch.children[1]?.entity).toMatchObject({
      kind: "fileReference",
      entityType: "SCORE",
      entityId: "score-1",
      fileId: "operation-1",
      eventId: "score-event-1",
      bucketPath: "analytics-ingestion/raw/project-1/operation-1.json",
    });
  });

  it("resolves score configs through the injected control database", async () => {
    const envelope: RawAnalyticsIngestionEnvelope = {
      formatVersion: 1,
      source: "score",
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "internal",
        ingestionSdkVersion: "1",
      },
      payload: [
        {
          id: "configured-score-event-1",
          type: "score-create",
          timestamp: "2026-07-18T14:00:00.123Z",
          body: {
            id: "configured-score-1",
            name: "request-name",
            traceId: "trace-1",
            value: 0.9,
            dataType: "NUMERIC",
            configId: "11111111-1111-4111-8111-111111111111",
            source: "EVAL",
            environment: "production",
          },
        },
      ],
    };
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);
    const findFirst = vi.fn(async () => ({
      id: "11111111-1111-4111-8111-111111111111",
      createdAt: new Date("2026-07-18T12:00:00.000Z"),
      updatedAt: new Date("2026-07-18T12:00:00.000Z"),
      projectId: "project-1",
      name: "configured-quality",
      dataType: "NUMERIC",
      isArchived: false,
      minValue: null,
      maxValue: 1,
      categories: null,
      description: null,
    }));

    const batch = await canonicalizer(body, {
      client: { scoreConfig: { findFirst } } as never,
    }).canonicalize(operation(body));

    expect(findFirst).toHaveBeenCalledWith({
      where: {
        projectId: "project-1",
        id: "11111111-1111-4111-8111-111111111111",
      },
    });
    expect(batch.children[0]?.entity).toMatchObject({
      kind: "score",
      name: "configured-quality",
      configId: "11111111-1111-4111-8111-111111111111",
      resolvedEnrichmentIds: {
        configId: "11111111-1111-4111-8111-111111111111",
      },
    });
  });

  it("enriches a durable dataset-run-item from Postgres context", async () => {
    const envelope: RawAnalyticsIngestionEnvelope = {
      formatVersion: 1,
      source: "dataset-run-item",
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "internal",
        ingestionSdkVersion: "1",
      },
      payload: [
        {
          id: "run-item-event-1",
          type: "dataset-run-item-create",
          timestamp: "2026-07-18T14:00:00.123456789Z",
          body: {
            id: "run-item-1",
            runId: "run-1",
            datasetId: "dataset-1",
            datasetItemId: "item-1",
            datasetVersion: "2026-07-18T12:00:00.000Z",
            traceId: "trace-1",
            observationId: "span-1",
            createdAt: "2026-07-18T13:59:59.000000001Z",
          },
        },
      ],
    };
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);
    const loadDatasetRunItemContext = vi.fn(async () => ({
      run: {
        name: "run one",
        description: "description",
        metadata: { owner: "team-a" },
        createdAt: new Date("2026-07-18T11:00:00.000Z"),
      },
      item: {
        input: { question: "life" },
        expectedOutput: { answer: 42 },
        metadata: { difficulty: "hard" },
        validFrom: new Date("2026-07-18T12:00:00.000Z"),
      },
    }));

    const batch = await canonicalizer(body, {
      loadDatasetRunItemContext,
    }).canonicalize(operation(body));

    expect(loadDatasetRunItemContext).toHaveBeenCalledWith({
      projectId: "project-1",
      datasetId: "dataset-1",
      datasetRunId: "run-1",
      datasetItemId: "item-1",
      datasetVersion: new Date("2026-07-18T12:00:00.000Z"),
    });
    expect(batch.children).toHaveLength(1);
    expect(batch.children[0]).toMatchObject({
      traceDeletionGeneration: 3n,
      projectDeletionGeneration: 2n,
      entity: {
        kind: "datasetRunItem",
        runItemId: "run-item-1",
        datasetRunId: "run-1",
        datasetItemId: "item-1",
        datasetId: "dataset-1",
        traceId: "trace-1",
        observationId: "span-1",
        datasetRunName: "run one",
        datasetRunMetadata: { owner: "team-a" },
        datasetItemInput: { question: "life" },
        datasetItemExpectedOutput: { answer: 42 },
        datasetItemMetadata: { difficulty: "hard" },
        sourceContract: "dataset-run-item",
        partitionDate: "2026-07-18",
        datasetDeletionGeneration: 0n,
        runDeletionGeneration: 0n,
      },
    });
  });

  it("canonicalizes a legacy trace into a stable synthetic root event", async () => {
    const envelope: RawAnalyticsIngestionEnvelope = {
      formatVersion: 1,
      source: "legacy-event",
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "javascript",
        ingestionSdkVersion: "2.9.0",
      },
      payload: [
        {
          id: "trace-event-1",
          type: "trace-create",
          timestamp: "2026-07-18T14:00:00.123456789Z",
          body: {
            id: "trace-1",
            timestamp: "2026-07-18T13:59:59.000000000Z",
            name: "legacy trace",
            input: { question: "why?" },
            metadata: { tenant: "acme" },
            environment: "production",
          },
        },
      ],
    };
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);

    const batch = await canonicalizer(body, {
      loadCurrentEvent: vi.fn(async () => null),
    } as never).canonicalize(operation(body));

    expect(batch.children).toHaveLength(1);
    expect(batch.children[0]).toMatchObject({
      expectedSourceVersion: null,
      entity: {
        kind: "event",
        traceId: "trace-1",
        spanId: "t-trace-1",
        parentSpanId: "",
        name: "legacy trace",
        traceName: "legacy trace",
        environment: "production",
        input: { question: "why?" },
        metadata: { tenant: "acme" },
        sourceVersion: 1_784_383_200_123_456_789n,
      },
    });
  });

  it("merges a legacy partial observation update with the visible Doris snapshot", async () => {
    const envelope: RawAnalyticsIngestionEnvelope = {
      formatVersion: 1,
      source: "legacy-event",
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "python",
        ingestionSdkVersion: "2.8.1",
      },
      payload: [
        {
          id: "span-update-event-1",
          type: "span-update",
          timestamp: "2026-07-18T14:00:01.000000001Z",
          body: {
            id: "span-1",
            traceId: "trace-1",
            output: { answer: 42 },
            metadata: { updated: true },
            environment: "production",
          },
        },
      ],
    };
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);
    const loadCurrentEvent = vi.fn(async () => ({
      sourceVersion: 1_753_190_400_000_000_000n,
      eventData: {
        projectId: "project-1",
        traceId: "trace-1",
        spanId: "span-1",
        parentSpanId: "t-trace-1",
        type: "SPAN",
        name: "existing span",
        environment: "production",
        startTimeISO: "2026-07-18T13:59:58.000Z",
        endTimeISO: "2026-07-18T13:59:59.000Z",
        input: { question: "life" },
        output: null,
        metadata: { kept: "yes" },
        providedUsageDetails: { input: 3 },
        source: "ingestion-api-legacy",
      },
    }));

    const batch = await canonicalizer(body, {
      loadCurrentEvent,
    } as never).canonicalize(operation(body));

    expect(loadCurrentEvent).toHaveBeenCalledWith({
      projectId: "project-1",
      traceId: "trace-1",
      spanId: "span-1",
    });
    expect(batch.children[0]).toMatchObject({
      expectedSourceVersion: 1_753_190_400_000_000_000n,
      entity: {
        traceId: "trace-1",
        spanId: "span-1",
        name: "existing span",
        startTime: 1_784_383_198_000_000_000n,
        endTime: 1_784_383_199_000_000_000n,
        input: { question: "life" },
        output: { answer: 42 },
        metadata: { kept: "yes", updated: true },
        providedUsageDetails: { input: 3 },
      },
    });
  });

  it("resolves a legacy partial update whose SDK omitted traceId", async () => {
    const envelope: RawAnalyticsIngestionEnvelope = {
      formatVersion: 1,
      source: "legacy-event",
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "javascript",
        ingestionSdkVersion: "2.9.0",
      },
      payload: [
        {
          id: "span-update-event-1",
          type: "span-update",
          timestamp: "2026-07-18T14:00:00.123456789Z",
          body: { id: "span-1", output: { answer: 42 } },
        },
      ],
    };
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);
    const loadCurrentEvent = vi.fn(async () => ({
      sourceVersion: 1_753_190_400_000_000_000n,
      eventData: {
        projectId: "project-1",
        traceId: "trace-1",
        spanId: "span-1",
        type: "SPAN",
        name: "existing span",
        environment: "production",
        startTimeISO: "2026-07-18T13:59:58.000Z",
        endTimeISO: "2026-07-18T13:59:59.000Z",
        metadata: {},
        source: "ingestion-api-legacy",
      },
    }));

    const batch = await canonicalizer(body, {
      loadCurrentEvent,
    } as never).canonicalize(operation(body));

    expect(loadCurrentEvent).toHaveBeenCalledWith({
      projectId: "project-1",
      traceId: undefined,
      spanId: "span-1",
    });
    expect(batch.children[0]?.entity).toMatchObject({
      traceId: "trace-1",
      spanId: "span-1",
      output: { answer: 42 },
    });
  });

  it("rejects raw content that does not match the durable receipt checksum", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope({
      formatVersion: 1,
      source: "otlp",
      attribution: {
        ingestionApiKey: "",
        ingestionSdkName: "",
        ingestionSdkVersion: "",
      },
      payload: [],
    });
    await expect(
      canonicalizer(`${body} `).canonicalize(operation(body)),
    ).rejects.toMatchObject({ code: "ANALYTICS_CONFLICT" });
  });

  it("routes raw storage failures through the durable retry state machine", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope({
      formatVersion: 1,
      source: "otlp",
      attribution: {
        ingestionApiKey: "",
        ingestionSdkName: "",
        ingestionSdkVersion: "",
      },
      payload: [],
    });
    const storageService = {
      downloadIfExists: vi.fn().mockRejectedValue(new Error("storage down")),
    } as never;

    await expect(
      canonicalizer(body, { storageService }).canonicalize(operation(body)),
    ).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      retryable: true,
    });
  });

  it("retries while a ledger-first raw artifact is still pending", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope({
      formatVersion: 1,
      source: "otlp",
      attribution: {
        ingestionApiKey: "",
        ingestionSdkName: "",
        ingestionSdkVersion: "",
      },
      payload: [],
    });
    const storageService = {
      downloadIfExists: vi.fn().mockResolvedValue(null),
    } as never;

    await expect(
      canonicalizer(body, { storageService }).canonicalize(operation(body)),
    ).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      retryable: true,
      tags: expect.objectContaining({ reasonCode: "RAW_ARTIFACT_PENDING" }),
    });
  });

  it("applies injected masking after reading raw storage and before OTLP canonicalization", async () => {
    const start = 1_714_488_530_686_000_001n;
    const end = 1_714_488_530_687_000_009n;
    const maskedPayload = [
      {
        resource: { attributes: [] },
        scopeSpans: [
          {
            scope: { name: "langfuse-sdk", version: "4.0.0" },
            spans: [
              {
                traceId: bufferId("aabbccdd11223344aabbccdd11223344"),
                spanId: bufferId("1122334455667788"),
                name: "masked-name",
                kind: 1,
                startTimeUnixNano: nanoTimestamp(start),
                endTimeUnixNano: nanoTimestamp(end),
                attributes: [],
                status: {},
              },
            ],
          },
        ],
      },
    ];
    const envelope: RawAnalyticsIngestionEnvelope = {
      formatVersion: 1,
      source: "otlp",
      maskingContext: {
        orgId: "org-1",
        propagatedHeaders: {
          "x-mask-tenant": "tenant-1",
        },
      },
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "python",
        ingestionSdkVersion: "4.0.0",
      },
      payload: [],
    };
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);
    const maskOtlp = vi.fn(async () => maskedPayload);

    const batch = await canonicalizer(body, { maskOtlp }).canonicalize(
      operation(body),
    );

    expect(maskOtlp).toHaveBeenCalledWith({
      projectId: "project-1",
      resourceSpans: [],
      orgId: "org-1",
      propagatedHeaders: {
        "x-mask-tenant": "tenant-1",
      },
    });
    expect(batch.children[0]?.entity).toMatchObject({
      kind: "event",
      name: "masked-name",
    });
  });

  it("rejects an internal event whose embedded project differs from the receipt", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope({
      formatVersion: 1,
      source: "internal-event",
      attribution: {
        ingestionApiKey: "pk-test",
        ingestionSdkName: "internal",
        ingestionSdkVersion: "1",
      },
      payload: [
        {
          envelopeTimestamp: "2026-07-18T14:00:00.123456789Z",
          eventData: {
            projectId: "other-project",
            traceId: "trace-1",
            spanId: "span-1",
            startTimeISO: "2026-07-18T14:00:00Z",
            endTimeISO: "2026-07-18T14:00:01Z",
            metadata: {},
            source: "internal",
          },
        },
      ],
    });

    await expect(
      canonicalizer(body).canonicalize(operation(body)),
    ).rejects.toMatchObject({ code: "ANALYTICS_VALIDATION_ERROR" });
  });
});
