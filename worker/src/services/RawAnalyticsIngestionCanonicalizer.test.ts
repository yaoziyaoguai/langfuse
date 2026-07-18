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
      download: vi.fn(async () => body),
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
            traceId: "trace-1",
            observationId: "span-1",
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
      traceId: "trace-1",
      observationId: "span-1",
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
