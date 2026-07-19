import { describe, expect, it } from "vitest";

import {
  AnalyticsPersistenceError,
  assertAnalyticsBatchBoundary,
  canonicalPayloadHash,
  deriveFileReferenceSourceTime,
  deriveOtlpSourceTime,
  deriveScoreSourceTime,
  deriveV4SourceTime,
  encodeFileReferenceIdentity,
  encodeEventIdentity,
  encodeScoreIdentity,
  INT64_MAX,
  normalizeVersionToken,
  toEventIdentity,
  type AnalyticsBatchSink,
  type AnalyticsLifecycleStore,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsEvent,
} from "..";

describe("canonical source-time contracts", () => {
  it("uses the mandatory v4 envelope timestamp for ordering and start fallback", () => {
    const first = deriveV4SourceTime({
      envelopeTimestamp: "2026-07-17T10:00:00.123456789Z",
      bodyStartTime: null,
      bodyEndTime: null,
    });
    const second = deriveV4SourceTime({
      envelopeTimestamp: "2026-07-17T10:05:00.000000001Z",
      bodyStartTime: "2026-07-17T10:00:00Z",
      bodyEndTime: "2026-07-17T10:10:00Z",
    });
    const third = deriveV4SourceTime({
      envelopeTimestamp: "2026-07-17T10:06:00.000000001Z",
      bodyStartTime: "2026-07-17T10:00:00Z",
      bodyEndTime: "2026-07-17T10:10:00Z",
    });

    expect(first).toMatchObject({
      sourceContract: "v4",
      startTime: first.sourceVersion,
      endTime: null,
      partitionDate: "2026-07-17",
    });
    expect(third.sourceVersion).toBeGreaterThan(second.sourceVersion);
    expect(third.endTime).toBe(second.endTime);
  });

  it("uses valid OTLP end time, but only falls back for a missing edge", () => {
    const complete = deriveOtlpSourceTime({
      startTimeUnixNano: "1784282400000000000",
      endTimeUnixNano: "1784282401000000000",
    });
    expect(complete).toMatchObject({
      sourceContract: "otlp",
      sourceVersion: 1_784_282_401_000_000_000n,
      startTime: 1_784_282_400_000_000_000n,
      endTime: 1_784_282_401_000_000_000n,
    });

    const incomplete = deriveOtlpSourceTime({
      startTimeUnixNano: "1784282400000000000",
      endTimeUnixNano: null,
    });
    expect(incomplete.endTime).toBe(incomplete.startTime);
    expect(incomplete.sourceVersion).toBe(incomplete.startTime);

    expect(() =>
      deriveOtlpSourceTime({
        startTimeUnixNano: null,
        endTimeUnixNano: null,
      }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));
    expect(() =>
      deriveOtlpSourceTime({
        startTimeUnixNano: "1784282400000000000",
        endTimeUnixNano: "invalid-present-value",
      }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));
  });

  it("never rounds protobuf seconds/nanos through an unsafe JS number", () => {
    expect(
      normalizeVersionToken({ seconds: "1784282400", nanos: 123_456_789 }),
    ).toBe(1_784_282_400_123_456_789n);
    expect(() =>
      normalizeVersionToken({
        seconds: Number.MAX_SAFE_INTEGER + 1,
        nanos: 0,
      }),
    ).toThrow();
    expect(() =>
      normalizeVersionToken({ seconds: "1784282400", nanos: 1_000_000_000 }),
    ).toThrow();
    expect(() => normalizeVersionToken(INT64_MAX)).toThrow();
  });

  it("derives score and file-reference versions only from their declared sources", () => {
    const score = deriveScoreSourceTime({
      timestamp: "2026-07-17T10:00:00.123456789Z",
      updatedAt: "2026-07-17T10:05:00.000000001Z",
    });
    expect(score).toEqual({
      sourceContract: "score",
      sourceVersion: 1_784_282_700_000_000_001n,
      timestamp: 1_784_282_400_123_456_789n,
      partitionDate: "2026-07-17",
    });
    expect(
      deriveScoreSourceTime({
        timestamp: "2026-07-17T10:00:00Z",
        updatedAt: null,
      }).sourceVersion,
    ).toBe(1_784_282_400_000_000_000n);
    expect(() =>
      deriveScoreSourceTime({ timestamp: null, updatedAt: null }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));

    expect(
      deriveFileReferenceSourceTime({
        parentSourceVersion: "1784282400123456789",
        parentPartitionDate: "2026-07-17",
      }),
    ).toEqual({
      sourceContract: "file-reference",
      sourceVersion: 1_784_282_400_123_456_789n,
      partitionDate: "2026-07-17",
    });
    expect(() =>
      deriveFileReferenceSourceTime({
        parentSourceVersion: "1784282400123456789",
        parentPartitionDate: "2026-02-30",
      }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));
  });

  it("rejects invalid calendar dates instead of letting Date normalize them", () => {
    expect(() => normalizeVersionToken("2026-02-30T10:00:00Z")).toThrow();
    expect(() => normalizeVersionToken("2026-07-17T10:00:00")).toThrow();
  });
});

describe("storage-neutral identity and hash", () => {
  it("round-trips Unicode identities by UTF-8 byte length", () => {
    const identity = {
      projectId: "项目🚀",
      traceId: "跟踪-α",
      spanId: "范围-😀",
    };
    expect(toEventIdentity(encodeEventIdentity(identity))).toEqual(identity);
    expect(() =>
      toEventIdentity(`${encodeEventIdentity(identity)}junk`),
    ).toThrow();
  });

  it("domain-separates event, score, and file-reference identities", () => {
    const event = encodeEventIdentity({
      projectId: "project-1",
      traceId: "trace-1",
      spanId: "shared-id",
    });
    const score = encodeScoreIdentity({
      projectId: "project-1",
      scoreId: "shared-id",
    });
    const file = encodeFileReferenceIdentity({
      projectId: "project-1",
      entityType: "EVENT",
      entityId: "trace-1/shared-id",
      fileId: "shared-id",
    });
    expect(new Set([event, score, file]).size).toBe(3);
  });

  it("hashes canonical camelCase content deterministically", () => {
    const event = {
      kind: "event",
      projectId: "project-1",
      traceId: "trace-1",
      spanId: "span-1",
      partitionDate: "2026-07-17",
      sourceContract: "v4",
      sourceVersion: 1_784_282_400_000_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 1,
      systemTimestamp: 1_784_282_401_000_000_000n,
      name: "生成",
      tags: ["alpha", "🚀"],
      metadata: { z: 1, a: { b: true } },
      resolvedEnrichmentIds: { promptId: "prompt-1", modelId: "model-1" },
    } as const;
    const reordered = {
      resolvedEnrichmentIds: { modelId: "model-1", promptId: "prompt-1" },
      metadata: { a: { b: true }, z: 1 },
      tags: ["alpha", "🚀"],
      name: "生成",
      systemTimestamp: 1_784_282_401_000_000_000n,
      schemaVersion: 1,
      canonicalizerVersion: "r1a-v1",
      sourceVersion: 1_784_282_400_000_000_000n,
      sourceContract: "v4",
      partitionDate: "2026-07-17",
      spanId: "span-1",
      traceId: "trace-1",
      projectId: "project-1",
      kind: "event",
    } as const;

    expect(canonicalPayloadHash(reordered)).toBe(canonicalPayloadHash(event));
    expect(canonicalPayloadHash({ ...event, tags: ["🚀", "alpha"] })).not.toBe(
      canonicalPayloadHash(event),
    );
    expect(() => canonicalPayloadHash({ value: Number.NaN })).toThrow();
  });
});

describe("persistence boundaries", () => {
  const event: CanonicalAnalyticsEvent = {
    kind: "event",
    projectId: "project-1",
    traceId: "trace-1",
    spanId: "span-1",
    parentSpanId: null,
    partitionDate: "2026-07-17",
    sourceContract: "v4",
    sourceVersion: 1_784_282_400_000_000_000n,
    canonicalizerVersion: "r1a-v1",
    schemaVersion: 1,
    canonicalPayloadHash: "a".repeat(64),
    systemTimestamp: 1_784_282_401_000_000_000n,
    type: "GENERATION",
    name: "generation",
    environment: "default",
    version: null,
    release: null,
    traceName: null,
    startTime: 1_784_282_400_000_000_000n,
    endTime: null,
    completionStartTime: null,
    userId: null,
    sessionId: null,
    level: "DEFAULT",
    statusMessage: null,
    isAppRoot: false,
    bookmarked: false,
    public: false,
    tags: [],
    input: null,
    output: null,
    metadata: {},
    providedModelName: null,
    internalModelId: null,
    promptId: null,
    promptName: null,
    promptVersion: null,
    modelParameters: {},
    providedUsageDetails: {},
    usageDetails: {},
    providedCostDetails: {},
    costDetails: {},
    totalCost: null,
    toolDefinitions: {},
    toolCalls: [],
    toolCallNames: [],
    source: "sdk",
    ingestionSdkName: "langfuse-js",
    ingestionSdkVersion: "4.0.0",
    serviceName: null,
    telemetrySdkLanguage: null,
    rawObjectKey: "raw/project-1/operation-1",
    eventBytes: 1,
    resolvedEnrichmentIds: {},
  };
  const batch: CanonicalAnalyticsBatch = {
    projectId: "project-1",
    operationId: "operation-1",
    canonicalizerVersion: "r1a-v1",
    schemaVersion: 1,
    acceptedAt: 1_784_282_401_000_000_000n,
    rawObjectKey: "raw/project-1/operation-1",
    children: [
      {
        entity: event,
        expectedSourceVersion: null,
        fenceGeneration: 1n,
        traceDeletionGeneration: 0n,
        projectDeletionGeneration: 0n,
      },
    ],
  };

  it("accepts a trusted, bounded operation batch and rejects scope leakage", () => {
    expect(() => assertAnalyticsBatchBoundary(batch)).not.toThrow();
    expect(() =>
      assertAnalyticsBatchBoundary({
        ...batch,
        children: [
          {
            ...batch.children[0],
            entity: { ...event, projectId: "other-project" },
          },
        ],
      }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));
    expect(() =>
      assertAnalyticsBatchBoundary({
        ...batch,
        children: [{ ...batch.children[0], fenceGeneration: 0n }],
      }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));
    expect(() =>
      assertAnalyticsBatchBoundary({
        ...batch,
        acceptedAt: batch.acceptedAt + 1n,
      }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));
    expect(() =>
      assertAnalyticsBatchBoundary({
        ...batch,
        children: [
          {
            ...batch.children[0],
            entity: { ...event, sourceContract: "score" } as never,
          },
        ],
      }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));
    expect(() =>
      assertAnalyticsBatchBoundary({
        ...batch,
        acceptedAt: INT64_MAX,
        children: [
          {
            ...batch.children[0],
            entity: { ...event, systemTimestamp: INT64_MAX },
          },
        ],
      }),
    ).toThrow(expect.objectContaining({ code: "ANALYTICS_VALIDATION_ERROR" }));
  });

  it("keeps sink and lifecycle contracts operation-scoped", async () => {
    const sink: AnalyticsBatchSink = {
      persist: async (input) => ({
        operationId: input.operationId,
        status: "QUEUED",
      }),
    };
    const lifecycle: AnalyticsLifecycleStore = {
      publishTraceTombstone: async (input) => ({
        projectId: input.projectId,
        traceId: input.traceId,
        generation: input.generation,
        visible: false,
        barrierLabel: "trace-barrier",
      }),
      publishProjectTombstone: async (input) => ({
        projectId: input.projectId,
        generation: input.generation,
        visible: false,
        barrierLabel: "project-barrier",
      }),
      getDeletionProgress: async () => null,
    };

    await expect(sink.persist(batch)).resolves.toEqual({
      operationId: "operation-1",
      status: "QUEUED",
    });
    await expect(
      lifecycle.publishTraceTombstone({
        operationId: "delete-1",
        projectId: "project-1",
        traceId: "trace-1",
        generation: 2n,
        createdAt: new Date("2026-07-18T00:00:00.000Z"),
      }),
    ).resolves.toMatchObject({ generation: 2n, visible: false });
  });

  it("exposes safe domain errors without transport details or raw causes", () => {
    const error = new AnalyticsPersistenceError("ANALYTICS_UNAVAILABLE", true, {
      correlationId: "corr-1",
      tags: {
        operationId: "operation-1",
        password: "do-not-expose",
        phase: "stream-load",
      } as never,
    });
    expect(error).toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      retryable: true,
      correlationId: "corr-1",
      tags: { operationId: "operation-1", phase: "stream-load" },
    });
    expect(error.message).not.toMatch(/sql|host|password|payload/i);
    expect(error.cause).toBeUndefined();
    expect(error.tags).not.toHaveProperty("password");
  });
});
