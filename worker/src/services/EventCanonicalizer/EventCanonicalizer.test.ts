import { describe, expect, it, vi } from "vitest";

import { deriveV4SourceTime } from "@langfuse/shared/analytics-persistence";

import { EventCanonicalizer } from ".";

function input() {
  return {
    projectId: "project-1",
    traceId: "trace-1",
    spanId: "span-1",
    parentSpanId: "span-parent",
    startTimeISO: "2026-07-17T10:00:00Z",
    endTimeISO: "2026-07-17T10:01:00Z",
    completionStartTime: "2026-07-17T10:00:30Z",
    name: "generation",
    type: "GENERATION",
    environment: "production",
    version: "release-version-1",
    release: "2026.07",
    traceName: "support-trace",
    userId: "user-1",
    sessionId: "session-1",
    level: "DEFAULT",
    statusMessage: "ok",
    tags: ["alpha"],
    bookmarked: true,
    public: false,
    isAppRoot: false,
    promptName: "support",
    promptVersion: "3",
    modelName: "gpt-test",
    modelParameters: { temperature: 0 },
    providedUsageDetails: { input: 10 },
    usageDetails: { input: 9 },
    providedCostDetails: {},
    costDetails: { input: 0.5 },
    toolDefinitions: { search: "{}" },
    toolCalls: ["search"],
    toolCallNames: ["search"],
    input: { question: "价格" },
    output: { answer: "42" },
    metadata: { nested: { region: "eu" } },
    source: "sdk",
    ingestionSdkName: "langfuse-js",
    ingestionSdkVersion: "4.0.0",
    serviceName: "api",
    telemetrySdkLanguage: "nodejs",
    eventBytes: 123,
  };
}

describe("EventCanonicalizer", () => {
  it("extracts prompt/model/usage/cost enrichment without physical row fields", async () => {
    const warnOnUsageTotalMismatch = vi.fn();
    const resolvePrompt = vi.fn().mockResolvedValue({
      id: "prompt-id-3",
      name: "support",
      version: 3,
    });
    const resolveGenerationUsage = vi.fn().mockResolvedValue({
      internalModelId: "model-id-1",
      usageDetails: { input: 10, output: 2, total: 12 },
      costDetails: { input: 0.5, output: 0.2, total: 0.7 },
      totalCost: 0.7,
      version: "release-version-1",
      release: "2026.07",
      traceName: "support-trace",
      usagePricingTierId: "tier-1",
      usagePricingTierName: "default",
    });
    const canonicalizer = new EventCanonicalizer({
      warnOnUsageTotalMismatch,
      resolvePrompt,
      resolveGenerationUsage,
    });

    const enriched = await canonicalizer.enrich({
      eventData: input(),
      rawObjectKey: "raw/project-1/operation-1",
    });

    expect(enriched).toMatchObject({
      projectId: "project-1",
      promptId: "prompt-id-3",
      promptVersion: 3,
      internalModelId: "model-id-1",
      usageDetails: { input: 10, output: 2, total: 12 },
      costDetails: { input: 0.5, output: 0.2, total: 0.7 },
      totalCost: 0.7,
      input: { question: "价格" },
      metadata: { nested: { region: "eu" } },
      rawObjectKey: "raw/project-1/operation-1",
    });
    expect(enriched).not.toHaveProperty("project_id");
    expect(enriched).not.toHaveProperty("event_ts");
    expect(enriched).not.toHaveProperty("metadata_names");
    expect(warnOnUsageTotalMismatch).toHaveBeenCalledOnce();
    expect(resolveGenerationUsage).toHaveBeenCalledWith(
      expect.objectContaining({
        input: '{"question":"价格"}',
        output: '{"answer":"42"}',
      }),
    );
  });

  it("maps invalid canonical JSON to a safe validation error", async () => {
    const canonicalizer = new EventCanonicalizer({
      warnOnUsageTotalMismatch: vi.fn(),
      resolvePrompt: vi.fn().mockResolvedValue(null),
      resolveGenerationUsage: vi.fn().mockResolvedValue(null),
    });

    await expect(
      canonicalizer.enrich({
        eventData: { ...input(), modelParameters: "{private-payload" },
        rawObjectKey: "raw/project-1/operation-1",
      }),
    ).rejects.toMatchObject({
      code: "ANALYTICS_VALIDATION_ERROR",
      message: "Analytics persistence input is invalid",
    });

    await expect(
      canonicalizer.canonicalize({
        eventData: {
          ...input(),
          completionStartTime: undefined,
          providedUsageDetails: { input: Number.NaN },
        },
        rawObjectKey: "raw/project-1/operation-1",
        sourceTime: deriveV4SourceTime({
          envelopeTimestamp: "2026-07-17T10:02:00Z",
        }),
        systemTimestamp: 1_784_282_600_000_000_000n,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 1,
      }),
    ).rejects.toMatchObject({
      code: "ANALYTICS_VALIDATION_ERROR",
      message: "Analytics persistence input is invalid",
    });
  });

  it("freezes source contract, stable system time, and canonical hash", async () => {
    const canonicalizer = new EventCanonicalizer({
      warnOnUsageTotalMismatch: vi.fn(),
      resolvePrompt: vi.fn().mockResolvedValue(null),
      resolveGenerationUsage: vi.fn().mockResolvedValue(null),
    });
    const sourceTime = deriveV4SourceTime({
      envelopeTimestamp: "2026-07-17T10:02:00.000000001Z",
      bodyStartTime: "2026-07-17T10:00:00Z",
      bodyEndTime: "2026-07-17T10:01:00Z",
    });

    const event = await canonicalizer.canonicalize({
      eventData: {
        ...input(),
        promptName: undefined,
        promptVersion: undefined,
      },
      rawObjectKey: "raw/project-1/operation-1",
      sourceTime,
      systemTimestamp: 1_784_282_600_000_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 1,
    });
    const replay = await canonicalizer.canonicalize({
      eventData: {
        ...input(),
        promptName: undefined,
        promptVersion: undefined,
      },
      rawObjectKey: "raw/project-1/operation-replay",
      sourceTime,
      systemTimestamp: 1_784_282_700_000_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 1,
    });

    expect(event).toMatchObject({
      kind: "event",
      sourceContract: "v4",
      sourceVersion: sourceTime.sourceVersion,
      startTime: sourceTime.startTime,
      endTime: sourceTime.endTime,
      partitionDate: "2026-07-17",
      systemTimestamp: 1_784_282_600_000_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 1,
      version: "release-version-1",
      release: "2026.07",
      traceName: "support-trace",
    });
    for (const physicalField of [
      "project_id",
      "event_ts",
      "is_deleted",
      "metadata_names",
      "metadata_values",
    ]) {
      expect(event).not.toHaveProperty(physicalField);
    }
    expect(event.canonicalPayloadHash).toMatch(/^[a-f0-9]{64}$/);
    expect(replay.canonicalPayloadHash).toBe(event.canonicalPayloadHash);

    await expect(
      canonicalizer.canonicalize({
        eventData: input(),
        rawObjectKey: "raw/project-1/operation-1",
        sourceTime: { ...sourceTime, sourceContract: "score" } as never,
        systemTimestamp: 1_784_282_600_000_000_000n,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 1,
      }),
    ).rejects.toMatchObject({ code: "ANALYTICS_VALIDATION_ERROR" });
  });

  it("preserves the complete experiment context in schema version 2", async () => {
    const canonicalizer = new EventCanonicalizer({
      warnOnUsageTotalMismatch: vi.fn(),
      resolvePrompt: vi.fn().mockResolvedValue(null),
      resolveGenerationUsage: vi.fn().mockResolvedValue(null),
    });

    const event = await canonicalizer.canonicalize({
      eventData: {
        ...input(),
        experimentId: "run-1",
        experimentName: "prompt experiment",
        experimentMetadataNames: ["owner", "nested.region"],
        experimentMetadataValues: ["team-a", "eu"],
        experimentDescription: "foundation round trip",
        experimentDatasetId: "dataset-1",
        experimentItemId: "item-1",
        experimentItemVersion: "2026-07-17T09:59:00.123Z",
        experimentItemRootSpanId: "span-1",
        experimentItemExpectedOutput: '{"answer":42}',
        experimentItemMetadataNames: ["difficulty"],
        experimentItemMetadataValues: ["hard"],
      },
      rawObjectKey: "raw/project-1/operation-experiment",
      sourceTime: deriveV4SourceTime({
        envelopeTimestamp: "2026-07-17T10:02:00.000000001Z",
        bodyStartTime: "2026-07-17T10:00:00Z",
        bodyEndTime: "2026-07-17T10:01:00Z",
      }),
      systemTimestamp: 1_784_282_600_000_000_000n,
      canonicalizerVersion: "2",
      schemaVersion: 2,
    });

    expect(event).toMatchObject({
      experimentId: "run-1",
      experimentName: "prompt experiment",
      experimentMetadata: { owner: "team-a", "nested.region": "eu" },
      experimentDescription: "foundation round trip",
      experimentDatasetId: "dataset-1",
      experimentItemId: "item-1",
      experimentItemVersion: 1_784_282_340_123_000_000n,
      experimentItemRootSpanId: "span-1",
      experimentItemExpectedOutput: '{"answer":42}',
      experimentItemMetadata: { difficulty: "hard" },
    });
  });
});
