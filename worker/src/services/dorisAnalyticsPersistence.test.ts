import { createHash } from "node:crypto";

import { LangfuseConflictError } from "@langfuse/shared";
import { encodeRawAnalyticsIngestionEnvelope } from "@langfuse/shared/analytics-persistence";
import { describe, expect, it, vi } from "vitest";

const reconcileRawAnalyticsIngestionReceipts = vi.hoisted(() =>
  vi.fn(async () => ({
    scanned: 0,
    recovered: 0,
    existing: 0,
    invalid: 0,
  })),
);
const expireUnreadyAnalyticsIngestionReceipts = vi.hoisted(() =>
  vi.fn(async () => 0),
);
const applyConfiguredIngestionMasking = vi.hoisted(() =>
  vi.fn(async (input: { data: unknown }) => ({
    success: true,
    data: input.data,
    masked: true,
  })),
);

vi.mock("@langfuse/shared/src/server", async (importOriginal) => ({
  ...(await importOriginal()),
  reconcileRawAnalyticsIngestionReceipts,
  expireUnreadyAnalyticsIngestionReceipts,
}));

vi.mock("@langfuse/shared/src/server/ingestion-masking", () => ({
  applyConfiguredIngestionMasking,
}));

import { EventCanonicalizer } from "./EventCanonicalizer";
import { RawAnalyticsIngestionCanonicalizer } from "./RawAnalyticsIngestionCanonicalizer";
import {
  createDorisAnalyticsPersistence,
  createLegacyCurrentEventLoader,
} from "./dorisAnalyticsPersistence";

describe("createDorisAnalyticsPersistence", () => {
  it("wires enterprise ingestion masking into the production Doris canonicalizer", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope({
      formatVersion: 1,
      source: "otlp",
      payload: [],
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
    });
    const composition = createDorisAnalyticsPersistence({
      runtimeEnv: {
        LANGFUSE_S3_EVENT_UPLOAD_BUCKET: "test-bucket",
      },
      prismaClient: {
        analyticsProjectDeletionGeneration: {
          findUnique: vi.fn(async () => null),
        },
      } as never,
      storageService: {
        downloadIfExists: vi.fn(async () => body),
      } as never,
      streamLoadTransport: {
        load: vi.fn(),
        reconcile: vi.fn(),
      },
      databaseName: "langfuse_test",
      eventCanonicalizer: new EventCanonicalizer({
        warnOnUsageTotalMismatch: vi.fn(),
        resolvePrompt: vi.fn(async () => null),
        resolveGenerationUsage: vi.fn(async () => null),
      }),
    });

    await expect(
      composition.canonicalizer.canonicalize({
        id: "operation-1",
        projectId: "project-1",
        sourceChecksum: createHash("sha256").update(body).digest("hex"),
        rawObjectKey: "raw/operation-1.json",
        acceptedAtNanos: 1_784_383_200_123_000_000n,
        canonicalizerVersion: "1",
        schemaVersion: 1,
      }),
    ).rejects.toMatchObject({
      code: "ANALYTICS_VALIDATION_ERROR",
    });
    expect(applyConfiguredIngestionMasking).toHaveBeenCalledWith({
      data: [],
      projectId: "project-1",
      orgId: "org-1",
      propagatedHeaders: {
        "x-mask-tenant": "tenant-1",
      },
    });

    applyConfiguredIngestionMasking.mockResolvedValueOnce({
      success: false,
      data: [],
      masked: false,
      error: "callback unavailable",
    });
    await expect(
      composition.canonicalizer.canonicalize({
        id: "operation-2",
        projectId: "project-1",
        sourceChecksum: createHash("sha256").update(body).digest("hex"),
        rawObjectKey: "raw/operation-2.json",
        acceptedAtNanos: 1_784_383_200_123_000_001n,
        canonicalizerVersion: "1",
        schemaVersion: 1,
      }),
    ).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      retryable: true,
      tags: {
        phase: "ingestion_masking",
        reasonCode: "MASKING_CALLBACK_FAILED",
      },
    });
  });

  it("owns the raw canonicalizer and queue processor for the durable path", async () => {
    const order: string[] = [];
    reconcileRawAnalyticsIngestionReceipts.mockImplementationOnce(async () => {
      order.push("reconcile");
      return { scanned: 1, recovered: 1, existing: 0, invalid: 0 };
    });
    expireUnreadyAnalyticsIngestionReceipts.mockImplementationOnce(async () => {
      order.push("expire");
      return 0;
    });
    const composition = createDorisAnalyticsPersistence({
      runtimeEnv: {
        LANGFUSE_S3_EVENT_UPLOAD_BUCKET: "test-bucket",
      },
      prismaClient: {} as never,
      storageService: {
        download: vi.fn(),
        downloadIfExists: vi.fn(async () => "matching raw"),
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
    await expect(composition.reconcileRaw(25, "page-2")).resolves.toEqual({
      scanned: 1,
      recovered: 1,
      existing: 0,
      invalid: 0,
    });
    expect(reconcileRawAnalyticsIngestionReceipts).toHaveBeenCalledWith(
      expect.objectContaining({ cursor: "page-2", limit: 25 }),
    );
    expect(expireUnreadyAnalyticsIngestionReceipts).toHaveBeenCalledWith(
      expect.objectContaining({ limit: 25 }),
    );
    expect(order).toEqual(["reconcile", "expire"]);
  });

  it("loads the entity head and visible Doris row as one legacy merge snapshot", async () => {
    const findUnique = vi.fn(async () => ({ sourceVersion: 42n }));
    const getObservation = vi.fn(async () => ({
      id: "span-1",
      traceId: "trace-1",
      projectId: "project-1",
      parentObservationId: "t-trace-1",
      type: "SPAN",
      name: "existing span",
      environment: "production",
      userId: null,
      sessionId: null,
      traceName: "trace",
      release: null,
      version: null,
      level: "DEFAULT",
      statusMessage: null,
      isAppRoot: false,
      bookmarked: false,
      public: false,
      tags: [],
      startTime: new Date("2026-07-18T13:59:58.000Z"),
      endTime: null,
      completionStartTime: null,
      promptId: null,
      promptName: null,
      promptVersion: null,
      internalModelId: null,
      providedModelName: null,
      providedUsageDetails: { input: 3 },
      usageDetails: { input: 3 },
      providedCostDetails: {},
      costDetails: {},
      input: { question: "life" },
      output: null,
      metadata: { kept: "yes" },
    })) as never;
    const load = createLegacyCurrentEventLoader({
      client: {
        analyticsEntityHead: { findUnique },
      } as never,
      getObservation,
    });

    await expect(
      load({
        projectId: "project-1",
        traceId: "trace-1",
        spanId: "span-1",
      }),
    ).resolves.toMatchObject({
      sourceVersion: 42n,
      eventData: {
        projectId: "project-1",
        traceId: "trace-1",
        spanId: "span-1",
        parentSpanId: "t-trace-1",
        name: "existing span",
        input: { question: "life" },
        metadata: { kept: "yes" },
      },
    });
    expect(getObservation).toHaveBeenCalledOnce();
  });

  it("locates the current trace when a legacy update omits traceId", async () => {
    const findUnique = vi.fn(async () => ({ sourceVersion: 42n }));
    const getObservation = vi.fn(async () => ({
      id: "span-1",
      traceId: "trace-1",
      projectId: "project-1",
      parentObservationId: null,
      type: "SPAN",
      name: "existing span",
      environment: "default",
      userId: null,
      sessionId: null,
      traceName: null,
      release: null,
      version: null,
      level: null,
      statusMessage: null,
      isAppRoot: false,
      bookmarked: false,
      public: false,
      tags: [],
      startTime: new Date("2026-07-18T13:59:58.000Z"),
      endTime: null,
      completionStartTime: null,
      promptId: null,
      promptName: null,
      promptVersion: null,
      internalModelId: null,
      providedModelName: null,
      providedUsageDetails: {},
      usageDetails: {},
      providedCostDetails: {},
      costDetails: {},
    })) as never;
    const load = createLegacyCurrentEventLoader({
      client: { analyticsEntityHead: { findUnique } } as never,
      getObservation,
    });

    await expect(
      load({ projectId: "project-1", spanId: "span-1" }),
    ).resolves.toMatchObject({
      sourceVersion: 42n,
      eventData: { traceId: "trace-1", spanId: "span-1" },
    });
    expect(getObservation).toHaveBeenCalledWith({
      projectId: "project-1",
      observationId: "span-1",
    });
  });

  it("terminalizes an ambiguous trace-less legacy update as a conflict", async () => {
    const load = createLegacyCurrentEventLoader({
      client: { analyticsEntityHead: { findUnique: vi.fn() } } as never,
      getObservation: vi.fn(async () => {
        throw new LangfuseConflictError("ambiguous observation");
      }),
    });

    await expect(
      load({ projectId: "project-1", spanId: "span-1" }),
    ).rejects.toMatchObject({
      code: "ANALYTICS_CONFLICT",
      retryable: false,
    });
  });
});
