import { createHash } from "node:crypto";

import type { AnalyticsIngestionOperation } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  acceptAnalyticsIngestion,
  assertRawAnalyticsBodySize,
  decodeRawAnalyticsIngestionEnvelope,
  encodeRawAnalyticsIngestionEnvelope,
  reconcileRawAnalyticsIngestionReceipts,
} from "./acceptAnalyticsIngestion";
import type { AnalyticsDurableProvenance } from "./analyticsDurableProvenance";

const acceptedAt = new Date("2026-07-18T14:00:00.123Z");
const envelope = {
  formatVersion: 1 as const,
  source: "otlp" as const,
  payload: [{ scopeSpans: [] }],
  attribution: {
    ingestionApiKey: "pk-test",
    ingestionSdkName: "python",
    ingestionSdkVersion: "4.0.0",
  },
};

const managedProvenance: AnalyticsDurableProvenance = {
  analyticsBackend: "DORIS",
  deploymentGeneration: 7n,
  workloadEpochFingerprint: "a".repeat(64),
  runtimeContractVersion: 2,
  producerRuntimeLeaseId: "producer-lease",
};

const legacyAdmission = vi.fn(async () => null);

describe("acceptAnalyticsIngestion", () => {
  it("round-trips durable masking context for OTLP replay", () => {
    const body = encodeRawAnalyticsIngestionEnvelope({
      ...envelope,
      maskingContext: {
        orgId: "org-1",
        propagatedHeaders: {
          "x-mask-tenant": "tenant-1",
        },
      },
    });

    expect(decodeRawAnalyticsIngestionEnvelope(body)).toMatchObject({
      maskingContext: {
        orgId: "org-1",
        propagatedHeaders: {
          "x-mask-tenant": "tenant-1",
        },
      },
    });
  });

  it("rejects malformed or non-OTLP masking context", () => {
    expect(() =>
      decodeRawAnalyticsIngestionEnvelope(
        JSON.stringify({
          ...envelope,
          maskingContext: {
            propagatedHeaders: {
              "x-mask-tenant": 42,
            },
          },
        }),
      ),
    ).toThrow(
      expect.objectContaining({
        code: "ANALYTICS_VALIDATION_ERROR",
      }),
    );
    expect(() =>
      decodeRawAnalyticsIngestionEnvelope(
        JSON.stringify({
          ...envelope,
          maskingContext: {
            orgId: "org-1",
            unexpected: "value",
          },
        }),
      ),
    ).toThrow(
      expect.objectContaining({
        code: "ANALYTICS_VALIDATION_ERROR",
      }),
    );
    expect(() =>
      encodeRawAnalyticsIngestionEnvelope({
        ...envelope,
        source: "score",
        maskingContext: {
          orgId: "org-1",
        },
      }),
    ).toThrow(
      expect.objectContaining({
        code: "ANALYTICS_VALIDATION_ERROR",
      }),
    );
  });

  it("creates a pending ledger receipt before raw upload and publishes it afterwards", async () => {
    const order: string[] = [];
    const uploadFileIfAbsent = vi.fn(async () => {
      order.push("raw");
      return "created" as const;
    });
    const createReceipt = vi.fn(async (input) => {
      order.push(input.publishReady ? "receipt:ready" : "receipt:pending");
      return {
        operation: { id: input.operationId } as AnalyticsIngestionOperation,
        created: true,
      };
    });

    await expect(
      acceptAnalyticsIngestion({
        projectId: "project-1",
        operationId: "operation-1",
        sourceOperationId: "source-1",
        envelope,
        acceptedAt,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 3,
        rawPrefix: "raw-prefix/",
        storageService: {
          uploadFileIfAbsent,
          downloadIfExists: vi.fn(async () => null),
        } as never,
        captureFoundationProvenance: legacyAdmission,
        createReceipt: createReceipt as never,
        findReceipt: vi.fn(async () => null) as never,
      }),
    ).resolves.toEqual({ operationId: "operation-1", status: "ACCEPTED" });

    const body = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "operation-1",
      projectId: "project-1",
      sourceOperationId: "source-1",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const checksum = createHash("sha256").update(body).digest("hex");
    expect(order).toEqual(["receipt:pending", "raw", "receipt:ready"]);
    expect(uploadFileIfAbsent).toHaveBeenCalledWith({
      fileName: "raw-prefix/analytics-ingestion/raw/project-1/operation-1.json",
      fileType: "application/json",
      data: body,
    });
    expect(createReceipt).toHaveBeenNthCalledWith(
      1,
      expect.objectContaining({
        operationId: "operation-1",
        projectId: "project-1",
        sourceOperationId: "source-1",
        sourceChecksum: checksum,
        rawObjectKey:
          "raw-prefix/analytics-ingestion/raw/project-1/operation-1.json",
        acceptedAt,
        acceptedAtNanos: 1_784_383_200_123_000_000n,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 3,
        recoverableUntil: new Date("2026-07-25T14:00:00.123Z"),
        statusExpiresAt: new Date("2026-08-17T14:00:00.123Z"),
        publishReady: false,
      }),
    );
    expect(createReceipt).toHaveBeenNthCalledWith(
      2,
      expect.objectContaining({ publishReady: true }),
    );
  });

  it("reconciles an identical prior raw write but rejects a checksum collision", async () => {
    const envelopeWithMaskingContext = {
      ...envelope,
      maskingContext: {
        orgId: "org-1",
        propagatedHeaders: {
          "x-mask-tenant": "tenant-1",
        },
      },
    };
    const body = encodeRawAnalyticsIngestionEnvelope(
      envelopeWithMaskingContext,
      {
        operationId: "operation-1",
        projectId: "project-1",
        sourceOperationId: "operation-1",
        acceptedAt,
        acceptedAtNanos: 1_784_383_200_123_000_000n,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 3,
      },
    );
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));
    const base = {
      projectId: "project-1",
      operationId: "operation-1",
      envelope: envelopeWithMaskingContext,
      acceptedAt: new Date("2026-07-19T14:00:00.123Z"),
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
      storageService: {
        uploadFileIfAbsent: vi.fn(async () => "already_exists" as const),
        downloadIfExists: vi.fn(async () => body),
      } as never,
      captureFoundationProvenance: legacyAdmission,
      createReceipt: createReceipt as never,
    };

    await expect(acceptAnalyticsIngestion(base)).resolves.toEqual({
      operationId: "operation-1",
      status: "ACCEPTED",
    });
    expect(createReceipt).toHaveBeenCalledOnce();
    expect(createReceipt).toHaveBeenCalledWith(
      expect.objectContaining({ acceptedAt }),
    );

    await expect(
      acceptAnalyticsIngestion({
        ...base,
        storageService: {
          uploadFileIfAbsent: vi.fn(async () => "already_exists" as const),
          downloadIfExists: vi.fn(async () => `${body} `),
        } as never,
      }),
    ).rejects.toMatchObject({ code: "ANALYTICS_CONFLICT" });
    expect(createReceipt).toHaveBeenCalledOnce();
  });

  it("does not publish an outbox when a concurrent raw upload has different bytes", async () => {
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));
    const downloadIfExists = vi
      .fn()
      .mockResolvedValueOnce(null)
      .mockResolvedValueOnce("different raw body");

    await expect(
      acceptAnalyticsIngestion({
        projectId: "project-1",
        operationId: "operation-1",
        envelope,
        acceptedAt,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 3,
        storageService: {
          downloadIfExists,
          uploadFileIfAbsent: vi.fn(async () => "already_exists" as const),
        } as never,
        captureFoundationProvenance: legacyAdmission,
        createReceipt: createReceipt as never,
        findReceipt: vi.fn(async () => null) as never,
      }),
    ).rejects.toMatchObject({ code: "ANALYTICS_CONFLICT" });

    expect(createReceipt).toHaveBeenCalledOnce();
    expect(createReceipt).toHaveBeenCalledWith(
      expect.objectContaining({ publishReady: false }),
    );
  });

  it("rejects non-JSON raw payloads before writing storage", async () => {
    const uploadFileIfAbsent = vi.fn();
    await expect(
      acceptAnalyticsIngestion({
        projectId: "project-1",
        operationId: "operation-1",
        envelope: {
          ...envelope,
          payload: { unsupported: 1n },
        },
        acceptedAt,
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 3,
        storageService: { uploadFileIfAbsent } as never,
        captureFoundationProvenance: legacyAdmission,
      }),
    ).rejects.toMatchObject({ code: "ANALYTICS_VALIDATION_ERROR" });
    expect(uploadFileIfAbsent).not.toHaveBeenCalled();
  });

  it("rejects a raw body above the frozen per-operation byte limit", () => {
    expect(() => assertRawAnalyticsBodySize("1234", 3)).toThrow(
      expect.objectContaining({
        code: "ANALYTICS_RESOURCE_EXHAUSTED",
        retryable: false,
      }),
    );
  });

  it("recreates a missing receipt and outbox from a self-describing raw object", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "orphan-operation",
      projectId: "project-1",
      sourceOperationId: "orphan-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));

    await expect(
      reconcileRawAnalyticsIngestionReceipts({
        storageService: {
          listFilesPage: vi.fn(async () => ({
            files: [
              {
                file: "prefix/analytics-ingestion/raw/project-1/orphan-operation.json",
                createdAt: acceptedAt,
              },
            ],
          })),
          download: vi.fn(async () => body),
        } as never,
        rawPrefix: "prefix/",
        admissionContext: null,
        createReceipt: createReceipt as never,
      }),
    ).resolves.toEqual({
      scanned: 1,
      recovered: 1,
      existing: 0,
      invalid: 0,
    });
    expect(createReceipt).toHaveBeenCalledWith(
      expect.objectContaining({
        operationId: "orphan-operation",
        projectId: "project-1",
        sourceOperationId: "orphan-source",
        sourceChecksum: createHash("sha256").update(body).digest("hex"),
        acceptedAt,
        acceptedAtNanos: 1_784_383_200_123_000_000n,
        rawObjectKey:
          "prefix/analytics-ingestion/raw/project-1/orphan-operation.json",
      }),
    );
  });

  it("captures managed provenance before raw storage and preserves it in the receipt", async () => {
    const order: string[] = [];
    const uploadFileIfAbsent = vi.fn(async (input: { data: string }) => {
      order.push("raw");
      expect(
        decodeRawAnalyticsIngestionEnvelope(input.data).receipt
          ?.analyticsProvenance,
      ).toEqual(managedProvenance);
      return "created" as const;
    });
    const createReceipt = vi.fn(async (input) => {
      order.push(input.publishReady ? "receipt:ready" : "receipt:pending");
      expect(input.producerProvenance).toEqual(managedProvenance);
      expect(input.admissionContext).toEqual({
        runtimeLeaseId: "producer-lease",
        backend: "doris",
        deploymentGeneration: 7n,
      });
      return {
        operation: { id: input.operationId } as AnalyticsIngestionOperation,
        created: true,
      };
    });
    const captureFoundationProvenance = vi.fn(async () => {
      order.push("admission");
      return managedProvenance;
    });

    await acceptAnalyticsIngestion({
      projectId: "project-1",
      operationId: "managed-operation",
      envelope,
      acceptedAt,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
      admissionContext: {
        runtimeLeaseId: "producer-lease",
        backend: "doris",
        deploymentGeneration: 7n,
      },
      storageService: {
        uploadFileIfAbsent,
        downloadIfExists: vi.fn(async () => null),
      } as never,
      captureFoundationProvenance,
      createReceipt: createReceipt as never,
      findReceipt: vi.fn(async () => null) as never,
    });

    expect(order).toEqual([
      "admission",
      "receipt:pending",
      "raw",
      "receipt:ready",
    ]);
    expect(captureFoundationProvenance).toHaveBeenCalledWith({
      client: undefined,
      admissionContext: {
        runtimeLeaseId: "producer-lease",
        backend: "doris",
        deploymentGeneration: 7n,
      },
      requiredContract: {
        schemaVersion: 3,
        canonicalizerVersion: "r1a-v1",
      },
    });
  });

  it("accepts an identical managed raw retry from another web lease in the same deployment", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "managed-retry",
      projectId: "project-1",
      sourceOperationId: "managed-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
      analyticsProvenance: managedProvenance,
    });
    const currentProvenance: AnalyticsDurableProvenance = {
      ...managedProvenance,
      producerRuntimeLeaseId: "retrying-web-lease",
    };
    const currentAdmission = {
      runtimeLeaseId: "retrying-web-lease",
      backend: "doris" as const,
      deploymentGeneration: 7n,
    };
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));

    await expect(
      acceptAnalyticsIngestion({
        projectId: "project-1",
        operationId: "managed-retry",
        sourceOperationId: "managed-source",
        envelope,
        acceptedAt: new Date("2026-07-19T14:00:00.123Z"),
        canonicalizerVersion: "r1a-v1",
        schemaVersion: 3,
        admissionContext: currentAdmission,
        storageService: {
          uploadFileIfAbsent: vi.fn(async () => "already_exists" as const),
          downloadIfExists: vi.fn(async () => body),
        } as never,
        captureFoundationProvenance: vi.fn(async () => currentProvenance),
        createReceipt: createReceipt as never,
      }),
    ).resolves.toEqual({ operationId: "managed-retry", status: "ACCEPTED" });
    expect(createReceipt).toHaveBeenCalledWith(
      expect.objectContaining({
        producerProvenance: managedProvenance,
        admissionContext: currentAdmission,
      }),
    );
  });

  it("reconciles managed raw work with the current lease without replacing the original producer", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "managed-orphan",
      projectId: "project-1",
      sourceOperationId: "managed-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
      analyticsProvenance: managedProvenance,
    });
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));
    const currentAdmission = {
      runtimeLeaseId: "recovery-lease",
      backend: "doris" as const,
      deploymentGeneration: 7n,
    };

    await expect(
      reconcileRawAnalyticsIngestionReceipts({
        storageService: {
          listFilesPage: vi.fn(async () => ({
            files: [
              {
                file: "prefix/analytics-ingestion/raw/project-1/managed-orphan.json",
                createdAt: acceptedAt,
              },
            ],
          })),
          download: vi.fn(async () => body),
        } as never,
        rawPrefix: "prefix/",
        admissionContext: currentAdmission,
        createReceipt: createReceipt as never,
      }),
    ).resolves.toEqual({
      scanned: 1,
      recovered: 1,
      existing: 0,
      invalid: 0,
    });
    expect(createReceipt).toHaveBeenCalledWith(
      expect.objectContaining({
        producerProvenance: managedProvenance,
        admissionContext: currentAdmission,
      }),
    );
  });

  it("skips existing raw receipts before applying the reconciliation limit", async () => {
    const firstBody = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "existing-operation",
      projectId: "project-1",
      sourceOperationId: "existing-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const secondBody = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "orphan-operation",
      projectId: "project-1",
      sourceOperationId: "orphan-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));

    await expect(
      reconcileRawAnalyticsIngestionReceipts({
        storageService: {
          listFilesPage: vi.fn(async () => ({
            files: [
              {
                file: "prefix/analytics-ingestion/raw/project-1/existing-operation.json",
                createdAt: acceptedAt,
              },
              {
                file: "prefix/analytics-ingestion/raw/project-1/orphan-operation.json",
                createdAt: new Date(acceptedAt.getTime() + 1),
              },
            ],
          })),
          download: vi.fn(async (key: string) =>
            key.includes("existing-operation") ? firstBody : secondBody,
          ),
        } as never,
        rawPrefix: "prefix/",
        limit: 1,
        admissionContext: null,
        findExistingRawObjectKeys: vi.fn(
          async () =>
            new Set([
              "prefix/analytics-ingestion/raw/project-1/existing-operation.json",
            ]),
        ),
        createReceipt: createReceipt as never,
      }),
    ).resolves.toEqual({
      scanned: 1,
      recovered: 1,
      existing: 0,
      invalid: 0,
    });
    expect(createReceipt).toHaveBeenCalledWith(
      expect.objectContaining({ operationId: "orphan-operation" }),
    );
  });

  it("continues past a full page of existing receipts to recover an object after the first 200", async () => {
    const existingObjects = Array.from({ length: 200 }, (_, index) => ({
      file: `prefix/analytics-ingestion/raw/project-1/existing-${index}.json`,
      createdAt: new Date(acceptedAt.getTime() + index),
    }));
    const orphanKey =
      "prefix/analytics-ingestion/raw/project-1/orphan-after-200.json";
    const orphanBody = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "orphan-after-200",
      projectId: "project-1",
      sourceOperationId: "orphan-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const listFilesPage = vi
      .fn()
      .mockResolvedValueOnce({
        files: existingObjects,
        nextCursor: "page-2",
      })
      .mockResolvedValueOnce({
        files: [{ file: orphanKey, createdAt: acceptedAt }],
      });
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));

    const firstPage = await reconcileRawAnalyticsIngestionReceipts({
      storageService: {
        listFilesPage,
        download: vi.fn(),
      } as never,
      rawPrefix: "prefix/",
      limit: 200,
      admissionContext: null,
      findExistingRawObjectKeys: vi.fn(
        async () => new Set(existingObjects.map((object) => object.file)),
      ),
      createReceipt: createReceipt as never,
    });
    const secondPage = await reconcileRawAnalyticsIngestionReceipts({
      storageService: {
        listFilesPage,
        download: vi.fn(async () => orphanBody),
      } as never,
      rawPrefix: "prefix/",
      limit: 200,
      cursor: firstPage.nextCursor,
      admissionContext: null,
      findExistingRawObjectKeys: vi.fn(async () => new Set<string>()),
      createReceipt: createReceipt as never,
    });

    expect(firstPage).toEqual({
      scanned: 0,
      recovered: 0,
      existing: 0,
      invalid: 0,
      nextCursor: "page-2",
    });
    expect(secondPage).toEqual({
      scanned: 1,
      recovered: 1,
      existing: 0,
      invalid: 0,
    });
    expect(listFilesPage).toHaveBeenNthCalledWith(
      1,
      "prefix/analytics-ingestion/raw/",
      { limit: 200 },
    );
    expect(listFilesPage).toHaveBeenNthCalledWith(
      2,
      "prefix/analytics-ingestion/raw/",
      { cursor: "page-2", limit: 200 },
    );
    expect(createReceipt).toHaveBeenCalledWith(
      expect.objectContaining({ operationId: "orphan-after-200" }),
    );
  });

  it("advances past a full invalid page before recovering the next-page orphan", async () => {
    const invalidObjects = Array.from({ length: 200 }, (_, index) => ({
      file: `prefix/analytics-ingestion/raw/project-1/invalid-${index}.json`,
      createdAt: new Date(acceptedAt.getTime() + index),
    }));
    const orphanKey =
      "prefix/analytics-ingestion/raw/project-1/orphan-after-invalid.json";
    const orphanBody = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "orphan-after-invalid",
      projectId: "project-1",
      sourceOperationId: "orphan-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const listFilesPage = vi
      .fn()
      .mockResolvedValueOnce({
        files: invalidObjects,
        nextCursor: "after-invalid",
      })
      .mockResolvedValueOnce({
        files: [{ file: orphanKey, createdAt: acceptedAt }],
      });
    const download = vi.fn(async (key: string) =>
      key === orphanKey ? orphanBody : "not-json",
    );
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));

    const firstPage = await reconcileRawAnalyticsIngestionReceipts({
      storageService: { listFilesPage, download } as never,
      rawPrefix: "prefix/",
      limit: 200,
      admissionContext: null,
      findExistingRawObjectKeys: vi.fn(async () => new Set<string>()),
      createReceipt: createReceipt as never,
    });
    const secondPage = await reconcileRawAnalyticsIngestionReceipts({
      storageService: { listFilesPage, download } as never,
      rawPrefix: "prefix/",
      limit: 200,
      cursor: firstPage.nextCursor,
      admissionContext: null,
      findExistingRawObjectKeys: vi.fn(async () => new Set<string>()),
      createReceipt: createReceipt as never,
    });

    expect(firstPage).toEqual({
      scanned: 200,
      recovered: 0,
      existing: 0,
      invalid: 200,
      nextCursor: "after-invalid",
    });
    expect(secondPage).toEqual({
      scanned: 1,
      recovered: 1,
      existing: 0,
      invalid: 0,
    });
  });

  it("continues past a permanently invalid receipt and advances the page cursor", async () => {
    const bodies = new Map(
      ["poison", "healthy"].map((operationId) => [
        `prefix/analytics-ingestion/raw/project-1/${operationId}.json`,
        encodeRawAnalyticsIngestionEnvelope(envelope, {
          operationId,
          projectId: "project-1",
          sourceOperationId: `${operationId}-source`,
          acceptedAt,
          acceptedAtNanos: 1_784_383_200_123_000_000n,
          canonicalizerVersion: "r1a-v1",
          schemaVersion: 3,
        }),
      ]),
    );
    const createReceipt = vi.fn(async (input) => {
      if (input.operationId === "poison") throw new Error("Project not found");
      return {
        operation: { id: input.operationId } as AnalyticsIngestionOperation,
        created: true,
      };
    });

    await expect(
      reconcileRawAnalyticsIngestionReceipts({
        storageService: {
          listFilesPage: vi.fn(async () => ({
            files: [...bodies.keys()].map((file, index) => ({
              file,
              createdAt: new Date(acceptedAt.getTime() + index),
            })),
            nextCursor: "page-2",
          })),
          download: vi.fn(async (file: string) => bodies.get(file)!),
        } as never,
        rawPrefix: "prefix/",
        admissionContext: null,
        findExistingRawObjectKeys: vi.fn(async () => new Set<string>()),
        createReceipt: createReceipt as never,
      }),
    ).resolves.toEqual({
      scanned: 2,
      recovered: 1,
      existing: 0,
      invalid: 1,
      nextCursor: "page-2",
    });
    expect(createReceipt).toHaveBeenCalledTimes(2);
  });

  it("classifies legacy raw receipts as permanently invalid after managed adoption", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "legacy-before-adoption",
      projectId: "project-1",
      sourceOperationId: "legacy-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const createReceipt = vi.fn(async () => {
      throw new Error("Managed analytics ingestion requires provenance");
    });

    await expect(
      reconcileRawAnalyticsIngestionReceipts({
        storageService: {
          listFilesPage: vi.fn(async () => ({
            files: [
              {
                file: "prefix/analytics-ingestion/raw/project-1/legacy-before-adoption.json",
                createdAt: acceptedAt,
              },
            ],
          })),
          download: vi.fn(async () => body),
        } as never,
        rawPrefix: "prefix/",
        admissionContext: {
          runtimeLeaseId: "managed-recovery",
          backend: "doris",
          deploymentGeneration: 7n,
        },
        findExistingRawObjectKeys: vi.fn(async () => new Set<string>()),
        createReceipt: createReceipt as never,
      }),
    ).resolves.toEqual({
      scanned: 1,
      recovered: 0,
      existing: 0,
      invalid: 1,
    });
    expect(createReceipt).toHaveBeenCalledWith(
      expect.objectContaining({
        producerProvenance: null,
        admissionContext: expect.objectContaining({
          runtimeLeaseId: "managed-recovery",
        }),
      }),
    );
  });

  it("aborts the page on a transient receipt failure so it can retry", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "transient",
      projectId: "project-1",
      sourceOperationId: "transient-source",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const createReceipt = vi.fn(async () => {
      throw new Error("database timeout");
    });

    await expect(
      reconcileRawAnalyticsIngestionReceipts({
        storageService: {
          listFilesPage: vi.fn(async () => ({
            files: [
              {
                file: "prefix/analytics-ingestion/raw/project-1/transient.json",
                createdAt: acceptedAt,
              },
            ],
            nextCursor: "page-2",
          })),
          download: vi.fn(async () => body),
        } as never,
        rawPrefix: "prefix/",
        admissionContext: null,
        findExistingRawObjectKeys: vi.fn(async () => new Set<string>()),
        createReceipt: createReceipt as never,
      }),
    ).rejects.toThrow("database timeout");
    expect(createReceipt).toHaveBeenCalledOnce();
  });
});
