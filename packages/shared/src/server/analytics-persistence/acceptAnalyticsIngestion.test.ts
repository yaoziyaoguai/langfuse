import { createHash } from "node:crypto";

import type { AnalyticsIngestionOperation } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  acceptAnalyticsIngestion,
  assertRawAnalyticsBodySize,
  encodeRawAnalyticsIngestionEnvelope,
  reconcileRawAnalyticsIngestionReceipts,
} from "./acceptAnalyticsIngestion";

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

describe("acceptAnalyticsIngestion", () => {
  it("persists raw input before creating the exact durable receipt", async () => {
    const order: string[] = [];
    const uploadFileIfAbsent = vi.fn(async () => {
      order.push("raw");
      return "created" as const;
    });
    const createReceipt = vi.fn(async (input) => {
      order.push("receipt");
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
          downloadIfExists: vi.fn(),
        } as never,
        createReceipt: createReceipt as never,
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
    expect(order).toEqual(["raw", "receipt"]);
    expect(uploadFileIfAbsent).toHaveBeenCalledWith({
      fileName: "raw-prefix/analytics-ingestion/raw/project-1/operation-1.json",
      fileType: "application/json",
      data: body,
    });
    expect(createReceipt).toHaveBeenCalledWith(
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
      }),
    );
  });

  it("reconciles an identical prior raw write but rejects a checksum collision", async () => {
    const body = encodeRawAnalyticsIngestionEnvelope(envelope, {
      operationId: "operation-1",
      projectId: "project-1",
      sourceOperationId: "operation-1",
      acceptedAt,
      acceptedAtNanos: 1_784_383_200_123_000_000n,
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
    });
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));
    const base = {
      projectId: "project-1",
      operationId: "operation-1",
      envelope,
      acceptedAt: new Date("2026-07-19T14:00:00.123Z"),
      canonicalizerVersion: "r1a-v1",
      schemaVersion: 3,
      storageService: {
        uploadFileIfAbsent: vi.fn(async () => "already_exists" as const),
        downloadIfExists: vi.fn(async () => body),
      } as never,
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
          listFiles: vi.fn(async () => [
            {
              file: "prefix/analytics-ingestion/raw/project-1/orphan-operation.json",
              createdAt: acceptedAt,
            },
          ]),
          download: vi.fn(async () => body),
        } as never,
        rawPrefix: "prefix/",
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
});
