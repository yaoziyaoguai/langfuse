import { createHash } from "node:crypto";

import type { AnalyticsIngestionOperation } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  acceptAnalyticsIngestion,
  assertRawAnalyticsBodySize,
  encodeRawAnalyticsIngestionEnvelope,
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

    const body = encodeRawAnalyticsIngestionEnvelope(envelope);
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
    const body = encodeRawAnalyticsIngestionEnvelope(envelope);
    const createReceipt = vi.fn(async (input) => ({
      operation: { id: input.operationId } as AnalyticsIngestionOperation,
      created: true,
    }));
    const base = {
      projectId: "project-1",
      operationId: "operation-1",
      envelope,
      acceptedAt,
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
});
