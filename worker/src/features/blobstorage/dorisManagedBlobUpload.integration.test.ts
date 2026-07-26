import { createHash, randomUUID } from "node:crypto";
import type { Readable } from "node:stream";

import { ParquetReader } from "@dsnp/parquetjs";
import type { BlobStorageIntegration } from "@prisma/client";
import {
  BlobStorageIntegrationFileType,
  BlobStorageIntegrationType,
  type StorageService,
} from "@langfuse/shared";
import { afterAll, beforeAll, describe, expect, it, vi } from "vitest";

vi.mock("@langfuse/shared/src/server", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("@langfuse/shared/src/server")>();
  return {
    ...actual,
    reserveAnalyticsIntegrationScratch: vi.fn().mockResolvedValue(true),
    renewAnalyticsIntegrationScratch: vi.fn().mockResolvedValue(true),
    releaseAnalyticsIntegrationScratch: vi.fn().mockResolvedValue(true),
  };
});

import {
  createBlobStorageService,
  uploadDorisBlobExecution,
} from "./handleBlobStorageIntegrationProjectJob";

const runIntegration =
  process.env.DORIS_TEST_MINIO_COMPOSE_PROJECT_NAME === "langfuse";
const integrationDescribe = runIntegration ? describe : describe.skip;

async function streamToBuffer(stream: Readable): Promise<Buffer> {
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}

integrationDescribe("Doris managed blob upload against MinIO", () => {
  const projectId = `project-${randomUUID()}`;
  const prefix = `u7-managed-${randomUUID()}/`;
  let storageService: StorageService;

  beforeAll(() => {
    storageService = createBlobStorageService({
      bucketName: "langfuse",
      endpoint: "http://127.0.0.1:9090",
      region: "us-east-1",
      accessKeyId: "minio",
      secretAccessKey: "miniosecret",
      forcePathStyle: true,
      type: BlobStorageIntegrationType.S3,
    });
  });

  afterAll(async () => {
    const files = await storageService.listFiles(prefix);
    if (files.length > 0) {
      await storageService.deleteFiles(files.map(({ file }) => file));
    }
  });

  it.each([
    BlobStorageIntegrationFileType.JSON,
    BlobStorageIntegrationFileType.CSV,
    BlobStorageIntegrationFileType.JSONL,
    BlobStorageIntegrationFileType.PARQUET,
  ])("uploads %s data and a checksum manifest", async (fileType) => {
    const executionId = `aie_${createHash("sha256")
      .update(fileType)
      .digest("hex")
      .slice(0, 28)}`;
    const integration = {
      projectId,
      prefix,
      type: BlobStorageIntegrationType.S3,
      bucketName: "langfuse",
      endpoint: "http://127.0.0.1:9090",
      region: "us-east-1",
      accessKeyId: "minio",
      secretAccessKey: null,
      forcePathStyle: true,
      enabled: true,
      exportFrequency: "daily",
      exportSource: "TRACES_OBSERVATIONS",
      fileType,
      compressed: false,
      exportMode: "FULL_HISTORY",
      exportFieldGroups: [],
      exportTuning: null,
      nextSyncAt: null,
      lastSyncAt: null,
      exportStartDate: null,
      runStartedAt: null,
      lastError: null,
      lastErrorAt: null,
      lastFailureNotificationSentAt: null,
      createdAt: new Date(),
      updatedAt: new Date(),
    } as BlobStorageIntegration;
    const row = {
      id: "trace-managed-1",
      timestamp: "2026-07-25T12:34:56.789Z",
      name: `Doris managed ${fileType}`,
      metadata: { exact: true },
    };

    await uploadDorisBlobExecution({
      read: {
        records: [
          {
            item: {
              deliveryKind: "TRACE",
              entityKey: row.id,
              deliveryIds: ["aid_0000000000000000000000000000"],
            },
            table: "traces",
            row,
          },
        ],
        missing: [],
      },
      executionId,
      integration,
      storageService,
      partSizeBytes: 5 * 1024 * 1024,
      maxConcurrentParts: 1,
      maxPartAttempts: 1,
      gzipLevel: undefined,
    });

    const dataKey =
      `${prefix}${projectId}/doris-executions/${executionId}/traces.` +
      fileType.toLowerCase();
    const manifestKey = `${prefix}${projectId}/manifests/${executionId}.json`;
    const manifestText = await storageService.download(manifestKey);
    const manifest = JSON.parse(manifestText) as {
      files: Array<{
        key: string;
        checksum: string;
        rowCount: number;
        fileType: string;
      }>;
    };

    let checksum: string;
    if (fileType === BlobStorageIntegrationFileType.PARQUET) {
      const stream = await storageService.downloadStreamIfExists(dataKey);
      expect(stream).not.toBeNull();
      const data = await streamToBuffer(stream!);
      checksum = createHash("sha256").update(data).digest("hex");
      expect(data.subarray(0, 4).toString("ascii")).toBe("PAR1");
      expect(data.subarray(-4).toString("ascii")).toBe("PAR1");

      const reader = await ParquetReader.openBuffer(data);
      try {
        await expect(reader.getCursor().next()).resolves.toEqual({
          id: row.id,
          metadata: JSON.stringify(row.metadata),
          name: row.name,
          timestamp: new Date(row.timestamp),
        });
      } finally {
        await reader.close();
      }
    } else {
      const data = await storageService.download(dataKey);
      checksum = createHash("sha256").update(data).digest("hex");
      expect(data).toContain(row.id);
      if (fileType === BlobStorageIntegrationFileType.JSON) {
        expect(JSON.parse(data)).toEqual([row]);
      } else if (fileType === BlobStorageIntegrationFileType.JSONL) {
        expect(JSON.parse(data.trim())).toEqual(row);
      } else {
        expect(data.split("\n")[0]).toContain("id");
        expect(data).toContain("Doris managed CSV");
      }
    }
    expect(manifest.files).toEqual([
      expect.objectContaining({
        key: dataKey,
        checksum,
        rowCount: 1,
        fileType,
      }),
    ]);
  });
});
