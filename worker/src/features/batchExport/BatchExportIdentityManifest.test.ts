import { createHash } from "node:crypto";
import { Readable } from "node:stream";
import { gzipSync } from "node:zlib";

import { describe, expect, it, vi } from "vitest";

import {
  openVerifiedBatchExportIdentityManifest,
  writeBatchExportIdentityManifest,
  type BatchExportIdentity,
} from "./BatchExportIdentityManifest";

const metadata = {
  batchExportId: "export-1",
  projectId: "project-1",
  tableName: "observations",
  generation: 3n,
  claimId: "claim-1",
  filterHash: "a".repeat(64),
} as const;

async function collect<T>(rows: AsyncIterable<T>): Promise<T[]> {
  const result: T[] = [];
  for await (const row of rows) result.push(row);
  return result;
}

describe("BatchExportIdentityManifest", () => {
  it("writes and verifies a canonical compressed identity manifest", async () => {
    let body = "";
    const uploadFile = vi.fn(async ({ data }: { data: Readable }) => {
      for await (const chunk of data) body += chunk.toString();
    });
    const identities: BatchExportIdentity[] = [
      { id: "observation-a", traceId: "trace-a" },
      { id: "observation-b", traceId: "trace-a" },
      { id: "observation-a", traceId: "trace-b" },
    ];

    const descriptor = await writeBatchExportIdentityManifest({
      storage: { uploadFile },
      objectKey: "batch-exports/manifests/export-1/3/claim-1.ndjson.gz.b64",
      metadata,
      identities: Readable.from(identities),
      maxRows: 10,
    });

    expect(descriptor).toMatchObject({
      rowCount: 3,
      objectKey: expect.any(String),
    });
    expect(descriptor.byteCount).toBe(BigInt(Buffer.byteLength(body)));
    await expect(
      openVerifiedBatchExportIdentityManifest({
        encodedBody: body,
        descriptor,
        expected: metadata,
        maxRows: 10,
      }),
    ).resolves.toBeDefined();
    const opened = await openVerifiedBatchExportIdentityManifest({
      encodedBody: body,
      descriptor,
      expected: metadata,
      maxRows: 10,
    });
    await expect(collect(opened)).resolves.toEqual(identities);
  });

  it("rejects duplicate, unsorted, and over-limit identities before upload completes", async () => {
    const uploadFile = vi.fn(async ({ data }: { data: Readable }) => {
      for await (const _chunk of data) {
        // Consume the stream so producer failures propagate to the upload.
      }
    });

    await expect(
      writeBatchExportIdentityManifest({
        storage: { uploadFile },
        objectKey: "attempt",
        metadata,
        identities: Readable.from([
          { id: "b", traceId: "trace" },
          { id: "a", traceId: "trace" },
        ]),
        maxRows: 10,
      }),
    ).rejects.toThrow("strictly sorted");

    await expect(
      writeBatchExportIdentityManifest({
        storage: { uploadFile },
        objectKey: "attempt",
        metadata,
        identities: Readable.from([{ id: "a" }, { id: "b" }]),
        maxRows: 1,
      }),
    ).rejects.toThrow("row limit");
  });

  it("rejects checksum, byte-count, metadata, and decompression-limit mismatches", async () => {
    let body = "";
    const descriptor = await writeBatchExportIdentityManifest({
      storage: {
        uploadFile: async ({ data }: { data: Readable }) => {
          for await (const chunk of data) body += chunk.toString();
        },
      },
      objectKey: "attempt",
      metadata,
      identities: Readable.from([{ id: "a" }]),
      maxRows: 10,
    });

    await expect(
      openVerifiedBatchExportIdentityManifest({
        encodedBody: `${body}x`,
        descriptor,
        expected: metadata,
        maxRows: 10,
      }),
    ).rejects.toThrow("byte count");
    await expect(
      openVerifiedBatchExportIdentityManifest({
        encodedBody: body,
        descriptor: { ...descriptor, checksum: "b".repeat(64) },
        expected: metadata,
        maxRows: 10,
      }),
    ).rejects.toThrow("checksum");
    await expect(
      openVerifiedBatchExportIdentityManifest({
        encodedBody: body,
        descriptor,
        expected: { ...metadata, projectId: "project-2" },
        maxRows: 10,
      }),
    ).rejects.toThrow("metadata");
    await expect(
      openVerifiedBatchExportIdentityManifest({
        encodedBody: body,
        descriptor,
        expected: metadata,
        maxRows: 0,
      }),
    ).rejects.toThrow("row limit");

    const truncatedBody = body.slice(0, -4);
    const truncated = await openVerifiedBatchExportIdentityManifest({
      encodedBody: truncatedBody,
      descriptor: {
        ...descriptor,
        checksum: createHash("sha256").update(truncatedBody).digest("hex"),
        byteCount: BigInt(Buffer.byteLength(truncatedBody)),
      },
      expected: metadata,
      maxRows: 10,
    });
    await expect(collect(truncated)).rejects.toThrow("decompression failed");

    const bombBody = gzipSync("x".repeat(20_000)).toString("base64");
    await expect(
      openVerifiedBatchExportIdentityManifest({
        encodedBody: bombBody,
        descriptor: {
          objectKey: "bomb",
          checksum: createHash("sha256").update(bombBody).digest("hex"),
          rowCount: 0,
          byteCount: BigInt(Buffer.byteLength(bombBody)),
          formatVersion: 1,
        },
        expected: metadata,
        maxRows: 1,
      }),
    ).rejects.toThrow("decompression limit");
  });

  it("streams a large manifest through a backpressured upload", async () => {
    const rowCount = 5_000;
    let body = "";
    const descriptor = await writeBatchExportIdentityManifest({
      storage: {
        uploadFile: async ({ data }: { data: Readable }) => {
          for await (const chunk of data) {
            await new Promise((resolve) => setImmediate(resolve));
            body += chunk.toString();
          }
        },
      },
      objectKey: "large-attempt",
      metadata,
      identities: Readable.from(
        Array.from({ length: rowCount }, (_, index) => ({
          id: `observation-${String(index).padStart(5, "0")}`,
        })),
      ),
      maxRows: rowCount,
    });

    expect(descriptor.rowCount).toBe(rowCount);
    const opened = await openVerifiedBatchExportIdentityManifest({
      encodedBody: Readable.from(
        Array.from({ length: Math.ceil(body.length / 17) }, (_, index) =>
          body.slice(index * 17, (index + 1) * 17),
        ),
      ),
      descriptor,
      expected: metadata,
      maxRows: rowCount,
    });
    const identities = await collect(opened);
    expect(identities).toHaveLength(rowCount);
    expect(identities.at(0)).toEqual({ id: "observation-00000" });
    expect(identities.at(-1)).toEqual({ id: "observation-04999" });
  });
});
