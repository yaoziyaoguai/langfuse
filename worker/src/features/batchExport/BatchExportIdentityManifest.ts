import { createHash } from "node:crypto";
import { PassThrough, Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { StringDecoder } from "node:string_decoder";
import { createGunzip, createGzip } from "node:zlib";

const FORMAT_VERSION = 1;
const SHA256_HEX = /^[a-f0-9]{64}$/;
const MAX_ID_BYTES = 512;
const MAX_LINE_BYTES = MAX_ID_BYTES * 2 + 128;
const HEADER_ALLOWANCE_BYTES = 4_096;

export type BatchExportIdentity = {
  readonly id: string;
  readonly traceId?: string;
};

export type BatchExportManifestMetadata = {
  readonly batchExportId: string;
  readonly projectId: string;
  readonly tableName: string;
  readonly generation: bigint;
  readonly claimId: string;
  readonly filterHash: string;
};

export type BatchExportManifestDescriptor = {
  readonly objectKey: string;
  readonly checksum: string;
  readonly rowCount: number;
  readonly byteCount: bigint;
  readonly formatVersion: typeof FORMAT_VERSION;
};

type ManifestStorage = {
  uploadFile(input: {
    readonly fileName: string;
    readonly fileType: string;
    readonly data: Readable;
  }): Promise<void>;
};

function requireMetadata(metadata: BatchExportManifestMetadata): void {
  if (
    !metadata.batchExportId ||
    !metadata.projectId ||
    !metadata.tableName ||
    metadata.generation < 1n ||
    !metadata.claimId ||
    !SHA256_HEX.test(metadata.filterHash)
  ) {
    throw new TypeError("Invalid batch export manifest metadata");
  }
}

function requireMaxRows(maxRows: number): void {
  if (!Number.isSafeInteger(maxRows) || maxRows < 1) {
    throw new TypeError("Invalid batch export manifest row limit");
  }
}

function requireIdentity(value: unknown): BatchExportIdentity {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new TypeError("Invalid batch export manifest identity");
  }
  const record = value as Record<string, unknown>;
  const keys = Object.keys(record).sort();
  const expectedKeys =
    record.traceId === undefined ? ["id"] : ["id", "traceId"];
  if (
    keys.length !== expectedKeys.length ||
    keys.some((key, index) => key !== expectedKeys[index]) ||
    typeof record.id !== "string" ||
    !record.id ||
    Buffer.byteLength(record.id) > MAX_ID_BYTES ||
    (record.traceId !== undefined &&
      (typeof record.traceId !== "string" ||
        !record.traceId ||
        Buffer.byteLength(record.traceId) > MAX_ID_BYTES))
  ) {
    throw new TypeError("Invalid batch export manifest identity");
  }
  return record.traceId === undefined
    ? { id: record.id }
    : { id: record.id, traceId: record.traceId as string };
}

function identityKey(identity: BatchExportIdentity): string {
  return `${identity.traceId ?? ""}\u0000${identity.id}`;
}

class Base64EncodeTransform extends Transform {
  private carry = Buffer.alloc(0);

  constructor() {
    super();
  }

  _transform(
    chunk: Buffer | string,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    const value = Buffer.concat([
      this.carry,
      Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk),
    ]);
    const completeLength = value.length - (value.length % 3);
    if (completeLength > 0) {
      this.push(value.subarray(0, completeLength).toString("base64"));
    }
    this.carry = value.subarray(completeLength);
    callback();
  }

  _flush(callback: (error?: Error | null) => void): void {
    if (this.carry.length > 0) this.push(this.carry.toString("base64"));
    callback();
  }
}

class Base64DecodeTransform extends Transform {
  private carry = "";

  _transform(
    chunk: Buffer | string,
    _encoding: BufferEncoding,
    callback: (error?: Error | null) => void,
  ): void {
    const value = this.carry + chunk.toString();
    const completeLength = value.length - (value.length % 4);
    if (completeLength > 0) {
      this.push(Buffer.from(value.slice(0, completeLength), "base64"));
    }
    this.carry = value.slice(completeLength);
    callback();
  }

  _flush(callback: (error?: Error | null) => void): void {
    if (this.carry.length > 0) {
      callback(new Error("Batch export manifest encoding is invalid"));
      return;
    }
    callback();
  }
}

export async function writeBatchExportIdentityManifest(input: {
  readonly storage: ManifestStorage;
  readonly objectKey: string;
  readonly metadata: BatchExportManifestMetadata;
  readonly identities: AsyncIterable<BatchExportIdentity>;
  readonly maxRows: number;
}): Promise<BatchExportManifestDescriptor> {
  requireMetadata(input.metadata);
  requireMaxRows(input.maxRows);
  if (!input.objectKey) throw new TypeError("Invalid manifest object key");

  let rowCount = 0;
  let previousKey: string | undefined;
  const lines = async function* () {
    yield `${JSON.stringify({
      version: FORMAT_VERSION,
      batchExportId: input.metadata.batchExportId,
      projectId: input.metadata.projectId,
      tableName: input.metadata.tableName,
      generation: input.metadata.generation.toString(),
      claimId: input.metadata.claimId,
      filterHash: input.metadata.filterHash,
    })}\n`;
    for await (const candidate of input.identities) {
      const identity = requireIdentity(candidate);
      const key = identityKey(identity);
      if (previousKey !== undefined && key <= previousKey) {
        throw new Error(
          "Batch export manifest identities must be strictly sorted",
        );
      }
      rowCount += 1;
      if (rowCount > input.maxRows) {
        throw new Error("Batch export manifest exceeded its row limit");
      }
      previousKey = key;
      yield `${JSON.stringify(identity)}\n`;
    }
  };

  const hash = createHash("sha256");
  let byteCount = 0n;
  const measure = new Transform({
    transform(chunk: Buffer | string, _encoding, callback) {
      const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      hash.update(value);
      byteCount += BigInt(value.length);
      callback(null, value);
    },
  });
  const output = new PassThrough();
  const pipelinePromise = pipeline(
    Readable.from(lines()),
    createGzip(),
    new Base64EncodeTransform(),
    measure,
    output,
  );
  const uploadPromise = input.storage.uploadFile({
    fileName: input.objectKey,
    fileType: "application/gzip+base64",
    data: output,
  });
  uploadPromise.catch((error: unknown) => output.destroy(error as Error));
  await Promise.all([pipelinePromise, uploadPromise]);

  return {
    objectKey: input.objectKey,
    checksum: hash.digest("hex"),
    rowCount,
    byteCount,
    formatVersion: FORMAT_VERSION,
  };
}

function parseHeader(
  value: unknown,
  expected: BatchExportManifestMetadata,
): void {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error("Batch export manifest metadata is invalid");
  }
  const header = value as Record<string, unknown>;
  if (
    header.version !== FORMAT_VERSION ||
    header.batchExportId !== expected.batchExportId ||
    header.projectId !== expected.projectId ||
    header.tableName !== expected.tableName ||
    header.generation !== expected.generation.toString() ||
    header.claimId !== expected.claimId ||
    header.filterHash !== expected.filterHash
  ) {
    throw new Error("Batch export manifest metadata does not match");
  }
}

export async function openVerifiedBatchExportIdentityManifest(input: {
  readonly encodedBody: string | Readable;
  readonly descriptor: BatchExportManifestDescriptor;
  readonly expected: BatchExportManifestMetadata;
  readonly maxRows: number;
}): Promise<AsyncIterable<BatchExportIdentity>> {
  requireMetadata(input.expected);
  requireMaxRows(input.maxRows);
  if (
    input.descriptor.formatVersion !== FORMAT_VERSION ||
    !SHA256_HEX.test(input.descriptor.checksum) ||
    !Number.isSafeInteger(input.descriptor.rowCount) ||
    input.descriptor.rowCount < 0 ||
    input.descriptor.rowCount > input.maxRows ||
    input.descriptor.byteCount < 1n
  ) {
    throw new Error("Batch export manifest descriptor is invalid");
  }
  const maxOutputLength =
    HEADER_ALLOWANCE_BYTES + input.maxRows * MAX_LINE_BYTES;
  const encodedSource =
    typeof input.encodedBody === "string"
      ? Readable.from([input.encodedBody])
      : input.encodedBody;
  const encodedHash = createHash("sha256");
  let encodedBytes = 0n;
  let encodedCharacters = 0;
  let paddingCharacters = 0;
  let paddingStarted = false;
  let encodedTail = "";
  const verifyEncoded = new Transform({
    transform(chunk: Buffer | string, _encoding, callback) {
      const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      encodedBytes += BigInt(value.length);
      if (encodedBytes > input.descriptor.byteCount) {
        callback(new Error("Batch export manifest byte count does not match"));
        return;
      }
      const text = value.toString("utf8");
      if (
        Buffer.byteLength(text) !== value.length ||
        !/^[A-Za-z0-9+/=]*$/.test(text)
      ) {
        callback(new Error("Batch export manifest encoding is invalid"));
        return;
      }
      for (const character of text) {
        if (character === "=") {
          paddingStarted = true;
          paddingCharacters += 1;
        } else if (paddingStarted) {
          callback(new Error("Batch export manifest encoding is invalid"));
          return;
        }
      }
      encodedCharacters += text.length;
      encodedTail = (encodedTail + text).slice(-4);
      encodedHash.update(value);
      callback(null, value);
    },
    flush(callback) {
      if (encodedBytes !== input.descriptor.byteCount) {
        callback(new Error("Batch export manifest byte count does not match"));
        return;
      }
      if (
        encodedCharacters === 0 ||
        encodedCharacters % 4 !== 0 ||
        paddingCharacters > 2 ||
        encodedTail.length !== 4 ||
        Buffer.from(encodedTail, "base64").toString("base64") !== encodedTail
      ) {
        callback(new Error("Batch export manifest encoding is invalid"));
        return;
      }
      if (encodedHash.digest("hex") !== input.descriptor.checksum) {
        callback(new Error("Batch export manifest checksum does not match"));
        return;
      }
      callback();
    },
  });
  const decompressed = new PassThrough();
  const decodePipeline = pipeline(
    encodedSource,
    verifyEncoded,
    new Base64DecodeTransform(),
    createGunzip(),
    decompressed,
  );
  let decodePipelineError: unknown;
  const observedDecodePipeline = decodePipeline.catch((error: unknown) => {
    decodePipelineError = error;
  });
  const lines = async function* () {
    const decoder = new StringDecoder("utf8");
    let carry = "";
    let outputBytes = 0;
    let lineIndex = 0;
    try {
      for await (const chunk of decompressed) {
        const value = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
        outputBytes += value.length;
        if (outputBytes > maxOutputLength) {
          decompressed.destroy();
          throw new Error("Batch export manifest decompression limit exceeded");
        }
        carry += decoder.write(value);
        let newline = carry.indexOf("\n");
        while (newline >= 0) {
          const line = carry.slice(0, newline);
          carry = carry.slice(newline + 1);
          const maxLineBytes =
            lineIndex === 0 ? HEADER_ALLOWANCE_BYTES : MAX_LINE_BYTES;
          if (Buffer.byteLength(line) > maxLineBytes) {
            throw new Error("Batch export manifest line is too large");
          }
          lineIndex += 1;
          yield line;
          newline = carry.indexOf("\n");
        }
        const maxCarryBytes =
          lineIndex === 0 ? HEADER_ALLOWANCE_BYTES : MAX_LINE_BYTES;
        if (Buffer.byteLength(carry) > maxCarryBytes) {
          throw new Error("Batch export manifest line is too large");
        }
      }
      carry += decoder.end();
      if (carry.length > 0) yield carry;
      await observedDecodePipeline;
      if (decodePipelineError) throw decodePipelineError;
    } catch (error) {
      if (
        error instanceof Error &&
        error.message.startsWith("Batch export manifest")
      ) {
        throw error;
      }
      throw new Error("Batch export manifest decompression failed");
    } finally {
      if (!decompressed.readableEnded && !decompressed.destroyed) {
        decompressed.destroy();
      }
      await observedDecodePipeline;
    }
  };
  const iterator = lines()[Symbol.asyncIterator]();
  const first = await iterator.next();
  if (first.done || !first.value) {
    throw new Error("Batch export manifest metadata is missing");
  }
  let header: unknown;
  try {
    header = JSON.parse(first.value);
  } catch {
    throw new Error("Batch export manifest metadata is invalid");
  }
  parseHeader(header, input.expected);

  return {
    async *[Symbol.asyncIterator]() {
      let rowCount = 0;
      let previousKey: string | undefined;
      let completed = false;
      try {
        for (;;) {
          const next = await iterator.next();
          if (next.done) break;
          const line = next.value;
          if (!line) {
            throw new Error("Batch export manifest identity is invalid");
          }
          let parsed: unknown;
          try {
            parsed = JSON.parse(line);
          } catch {
            throw new Error("Batch export manifest identity is invalid");
          }
          const identity = requireIdentity(parsed);
          const key = identityKey(identity);
          if (previousKey !== undefined && key <= previousKey) {
            throw new Error(
              "Batch export manifest identities are not strictly sorted",
            );
          }
          rowCount += 1;
          if (rowCount > input.maxRows) {
            throw new Error("Batch export manifest exceeded its row limit");
          }
          previousKey = key;
          yield identity;
        }
        if (rowCount !== input.descriptor.rowCount) {
          throw new Error("Batch export manifest row count does not match");
        }
        completed = true;
      } finally {
        if (!completed) await iterator.return?.();
      }
    },
  };
}
