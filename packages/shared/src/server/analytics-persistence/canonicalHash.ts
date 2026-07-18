import { createHash } from "node:crypto";

export const INT64_MIN = -9_223_372_036_854_775_808n;
export const INT64_MAX = 9_223_372_036_854_775_807n;

export type VersionTokenInput =
  | bigint
  | string
  | {
      readonly seconds: number | string | bigint;
      readonly nanos: number;
    };

const RFC3339_RE =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})$/;

function parseRfc3339(value: string): bigint {
  const match = RFC3339_RE.exec(value);
  if (!match) throw new Error("Invalid RFC3339 source timestamp");
  const [, year, month, day, hour, minute, second, fraction, offset] = match;
  const y = Number(year);
  const m = Number(month);
  const d = Number(day);
  const h = Number(hour);
  const min = Number(minute);
  const sec = Number(second);
  const maxDay = new Date(Date.UTC(y, m, 0)).getUTCDate();
  if (
    m < 1 ||
    m > 12 ||
    d < 1 ||
    d > maxDay ||
    h > 23 ||
    min > 59 ||
    sec > 59
  ) {
    throw new Error("Invalid RFC3339 source timestamp");
  }
  if (offset !== "Z") {
    const offsetHour = Number(offset.slice(1, 3));
    const offsetMinute = Number(offset.slice(4, 6));
    if (offsetHour > 23 || offsetMinute > 59) {
      throw new Error("Invalid RFC3339 source timestamp");
    }
  }
  const wholeSecondMs = Date.parse(
    `${year}-${month}-${day}T${hour}:${minute}:${second}${offset}`,
  );
  if (!Number.isFinite(wholeSecondMs)) {
    throw new Error("Invalid RFC3339 source timestamp");
  }
  const nanos = BigInt((fraction ?? "").padEnd(9, "0"));
  return BigInt(wholeSecondMs) * 1_000_000n + nanos;
}

export function normalizeVersionToken(input: VersionTokenInput): bigint {
  let token: bigint;
  if (typeof input === "bigint") {
    token = input;
  } else if (typeof input === "string") {
    const trimmed = input.trim();
    if (!trimmed) throw new Error("Source timestamp is required");
    token = /^-?\d+$/.test(trimmed) ? BigInt(trimmed) : parseRfc3339(trimmed);
  } else {
    if (
      (typeof input.seconds === "number" &&
        !Number.isSafeInteger(input.seconds)) ||
      !Number.isInteger(input.nanos) ||
      input.nanos < 0 ||
      input.nanos > 999_999_999
    ) {
      throw new Error("Invalid protobuf source timestamp");
    }
    token = BigInt(input.seconds) * 1_000_000_000n + BigInt(input.nanos);
  }
  if (token < INT64_MIN || token >= INT64_MAX) {
    throw new Error("Source timestamp is outside the supported sequence range");
  }
  return token;
}

export function partitionDateFromVersionToken(token: bigint): string {
  const millis =
    token >= 0n ? token / 1_000_000n : (token - 999_999n) / 1_000_000n;
  const asNumber = Number(millis);
  const date = new Date(asNumber);
  if (!Number.isSafeInteger(asNumber) || Number.isNaN(date.getTime())) {
    throw new Error("Source timestamp is outside the supported calendar range");
  }
  return date.toISOString().slice(0, 10);
}

export interface EventIdentity {
  readonly projectId: string;
  readonly traceId: string;
  readonly spanId: string;
}

export interface ScoreIdentity {
  readonly projectId: string;
  readonly scoreId: string;
}

export interface FileReferenceIdentity {
  readonly projectId: string;
  readonly entityType: "EVENT" | "SCORE";
  readonly entityId: string;
  readonly fileId: string;
}

function encodeField(value: string): Buffer {
  const bytes = Buffer.from(value, "utf8");
  return Buffer.concat([Buffer.from(`${bytes.byteLength}:`, "ascii"), bytes]);
}

export function encodeEventIdentity(identity: EventIdentity): string {
  if (!identity.projectId || !identity.traceId || !identity.spanId) {
    throw new Error("Event identity is invalid");
  }
  return Buffer.concat([
    Buffer.from("event\0", "ascii"),
    encodeField(identity.projectId),
    encodeField(identity.traceId),
    encodeField(identity.spanId),
  ]).toString("base64url");
}

export function encodeScoreIdentity(identity: ScoreIdentity): string {
  if (!identity.projectId || !identity.scoreId) {
    throw new Error("Score identity is invalid");
  }
  return Buffer.concat([
    Buffer.from("score\0", "ascii"),
    encodeField(identity.projectId),
    encodeField(identity.scoreId),
  ]).toString("base64url");
}

export function encodeFileReferenceIdentity(
  identity: FileReferenceIdentity,
): string {
  if (!identity.projectId || !identity.entityId || !identity.fileId) {
    throw new Error("File-reference identity is invalid");
  }
  return Buffer.concat([
    Buffer.from("file-reference\0", "ascii"),
    encodeField(identity.projectId),
    encodeField(identity.entityType),
    encodeField(identity.entityId),
    encodeField(identity.fileId),
  ]).toString("base64url");
}

function decodeField(
  bytes: Buffer,
  offset: number,
): { readonly value: string; readonly nextOffset: number } {
  const colon = bytes.indexOf(58, offset);
  if (colon < 0) throw new Error("Event identity is malformed");
  const lengthText = bytes.subarray(offset, colon).toString("ascii");
  if (!/^\d+$/.test(lengthText)) throw new Error("Event identity is malformed");
  const length = Number(lengthText);
  const start = colon + 1;
  const end = start + length;
  if (!Number.isSafeInteger(length) || end > bytes.byteLength) {
    throw new Error("Event identity is malformed");
  }
  return {
    value: bytes.subarray(start, end).toString("utf8"),
    nextOffset: end,
  };
}

export function toEventIdentity(encoded: string): EventIdentity {
  if (!/^[A-Za-z0-9_-]+$/.test(encoded)) {
    throw new Error("Event identity is malformed");
  }
  const bytes = Buffer.from(encoded, "base64url");
  if (bytes.toString("base64url") !== encoded) {
    throw new Error("Event identity is malformed");
  }
  const prefix = Buffer.from("event\0", "ascii");
  if (!bytes.subarray(0, prefix.byteLength).equals(prefix)) {
    throw new Error("Event identity is malformed");
  }
  const fields: string[] = [];
  let offset = prefix.byteLength;
  for (let index = 0; index < 3; index += 1) {
    const decoded = decodeField(bytes, offset);
    fields.push(decoded.value);
    offset = decoded.nextOffset;
  }
  if (offset !== bytes.byteLength)
    throw new Error("Event identity is malformed");
  const identity = {
    projectId: fields[0] ?? "",
    traceId: fields[1] ?? "",
    spanId: fields[2] ?? "",
  };
  encodeEventIdentity(identity);
  return identity;
}

function lengthPrefix(value: string): string {
  return `${Buffer.byteLength(value, "utf8")}:${value}`;
}

function canonicalize(value: unknown, inArray = false): string {
  if (value === null) return "null";
  if (value === undefined) {
    if (inArray) throw new Error("Undefined array values are not canonical");
    return "undefined";
  }
  if (typeof value === "bigint")
    return `bigint:${lengthPrefix(value.toString())}`;
  if (typeof value === "string") return `string:${lengthPrefix(value)}`;
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number") {
    if (!Number.isFinite(value))
      throw new Error("Non-finite numbers are not canonical");
    return `number:${lengthPrefix(Object.is(value, -0) ? "0" : String(value))}`;
  }
  if (Array.isArray(value)) {
    return `array:[${value.map((item) => canonicalize(item, true)).join(",")}]`;
  }
  if (typeof value === "object") {
    const object = value as Record<string, unknown>;
    const keys = Object.keys(object)
      .filter((key) => object[key] !== undefined)
      .sort();
    return `object:{${keys
      .map((key) => `${lengthPrefix(key)}=${canonicalize(object[key])}`)
      .join(",")}}`;
  }
  throw new Error("Unsupported canonical payload value");
}

export function canonicalPayloadHash(payload: unknown): string {
  return createHash("sha256")
    .update("langfuse-analytics-canonical-v1\0", "utf8")
    .update(canonicalize(payload), "utf8")
    .digest("hex");
}
