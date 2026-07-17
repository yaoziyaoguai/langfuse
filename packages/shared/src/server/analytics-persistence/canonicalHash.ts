// U3 storage-neutral canonical Source Version Contract — pure logic core.
//
// The timestamp-token normalization, typed-identity encoding, and canonical
// payload hash that the Doris physical design (U1), the durable writer (U4),
// and the read compiler (U5) depend on. No Doris, no ClickHouse, no physical
// row type crosses this boundary; these functions are deterministic and free of
// processing-clock / arrival-order dependence.
//
// Invariants (frozen by the plan PRD §6.2 and the U1 corpus):
//   * version_token = UTC Unix-epoch NANOSECONDS as a checked signed BIGINT.
//     Equivalent RFC3339 / protobuf / decimal expressions yield the same token.
//     TypeScript never rounds through `number` (lossy beyond 2^53). Ordinary
//     sequence < INT64_MAX; a terminal delete carries INT64_MAX.
//   * event identity = collision-free, length-prefixed (trace_id, span_id) pair.
//   * canonical_payload_hash = SHA-256 of a domain-separated, length-prefixed,
//     key-sorted tuple of schema_version + typed identity + normalized version
//     token + deterministic canonical child JSON. Arrays and Unicode code points
//     are preserved verbatim (no content-rearranging normalization).

import { createHash } from "node:crypto";

export const INT64_MAX = 9_223_372_036_854_775_807n;

// ---------------------------------------------------------------------------
// Version token normalization
// ---------------------------------------------------------------------------

export type VersionTokenInput =
  | bigint
  | string
  | { readonly seconds: number | string | bigint; readonly nanos: number };

/**
 * Normalize a raw source-time expression to UTC Unix-epoch nanoseconds (BigInt).
 * Equivalent RFC3339 / decimal-epoch-nanos / protobuf {seconds,nanos} inputs
 * collapse to the same token. Throws on unparseable input or values > INT64_MAX
 * (the reserved terminal-delete token boundary).
 */
export function normalizeVersionToken(input: VersionTokenInput): bigint {
  let nanos: bigint;
  if (typeof input === "bigint") {
    nanos = input;
  } else if (typeof input === "string") {
    nanos = parseTimeString(input);
  } else if (
    typeof input === "object" &&
    input !== null &&
    "seconds" in input
  ) {
    const secs = BigInt(input.seconds);
    const ns = BigInt(input.nanos ?? 0);
    nanos = secs * 1_000_000_000n + ns;
  } else {
    throw new Error(
      `Cannot normalize version token: unsupported input ${String(input)}`,
    );
  }
  if (nanos > INT64_MAX) {
    throw new Error(
      `Version token ${nanos} exceeds INT64_MAX (reserved for terminal delete)`,
    );
  }
  return nanos;
}

const ISO_RE =
  /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})?$/;

function parseTimeString(s: string): bigint {
  const trimmed = s.trim();
  if (trimmed === "") {
    throw new Error("Cannot normalize empty version token");
  }
  // All-digits => decimal epoch-nanoseconds.
  if (/^-?\d+$/.test(trimmed)) {
    return BigInt(trimmed);
  }
  const m = ISO_RE.exec(trimmed);
  if (!m) {
    throw new Error(`Cannot parse version token timestamp: ${s}`);
  }
  const secondsMs = Date.parse(`${m[1]}${m[3] ?? "Z"}`);
  if (!Number.isFinite(secondsMs)) {
    throw new Error(`Cannot parse version token timestamp: ${s}`);
  }
  const frac = m[2] ? m[2].padEnd(9, "0").slice(0, 9) : "000000000";
  return BigInt(secondsMs) * 1_000_000n + BigInt(frac);
}

// ---------------------------------------------------------------------------
// Typed event identity (collision-free, length-prefixed)
// ---------------------------------------------------------------------------

export interface EventIdentity {
  readonly project_id: string;
  readonly partition_date: string; // immutable UTC date (YYYY-MM-DD)
  readonly trace_id: string;
  readonly span_id: string;
}

/**
 * Length-prefixed encoding of (project_id, partition_date, trace_id, span_id).
 * Length-prefixing prevents ambiguous concatenation collisions such as
 * (trace, 12spans) vs (trace1, 2spans). Reversible by toEventIdentity.
 */
export function encodeEventIdentity(id: EventIdentity): string {
  return (
    lengthPrefix(id.project_id) +
    lengthPrefix(id.partition_date) +
    lengthPrefix(id.trace_id) +
    lengthPrefix(id.span_id)
  );
}

export function toEventIdentity(encoded: string): EventIdentity {
  let rest = encoded;
  const fields: string[] = [];
  for (let i = 0; i < 4 && rest.length > 0; i++) {
    const [value, next] = readLengthPrefix(rest);
    fields.push(value);
    rest = next;
  }
  return {
    project_id: fields[0],
    partition_date: fields[1],
    trace_id: fields[2],
    span_id: fields[3],
  };
}

function lengthPrefix(s: string): string {
  return `${Buffer.byteLength(s, "utf8")}:${s}`;
}

function readLengthPrefix(s: string): [value: string, rest: string] {
  const colon = s.indexOf(":");
  if (colon < 0) throw new Error(`Malformed length-prefixed identity: ${s}`);
  const len = Number(s.slice(0, colon));
  const start = colon + 1;
  const buf = Buffer.from(s, "utf8").subarray(start, start + len);
  return [buf.toString("utf8"), s.slice(start + len)];
}

// ---------------------------------------------------------------------------
// Canonical payload hash
// ---------------------------------------------------------------------------

export interface CanonicalAnalyticsEvent {
  /** Domain-separating canonicalizer/schema version, e.g. "v4@1". */
  readonly schema_version: string;
  readonly identity: EventIdentity;
  readonly version_token: bigint;
  readonly type: string;
  readonly name: string;
  /** Start time as RFC3339 or epoch-nanoseconds; normalized before hashing. */
  readonly start_time: string;
  /** Resolved enrichment identifiers included in the hash (R10 replay contract). */
  readonly resolved_enrichment_ids?: Readonly<Record<string, string>>;
  readonly tags?: readonly string[];
  readonly [extra: string]: unknown;
}

/**
 * Deterministic SHA-256 of a domain-separated, key-sorted, length-prefixed
 * tuple of the canonical event. JSON object key order does not matter; RFC3339
 * and epoch-nanosecond start times hash identically; array order and Unicode
 * code points are preserved; schema_version domain-separates the hash.
 */
export function canonicalPayloadHash(event: CanonicalAnalyticsEvent): string {
  const normalized: Record<string, unknown> = { ...event };
  // Normalize the timestamp so equivalent expressions hash identically.
  normalized.start_time = normalizeVersionToken(event.start_time).toString(10);
  return createHash("sha256")
    .update(canonicalize(normalized), "utf8")
    .digest("hex");
}

function canonicalize(value: unknown): string {
  if (value === null || value === undefined) return "n";
  if (typeof value === "bigint") return `b${lengthPrefix(value.toString(10))}`;
  if (typeof value === "string") return `s${lengthPrefix(value)}`;
  if (typeof value === "boolean") return value ? "t" : "f";
  if (typeof value === "number") return `d${lengthPrefix(value.toString())}`;
  if (Array.isArray(value)) {
    return `a[${value.map((v) => canonicalize(v)).join(",")}]`;
  }
  if (typeof value === "object") {
    const obj = value as Record<string, unknown>;
    const keys = Object.keys(obj).sort();
    return `o{${keys.map((k) => `${lengthPrefix(k)}:${canonicalize(obj[k])}`).join(",")}}`;
  }
  return `u${lengthPrefix(String(value))}`;
}
