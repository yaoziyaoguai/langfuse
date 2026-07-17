// U3 contract tests for the storage-neutral canonical Source Version Contract.
//
// These freeze the pure-logic core that U4 (durable writer), U5 (reads), and the
// Doris physical design (U1) all depend on: timestamp-token normalization,
// canonical payload hash determinism, typed identity encoding, and the domain
// error taxonomy. No Doris, no ClickHouse, no physical row types cross this
// boundary.

import { describe, expect, it } from "vitest";
import {
  canonicalPayloadHash,
  encodeEventIdentity,
  INT64_MAX,
  normalizeVersionToken,
  toEventIdentity,
  type CanonicalAnalyticsEvent,
  type EventIdentity,
} from "../canonicalHash";

describe("normalizeVersionToken", () => {
  it("normalizes equivalent RFC3339 / epoch-nanos / protobuf expressions to the same BigInt token", () => {
    // Authoritative UTC epoch-ms for 2026-07-17T10:00:00Z (no hand computation).
    const baseMs = Date.UTC(2026, 6, 17, 10, 0, 0);
    const baseNs = BigInt(baseMs) * 1_000_000n;
    expect(normalizeVersionToken("2026-07-17T10:00:00.123456789Z")).toBe(
      baseNs + 123_456_789n,
    );
    expect(normalizeVersionToken(baseNs + 123_456_789n)).toBe(
      baseNs + 123_456_789n,
    );
    // Protobuf {seconds, nanos} -> same epoch-nanos token.
    expect(normalizeVersionToken({ seconds: baseMs / 1000, nanos: 0 })).toBe(
      baseNs,
    );
    expect(normalizeVersionToken("2026-07-17T10:00:00Z")).toBe(baseNs);
  });

  it("does not lose precision through JS number (beyond 2^53)", () => {
    const big = 9_000_719_925_474_099_3n; // > 2^53
    expect(normalizeVersionToken(big)).toBe(big);
    expect(normalizeVersionToken("90007199254740993")).toBe(big);
  });

  it("reserves INT64_MAX for terminal delete and rejects out-of-range", () => {
    expect(INT64_MAX).toBe(9_223_372_036_854_775_807n);
    expect(() => normalizeVersionToken(INT64_MAX + 1n)).toThrow();
  });

  it("rejects unparseable / invalid source time", () => {
    expect(() => normalizeVersionToken("not-a-time")).toThrow();
    expect(() => normalizeVersionToken("")).toThrow();
  });
});

describe("encodeEventIdentity / toEventIdentity", () => {
  it("is collision-free: equal span_id under different trace is distinct", () => {
    const a = encodeEventIdentity({
      project_id: "p1",
      partition_date: "2026-07-17",
      trace_id: "traceA",
      span_id: "0000000000000001",
    });
    const b = encodeEventIdentity({
      project_id: "p1",
      partition_date: "2026-07-17",
      trace_id: "traceB",
      span_id: "0000000000000001",
    });
    expect(a).not.toBe(b);
  });

  it("is length-prefixed so ambiguous concatenation cannot collide", () => {
    // ("trace", "12spans") vs ("trace1", "2spans") must NOT collide under naive
    // concatenation; length-prefixing prevents it.
    const x = encodeEventIdentity({
      project_id: "p1",
      partition_date: "2026-07-17",
      trace_id: "trace",
      span_id: "12spans",
    });
    const y = encodeEventIdentity({
      project_id: "p1",
      partition_date: "2026-07-17",
      trace_id: "trace1",
      span_id: "2spans",
    });
    expect(x).not.toBe(y);
  });

  it("round-trips through the typed identity", () => {
    const id: EventIdentity = {
      project_id: "p1",
      partition_date: "2026-07-17",
      trace_id: "t",
      span_id: "s",
    };
    expect(toEventIdentity(encodeEventIdentity(id))).toEqual(id);
  });
});

describe("canonicalPayloadHash", () => {
  const baseEvent = (
    over: Partial<CanonicalAnalyticsEvent>,
  ): CanonicalAnalyticsEvent => ({
    schema_version: "v4@1",
    identity: {
      project_id: "p1",
      partition_date: "2026-07-17",
      trace_id: "t1",
      span_id: "s1",
    },
    version_token: 1000n,
    type: "generation",
    name: "gen",
    start_time: "2026-07-17T10:00:00Z",
    resolved_enrichment_ids: { prompt_id: "prm-1", model_price_id: "mp-1" },
    ...over,
  });

  it("is deterministic regardless of JSON object key order", () => {
    const a = baseEvent({});
    const b = baseEvent({});
    // Re-serialize with shuffled top-level key order: hash must be identical.
    const shuffled: CanonicalAnalyticsEvent = {
      ...baseEvent({}),
      identity: {
        trace_id: "t1",
        span_id: "s1",
        project_id: "p1",
        partition_date: "2026-07-17",
      },
      resolved_enrichment_ids: { model_price_id: "mp-1", prompt_id: "prm-1" },
    };
    expect(canonicalPayloadHash(shuffled)).toBe(canonicalPayloadHash(a));
    expect(canonicalPayloadHash(b)).toBe(canonicalPayloadHash(a));
  });

  it("treats equivalent timestamps (RFC3339 vs epoch-nanos) as the same content", () => {
    const epochNanos = String(
      BigInt(Date.UTC(2026, 6, 17, 10, 0, 0)) * 1_000_000n,
    );
    const a = baseEvent({ start_time: "2026-07-17T10:00:00Z" });
    const b = baseEvent({ start_time: epochNanos });
    expect(canonicalPayloadHash(b)).toBe(canonicalPayloadHash(a));
  });

  it("changes when a meaningful field changes (name, version, identity, enrichment)", () => {
    const a = baseEvent({});
    expect(canonicalPayloadHash(baseEvent({ name: "gen-2" }))).not.toBe(
      canonicalPayloadHash(a),
    );
    expect(canonicalPayloadHash(baseEvent({ version_token: 2000n }))).not.toBe(
      canonicalPayloadHash(a),
    );
    expect(
      canonicalPayloadHash(
        baseEvent({
          identity: {
            project_id: "p1",
            partition_date: "2026-07-17",
            trace_id: "t2",
            span_id: "s1",
          },
        }),
      ),
    ).not.toBe(canonicalPayloadHash(a));
    expect(
      canonicalPayloadHash(
        baseEvent({
          resolved_enrichment_ids: {
            prompt_id: "prm-2",
            model_price_id: "mp-1",
          },
        }),
      ),
    ).not.toBe(canonicalPayloadHash(a));
  });

  it("preserves array order and Unicode code points (no content-rearranging normalization)", () => {
    const a = baseEvent({ tags: ["🚀", "价格", "beta"] });
    const b = baseEvent({ tags: ["价格", "🚀", "beta"] });
    expect(canonicalPayloadHash(a)).not.toBe(canonicalPayloadHash(b));
    expect(
      canonicalPayloadHash(baseEvent({ tags: ["🚀", "价格", "beta"] })),
    ).toBe(canonicalPayloadHash(a));
  });

  it("is domain-separated: same payload under different schema versions hashes differently", () => {
    const a = baseEvent({});
    const b = baseEvent({ schema_version: "v4@2" });
    expect(canonicalPayloadHash(a)).not.toBe(canonicalPayloadHash(b));
  });
});
