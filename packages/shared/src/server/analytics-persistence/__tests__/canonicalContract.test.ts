import { describe, expect, it } from "vitest";

import {
  canonicalEntityPayloadHash,
  canonicalPayloadHash,
  encodeEventIdentity,
  encodeFileReferenceIdentity,
  encodeScoreIdentity,
  INT64_MAX,
  INT64_MIN,
  normalizeVersionToken,
  partitionDateFromVersionToken,
  toEventIdentity,
  toFileReferenceIdentity,
  toScoreIdentity,
  type EventIdentity,
} from "../canonicalHash";

describe("normalizeVersionToken", () => {
  it("normalizes equivalent RFC3339, decimal, and protobuf values exactly", () => {
    const expected = 1_784_282_400_123_456_789n;
    expect(normalizeVersionToken("2026-07-17T10:00:00.123456789Z")).toBe(
      expected,
    );
    expect(normalizeVersionToken(expected.toString())).toBe(expected);
    expect(
      normalizeVersionToken({ seconds: "1784282400", nanos: 123_456_789 }),
    ).toBe(expected);
  });

  it("enforces the signed BIGINT range and reserves terminal delete", () => {
    expect(normalizeVersionToken(INT64_MIN)).toBe(INT64_MIN);
    expect(() => normalizeVersionToken(INT64_MIN - 1n)).toThrow();
    expect(() => normalizeVersionToken(INT64_MAX)).toThrow();
  });

  it("floors negative sub-millisecond values into the previous UTC day", () => {
    expect(partitionDateFromVersionToken(-1n)).toBe("1969-12-31");
  });
});

describe("event identity", () => {
  it("does not collide for ambiguous pairs or equal span IDs across traces", () => {
    const identity = (traceId: string, spanId: string) =>
      encodeEventIdentity({
        projectId: "project-1",
        traceId,
        spanId,
      });
    expect(identity("trace", "12spans")).not.toBe(identity("trace1", "2spans"));
    expect(identity("trace-a", "span-1")).not.toBe(
      identity("trace-b", "span-1"),
    );
  });

  it("keeps the logical entity key stable across a forbidden partition mutation", () => {
    const base = {
      projectId: "project-1",
      traceId: "trace-1",
      spanId: "span-1",
    };
    expect(
      encodeEventIdentity({
        ...base,
        partitionDate: "2026-07-17",
      } as EventIdentity),
    ).toBe(
      encodeEventIdentity({
        ...base,
        partitionDate: "2026-07-18",
      } as EventIdentity),
    );
  });

  it("round-trips a typed identity", () => {
    const identity: EventIdentity = {
      projectId: "project-1",
      traceId: "trace-1",
      spanId: "span-1",
    };
    expect(toEventIdentity(encodeEventIdentity(identity))).toEqual(identity);
    expect(() =>
      toEventIdentity(`${encodeEventIdentity(identity)}=`),
    ).toThrow();
  });
});

describe("deletion identity decoding", () => {
  it("round-trips score and file-reference primary keys", () => {
    const score = { projectId: "project-1", scoreId: "score-1" };
    const file = {
      projectId: "project-1",
      entityType: "EVENT" as const,
      entityId: "span-1",
      fileId: "file-1",
    };
    expect(toScoreIdentity(encodeScoreIdentity(score))).toEqual(score);
    expect(toFileReferenceIdentity(encodeFileReferenceIdentity(file))).toEqual(
      file,
    );
  });
});

describe("canonicalPayloadHash", () => {
  const payload = {
    schemaVersion: 1,
    sourceVersion: 1_784_282_400_123_456_789n,
    identity: {
      projectId: "project-1",
      partitionDate: "2026-07-17",
      traceId: "trace-1",
      spanId: "span-1",
    },
    tags: ["🚀", "价格", "beta"],
    resolvedEnrichmentIds: { promptId: "prompt-1", modelId: "model-1" },
  } as const;

  it("ignores object key order but preserves array order", () => {
    const reordered = {
      resolvedEnrichmentIds: { modelId: "model-1", promptId: "prompt-1" },
      tags: ["🚀", "价格", "beta"],
      identity: {
        spanId: "span-1",
        traceId: "trace-1",
        partitionDate: "2026-07-17",
        projectId: "project-1",
      },
      sourceVersion: 1_784_282_400_123_456_789n,
      schemaVersion: 1,
    } as const;
    expect(canonicalPayloadHash(reordered)).toBe(canonicalPayloadHash(payload));
    expect(
      canonicalPayloadHash({ ...payload, tags: ["价格", "🚀", "beta"] }),
    ).not.toBe(canonicalPayloadHash(payload));
  });

  it("domain-separates meaningful version and enrichment changes", () => {
    expect(canonicalPayloadHash({ ...payload, schemaVersion: 2 })).not.toBe(
      canonicalPayloadHash(payload),
    );
    expect(
      canonicalPayloadHash({
        ...payload,
        resolvedEnrichmentIds: {
          promptId: "prompt-2",
          modelId: "model-1",
        },
      }),
    ).not.toBe(canonicalPayloadHash(payload));
  });

  it("keeps entity identity stable across ingestion-local receipt fields", () => {
    const first = {
      ...payload,
      rawObjectKey: "raw/project-1/operation-1",
      systemTimestamp: 1_784_282_600_000_000_000n,
    };
    const replay = {
      ...first,
      rawObjectKey: "raw/project-1/operation-replay",
      systemTimestamp: 1_784_282_700_000_000_000n,
    };

    expect(canonicalEntityPayloadHash(replay)).toBe(
      canonicalEntityPayloadHash(first),
    );
  });
});
