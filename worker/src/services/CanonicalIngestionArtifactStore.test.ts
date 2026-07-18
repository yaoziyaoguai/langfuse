import { createHash } from "node:crypto";

import {
  canonicalPayloadHash,
  normalizeVersionToken,
  type CanonicalAnalyticsBatch,
  type CanonicalAnalyticsScore,
} from "@langfuse/shared/analytics-persistence";
import { describe, expect, it } from "vitest";

import {
  CanonicalIngestionArtifactStore,
  canonicalArtifactObjectKey,
  decodeCanonicalArtifact,
  encodeCanonicalArtifact,
  type ConditionalCanonicalObjectStore,
} from "./CanonicalIngestionArtifactStore";

const acceptedAt = normalizeVersionToken("2026-07-17T12:00:00.123456789Z");

function score(
  scoreId: string,
  metadata: Readonly<Record<string, string>>,
): CanonicalAnalyticsScore {
  const hashInput = { scoreId, metadata };
  return {
    kind: "score",
    projectId: "project-1",
    partitionDate: "2026-07-17",
    sourceContract: "score",
    sourceVersion: acceptedAt + BigInt(scoreId === "score-a" ? 1 : 2),
    canonicalizerVersion: "1",
    schemaVersion: 3,
    canonicalPayloadHash: canonicalPayloadHash(hashInput),
    systemTimestamp: acceptedAt,
    rawObjectKey: "events/project-1/raw/operation-1.json",
    resolvedEnrichmentIds: {},
    scoreId,
    traceId: "trace-1",
    observationId: null,
    sessionId: null,
    timestamp: acceptedAt,
    name: "quality",
    source: "API",
    dataType: "NUMERIC",
    numericValue: 0.9,
    stringValue: null,
    longStringValue: null,
    booleanValue: null,
    comment: null,
    authorUserId: null,
    configId: null,
    queueId: null,
    environment: "default",
    metadata,
  };
}

function batch(
  children: readonly CanonicalAnalyticsScore[],
): CanonicalAnalyticsBatch {
  return {
    projectId: "project-1",
    operationId: "operation-1",
    canonicalizerVersion: "1",
    schemaVersion: 3,
    acceptedAt,
    rawObjectKey: "events/project-1/raw/operation-1.json",
    children: children.map((entity) => ({
      entity,
      expectedSourceVersion: null,
      fenceGeneration: 9_223_372_036_854_000_000n,
      traceDeletionGeneration: 0n,
      projectDeletionGeneration: 0n,
    })),
  };
}

class MemoryConditionalObjectStore implements ConditionalCanonicalObjectStore {
  readonly objects = new Map<string, string>();

  async putIfAbsent(input: {
    readonly key: string;
    readonly body: string;
    readonly contentType: "application/json";
  }): Promise<"created" | "already_exists"> {
    if (this.objects.has(input.key)) return "already_exists";
    this.objects.set(input.key, input.body);
    return "created";
  }

  async get(key: string): Promise<string | null> {
    return this.objects.get(key) ?? null;
  }
}

describe("CanonicalIngestionArtifactStore", () => {
  it("encodes stable child ordering and round-trips exact bigint fields", () => {
    const first = batch([
      score("score-b", { z: "last", a: "first" }),
      score("score-a", { a: "first", z: "last" }),
    ]);
    const reordered = batch([
      score("score-a", { z: "last", a: "first" }),
      score("score-b", { a: "first", z: "last" }),
    ]);

    const encoded = encodeCanonicalArtifact(first);
    expect(encodeCanonicalArtifact(reordered)).toEqual(encoded);
    expect(encoded.checksum).toBe(
      createHash("sha256").update(encoded.body).digest("hex"),
    );

    const restored = decodeCanonicalArtifact(encoded.body, encoded.checksum);
    expect(restored.acceptedAt).toBe(acceptedAt);
    expect(restored.children[0]?.entity).toMatchObject({ scoreId: "score-a" });
    expect(restored.children[0]?.entity.sourceVersion).toBe(acceptedAt + 1n);
    expect(restored.children[0]?.fenceGeneration).toBe(
      9_223_372_036_854_000_000n,
    );
  });

  it("uses a fence-specific safe key", () => {
    expect(
      canonicalArtifactObjectKey({
        prefix: "events/",
        projectId: "project/with/slash",
        operationId: "operation\\with\\slash",
        fenceGeneration: 7n,
      }),
    ).toMatch(/^events\/canonical-ingestion\/[^/]+\/[^/]+\/fence-7\.json$/);
  });

  it("preserves the first immutable artifact on a conditional-create collision", async () => {
    const objectStore = new MemoryConditionalObjectStore();
    const store = new CanonicalIngestionArtifactStore(objectStore);
    const key = "events/canonical-ingestion/project/operation/fence-1.json";
    const original = batch([score("score-a", { version: "first" })]);
    const conflicting = batch([score("score-a", { version: "second" })]);

    const created = await store.putIfAbsent(key, original);
    const replay = await store.putIfAbsent(key, original);

    expect(created.outcome).toBe("created");
    expect(replay.outcome).toBe("already_exists");
    await expect(store.putIfAbsent(key, conflicting)).rejects.toThrow(
      "Canonical artifact conditional-create collision",
    );
    await expect(store.get(key, created.checksum)).resolves.toEqual(original);
    await expect(store.getIfExists(key)).resolves.toEqual({
      batch: original,
      checksum: created.checksum,
    });
  });

  it("rejects a missing or checksum-mismatched artifact", async () => {
    const store = new CanonicalIngestionArtifactStore(
      new MemoryConditionalObjectStore(),
    );

    await expect(store.get("missing", "a".repeat(64))).rejects.toThrow(
      "Canonical artifact is unavailable",
    );
    await expect(store.get("missing", "not-a-checksum")).rejects.toThrow(
      "Canonical artifact checksum is invalid",
    );
  });
});
