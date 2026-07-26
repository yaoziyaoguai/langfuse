import { describe, expect, it } from "vitest";

import { fingerprintAnalyticsQueueNamespace } from "./analyticsQueueNamespace";

const shardCountKeys = [
  "LANGFUSE_INGESTION_QUEUE_SHARD_COUNT",
  "LANGFUSE_INGESTION_SECONDARY_QUEUE_SHARD_COUNT",
  "LANGFUSE_OTEL_INGESTION_QUEUE_SHARD_COUNT",
  "LANGFUSE_OTEL_INGESTION_SECONDARY_QUEUE_SHARD_COUNT",
  "LANGFUSE_TRACE_UPSERT_QUEUE_SHARD_COUNT",
  "LANGFUSE_EVAL_EXECUTION_QUEUE_SHARD_COUNT",
  "LANGFUSE_EVAL_EXECUTION_SECONDARY_QUEUE_SHARD_COUNT",
  "LANGFUSE_LLM_AS_JUDGE_EXECUTION_QUEUE_SHARD_COUNT",
  "LANGFUSE_CODE_EVAL_EXECUTION_QUEUE_SHARD_COUNT",
] as const;

describe("analytics queue namespace fingerprint", () => {
  it("binds endpoint, logical database, prefix, and queue layout", () => {
    const base = {
      REDIS_CLUSTER_ENABLED: "false",
      REDIS_SENTINEL_ENABLED: "false",
      REDIS_CONNECTION_STRING: "redis://user:secret@redis.internal:6379/4",
      REDIS_KEY_PREFIX: "tenant-a",
    };
    const fingerprint = fingerprintAnalyticsQueueNamespace(base);

    expect(fingerprint).toMatch(/^[a-f0-9]{64}$/);
    expect(
      fingerprintAnalyticsQueueNamespace({
        ...base,
        REDIS_CONNECTION_STRING:
          "redis://different:credentials@redis.internal:6379/4",
      }),
    ).toBe(fingerprint);
    expect(
      fingerprintAnalyticsQueueNamespace({
        ...base,
        REDIS_CONNECTION_STRING: "redis://redis.internal:6379/5",
      }),
    ).not.toBe(fingerprint);
    expect(
      fingerprintAnalyticsQueueNamespace({
        ...base,
        REDIS_KEY_PREFIX: "tenant-b",
      }),
    ).not.toBe(fingerprint);
  });

  it("normalizes cluster node order without collapsing topology changes", () => {
    const first = fingerprintAnalyticsQueueNamespace({
      REDIS_CLUSTER_ENABLED: "true",
      REDIS_SENTINEL_ENABLED: "false",
      REDIS_CLUSTER_NODES: "redis-b:6380,redis-a:6379",
      REDIS_KEY_PREFIX: "analytics",
    });
    const reordered = fingerprintAnalyticsQueueNamespace({
      REDIS_CLUSTER_ENABLED: "true",
      REDIS_SENTINEL_ENABLED: "false",
      REDIS_CLUSTER_NODES: "redis-a:6379,redis-b:6380",
      REDIS_KEY_PREFIX: "analytics",
    });
    const changed = fingerprintAnalyticsQueueNamespace({
      REDIS_CLUSTER_ENABLED: "true",
      REDIS_SENTINEL_ENABLED: "false",
      REDIS_CLUSTER_NODES: "redis-a:6379,redis-c:6380",
      REDIS_KEY_PREFIX: "analytics",
    });

    expect(reordered).toBe(first);
    expect(changed).not.toBe(first);
  });

  it.each(shardCountKeys)("binds physical queue layout from %s", (key) => {
    const base = {
      REDIS_CLUSTER_ENABLED: "false",
      REDIS_SENTINEL_ENABLED: "false",
      REDIS_CONNECTION_STRING: "redis://redis.internal:6379/4",
      REDIS_KEY_PREFIX: "analytics",
    };

    expect(fingerprintAnalyticsQueueNamespace({ ...base, [key]: 2 })).not.toBe(
      fingerprintAnalyticsQueueNamespace(base),
    );
  });

  it("fingerprints Redis and Sentinel TLS independently", () => {
    const base = {
      REDIS_CLUSTER_ENABLED: "false",
      REDIS_SENTINEL_ENABLED: "true",
      REDIS_SENTINEL_NODES: "sentinel.internal:26379",
      REDIS_SENTINEL_MASTER_NAME: "langfuse-primary",
      REDIS_KEY_PREFIX: "analytics",
    };
    const plaintext = fingerprintAnalyticsQueueNamespace(base);
    const redisTls = fingerprintAnalyticsQueueNamespace({
      ...base,
      REDIS_TLS_ENABLED: "true",
    });
    const sentinelTls = fingerprintAnalyticsQueueNamespace({
      ...base,
      REDIS_SENTINEL_TLS_ENABLED: "true",
    });
    const bothTls = fingerprintAnalyticsQueueNamespace({
      ...base,
      REDIS_TLS_ENABLED: "true",
      REDIS_SENTINEL_TLS_ENABLED: "true",
    });

    expect(new Set([plaintext, redisTls, sentinelTls, bothTls])).toHaveLength(
      4,
    );
  });

  it("rejects ambiguous or malformed configurations without echoing secrets", () => {
    expect(() =>
      fingerprintAnalyticsQueueNamespace({
        REDIS_CLUSTER_ENABLED: "true",
        REDIS_SENTINEL_ENABLED: "true",
      }),
    ).toThrow("topology is ambiguous");

    const secret = "must-not-be-echoed";
    try {
      fingerprintAnalyticsQueueNamespace({
        REDIS_CONNECTION_STRING: `not-a-url-${secret}`,
      });
      throw new Error("Expected malformed Redis URL rejection");
    } catch (error) {
      expect(String(error)).not.toContain(secret);
    }

    for (const suffix of ["?db=5", "?keyPrefix=shadow%3A", "#fragment"]) {
      expect(() =>
        fingerprintAnalyticsQueueNamespace({
          REDIS_CONNECTION_STRING: `redis://user:${secret}@redis.internal:6379/4${suffix}`,
        }),
      ).toThrow(/query or fragment/i);
    }

    expect(() =>
      fingerprintAnalyticsQueueNamespace({
        REDIS_CLUSTER_ENABLED: "false",
        REDIS_SENTINEL_ENABLED: "true",
        REDIS_SENTINEL_NODES: "sentinel.internal:26379",
      }),
    ).toThrow(/master name/i);
  });
});
