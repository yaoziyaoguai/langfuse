import { createHash } from "node:crypto";

import { env } from "../../env";
import {
  communityAnalyticsQueueShardCountsFromConfig,
  expandCommunityAnalyticsQueueInventory,
  type AnalyticsQueueShardCountConfig,
} from "./analyticsQueueInventory";

const SHA256_HEX = /^[a-f0-9]{64}$/;

export type AnalyticsQueueNamespaceConfig = AnalyticsQueueShardCountConfig & {
  readonly REDIS_CLUSTER_ENABLED?: string | null;
  readonly REDIS_CLUSTER_NODES?: string | null;
  readonly REDIS_SENTINEL_ENABLED?: string | null;
  readonly REDIS_SENTINEL_NODES?: string | null;
  readonly REDIS_SENTINEL_MASTER_NAME?: string | null;
  readonly REDIS_SENTINEL_TLS_ENABLED?: string | null;
  readonly REDIS_CONNECTION_STRING?: string | null;
  readonly REDIS_HOST?: string | null;
  readonly REDIS_PORT?: number | null;
  readonly REDIS_KEY_PREFIX?: string | null;
  readonly REDIS_TLS_ENABLED?: string | null;
};

type RedisEndpoint = { readonly host: string; readonly port: number };

function normalizeEndpoint(host: string, port: number): RedisEndpoint {
  const normalizedHost = host.trim().toLowerCase();
  if (
    !normalizedHost ||
    !Number.isSafeInteger(port) ||
    port < 1 ||
    port > 65_535
  ) {
    throw new TypeError("Invalid Redis queue namespace endpoint");
  }
  return { host: normalizedHost, port };
}

function parseNodes(value: string | null | undefined): RedisEndpoint[] {
  if (!value) {
    throw new TypeError("Redis queue namespace nodes are required");
  }
  return value
    .split(",")
    .map((entry) => {
      const separator = entry.lastIndexOf(":");
      if (separator < 1) {
        throw new TypeError("Invalid Redis queue namespace node");
      }
      return normalizeEndpoint(
        entry.slice(0, separator),
        Number(entry.slice(separator + 1)),
      );
    })
    .sort((left, right) =>
      `${left.host}:${left.port}`.localeCompare(`${right.host}:${right.port}`),
    );
}

function parseConnectionString(value: string): {
  readonly protocol: "redis:" | "rediss:";
  readonly endpoint: RedisEndpoint;
  readonly database: number;
} {
  let parsed: URL;
  try {
    parsed = new URL(value);
  } catch {
    throw new TypeError("Invalid Redis queue namespace connection string");
  }
  if (parsed.protocol !== "redis:" && parsed.protocol !== "rediss:") {
    throw new TypeError("Invalid Redis queue namespace protocol");
  }
  // ioredis accepts arbitrary query options (including db, keyPrefix, path,
  // host, and port). Accepting them without reproducing its full merge rules
  // would let two physical namespaces share one deployment fingerprint.
  if (parsed.search || parsed.hash) {
    throw new TypeError(
      "Redis queue namespace connection string cannot contain query or fragment options",
    );
  }
  const databasePath = parsed.pathname.replace(/^\//, "");
  const database = databasePath === "" ? 0 : Number(databasePath);
  if (!Number.isSafeInteger(database) || database < 0) {
    throw new TypeError("Invalid Redis queue namespace database");
  }
  return {
    protocol: parsed.protocol,
    endpoint: normalizeEndpoint(parsed.hostname, Number(parsed.port || 6379)),
    database,
  };
}

function queuePrefix(input: {
  readonly queueName: string;
  readonly keyPrefix: string;
  readonly clustered: boolean;
}): string {
  if (!input.clustered) return input.keyPrefix;
  return input.keyPrefix
    ? `{${input.keyPrefix}:${input.queueName}}`
    : `{${input.queueName}}`;
}

export function fingerprintAnalyticsQueueNamespace(
  config: AnalyticsQueueNamespaceConfig,
): string {
  const clustered = config.REDIS_CLUSTER_ENABLED === "true";
  const sentinel = config.REDIS_SENTINEL_ENABLED === "true";
  if (clustered && sentinel) {
    throw new TypeError("Redis queue namespace topology is ambiguous");
  }
  if (sentinel && !config.REDIS_SENTINEL_MASTER_NAME?.trim()) {
    throw new TypeError("Redis Sentinel master name is required");
  }

  const keyPrefix = config.REDIS_KEY_PREFIX ?? "";
  const connection = clustered
    ? {
        mode: "cluster" as const,
        nodes: parseNodes(config.REDIS_CLUSTER_NODES),
        database: 0,
        tls: config.REDIS_TLS_ENABLED === "true",
      }
    : sentinel
      ? {
          mode: "sentinel" as const,
          nodes: parseNodes(config.REDIS_SENTINEL_NODES),
          masterName: config.REDIS_SENTINEL_MASTER_NAME!.trim(),
          database: 0,
          redisTls: config.REDIS_TLS_ENABLED === "true",
          sentinelTls: config.REDIS_SENTINEL_TLS_ENABLED === "true",
        }
      : config.REDIS_CONNECTION_STRING
        ? {
            mode: "url" as const,
            ...parseConnectionString(config.REDIS_CONNECTION_STRING),
          }
        : config.REDIS_HOST
          ? {
              mode: "host" as const,
              endpoint: normalizeEndpoint(
                config.REDIS_HOST,
                config.REDIS_PORT ?? 6379,
              ),
              database: 0,
              tls: config.REDIS_TLS_ENABLED === "true",
            }
          : { mode: "unconfigured" as const };

  const queues = expandCommunityAnalyticsQueueInventory(
    communityAnalyticsQueueShardCountsFromConfig(config),
  ).map(({ family, name, shardIndex }) => ({
    family,
    name,
    shardIndex,
    prefix: queuePrefix({ queueName: name, keyPrefix, clustered }),
  }));
  return createHash("sha256")
    .update(JSON.stringify({ version: 2, connection, queues }))
    .digest("hex");
}

export function fingerprintConfiguredAnalyticsQueueNamespace(): string {
  const fingerprint = fingerprintAnalyticsQueueNamespace(env);
  if (!SHA256_HEX.test(fingerprint)) {
    throw new Error("Invalid analytics queue namespace fingerprint");
  }
  return fingerprint;
}
