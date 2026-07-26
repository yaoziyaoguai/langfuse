import { describe, expect, it, vi } from "vitest";
import type { Redis } from "ioredis";

vi.mock("../../env", () => ({
  env: {
    REDIS_CLUSTER_ENABLED: "false",
    REDIS_ENABLE_AUTO_PIPELINING: "false",
    REDIS_SENTINEL_ENABLED: "false",
  },
}));

import {
  createAnalyticsQueuePublisherClusterOptions,
  createAnalyticsQueuePublisherRedisOptions,
  redisErrorForLogging,
  scanKeys,
} from "./redis";

type ScanCall = [string, "MATCH", string, "COUNT", number];

const createRedisStub = (
  scanResults: Array<[string, string[]]>,
  options: { keyPrefix?: string } = {},
) => {
  let callIndex = 0;
  const scan = vi.fn(
    async (..._args: ScanCall): Promise<[string, string[]]> =>
      scanResults[callIndex++] ?? ["0", []],
  );

  return {
    client: { options, scan } as unknown as Redis,
    scan,
  };
};

describe("scanKeys", () => {
  it("scans every cursor page and returns unique keys", async () => {
    const { client, scan } = createRedisStub([
      ["42", ["cache:first", "cache:second"]],
      ["0", ["cache:second", "cache:third"]],
    ]);

    await expect(scanKeys(client, "cache:*")).resolves.toEqual([
      "cache:first",
      "cache:second",
      "cache:third",
    ]);

    expect(scan).toHaveBeenNthCalledWith(
      1,
      "0",
      "MATCH",
      "cache:*",
      "COUNT",
      1000,
    );
    expect(scan).toHaveBeenNthCalledWith(
      2,
      "42",
      "MATCH",
      "cache:*",
      "COUNT",
      1000,
    );
  });

  it("scans physical prefixed keys but returns logical keys", async () => {
    const { client, scan } = createRedisStub(
      [["0", ["tenant:api-key:first", "tenant:api-key:second"]]],
      { keyPrefix: "tenant:" },
    );

    await expect(scanKeys(client, "api-key:*")).resolves.toEqual([
      "api-key:first",
      "api-key:second",
    ]);

    expect(scan).toHaveBeenCalledWith(
      "0",
      "MATCH",
      "tenant:api-key:*",
      "COUNT",
      1000,
    );
  });
});

describe("analytics queue publisher Redis options", () => {
  it("bounds reconnects and never resends an unfulfilled command", () => {
    const options = createAnalyticsQueuePublisherRedisOptions();

    expect(options).toMatchObject({
      enableOfflineQueue: false,
      autoResendUnfulfilledCommands: false,
      maxRetriesPerRequest: 1,
      commandTimeout: 30_000,
      connectTimeout: 10_000,
    });
    expect(options.retryStrategy?.(1)).toBe(1_000);
    expect(options.retryStrategy?.(3)).toBe(3_000);
    expect(options.retryStrategy?.(4)).toBeNull();
    expect(options.sentinelRetryStrategy?.(1)).toBe(1_000);
    expect(options.sentinelRetryStrategy?.(3)).toBe(3_000);
    expect(options.sentinelRetryStrategy?.(4)).toBeNull();
    expect(options.reconnectOnError?.(new Error("READONLY replica"))).toBe(1);
    expect(options.reconnectOnError?.(new Error("connection reset"))).toBe(
      false,
    );

    const clusterOptions = createAnalyticsQueuePublisherClusterOptions();
    expect(clusterOptions.enableOfflineQueue).toBe(false);
    expect(clusterOptions.clusterRetryStrategy?.(1)).toBe(1_000);
    expect(clusterOptions.clusterRetryStrategy?.(3)).toBe(3_000);
    expect(clusterOptions.clusterRetryStrategy?.(4)).toBeNull();
  });

  it("removes AUTH command arguments and URL credentials from logged errors", () => {
    const secret = "must-not-reach-logs";
    const unsafe = Object.assign(
      new Error(`WRONGPASS via redis://user:${secret}@redis.internal:6379`),
      {
        command: { name: "auth", args: ["user", secret] },
        password: secret,
      },
    );

    const safe = redisErrorForLogging(unsafe);

    expect(safe).not.toBe(unsafe);
    expect(safe.message).not.toContain(secret);
    expect(safe.stack).not.toContain(secret);
    expect("command" in safe).toBe(false);
    expect("password" in safe).toBe(false);
  });

  it("redacts AUTH arguments embedded directly in an error message", () => {
    const secret = "must-not-reach-logs";
    const safe = redisErrorForLogging(
      new Error(`Redis command failed: AUTH default ${secret}`),
    );

    expect(safe.message).toContain("AUTH [redacted]");
    expect(safe.message).not.toContain(secret);
    expect(safe.stack).not.toContain(secret);
  });
});
