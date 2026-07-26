import { PassThrough, Readable } from "node:stream";

import { beforeEach, describe, expect, it, vi } from "vitest";

const mysql = vi.hoisted(() => {
  const pool = {
    query: vi.fn(),
    getConnection: vi.fn(),
    end: vi.fn().mockResolvedValue(undefined),
  };
  return {
    pool,
    createPool: vi.fn(() => pool),
  };
});

vi.mock("mysql2/promise", () => ({
  default: { createPool: mysql.createPool },
}));

import { DorisClient, DorisClientManager } from "../client";
import type { DorisQueryConfig } from "../config";
import {
  fenceAnalyticsRuntimeIo,
  resetAnalyticsRuntimeIoFenceForTests,
} from "../../analytics-persistence/analyticsRuntimeIoFence";

const config: DorisQueryConfig = {
  host: "doris-fe.internal",
  port: 9030,
  database: "langfuse",
  user: "langfuse_web_query",
  password: "secret-v1",
  tls: true,
  maxConnections: 25,
  connectTimeoutMs: 10_000,
  queryTimeoutMs: 30_000,
};

describe("DorisClientManager", () => {
  beforeEach(() => {
    resetAnalyticsRuntimeIoFenceForTests();
  });

  it("pins Doris DATETIME conversion to UTC", () => {
    new DorisClient(config);

    expect(mysql.createPool).toHaveBeenCalledWith(
      expect.objectContaining({ timezone: "Z" }),
    );
  });

  it("reuses an identical pool and closes every rotation pool", async () => {
    const close = vi.fn().mockResolvedValue(undefined);
    const factory = vi.fn(() => ({ close }) as unknown as DorisClient);
    const manager = new DorisClientManager(factory);

    expect(manager.getClient(config)).toBe(manager.getClient({ ...config }));
    manager.getClient({ ...config, password: "secret-v2" });
    expect(factory).toHaveBeenCalledTimes(2);

    await manager.closeAllConnections();
    expect(close).toHaveBeenCalledTimes(2);
  });

  it("redacts TLS CA read failures at construction", () => {
    let thrown: unknown;
    try {
      new DorisClient({
        ...config,
        tlsCaPath: "/missing/private/doris-ca.pem",
      });
    } catch (error) {
      thrown = error;
    }

    expect(thrown).toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      message: "Analytics storage is unavailable",
    });
    expect((thrown as Error).message).not.toContain("/missing/private");
  });

  it("destroys an in-flight query connection when the caller aborts", async () => {
    let rejectQuery: ((reason: Error) => void) | undefined;
    const connection = {
      query: vi.fn(
        () =>
          new Promise<never>((_resolve, reject) => {
            rejectQuery = reject;
          }),
      ),
      destroy: vi.fn(() => rejectQuery?.(new Error("Connection destroyed"))),
      release: vi.fn(),
    };
    mysql.pool.getConnection.mockResolvedValueOnce(connection);
    const controller = new AbortController();
    const query = new DorisClient(config).query("SELECT SLEEP(10)", [], {
      signal: controller.signal,
    });

    await vi.waitFor(() => expect(connection.query).toHaveBeenCalledOnce());
    controller.abort();

    await expect(query).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
    });
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(connection.release).not.toHaveBeenCalled();
  });

  it("destroys an in-flight query connection when the runtime lease is fenced", async () => {
    let rejectQuery: ((reason: Error) => void) | undefined;
    const connection = {
      query: vi.fn(
        () =>
          new Promise<never>((_resolve, reject) => {
            rejectQuery = reject;
          }),
      ),
      destroy: vi.fn(() => rejectQuery?.(new Error("Connection destroyed"))),
      release: vi.fn(),
    };
    mysql.pool.getConnection.mockResolvedValueOnce(connection);
    const query = new DorisClient(config).query("SELECT SLEEP(10)");

    await vi.waitFor(() => expect(connection.query).toHaveBeenCalledOnce());
    fenceAnalyticsRuntimeIo();

    await expect(query).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
    });
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(connection.release).not.toHaveBeenCalled();
  });

  it("streams rows with bounded transport backpressure and releases the connection", async () => {
    const stream = Readable.from([{ id: "row-1" }, { id: "row-2" }]);
    const coreQuery = vi.fn(() => ({ stream: vi.fn(() => stream) }));
    const connection = {
      connection: { query: coreQuery },
      destroy: vi.fn(),
      release: vi.fn(),
    };
    mysql.pool.getConnection.mockResolvedValueOnce(connection);
    const rows = [];

    for await (const row of new DorisClient(config).streamQuery<{ id: string }>(
      "SELECT id FROM events_current",
    )) {
      rows.push(row);
    }

    expect(rows).toEqual([{ id: "row-1" }, { id: "row-2" }]);
    expect(coreQuery).toHaveBeenCalledOnce();
    expect(connection.release).toHaveBeenCalledOnce();
    expect(connection.destroy).not.toHaveBeenCalled();
  });

  it("destroys an in-flight streaming connection when the runtime is fenced", async () => {
    const stream = new PassThrough({ objectMode: true });
    const connection = {
      connection: {
        query: vi.fn(() => ({ stream: vi.fn(() => stream) })),
      },
      destroy: vi.fn(() => stream.destroy(new Error("Connection destroyed"))),
      release: vi.fn(),
    };
    mysql.pool.getConnection.mockResolvedValueOnce(connection);
    const consume = async () => {
      for await (const _row of new DorisClient(config).streamQuery(
        "SELECT SLEEP(10)",
      )) {
        // Keep consuming until the runtime fence destroys the transport.
      }
    };
    const operation = consume();
    await vi.waitFor(() =>
      expect(connection.connection.query).toHaveBeenCalledOnce(),
    );

    fenceAnalyticsRuntimeIo();

    await expect(operation).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
    });
    expect(connection.destroy).toHaveBeenCalledOnce();
    expect(connection.release).not.toHaveBeenCalled();
    expect(globalThis.analyticsRuntimeIoFenceListeners?.size).toBe(0);
  });

  it("rejects held and new Doris clients after the runtime is fenced", async () => {
    const client = new DorisClient(config);
    const manager = new DorisClientManager();
    mysql.pool.query.mockClear();
    fenceAnalyticsRuntimeIo();

    await expect(client.query("SELECT 1")).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
    });
    expect(() => manager.getClient(config)).toThrow(
      "Analytics persistence is unavailable",
    );
    expect(mysql.pool.query).not.toHaveBeenCalled();
  });
});
