import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

import mysql, {
  type Pool,
  type PoolOptions,
  type RowDataPacket,
} from "mysql2/promise";
import type { Connection as CoreConnection } from "mysql2";

import type { DorisQueryConfig } from "./config";
import { toDorisError } from "./errors";
import {
  assertAnalyticsRuntimeIoAllowed,
  onAnalyticsRuntimeIoFenced,
  withAnalyticsRuntimeIoAbortSignal,
} from "../analytics-persistence/analyticsRuntimeIoFence";

export interface DorisQueryExecutor {
  query<T extends object = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
    options?: { readonly signal?: AbortSignal },
  ): Promise<readonly T[]>;
  streamQuery?<T extends object = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
    options?: { readonly signal?: AbortSignal },
  ): AsyncIterable<T>;
}

function poolOptions(config: DorisQueryConfig): PoolOptions {
  return {
    host: config.host,
    port: config.port,
    user: config.user,
    password: config.password,
    database: config.database,
    connectionLimit: config.maxConnections,
    connectTimeout: config.connectTimeoutMs,
    timezone: "Z",
    enableKeepAlive: true,
    multipleStatements: false,
    ssl: config.tls
      ? {
          rejectUnauthorized: true,
          verifyIdentity: true,
          ca: config.tlsCaPath
            ? readFileSync(config.tlsCaPath, "utf8")
            : undefined,
        }
      : undefined,
  };
}

/** Thin parameterized MySQL-protocol transport. Domain repositories own SQL. */
export class DorisClient implements DorisQueryExecutor {
  private readonly pool: Pool;
  private readonly queryTimeoutMs: number;

  constructor(config: DorisQueryConfig) {
    try {
      this.pool = mysql.createPool(poolOptions(config));
    } catch (error) {
      throw toDorisError(error);
    }
    this.queryTimeoutMs = config.queryTimeoutMs;
  }

  async query<T extends object = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
    options?: { readonly signal?: AbortSignal },
  ): Promise<readonly T[]> {
    try {
      return await withAnalyticsRuntimeIoAbortSignal({
        timeoutMs: this.queryTimeoutMs,
        signal: options?.signal,
        execute: (signal) => this.queryWithSignal<T>(sql, params, signal),
      });
    } catch (error) {
      throw toDorisError(error);
    }
  }

  private async queryWithSignal<T extends object>(
    sql: string,
    params: readonly unknown[],
    signal: AbortSignal,
  ): Promise<readonly T[]> {
    const connection = await this.pool.getConnection();
    let destroyed = false;
    const abort = () => {
      destroyed = true;
      connection.destroy();
    };

    try {
      if (signal.aborted) {
        abort();
        throw new Error("Doris query was cancelled");
      }
      signal.addEventListener("abort", abort, { once: true });
      const [rows] = await connection.query<RowDataPacket[]>({
        sql,
        values: [...params],
        timeout: this.queryTimeoutMs,
      });
      return rows as unknown as readonly T[];
    } finally {
      signal.removeEventListener("abort", abort);
      if (!destroyed) connection.release();
    }
  }

  async *streamQuery<T extends object = Record<string, unknown>>(
    sql: string,
    params: readonly unknown[] = [],
    options?: { readonly signal?: AbortSignal },
  ): AsyncIterable<T> {
    assertAnalyticsRuntimeIoAllowed();
    let connection: Awaited<ReturnType<Pool["getConnection"]>> | undefined;
    let destroyed = false;
    let timedOut = false;
    const controller = new AbortController();
    const forwardCallerAbort = () => controller.abort(options?.signal?.reason);
    const abort = () => {
      destroyed = true;
      connection?.destroy();
    };
    const timeout = setTimeout(() => {
      timedOut = true;
      controller.abort(new Error("Doris query timed out"));
    }, this.queryTimeoutMs);
    timeout.unref();
    if (options?.signal?.aborted) forwardCallerAbort();
    else
      options?.signal?.addEventListener("abort", forwardCallerAbort, {
        once: true,
      });
    controller.signal.addEventListener("abort", abort, { once: true });
    const removeFenceListener = onAnalyticsRuntimeIoFenced(() => {
      let reason: unknown;
      try {
        assertAnalyticsRuntimeIoAllowed();
      } catch (error) {
        reason = error;
      }
      controller.abort(reason);
    });

    try {
      connection = await this.pool.getConnection();
      if (controller.signal.aborted) {
        abort();
        throw new Error("Doris query was cancelled");
      }
      const core = connection.connection as unknown as CoreConnection;
      const stream = core
        .query({
          sql,
          values: [...params],
          timeout: this.queryTimeoutMs,
        })
        .stream({ highWaterMark: 100 });
      for await (const row of stream) {
        if (controller.signal.aborted) {
          throw new Error("Doris query was cancelled");
        }
        yield row as T;
      }
      if (timedOut) throw new Error("Doris query timed out");
      assertAnalyticsRuntimeIoAllowed();
    } catch (error) {
      if (controller.signal.aborted) {
        const reason = controller.signal.reason;
        throw toDorisError(
          timedOut
            ? new Error("Doris query timed out")
            : reason instanceof Error
              ? reason
              : error,
        );
      }
      throw toDorisError(error);
    } finally {
      clearTimeout(timeout);
      removeFenceListener();
      controller.signal.removeEventListener("abort", abort);
      options?.signal?.removeEventListener("abort", forwardCallerAbort);
      if (connection && !destroyed) connection.release();
    }
  }

  async execute(sql: string, params: readonly unknown[] = []): Promise<void> {
    try {
      await withAnalyticsRuntimeIoAbortSignal({
        timeoutMs: this.queryTimeoutMs,
        execute: (signal) => this.executeWithSignal(sql, params, signal),
      });
    } catch (error) {
      throw toDorisError(error);
    }
  }

  private async executeWithSignal(
    sql: string,
    params: readonly unknown[],
    signal: AbortSignal,
  ): Promise<void> {
    const connection = await this.pool.getConnection();
    let destroyed = false;
    const abort = () => {
      destroyed = true;
      connection.destroy();
    };

    try {
      if (signal.aborted) {
        abort();
        throw new Error("Doris query was cancelled");
      }
      signal.addEventListener("abort", abort, { once: true });
      await connection.query({
        sql,
        values: [...params],
        timeout: this.queryTimeoutMs,
      });
    } finally {
      signal.removeEventListener("abort", abort);
      if (!destroyed) connection.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}

type DorisClientFactory = (config: DorisQueryConfig) => DorisClient;

/** Reuses pools per workload configuration and supports overlap during rotation. */
export class DorisClientManager {
  private static instance: DorisClientManager | undefined;
  private readonly clients = new Map<string, DorisClient>();

  constructor(
    private readonly createClient: DorisClientFactory = (config) =>
      new DorisClient(config),
  ) {}

  static getInstance(): DorisClientManager {
    DorisClientManager.instance ??= new DorisClientManager();
    return DorisClientManager.instance;
  }

  getClient(config: DorisQueryConfig): DorisClient {
    assertAnalyticsRuntimeIoAllowed();
    const key = createHash("sha256")
      .update(
        JSON.stringify({
          ...config,
          passwordHash: createHash("sha256")
            .update(config.password)
            .digest("hex"),
          password: undefined,
        }),
      )
      .digest("hex");
    const existing = this.clients.get(key);
    if (existing) return existing;

    const client = this.createClient(config);
    this.clients.set(key, client);
    return client;
  }

  async closeAllConnections(): Promise<void> {
    const clients = [...this.clients.values()];
    this.clients.clear();
    await Promise.all(clients.map((client) => client.close()));
  }
}
