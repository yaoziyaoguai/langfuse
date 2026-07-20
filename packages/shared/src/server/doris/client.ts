import { readFileSync } from "node:fs";
import { createHash } from "node:crypto";

import mysql, {
  type Pool,
  type PoolOptions,
  type RowDataPacket,
} from "mysql2/promise";

import type { DorisQueryConfig } from "./config";
import { toDorisError } from "./errors";

export interface DorisQueryExecutor {
  query<T extends object = Record<string, unknown>>(
    sql: string,
    params?: readonly unknown[],
    options?: { readonly signal?: AbortSignal },
  ): Promise<readonly T[]>;
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
      if (options?.signal) {
        return await this.queryWithSignal<T>(sql, params, options.signal);
      }
      const [rows] = await this.pool.query<RowDataPacket[]>({
        sql,
        values: [...params],
        timeout: this.queryTimeoutMs,
      });
      return rows as unknown as readonly T[];
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

  async execute(sql: string, params: readonly unknown[] = []): Promise<void> {
    try {
      await this.pool.query({
        sql,
        values: [...params],
        timeout: this.queryTimeoutMs,
      });
    } catch (error) {
      throw toDorisError(error);
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
