import { describe, expect, it, vi } from "vitest";

const mysql = vi.hoisted(() => ({
  createPool: vi.fn(() => ({
    query: vi.fn(),
    end: vi.fn().mockResolvedValue(undefined),
  })),
}));

vi.mock("mysql2/promise", () => ({
  default: { createPool: mysql.createPool },
}));

import { DorisClient, DorisClientManager } from "../client";
import type { DorisQueryConfig } from "../config";

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
});
