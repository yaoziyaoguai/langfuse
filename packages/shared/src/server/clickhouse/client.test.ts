import { beforeEach, describe, expect, it, vi } from "vitest";
import { type ClickHouseSettings } from "@clickhouse/client";

const mocks = vi.hoisted(() => ({
  createClient: vi.fn(),
  asyncDispose: vi.fn(async () => undefined),
  close: vi.fn(async () => undefined),
  command: vi.fn(),
  insert: vi.fn(),
  query: vi.fn(),
  env: {
    CLICKHOUSE_URL: "http://localhost:8123",
    CLICKHOUSE_READ_ONLY_URL: undefined,
    CLICKHOUSE_EVENTS_READ_ONLY_URL: undefined,
    CLICKHOUSE_USER: "default",
    CLICKHOUSE_PASSWORD: "",
    CLICKHOUSE_DB: "default",
    CLICKHOUSE_KEEP_ALIVE_IDLE_SOCKET_TTL: 9000,
    CLICKHOUSE_MAX_OPEN_CONNECTIONS: 25,
    CLICKHOUSE_ASYNC_INSERT_MAX_DATA_SIZE: undefined,
    CLICKHOUSE_ASYNC_INSERT_BUSY_TIMEOUT_MS: undefined,
    CLICKHOUSE_ASYNC_INSERT_BUSY_TIMEOUT_MIN_MS: undefined,
    CLICKHOUSE_LIGHTWEIGHT_DELETE_MODE: "alter_update",
    CLICKHOUSE_UPDATE_PARALLEL_MODE: "auto",
    CLICKHOUSE_DISABLE_LAZY_MATERIALIZATION: "auto",
    LANGFUSE_ANALYTICS_BACKEND: "clickhouse" as "clickhouse" | "doris",
    LANGFUSE_LOG_LEVEL: "error",
    NEXT_PUBLIC_LANGFUSE_CLOUD_REGION: undefined,
  },
}));

vi.mock("../../env", () => ({ env: mocks.env }));
vi.mock("@clickhouse/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("@clickhouse/client")>();

  return {
    ...actual,
    createClient: mocks.createClient,
  };
});

import { ClickHouseClientManager, clickhouseClient } from "./client";
import { setClickHouseCompatibilityVersionForTests } from "./compatibility";
import {
  fenceAnalyticsRuntimeIo,
  resetAnalyticsRuntimeIoFenceForTests,
} from "../analytics-persistence/analyticsRuntimeIoFence";

describe("ClickHouseClientManager compatibility settings", () => {
  beforeEach(async () => {
    resetAnalyticsRuntimeIoFenceForTests();
    await ClickHouseClientManager.getInstance().closeAllConnections();

    mocks.close.mockClear();
    mocks.asyncDispose.mockClear();
    mocks.createClient.mockReset();
    mocks.query.mockReset();
    mocks.command.mockReset();
    mocks.insert.mockReset();
    mocks.createClient.mockReturnValue({
      [Symbol.asyncDispose]: mocks.asyncDispose,
      close: mocks.close,
      command: mocks.command,
      insert: mocks.insert,
      query: mocks.query,
    });
    mocks.env.CLICKHOUSE_DISABLE_LAZY_MATERIALIZATION = "auto";
    mocks.env.LANGFUSE_ANALYTICS_BACKEND = "clickhouse";
    setClickHouseCompatibilityVersionForTests(null);
  });

  it("applies resolved compatibility settings globally", () => {
    setClickHouseCompatibilityVersionForTests("26.5.1.882");

    clickhouseClient();

    expect(mocks.createClient).toHaveBeenCalledTimes(1);
    expect(
      mocks.createClient.mock.calls[0][0].clickhouse_settings,
    ).toMatchObject({
      query_plan_optimize_lazy_materialization: 0,
    });
  });

  it("lets explicit client settings override compatibility settings", () => {
    setClickHouseCompatibilityVersionForTests("26.5.1.882");

    clickhouseClient({
      clickhouse_settings: {
        query_plan_optimize_lazy_materialization: 1,
      } as ClickHouseSettings,
    });

    expect(mocks.createClient).toHaveBeenCalledTimes(1);
    expect(
      mocks.createClient.mock.calls[0][0].clickhouse_settings
        .query_plan_optimize_lazy_materialization,
    ).toBe(1);
  });

  it("uses a new cached client key after compatibility settings change", () => {
    clickhouseClient();
    setClickHouseCompatibilityVersionForTests("26.5.1.882");
    clickhouseClient();

    expect(mocks.createClient).toHaveBeenCalledTimes(2);
  });

  it("sets ClickHouse server timeout after the default client request timeout", () => {
    clickhouseClient();

    expect(
      mocks.createClient.mock.calls[0][0].clickhouse_settings,
    ).toMatchObject({
      timeout_before_checking_execution_speed: 0,
      max_execution_time: 35,
    });
  });

  it("sets ClickHouse server timeout just after the client request timeout", () => {
    clickhouseClient({ request_timeout: 120_000 });

    expect(
      mocks.createClient.mock.calls[0][0].clickhouse_settings,
    ).toMatchObject({
      timeout_before_checking_execution_speed: 0,
      max_execution_time: 125,
    });
  });

  it("lets explicit client settings override derived timeout settings", () => {
    clickhouseClient({
      request_timeout: 120_000,
      clickhouse_settings: {
        timeout_before_checking_execution_speed: 10,
        max_execution_time: 60,
      } as ClickHouseSettings,
    });

    expect(
      mocks.createClient.mock.calls[0][0].clickhouse_settings,
    ).toMatchObject({
      timeout_before_checking_execution_speed: 10,
      max_execution_time: 60,
    });
  });

  it("fails before creating a ClickHouse client when Doris is selected", () => {
    mocks.env.LANGFUSE_ANALYTICS_BACKEND = "doris";

    expect(() => clickhouseClient()).toThrow(
      "Analytics persistence feature is unsupported",
    );
    expect(mocks.createClient).not.toHaveBeenCalled();
  });

  it("rejects new ClickHouse client acquisition after the runtime is fenced", () => {
    fenceAnalyticsRuntimeIo();

    expect(() => clickhouseClient()).toThrow(
      "Analytics persistence is unavailable",
    );
    expect(mocks.createClient).not.toHaveBeenCalled();
  });

  it("rejects requests through a held ClickHouse client after the runtime is fenced", async () => {
    const client = clickhouseClient();
    fenceAnalyticsRuntimeIo();

    for (const request of [
      () => client.query({ query: "SELECT 1" }),
      () => client.command({ query: "SELECT 1" }),
      () =>
        client.insert({
          table: "events",
          values: [],
          format: "JSONEachRow",
        }),
    ]) {
      expect(request).toThrow("Analytics persistence is unavailable");
    }
    expect(mocks.query).not.toHaveBeenCalled();
    expect(mocks.command).not.toHaveBeenCalled();
    expect(mocks.insert).not.toHaveBeenCalled();
    await expect(client.close()).resolves.toBeUndefined();
  });

  it("aborts an in-flight ClickHouse request when the runtime lease is fenced", async () => {
    let requestSignal: AbortSignal | undefined;
    mocks.query.mockImplementationOnce(
      ({ abort_signal }: { abort_signal?: AbortSignal }) =>
        new Promise<never>((_resolve, reject) => {
          requestSignal = abort_signal;
          if (!abort_signal) {
            reject(new Error("missing runtime abort signal"));
            return;
          }
          abort_signal.addEventListener(
            "abort",
            () => reject(abort_signal.reason),
            { once: true },
          );
        }),
    );
    const client = clickhouseClient({ request_timeout: 60_000 });
    const request = client.query({ query: "SELECT sleep(10)" });

    await vi.waitFor(() => expect(requestSignal).toBeDefined());
    fenceAnalyticsRuntimeIo();

    expect(requestSignal?.aborted).toBe(true);
    await expect(request).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
    });
  });

  it("allows standard async disposal after the runtime is fenced", async () => {
    const client = clickhouseClient();
    fenceAnalyticsRuntimeIo();

    await expect(client[Symbol.asyncDispose]()).resolves.toBeUndefined();
    expect(mocks.asyncDispose).toHaveBeenCalledOnce();
  });
});
