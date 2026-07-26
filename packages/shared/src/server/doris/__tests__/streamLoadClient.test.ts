import http from "node:http";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import {
  fenceAnalyticsRuntimeIo,
  resetAnalyticsRuntimeIoFenceForTests,
} from "../../analytics-persistence/analyticsRuntimeIoFence";
import { DorisStreamLoadClient } from "../streamLoadClient";

const servers: http.Server[] = [];

beforeEach(() => {
  resetAnalyticsRuntimeIoFenceForTests();
});

async function listen(
  handler: http.RequestListener,
): Promise<{ origin: string; server: http.Server }> {
  const server = http.createServer(handler);
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string") throw new Error("missing port");
  return { origin: `http://127.0.0.1:${address.port}`, server };
}

afterEach(async () => {
  await Promise.all(
    servers.splice(0).map((server) => {
      server.closeAllConnections();
      return new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      );
    }),
  );
});

describe("DorisStreamLoadClient", () => {
  it("rejects a load before contacting Doris after the runtime is fenced", async () => {
    const contacted = vi.fn();
    const fe = await listen((_req, res) => {
      contacted();
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          Status: "Success",
          Label: "operation_batch_1",
          NumberTotalRows: 1,
          NumberFilteredRows: 0,
        }),
      );
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    fenceAnalyticsRuntimeIo();
    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "ANALYTICS_UNAVAILABLE" });
    expect(contacted).not.toHaveBeenCalled();
  });

  it("aborts an in-flight Stream Load when the runtime is fenced", async () => {
    let markContacted!: () => void;
    const contacted = new Promise<void>((resolve) => {
      markContacted = resolve;
    });
    const fe = await listen((_req, _res) => {
      markContacted();
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
      requestTimeoutMs: 5_000,
    });
    const completion = client.load({
      table: "events_current",
      label: "operation_batch_1",
      ndjsonBody: "{}\n",
    });
    await contacted;

    fenceAnalyticsRuntimeIo();
    const result = await Promise.race([
      completion.catch((error: unknown) => error),
      new Promise<null>((resolve) => setTimeout(() => resolve(null), 100)),
    ]);
    if (result === null) {
      fe.server.closeAllConnections();
      await completion.catch(() => undefined);
    }
    expect(result).toMatchObject({ code: "ANALYTICS_UNAVAILABLE" });
  });

  it("preserves method, body, and auth only across an allowlisted 307", async () => {
    const body = '{"project_id":"p1"}\n';
    const be = await listen((req, res) => {
      expect(req.method).toBe("PUT");
      expect(req.headers.authorization).toMatch(/^Basic /);
      let received = "";
      req.setEncoding("utf8");
      req.on("data", (chunk) => (received += chunk));
      req.on("end", () => {
        expect(received).toBe(body);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            Status: "Success",
            Label: "operation_batch_1",
            NumberTotalRows: 1,
            NumberFilteredRows: 0,
          }),
        );
      });
    });
    const fe = await listen((_req, res) => {
      res.writeHead(307, { location: `${be.origin}/api/load` });
      res.end();
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [be.origin],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: body,
      }),
    ).resolves.toMatchObject({ committed: true, numberTotalRows: 1 });
  });

  it("sends explicit batch-delete headers for lifecycle key loads", async () => {
    const fe = await listen((req, res) => {
      expect(req.headers.merge_type).toBe("DELETE");
      expect(req.headers.columns).toBe(
        "project_id,partition_date,trace_id,span_id,version_token",
      );
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            Status: "Success",
            Label: "trace_delete_1",
            NumberTotalRows: 1,
            NumberFilteredRows: 0,
          }),
        );
      });
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "trace_delete_1",
        ndjsonBody: "{}\n",
        columns: [
          "project_id",
          "partition_date",
          "trace_id",
          "span_id",
          "version_token",
        ],
        mergeType: "DELETE",
      }),
    ).resolves.toMatchObject({ committed: true });
  });

  it("rejects an unallowlisted redirect before contacting the target", async () => {
    const target = vi.fn();
    const hostile = await listen((req, res) => {
      target(req.headers.authorization);
      res.end();
    });
    const fe = await listen((_req, res) => {
      res.writeHead(307, { location: `${hostile.origin}/steal` });
      res.end();
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "REDIRECT_REJECTED" });
    expect(target).not.toHaveBeenCalled();
  });

  it("validates and pins the original redirect before rewriting its origin", async () => {
    const body = '{"project_id":"p1"}\n';
    const target = vi.fn();
    const reachableBe = await listen((req, res) => {
      target(req.headers.authorization);
      let received = "";
      req.setEncoding("utf8");
      req.on("data", (chunk) => (received += chunk));
      req.on("end", () => {
        expect(req.url).toBe("/api/langfuse/events_current/_stream_load");
        expect(received).toBe(body);
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            Status: "Success",
            Label: "operation_batch_1",
            NumberTotalRows: 1,
            NumberFilteredRows: 0,
          }),
        );
      });
    });
    const originalBeOrigin = "http://doris-be.internal:8040";
    const fe = await listen((_req, res) => {
      res.writeHead(307, {
        location: `${originalBeOrigin}/api/langfuse/events_current/_stream_load`,
      });
      res.end();
    });
    const resolveAddresses = vi.fn(async (hostname: string) => {
      if (hostname === "doris-be.internal") return ["172.29.0.3"];
      if (hostname === "127.0.0.1") return ["127.0.0.1"];
      return [hostname];
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [originalBeOrigin],
      allowedRedirectAddresses: ["172.29.0.3"],
      redirectOriginRewriteMap: {
        [originalBeOrigin]: reachableBe.origin,
      },
      allowedRewriteAddresses: ["127.0.0.1"],
      resolveAddresses,
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: body,
      }),
    ).resolves.toMatchObject({ committed: true });
    expect(resolveAddresses.mock.calls.map(([hostname]) => hostname)).toEqual([
      "doris-be.internal",
      "127.0.0.1",
    ]);
    expect(target).toHaveBeenCalledWith(expect.stringMatching(/^Basic /));
  });

  it("rejects a rewrite target whose DNS result is not independently pinned", async () => {
    const target = vi.fn();
    const reachableBe = await listen((req, res) => {
      target(req.headers.authorization);
      res.end();
    });
    const originalBeOrigin = "http://doris-be.internal:8040";
    const fe = await listen((_req, res) => {
      res.writeHead(307, { location: `${originalBeOrigin}/api/load` });
      res.end();
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [originalBeOrigin],
      allowedRedirectAddresses: ["172.29.0.3"],
      redirectOriginRewriteMap: {
        [originalBeOrigin]: reachableBe.origin,
      },
      allowedRewriteAddresses: ["10.0.0.12"],
      resolveAddresses: async (hostname) =>
        hostname === "doris-be.internal" ? ["172.29.0.3"] : ["127.0.0.1"],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "REDIRECT_REJECTED" });
    expect(target).not.toHaveBeenCalled();
  });

  it("rejects an unpinned original redirect before resolving or contacting the rewrite target", async () => {
    const target = vi.fn();
    const reachableBe = await listen((req, res) => {
      target(req.headers.authorization);
      res.end();
    });
    const originalBeOrigin = "http://doris-be.internal:8040";
    const fe = await listen((_req, res) => {
      res.writeHead(307, { location: `${originalBeOrigin}/api/load` });
      res.end();
    });
    const resolveAddresses = vi.fn(async (hostname: string) =>
      hostname === "doris-be.internal" ? ["172.29.0.99"] : ["127.0.0.1"],
    );
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [originalBeOrigin],
      allowedRedirectAddresses: ["172.29.0.3"],
      redirectOriginRewriteMap: {
        [originalBeOrigin]: reachableBe.origin,
      },
      allowedRewriteAddresses: ["127.0.0.1"],
      resolveAddresses,
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "REDIRECT_REJECTED" });
    expect(resolveAddresses).toHaveBeenCalledTimes(1);
    expect(resolveAddresses).toHaveBeenCalledWith("doris-be.internal");
    expect(target).not.toHaveBeenCalled();
  });

  it("discards Doris redirect userinfo and uses only the configured load credential", async () => {
    const authorization = vi.fn();
    const be = await listen((req, res) => {
      authorization(req.headers.authorization);
      req.resume();
      req.on("end", () => {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify({
            Status: "Success",
            Label: "operation_batch_1",
            NumberTotalRows: 1,
            NumberFilteredRows: 0,
          }),
        );
      });
    });
    const beUrl = new URL(be.origin);
    const fe = await listen((_req, res) => {
      res.writeHead(307, {
        location: `http://root:@${beUrl.host}/api/load`,
      });
      res.end();
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [be.origin],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).resolves.toMatchObject({ committed: true });
    expect(authorization).toHaveBeenCalledWith(
      `Basic ${Buffer.from("load:secret").toString("base64")}`,
    );
  });

  it("never follows a second 307 after an origin rewrite", async () => {
    const finalTarget = vi.fn();
    const finalBe = await listen((req, res) => {
      finalTarget(req.headers.authorization);
      res.end();
    });
    const reachableBe = await listen((_req, res) => {
      res.writeHead(307, { location: `${finalBe.origin}/steal` });
      res.end();
    });
    const originalBeOrigin = "http://doris-be.internal:8040";
    const fe = await listen((_req, res) => {
      res.writeHead(307, { location: `${originalBeOrigin}/api/load` });
      res.end();
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [originalBeOrigin],
      allowedRedirectAddresses: ["172.29.0.3"],
      redirectOriginRewriteMap: {
        [originalBeOrigin]: reachableBe.origin,
      },
      allowedRewriteAddresses: ["127.0.0.1"],
      resolveAddresses: async (hostname) =>
        hostname === "doris-be.internal" ? ["172.29.0.3"] : ["127.0.0.1"],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "REDIRECT_REJECTED" });
    expect(finalTarget).not.toHaveBeenCalled();
  });

  it("pins the configured FE to the resolved address allowlist", async () => {
    const contacted = vi.fn();
    const fe = await listen((_req, res) => {
      contacted();
      res.end();
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedFeAddresses: ["10.0.0.10"],
      allowedRedirectOrigins: [],
      resolveAddresses: async () => ["10.0.0.99"],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "REDIRECT_REJECTED" });
    expect(contacted).not.toHaveBeenCalled();
  });

  it("exposes explicit label reconciliation without classifying unknown as success", async () => {
    const reconcileLabelStatus = vi.fn().mockResolvedValue({
      status: "VISIBLE",
      visible: true,
    });
    const client = new DorisStreamLoadClient({
      feOrigin: "http://127.0.0.1:1",
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
      reconcileLabelStatus,
    });

    await expect(
      client.reconcile({ label: "operation_batch_1" }),
    ).resolves.toEqual({ status: "VISIBLE", visible: true });
    expect(reconcileLabelStatus).toHaveBeenCalledWith("operation_batch_1");
  });

  it("reconciles a label against the pinned FE load-state endpoint", async () => {
    const fe = await listen((req, res) => {
      expect(req.method).toBe("GET");
      expect(req.url).toBe(
        "/api/langfuse/get_load_state?label=operation_batch_1",
      );
      expect(req.headers.authorization).toMatch(/^Basic /);
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ code: 0, data: "VISIBLE" }));
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    await expect(
      client.reconcile({ label: "operation_batch_1" }),
    ).resolves.toEqual({ status: "VISIBLE", visible: true });
  });

  it("treats a non-2xx Success response as requiring reconciliation", async () => {
    const fe = await listen((_req, res) => {
      res.writeHead(500, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          Status: "Success",
          Label: "operation_batch_1",
          NumberTotalRows: 1,
          NumberFilteredRows: 0,
        }),
      );
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).resolves.toMatchObject({
      committed: false,
      requiresReconciliation: true,
    });
  });

  it("classifies Doris memory pressure as retryable storage unavailability", async () => {
    const fe = await listen((_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          Status: "Fail",
          Label: "operation_batch_1",
          Message:
            "[MEM_LIMIT_EXCEEDED] failed to allocate memory for stream load",
          NumberTotalRows: 0,
          NumberFilteredRows: 0,
        }),
      );
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({
      code: "ANALYTICS_UNAVAILABLE",
      retryable: true,
    });
  });

  it("rejects filtered rows even if Doris returns Success", async () => {
    const fe = await listen((_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          Status: "Success",
          Label: "operation_batch_1",
          NumberTotalRows: 1,
          NumberFilteredRows: 1,
          ErrorURL: "http://doris.internal/error-log",
        }),
      );
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "FILTERED_ROWS" });
  });

  it("caps response bytes even when multi-byte text is below the character limit", async () => {
    const fe = await listen((_req, res) => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        JSON.stringify({
          Status: "Success",
          Label: "operation_batch_1",
          NumberTotalRows: 1,
          NumberFilteredRows: 0,
          Message: "界".repeat(30_000),
        }),
      );
    });
    const client = new DorisStreamLoadClient({
      feOrigin: fe.origin,
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    await expect(
      client.load({
        table: "events_current",
        label: "operation_batch_1",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "ANALYTICS_UNAVAILABLE" });
  });

  it("rejects invalid identifiers and labels before making a request", async () => {
    const client = new DorisStreamLoadClient({
      feOrigin: "http://127.0.0.1:1",
      database: "langfuse",
      user: "load",
      password: "secret",
      requireTls: false,
      allowedRedirectOrigins: [],
    });

    await expect(
      client.load({
        table: "events_current; DROP TABLE events_current",
        label: "invalid label with spaces",
        ndjsonBody: "{}\n",
      }),
    ).rejects.toMatchObject({ code: "INVALID_REQUEST" });
  });
});
