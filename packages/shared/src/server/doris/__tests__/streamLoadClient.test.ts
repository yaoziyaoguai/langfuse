import http from "node:http";
import { afterEach, describe, expect, it, vi } from "vitest";

import { DorisStreamLoadClient } from "../streamLoadClient";

const servers: http.Server[] = [];

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
