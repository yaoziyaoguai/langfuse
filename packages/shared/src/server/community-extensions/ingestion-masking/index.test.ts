import {
  createServer,
  type IncomingMessage,
  type ServerResponse,
} from "node:http";
import type { AddressInfo } from "node:net";

import type { SharedEnv } from "../../../env";
import {
  afterAll,
  afterEach,
  beforeAll,
  describe,
  expect,
  it,
  vi,
} from "vitest";

const instrumentationMocks = vi.hoisted(() => ({
  getCurrentSpan: vi.fn(() => undefined),
  recordHistogram: vi.fn(),
  recordIncrement: vi.fn(),
  traceException: vi.fn(),
}));

vi.mock("../../instrumentation", () => instrumentationMocks);

import { applyCommunityIngestionMasking, readResponseBodyWithLimit } from ".";

const requests: Array<{
  headers: Record<string, string>;
  body: unknown;
}> = [];
let origin = "";
let transientAttempts = 0;
let redirectAttempts = 0;

const server = createServer(
  async (request: IncomingMessage, response: ServerResponse) => {
    const body = await readBody(request);
    if (request.url === "/success") {
      requests.push({
        headers: Object.fromEntries(
          Object.entries(request.headers).flatMap(([key, value]) =>
            typeof value === "string" ? [[key, value]] : [],
          ),
        ),
        body: JSON.parse(body),
      });
      respondJson(response, 200, { value: "[masked]" });
      return;
    }
    if (request.url === "/client-error") {
      transientAttempts += 1;
      respondJson(response, 400, { error: "invalid" });
      return;
    }
    if (request.url === "/transient") {
      transientAttempts += 1;
      respondJson(response, 503, { error: "unavailable" });
      return;
    }
    if (request.url === "/invalid-json") {
      response.writeHead(200, { "content-type": "application/json" });
      response.end("not json");
      return;
    }
    if (request.url === "/redirect") {
      redirectAttempts += 1;
      response.writeHead(307, { location: `${origin}/success` });
      response.end();
      return;
    }
    respondJson(response, 404, { error: "not found" });
  },
);

function environment(overrides: Partial<SharedEnv> = {}): SharedEnv {
  return {
    LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED: "true",
    LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL: `${origin}/success`,
    LANGFUSE_COMMUNITY_MASKING_CALLBACK_TIMEOUT_MS: 500,
    LANGFUSE_COMMUNITY_MASKING_CALLBACK_FAIL_CLOSED: "true",
    LANGFUSE_COMMUNITY_MASKING_MAX_RETRIES: 1,
    LANGFUSE_COMMUNITY_MASKING_PROPAGATED_HEADERS: ["x-mask-tenant"],
    LANGFUSE_COMMUNITY_MASKING_WHITELISTED_HOST: [],
    LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IPS: ["127.0.0.1"],
    LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IP_SEGMENTS: [],
    ...overrides,
  } as SharedEnv;
}

describe("applyCommunityIngestionMasking", () => {
  beforeAll(async () => {
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const address = server.address() as AddressInfo;
    origin = `http://127.0.0.1:${address.port}`;
  });
  afterEach(() => {
    requests.length = 0;
    transientAttempts = 0;
    redirectAttempts = 0;
    vi.restoreAllMocks();
    vi.clearAllMocks();
  });
  afterAll(
    () =>
      new Promise<void>((resolve, reject) =>
        server.close((error) => (error ? reject(error) : resolve())),
      ),
  );

  it("returns the original payload when Community Extensions is disabled", async () => {
    const data = { value: "secret" };

    await expect(
      applyCommunityIngestionMasking(
        { data, projectId: "project-1" },
        environment({ LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED: "false" }),
      ),
    ).resolves.toEqual({ success: true, data, masked: false });
    expect(requests).toHaveLength(0);
  });

  it("forwards only allowlisted headers and returns the callback payload", async () => {
    await expect(
      applyCommunityIngestionMasking(
        {
          data: { value: "secret" },
          projectId: "project-1",
          orgId: "org-1",
          propagatedHeaders: {
            "x-mask-tenant": "tenant-1",
            authorization: "must-not-leave",
            "x-langfuse-project-id": "attacker-value",
          },
        },
        environment(),
      ),
    ).resolves.toEqual({
      success: true,
      data: { value: "[masked]" },
      masked: true,
    });

    expect(requests).toEqual([
      {
        body: { value: "secret" },
        headers: expect.objectContaining({
          "content-type": "application/json",
          "x-langfuse-org-id": "org-1",
          "x-langfuse-project-id": "project-1",
          "x-mask-tenant": "tenant-1",
        }),
      },
    ]);
    expect(requests[0]?.headers).not.toHaveProperty("authorization");
  });

  it("does not retry a permanent callback error and fails open when configured", async () => {
    const data = { value: "secret" };

    await expect(
      applyCommunityIngestionMasking(
        { data, projectId: "project-1" },
        environment({
          LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL: `${origin}/client-error`,
          LANGFUSE_COMMUNITY_MASKING_CALLBACK_FAIL_CLOSED: "false",
          LANGFUSE_COMMUNITY_MASKING_MAX_RETRIES: 5,
        }),
      ),
    ).resolves.toEqual({
      success: true,
      data,
      masked: false,
      error: "HTTP 400",
    });
    expect(transientAttempts).toBe(1);
  });

  it("retries transient errors and fails closed", async () => {
    const data = { value: "secret" };

    await expect(
      applyCommunityIngestionMasking(
        { data, projectId: "project-1" },
        environment({
          LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL: `${origin}/transient`,
        }),
      ),
    ).resolves.toEqual({
      success: false,
      data,
      masked: false,
      error: "HTTP 503",
    });
    expect(transientAttempts).toBe(2);
  });

  it("rejects an invalid JSON response in fail-closed mode", async () => {
    const data = { value: "secret" };

    await expect(
      applyCommunityIngestionMasking(
        { data, projectId: "project-1" },
        environment({
          LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL: `${origin}/invalid-json`,
        }),
      ),
    ).resolves.toEqual({
      success: false,
      data,
      masked: false,
      error: "Callback returned invalid JSON",
    });
    expect(instrumentationMocks.recordHistogram).toHaveBeenCalledOnce();
    expect(instrumentationMocks.recordHistogram).toHaveBeenCalledWith(
      "langfuse.community.ingestion_masking.callback_duration_ms",
      expect.any(Number),
      { attempt: "1", status: "error" },
    );
  });

  it("cancels a non-success response body before returning", async () => {
    let cancelled = false;
    vi.spyOn(globalThis, "fetch").mockResolvedValue(
      new Response(
        new ReadableStream<Uint8Array>({
          cancel() {
            cancelled = true;
          },
        }),
        { status: 400 },
      ),
    );

    await expect(
      applyCommunityIngestionMasking(
        { data: { value: "secret" }, projectId: "project-1" },
        environment({
          LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL: "http://127.0.0.1/callback",
          LANGFUSE_COMMUNITY_MASKING_MAX_RETRIES: 0,
        }),
      ),
    ).resolves.toMatchObject({ success: false, error: "HTTP 400" });
    expect(cancelled).toBe(true);
  });

  it("rejects private callback targets unless the operator explicitly allowlists them", async () => {
    const data = { value: "secret" };

    await expect(
      applyCommunityIngestionMasking(
        { data, projectId: "project-1" },
        environment({
          LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IPS: [],
        }),
      ),
    ).resolves.toEqual({
      success: false,
      data,
      masked: false,
      error: "Blocked IP address detected",
    });
    expect(requests).toHaveLength(0);
  });

  it("rejects callback URLs containing embedded credentials", async () => {
    const data = { value: "secret" };
    const callbackUrl = new URL(`${origin}/success`);
    callbackUrl.username = "user";
    callbackUrl.password = "password";

    await expect(
      applyCommunityIngestionMasking(
        { data, projectId: "project-1" },
        environment({
          LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL: callbackUrl.toString(),
        }),
      ),
    ).resolves.toEqual({
      success: false,
      data,
      masked: false,
      error:
        "URL credentials are not allowed. Use authentication headers instead.",
    });
    expect(requests).toHaveLength(0);
  });

  it("rejects plaintext callback URLs in a Cloud runtime", async () => {
    const data = { value: "secret" };

    await expect(
      applyCommunityIngestionMasking(
        { data, projectId: "project-1" },
        environment({ NEXT_PUBLIC_LANGFUSE_CLOUD_REGION: "DEV" }),
      ),
    ).resolves.toEqual({
      success: false,
      data,
      masked: false,
      error: "Community masking callback must use HTTPS in Langfuse Cloud",
    });
    expect(requests).toHaveLength(0);
  });

  it("ignores private-target allowlists in a Cloud runtime", async () => {
    const data = { value: "secret" };

    await expect(
      applyCommunityIngestionMasking(
        { data, projectId: "project-1" },
        environment({
          NEXT_PUBLIC_LANGFUSE_CLOUD_REGION: "DEV",
          LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL:
            "https://169.254.169.254/latest/meta-data",
          LANGFUSE_COMMUNITY_MASKING_WHITELISTED_HOST: ["169.254.169.254"],
          LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IPS: ["169.254.169.254"],
        }),
      ),
    ).resolves.toEqual({
      success: false,
      data,
      masked: false,
      error: "Blocked hostname detected",
    });
    expect(requests).toHaveLength(0);
  });

  it("never follows redirects from the configured callback endpoint", async () => {
    const data = { value: "secret" };

    const result = await applyCommunityIngestionMasking(
      { data, projectId: "project-1" },
      environment({
        LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL: `${origin}/redirect`,
        LANGFUSE_COMMUNITY_MASKING_MAX_RETRIES: 0,
      }),
    );

    expect(result).toMatchObject({
      success: false,
      data,
      masked: false,
    });
    expect(result.error).toContain("Maximum redirects (0) exceeded");
    expect(redirectAttempts).toBe(1);
    expect(requests).toHaveLength(0);
  });
});

describe("readResponseBodyWithLimit", () => {
  it("cancels a response rejected by its content length", async () => {
    let cancelled = false;
    const response = new Response(
      new ReadableStream<Uint8Array>({
        cancel() {
          cancelled = true;
        },
      }),
      { headers: { "content-length": "8" }, status: 200 },
    );

    await expect(readResponseBodyWithLimit(response, 4)).resolves.toBeNull();
    expect(cancelled).toBe(true);
  });

  it("cancels a chunked response as soon as the byte limit is exceeded", async () => {
    let cancelled = false;
    const response = new Response(
      new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new Uint8Array([1, 2, 3, 4]));
          controller.enqueue(new Uint8Array([5, 6, 7, 8]));
        },
        cancel() {
          cancelled = true;
        },
      }),
      { status: 200 },
    );

    await expect(readResponseBodyWithLimit(response, 4)).resolves.toBeNull();
    expect(cancelled).toBe(true);
  });
});

async function readBody(request: IncomingMessage): Promise<string> {
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
  }
  return Buffer.concat(chunks).toString("utf8");
}

function respondJson(
  response: ServerResponse,
  status: number,
  body: unknown,
): void {
  response.writeHead(status, { "content-type": "application/json" });
  response.end(JSON.stringify(body));
}
