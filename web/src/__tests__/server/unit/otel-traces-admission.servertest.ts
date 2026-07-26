import type { NextApiRequest, NextApiResponse } from "next";
import { beforeEach, describe, expect, it, vi } from "vitest";

const state = vi.hoisted(() => ({
  backend: "doris" as "clickhouse" | "doris",
}));

const mocks = vi.hoisted(() => ({
  analyticsAdmissionContext: null as {
    runtimeLeaseId: string;
    backend: "clickhouse" | "doris";
    deploymentGeneration: bigint;
  } | null,
  closeAllConnections: vi.fn(async () => undefined),
  getWebAnalyticsAdmissionContext: vi.fn(),
  markProjectAsOtelUser: vi.fn(),
  processorConfigs: [] as Array<Record<string, unknown>>,
  publishToAnalyticsBackend: vi.fn(),
}));

vi.mock("@/src/env.mjs", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return state.backend;
    },
    LANGFUSE_INGESTION_MASKING_PROPAGATED_HEADERS: [],
  },
}));

vi.mock("@/src/features/public-api/server/createAuthedProjectAPIRoute", () => ({
  createAuthedProjectAPIRoute: ({ fn }: { fn: unknown }) => fn,
}));

vi.mock("@/src/features/public-api/server/withMiddlewares", () => ({
  withMiddlewares: ({ POST }: { POST: unknown }) => POST,
}));

vi.mock("@/src/server/analyticsRuntime", () => ({
  getWebAnalyticsAdmissionContext: mocks.getWebAnalyticsAdmissionContext,
}));

vi.mock("@langfuse/shared", () => ({
  ForbiddenError: class extends Error {},
}));

vi.mock("@langfuse/shared/src/server", () => ({
  ClickHouseClientManager: {
    getInstance: () => ({
      closeAllConnections: mocks.closeAllConnections,
    }),
  },
  createIngestionAttribution: () => ({
    ingestionSdkName: "python",
    ingestionSdkVersion: "4.0.0",
  }),
  getCurrentSpan: () => null,
  getLangfuseHeaderValue: () => undefined,
  logger: { debug: vi.fn(), error: vi.fn(), warn: vi.fn() },
  markProjectAsOtelUser: mocks.markProjectAsOtelUser,
  markProjectIngestFailure: vi.fn(),
  OtelIngestionProcessor: class {
    constructor(config: Record<string, unknown>) {
      mocks.processorConfigs.push(config);
    }

    publishToAnalyticsBackend = mocks.publishToAnalyticsBackend;
  },
  redis: null,
}));

import handler from "@/src/pages/api/public/otel/v1/traces";

type RouteInput = {
  req: NextApiRequest;
  res: NextApiResponse;
  auth: {
    scope: {
      projectId: string;
      publicKey: string;
      orgId: string;
      isIngestionSuspended: boolean;
    };
  };
};

const route = handler as unknown as (input: RouteInput) => Promise<unknown>;

function requestWith(resourceSpans: unknown[]): NextApiRequest {
  const body = Buffer.from(JSON.stringify({ resourceSpans }));
  const req = {
    headers: { "content-type": "application/json" },
    on(event: string, listener: (value?: Buffer) => void) {
      if (event === "data") listener(body);
      if (event === "end") listener();
      return req;
    },
  };
  return req as unknown as NextApiRequest;
}

async function invokeRoute() {
  const resourceSpans = [{ scopeSpans: [] }];
  const res = {
    setHeader: vi.fn(),
    status: vi.fn(),
  } as unknown as NextApiResponse;
  await route({
    req: requestWith(resourceSpans),
    res,
    auth: {
      scope: {
        projectId: "project-1",
        publicKey: "pk-lf-test",
        orgId: "org-1",
        isIngestionSuspended: false,
      },
    },
  });
  return { res, resourceSpans };
}

describe("public OTLP traces analytics admission", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.processorConfigs.length = 0;
    mocks.analyticsAdmissionContext = null;
    mocks.getWebAnalyticsAdmissionContext.mockImplementation(
      () => mocks.analyticsAdmissionContext,
    );
    mocks.publishToAnalyticsBackend.mockResolvedValue({
      operationId: "operation-1",
      status: "ACCEPTED",
    });
    state.backend = "doris";
  });

  it("passes the current managed Doris admission context to the processor", async () => {
    mocks.analyticsAdmissionContext = {
      runtimeLeaseId: "web-runtime-1",
      backend: "doris",
      deploymentGeneration: 7n,
    };

    const { res, resourceSpans } = await invokeRoute();

    expect(mocks.processorConfigs).toEqual([
      expect.objectContaining({
        analyticsAdmissionContext: mocks.analyticsAdmissionContext,
      }),
    ]);
    expect(mocks.publishToAnalyticsBackend).toHaveBeenCalledWith(
      resourceSpans,
      "doris",
    );
    expect(res.setHeader).toHaveBeenCalledWith(
      "x-langfuse-ingestion-operation-id",
      "operation-1",
    );
  });

  it("keeps marker-absent Doris acceptance on legacy admission", async () => {
    await invokeRoute();

    expect(mocks.processorConfigs).toEqual([
      expect.objectContaining({ analyticsAdmissionContext: null }),
    ]);
  });

  it("keeps ClickHouse ingestion on the queue-backed path", async () => {
    state.backend = "clickhouse";

    const { resourceSpans } = await invokeRoute();

    expect(mocks.publishToAnalyticsBackend).toHaveBeenCalledWith(
      resourceSpans,
      "clickhouse",
    );
  });
});
