import { createMocks, type Body } from "node-mocks-http";
import type { NextApiRequest, NextApiResponse } from "next";

const mocks = vi.hoisted(() => {
  class AnalyticsQueryError extends Error {
    constructor(
      readonly code: string,
      message: string,
    ) {
      super(message);
    }
  }

  return {
    AnalyticsQueryError,
    getServerAuthSession: vi.fn(),
    sendAdminAccessWebhook: vi.fn(),
    streamAnalyticsQuery: vi.fn(),
  };
});

vi.mock("@langfuse/shared", () => ({
  RESOURCE_LIMIT_ERROR_MESSAGE: "Query resource limit exceeded",
}));

vi.mock("@langfuse/shared/src/server", () => ({
  redis: undefined,
  logger: { debug: vi.fn(), error: vi.fn() },
  ClickHouseClientManager: {
    getInstance: () => ({ closeAllConnections: vi.fn() }),
  },
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: { project: { findFirst: vi.fn() } },
}));

vi.mock("@langfuse/shared/query/server", () => ({
  AnalyticsQueryError: mocks.AnalyticsQueryError,
  streamAnalyticsQuery: mocks.streamAnalyticsQuery,
}));

vi.mock("../../../server/auth", () => ({
  getServerAuthSession: mocks.getServerAuthSession,
}));

vi.mock("../../../server/adminAccessWebhook", () => ({
  sendAdminAccessWebhook: mocks.sendAdminAccessWebhook,
}));

import handler from "../../../pages/api/dashboard/execute-query-stream";

function createPostMocks(body: unknown) {
  const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
    method: "POST",
    body: body as Body,
  });
  res.flushHeaders = vi.fn();
  return { req, res };
}

function parseSSEEvents(raw: string): Array<{ event: string; data: unknown }> {
  return raw
    .split("\n\n")
    .filter(Boolean)
    .map((block) => ({
      event: block.match(/^event: (.+)$/m)?.[1] ?? "",
      data: JSON.parse(block.match(/^data: (.+)$/m)?.[1] ?? "null"),
    }));
}

const body = {
  projectId: "project-1",
  query: {
    view: "traces" as const,
    dimensions: [],
    metrics: [{ measure: "count", aggregation: "count" }],
    filters: [],
    timeDimension: null,
    fromTimestamp: "2026-07-17T00:00:00.000Z",
    toTimestamp: "2026-07-18T00:00:00.000Z",
    orderBy: null,
  },
  version: "v2" as const,
};

describe("execute-query-stream storage seam", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.getServerAuthSession.mockResolvedValue({
      user: {
        id: "user-1",
        email: "user@example.com",
        v4BetaEnabled: true,
        organizations: [
          {
            id: "org-1",
            projects: [{ id: "project-1" }],
          },
        ],
      },
    });
  });

  it("streams backend-neutral progress and row events", async () => {
    mocks.streamAnalyticsQuery.mockImplementationOnce(async function* () {
      yield { type: "progress", progress: { readRows: 1 } };
      yield { type: "row", row: { count_count: 1 } };
    });
    const { req, res } = createPostMocks(body);

    await handler(req, res);

    expect(parseSSEEvents(res._getData())).toEqual([
      { event: "progress", data: { readRows: 1 } },
      { event: "row", data: { count_count: 1 } },
      { event: "done", data: {} },
    ]);
    expect(mocks.streamAnalyticsQuery).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
        version: "v2",
        signal: expect.any(AbortSignal),
      }),
    );
  });

  it("maps backend-neutral resource failures without a done event", async () => {
    mocks.streamAnalyticsQuery.mockImplementationOnce(async function* () {
      throw new mocks.AnalyticsQueryError(
        "RESOURCE_EXHAUSTED",
        "Query resource limit exceeded",
      );
    });
    const { req, res } = createPostMocks(body);

    await handler(req, res);

    expect(parseSSEEvents(res._getData())).toEqual([
      {
        event: "error",
        data: { message: "Query resource limit exceeded" },
      },
    ]);
  });
});
