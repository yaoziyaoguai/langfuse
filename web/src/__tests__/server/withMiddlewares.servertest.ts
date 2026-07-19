import type { NextApiRequest, NextApiResponse } from "next";
import {
  LEGACY_PUBLIC_API_METRICS_CLICKHOUSE_RESOURCE_ERROR_MESSAGE,
  withMiddlewares,
} from "@/src/features/public-api/server/withMiddlewares";
import { clickHouseRouteForRequest } from "@/src/features/public-api/server/clickHouseRequestTags";
import {
  BaseError,
  InvalidRequestError,
  LangfuseNotFoundError,
  UnauthorizedError,
  ServiceUnavailableError,
} from "@langfuse/shared";
import {
  ClickHouseResourceError,
  DorisError,
  logger,
  traceException,
} from "@langfuse/shared/src/server";
import { createMocks } from "node-mocks-http";
import { z } from "zod";
import { Prisma } from "@prisma/client";
import { COMMUNITY_CAPABILITIES } from "@/src/features/capabilities/communityAvailability";

const envState = vi.hoisted(() => ({ backend: "clickhouse" }));

vi.mock("@/src/env.mjs", () => ({
  env: {
    get LANGFUSE_ANALYTICS_BACKEND() {
      return envState.backend;
    },
  },
}));

// Mock the logger and traceException
vi.mock("@langfuse/shared/src/server", async () => ({
  ...(await vi.importActual("@langfuse/shared/src/server")),
  logger: {
    info: vi.fn(),
    warn: vi.fn(),
    error: vi.fn(),
    debug: vi.fn(),
  },
  traceException: vi.fn(),
}));

describe("clickHouseRouteForRequest", () => {
  const request = (method: string | undefined, url: string | undefined) =>
    ({ method, url }) as NextApiRequest;

  it("uses only the request pathname", () => {
    expect(
      clickHouseRouteForRequest(
        request(
          "GET",
          "/api/public/v2/traces?projectId=project-1&secret=do-not-log",
        ),
      ),
    ).toBe("GET /api/public/v2/traces");
  });

  it("removes search params from malformed urls in the fallback path", () => {
    expect(
      clickHouseRouteForRequest(
        request("POST", "http://[::1?secret=do-not-log#fragment"),
      ),
    ).toBe("POST http://[::1");
  });

  it("falls back to UNKNOWN method for missing methods", () => {
    expect(
      clickHouseRouteForRequest(request(undefined, "/api/public/health")),
    ).toBe("UNKNOWN /api/public/health");
  });
});

describe("withMiddlewares error handling", () => {
  beforeEach(() => {
    envState.backend = "clickhouse";
    vi.clearAllMocks();
  });

  it("fails closed before invoking a Doris-unsupported handler", async () => {
    envState.backend = "doris";
    const endpoint = vi.fn();
    const handler = withMiddlewares({ GET: endpoint });
    const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
      method: "GET",
      url: "/api/public/experiments",
    });

    await handler(req, res);

    expect(res._getStatusCode()).toBe(501);
    expect(JSON.parse(res._getData())).toEqual(
      COMMUNITY_CAPABILITIES.experiments,
    );
    expect(endpoint).not.toHaveBeenCalled();
  });

  describe("BaseError handling", () => {
    it("preserves structured Doris time-range validation metadata", async () => {
      const error = Object.assign(
        new InvalidRequestError(
          "Analytics query requires a valid bounded UTC time range",
        ),
        {
          code: "InvalidTimeRange",
          maxDays: 30,
          acceptedRange: {
            from: new Date("2026-01-01T00:00:00.000Z"),
            to: new Date("2026-02-01T00:00:00.001Z"),
          },
        },
      );

      const handler = withMiddlewares({
        GET: async () => {
          throw error;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "GET",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(400);
      expect(JSON.parse(res._getData())).toEqual({
        message: "Analytics query requires a valid bounded UTC time range",
        error: "InvalidTimeRange",
        code: "InvalidTimeRange",
        maxDays: 30,
        acceptedRange: {
          from: "2026-01-01T00:00:00.000Z",
          to: "2026-02-01T00:00:00.001Z",
        },
      });
      expect(logger.warn).toHaveBeenCalledWith(error);
      expect(traceException).not.toHaveBeenCalled();
    });

    it("should handle BaseError with 4xx status code", async () => {
      const error = new BaseError("BadRequest", 400, "Bad Request", false);

      const handler = withMiddlewares({
        POST: async () => {
          throw error;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "POST",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(400);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Bad Request",
        error: "BadRequest",
      });
      expect(logger.warn).toHaveBeenCalledWith(error);
      expect(logger.error).not.toHaveBeenCalled();
      expect(traceException).not.toHaveBeenCalled();
    });

    it("should handle BaseError with 5xx status code and trace exception", async () => {
      const error = new BaseError(
        "ServiceUnavailable",
        503,
        "Internal Error",
        true,
      );

      const handler = withMiddlewares({
        GET: async () => {
          throw error;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "GET",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(503);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Internal Error",
        error: "ServiceUnavailable",
      });
      // Should trace 5xx errors
      expect(traceException).toHaveBeenCalledWith(error);
    });
  });

  describe("LangfuseNotFoundError handling", () => {
    it("should handle LangfuseNotFoundError and log as info", async () => {
      const error = new LangfuseNotFoundError("Resource not found");

      const handler = withMiddlewares({
        GET: async () => {
          throw error;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "GET",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(404);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Resource not found",
        error: "LangfuseNotFoundError",
      });
      // Should log as info, not error
      expect(logger.info).toHaveBeenCalledWith(error);
      expect(logger.error).not.toHaveBeenCalled();
    });
  });

  describe("UnauthorizedError handling", () => {
    it("should handle UnauthorizedError and log as info", async () => {
      const error = new UnauthorizedError("Invalid credentials");

      const handler = withMiddlewares({
        POST: async () => {
          throw error;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "POST",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(401);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Invalid credentials",
        error: "UnauthorizedError",
      });
      // Should log as info, not error
      expect(logger.info).toHaveBeenCalledWith(error);
      expect(logger.error).not.toHaveBeenCalled();
    });
  });

  describe("MethodNotAllowedError handling", () => {
    it("should throw MethodNotAllowedError for unsupported methods", async () => {
      const handler = withMiddlewares({
        GET: async () => {},
        // POST is not defined
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "POST",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(405);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Method not allowed",
        error: "MethodNotAllowedError",
      });
    });
  });

  describe("ClickHouseResourceError handling", () => {
    it("should handle ClickHouseResourceError with 422 status", async () => {
      const originalError = new Error("Memory limit exceeded: maximum: 10GB");
      const resourceError = new ClickHouseResourceError(
        "MEMORY_LIMIT",
        originalError,
      );

      const handler = withMiddlewares({
        POST: async () => {
          throw resourceError;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "POST",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(422);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData["message"]).toBeDefined();
      expect(jsonData["message"]).toContain(
        ClickHouseResourceError.ERROR_ADVICE_MESSAGE,
      );
      expect(jsonData["error"]).toBe("Request timed out");
    });

    it("should include tags from the error in the warn log", async () => {
      const originalError = new Error("Memory limit exceeded");
      const resourceError = new ClickHouseResourceError(
        "MEMORY_LIMIT",
        originalError,
        {
          tag_schema_version: "1",
          surface: "publicapi",
          route: "GET /api/public/test",
        },
      );

      const handler = withMiddlewares({
        GET: async () => {
          throw resourceError;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "GET",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(422);
      expect(logger.warn).toHaveBeenCalledTimes(1);
      expect(logger.warn).toHaveBeenCalledWith(
        "ClickHouse resource limit exceeded",
        expect.objectContaining({
          errorType: "MEMORY_LIMIT",
          tags: {
            tag_schema_version: "1",
            surface: "publicapi",
            route: "GET /api/public/test",
          },
        }),
      );
    });

    it("should handle ClickHouseResourceError with custom advice", async () => {
      const originalError = new Error("Timeout exceeded");
      const resourceError = new ClickHouseResourceError(
        "TIMEOUT",
        originalError,
      );

      const handler = withMiddlewares(
        {
          GET: async () => {
            throw resourceError;
          },
        },
        {
          clickHouseResourceErrorMessage:
            LEGACY_PUBLIC_API_METRICS_CLICKHOUSE_RESOURCE_ERROR_MESSAGE,
        },
      );

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "GET",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(422);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData["message"]).toBe(
        LEGACY_PUBLIC_API_METRICS_CLICKHOUSE_RESOURCE_ERROR_MESSAGE,
      );
      expect(jsonData["message"]).toContain(
        "https://langfuse.com/docs/metrics/features/metrics-api",
      );
    });
  });

  describe("DorisError handling", () => {
    it("returns a sanitized 503 for retryable failures", async () => {
      const resourceError = new DorisError("ANALYTICS_TIMEOUT", true, {
        correlationId: "request-1",
      });
      const handler = withMiddlewares({
        POST: async () => {
          throw resourceError;
        },
      });
      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "POST",
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(503);
      expect(JSON.parse(res._getData())).toEqual({
        message:
          "Analytics storage is temporarily unavailable. Please retry the request.\nSee https://langfuse.com/docs/api-and-data-platform/features/public-api for more details.",
        error: "ANALYTICS_TIMEOUT",
      });
      expect(logger.warn).toHaveBeenCalledWith(
        "Analytics storage request failed",
        {
          code: "ANALYTICS_TIMEOUT",
          retryable: true,
          correlationId: "request-1",
        },
      );
      expect(traceException).not.toHaveBeenCalled();
    });
  });

  describe("Prisma exception handling", () => {
    it("should handle Prisma exceptions with generic 500 error", async () => {
      // Create a real Prisma error
      const prismaError = new Prisma.PrismaClientKnownRequestError(
        "Unique constraint failed",
        { code: "P2002", clientVersion: "5.0.0" },
      );

      const handler = withMiddlewares({
        POST: async () => {
          throw prismaError;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "POST",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(500);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Internal Server Error",
        error: "An unknown error occurred",
      });

      expect(traceException).toHaveBeenCalledWith(prismaError);
    });
  });

  describe("Zod validation error handling", () => {
    it("should handle Zod validation errors with 400 status", async () => {
      const schema = z.object({
        name: z.string(),
        age: z.number(),
      });

      const handler = withMiddlewares({
        POST: async () => {
          // This will throw a ZodError
          schema.parse({ name: "John", age: "not a number" });
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "POST",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(400);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Invalid request data",
        error: expect.arrayContaining([
          expect.objectContaining({
            code: "invalid_type",
            path: ["age"],
          }),
        ]),
      });
      expect(logger.warn).toHaveBeenCalledWith(expect.any(z.ZodError));
      expect(logger.error).not.toHaveBeenCalled();
      expect(traceException).not.toHaveBeenCalled();
    });
  });

  describe("ServiceUnavailableError handling", () => {
    it("should handle ServiceUnavailableError with 503 status", async () => {
      const error = new ServiceUnavailableError(
        "Storage service temporarily unavailable due to network issues",
      );

      const handler = withMiddlewares({
        POST: async () => {
          throw error;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "POST",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(503);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message:
          "Storage service temporarily unavailable due to network issues",
        error: "ServiceUnavailableError",
      });
      // Should trace 5xx errors
      expect(traceException).toHaveBeenCalledWith(error);
    });
  });

  describe("Generic error handling", () => {
    it("should keep handler RangeError instances on the generic 500 path", async () => {
      const error = new RangeError("Invalid string length");

      const handler = withMiddlewares({
        GET: async () => {
          throw error;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "GET",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(500);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Internal Server Error",
        error: "Invalid string length",
      });
      expect(logger.error).toHaveBeenCalledWith(error);
      expect(traceException).toHaveBeenCalledWith(error);
    });

    it("should handle generic Error instances with 500 status", async () => {
      const error = new Error("Something went wrong");

      const handler = withMiddlewares({
        DELETE: async () => {
          throw error;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "DELETE",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(500);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Internal Server Error",
        error: "Something went wrong",
      });

      expect(traceException).toHaveBeenCalledWith(error);
    });

    it("should handle non-Error thrown values with 500 status", async () => {
      const handler = withMiddlewares({
        PATCH: async () => {
          throw "string error";
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "PATCH",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(500);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Internal Server Error",
        error: "An unknown error occurred",
      });
    });

    it("should handle null/undefined errors with 500 status", async () => {
      const handler = withMiddlewares({
        PUT: async () => {
          throw null;
        },
      });

      const { req, res } = createMocks<NextApiRequest, NextApiResponse>({
        method: "PUT",
        headers: {
          "x-langfuse-public-key": "test-key",
        },
      });

      await handler(req, res);

      expect(res._getStatusCode()).toBe(500);
      const jsonData = JSON.parse(res._getData());
      expect(jsonData).toMatchObject({
        message: "Internal Server Error",
        error: "An unknown error occurred",
      });
    });
  });
});
