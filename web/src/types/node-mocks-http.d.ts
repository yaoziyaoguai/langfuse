import type { NextApiRequest, NextApiResponse } from "next";
import "node-mocks-http";

declare module "node-mocks-http" {
  /**
   * node-mocks-http 1.18 narrows Node `ServerResponse` subclasses to Express
   * responses. Preserve Next's response type for API-route tests.
   */
  export function createMocks<
    TRequest extends NextApiRequest = NextApiRequest,
    TResponse extends NextApiResponse = NextApiResponse,
  >(
    reqOptions?: RequestOptions,
    resOptions?: ResponseOptions,
  ): {
    req: MockRequest<TRequest>;
    res: MockResponse<TResponse>;
  };
}
