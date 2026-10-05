import crypto from "node:crypto";
import { type IncomingHttpHeaders } from "node:http";
import { type NextApiRequest, type NextApiResponse } from "next";

import { env } from "@/src/env.mjs";
import { logger } from "@langfuse/shared/src/server";

type AdminAuthOptions = {
  isAllowedOnLangfuseCloud?: boolean;
};

type AdminAuthResult =
  | { isAuthorized: true }
  | { isAuthorized: false; error: string };

const verifyBearerToken = (
  authorization: string,
  options: AdminAuthOptions,
): AdminAuthResult => {
  if (
    !options.isAllowedOnLangfuseCloud &&
    env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION &&
    env.NEXT_PUBLIC_LANGFUSE_CLOUD_REGION !== "DEV"
  ) {
    return {
      isAuthorized: false,
      error: "Not accessible on Langfuse Cloud",
    };
  }

  if (!env.ADMIN_API_KEY) {
    logger.error("ADMIN_API_KEY is not set");
    return { isAuthorized: false, error: "ADMIN_API_KEY is not set" };
  }

  const [scheme, token, extra] = authorization.split(" ");
  if (scheme !== "Bearer" || !token || extra) {
    return {
      isAuthorized: false,
      error: "Unauthorized: Invalid token",
    };
  }

  const tokenBuffer = Buffer.from(token);
  const configuredKeyBuffer = Buffer.from(env.ADMIN_API_KEY);
  if (
    tokenBuffer.length !== configuredKeyBuffer.length ||
    !crypto.timingSafeEqual(tokenBuffer, configuredKeyBuffer)
  ) {
    return {
      isAuthorized: false,
      error: "Unauthorized: Invalid token",
    };
  }

  return { isAuthorized: true };
};

const verifyAdminHeaders = (
  headers: IncomingHttpHeaders,
  options: AdminAuthOptions,
): AdminAuthResult => {
  if (!headers.authorization) {
    return {
      isAuthorized: false,
      error: "Unauthorized: No authorization header provided",
    };
  }
  return verifyBearerToken(headers.authorization, options);
};

export class AdminApiAuthService {
  static verifyAdminAuthFromAuthString(
    authorization: string,
    options: AdminAuthOptions = {},
  ): AdminAuthResult {
    return verifyBearerToken(authorization, options);
  }

  static handleAdminAuth(
    req: NextApiRequest,
    res: NextApiResponse,
    options: AdminAuthOptions = {},
  ): boolean {
    const result = verifyAdminHeaders(req.headers, options);
    if (result.isAuthorized) return true;

    const status = result.error.startsWith("Unauthorized") ? 401 : 403;
    res.status(status).json({ error: result.error });
    return false;
  }
}
