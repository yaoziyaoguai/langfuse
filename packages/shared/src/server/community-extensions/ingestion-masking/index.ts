import { env, type SharedEnv } from "../../../env";
import {
  recordHistogram,
  recordIncrement,
  traceException,
} from "../../instrumentation";
import { logger } from "../../logger";
import type {
  IngestionMaskingInput,
  IngestionMaskingResult,
} from "../../ingestion-masking/types";
import {
  CircularRedirectError,
  fetchWithSecureRedirects,
  MaxRedirectsExceededError,
  OutboundUrlValidationError,
  RedirectValidationError,
} from "../../outbound-url";
import {
  COMMUNITY_MASKING_URL_VALIDATION_LOG_CONTEXT,
  communityMaskingWhitelistFromEnv,
  validateCommunityMaskingCallbackUrl,
} from "./validation";

const MAX_CALLBACK_RESPONSE_BYTES = 16 * 1024 * 1024;

type CommunityMaskingEnv = Pick<
  SharedEnv,
  | "LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED"
  | "LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL"
  | "LANGFUSE_COMMUNITY_MASKING_CALLBACK_TIMEOUT_MS"
  | "LANGFUSE_COMMUNITY_MASKING_CALLBACK_FAIL_CLOSED"
  | "LANGFUSE_COMMUNITY_MASKING_MAX_RETRIES"
  | "LANGFUSE_COMMUNITY_MASKING_PROPAGATED_HEADERS"
  | "LANGFUSE_COMMUNITY_MASKING_WHITELISTED_HOST"
  | "LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IPS"
  | "LANGFUSE_COMMUNITY_MASKING_WHITELISTED_IP_SEGMENTS"
  | "NEXT_PUBLIC_LANGFUSE_CLOUD_REGION"
>;

type CommunityMaskingConfig = {
  callbackUrl: string;
  timeoutMs: number;
  failClosed: boolean;
  maxRetries: number;
  propagatedHeaders: readonly string[];
  validationEnvironment: CommunityMaskingEnv;
};

export async function readResponseBodyWithLimit(
  response: Response,
  maxBytes: number = MAX_CALLBACK_RESPONSE_BYTES,
): Promise<Uint8Array | null> {
  if (!Number.isSafeInteger(maxBytes) || maxBytes < 0) {
    throw new TypeError("Callback response size limit must be non-negative");
  }

  const rawContentLength = response.headers.get("content-length");
  if (rawContentLength !== null) {
    const contentLength = Number(rawContentLength);
    if (Number.isFinite(contentLength) && contentLength > maxBytes) {
      if (response.body) {
        await response.body
          .cancel("Callback response exceeds size limit")
          .catch(() => undefined);
      }
      return null;
    }
  }

  if (!response.body) return new Uint8Array();

  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let totalBytes = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    totalBytes += value.byteLength;
    if (totalBytes > maxBytes) {
      await reader
        .cancel("Callback response exceeds size limit")
        .catch(() => undefined);
      return null;
    }
    chunks.push(value);
  }

  const bytes = new Uint8Array(totalBytes);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
  }
  return bytes;
}

export function getCommunityMaskingConfig(
  environment: CommunityMaskingEnv = env,
): CommunityMaskingConfig | null {
  if (
    environment.LANGFUSE_COMMUNITY_EXTENSIONS_ENABLED !== "true" ||
    !environment.LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL
  ) {
    return null;
  }
  return {
    callbackUrl: environment.LANGFUSE_COMMUNITY_MASKING_CALLBACK_URL,
    timeoutMs: environment.LANGFUSE_COMMUNITY_MASKING_CALLBACK_TIMEOUT_MS,
    failClosed:
      environment.LANGFUSE_COMMUNITY_MASKING_CALLBACK_FAIL_CLOSED === "true",
    maxRetries: environment.LANGFUSE_COMMUNITY_MASKING_MAX_RETRIES,
    propagatedHeaders:
      environment.LANGFUSE_COMMUNITY_MASKING_PROPAGATED_HEADERS,
    validationEnvironment: environment,
  };
}

export async function applyCommunityIngestionMasking<T>(
  input: IngestionMaskingInput<T>,
  environment: CommunityMaskingEnv = env,
): Promise<IngestionMaskingResult<T>> {
  const config = getCommunityMaskingConfig(environment);
  if (!config) return { success: true, data: input.data, masked: false };

  let lastError = "unknown callback failure";
  for (let attempt = 0; attempt <= config.maxRetries; attempt += 1) {
    const result = await requestMaskedData(input, config, attempt + 1);
    if (result.success) {
      recordIncrement("langfuse.community.ingestion_masking.success", 1, {
        attempt: String(attempt + 1),
      });
      return {
        success: true,
        data: result.data,
        masked: true,
      };
    }

    lastError = result.error;
    logger.warn("Community ingestion masking callback failed", {
      projectId: input.projectId,
      orgId: input.orgId,
      attempt: attempt + 1,
      maxAttempts: config.maxRetries + 1,
      retryable: result.retryable,
      error: result.error,
    });
    if (!result.retryable || attempt === config.maxRetries) break;
    await new Promise((resolve) =>
      setTimeout(resolve, Math.min(100 * 2 ** attempt, 1_000)),
    );
  }

  recordIncrement("langfuse.community.ingestion_masking.failure", 1, {
    fail_closed: String(config.failClosed),
  });
  traceException(new Error(`Community ingestion masking failed: ${lastError}`));
  if (config.failClosed) {
    return {
      success: false,
      data: input.data,
      masked: false,
      error: lastError,
    };
  }
  return {
    success: true,
    data: input.data,
    masked: false,
    error: lastError,
  };
}

async function requestMaskedData<T>(
  input: IngestionMaskingInput<T>,
  config: CommunityMaskingConfig,
  attempt: number,
): Promise<
  | { success: true; data: T }
  | { success: false; error: string; retryable: boolean }
> {
  const startedAt = Date.now();
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), config.timeoutMs);

  try {
    const headers = new Headers({ "Content-Type": "application/json" });
    for (const headerName of config.propagatedHeaders) {
      const value = input.propagatedHeaders?.[headerName];
      if (value !== undefined) headers.set(headerName, value);
    }
    headers.set("X-Langfuse-Org-Id", input.orgId ?? "");
    headers.set("X-Langfuse-Project-Id", input.projectId);

    await validateCommunityMaskingCallbackUrl(
      config.callbackUrl,
      config.validationEnvironment,
    );
    const whitelist = communityMaskingWhitelistFromEnv(
      config.validationEnvironment,
    );
    const { response } = await fetchWithSecureRedirects(
      config.callbackUrl,
      {
        method: "POST",
        headers,
        body: JSON.stringify(input.data),
        signal: controller.signal,
      },
      {
        // Masking payloads contain raw analytics. The callback endpoint is an
        // exact operator-controlled destination, so no redirect is legitimate.
        maxRedirects: 0,
        additionalSensitiveHeaders: [
          ...config.propagatedHeaders,
          "x-langfuse-org-id",
          "x-langfuse-project-id",
        ],
        redirectValidation: {
          validateUrl: async (url) =>
            validateCommunityMaskingCallbackUrl(
              url,
              config.validationEnvironment,
            ),
          whitelist,
          logContext: COMMUNITY_MASKING_URL_VALIDATION_LOG_CONTEXT,
        },
      },
    );
    if (!response.ok) {
      await response.body
        ?.cancel("Callback returned an error")
        .catch(() => undefined);
      recordDuration(startedAt, false, attempt);
      return {
        success: false,
        error: `HTTP ${response.status}`,
        retryable: response.status === 429 || response.status >= 500,
      };
    }

    const bytes = await readResponseBodyWithLimit(response);
    if (bytes === null) {
      recordDuration(startedAt, false, attempt);
      return {
        success: false,
        error: "Callback response exceeds size limit",
        retryable: false,
      };
    }
    try {
      const data = JSON.parse(new TextDecoder().decode(bytes)) as T;
      recordDuration(startedAt, true, attempt);
      return {
        success: true,
        data,
      };
    } catch {
      recordDuration(startedAt, false, attempt);
      return {
        success: false,
        error: "Callback returned invalid JSON",
        retryable: false,
      };
    }
  } catch (error) {
    recordDuration(startedAt, false, attempt);
    return {
      success: false,
      error:
        error instanceof Error && error.name === "AbortError"
          ? "Callback timed out"
          : error instanceof Error
            ? error.message
            : "Callback request failed",
      retryable: !isSecurityValidationError(error),
    };
  } finally {
    clearTimeout(timeout);
  }
}

function isSecurityValidationError(error: unknown): boolean {
  return (
    error instanceof OutboundUrlValidationError ||
    error instanceof RedirectValidationError ||
    error instanceof MaxRedirectsExceededError ||
    error instanceof CircularRedirectError
  );
}

function recordDuration(
  startedAt: number,
  success: boolean,
  attempt: number,
): void {
  recordHistogram(
    "langfuse.community.ingestion_masking.callback_duration_ms",
    Date.now() - startedAt,
    {
      status: success ? "success" : "error",
      attempt: String(attempt),
    },
  );
}
