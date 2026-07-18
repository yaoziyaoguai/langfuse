export type DorisErrorCode =
  | "ANALYTICS_UNAVAILABLE"
  | "ANALYTICS_TIMEOUT"
  | "FILTERED_ROWS"
  | "INVALID_REQUEST"
  | "LOAD_REJECTED"
  | "REDIRECT_REJECTED"
  | "SCHEMA_MISMATCH";

const SAFE_MESSAGES: Record<DorisErrorCode, string> = {
  ANALYTICS_UNAVAILABLE: "Analytics storage is unavailable",
  ANALYTICS_TIMEOUT: "Analytics storage request timed out",
  FILTERED_ROWS: "Analytics storage rejected one or more rows",
  INVALID_REQUEST: "Analytics storage request is invalid",
  LOAD_REJECTED: "Analytics storage rejected the load",
  REDIRECT_REJECTED: "Analytics storage redirect was rejected",
  SCHEMA_MISMATCH: "Analytics storage schema is incompatible",
};

/**
 * 对外只暴露稳定错误码和安全消息；底层 SQL、地址、凭据与载荷不得穿透边界。
 */
export class DorisError extends Error {
  readonly name = "DorisError";

  constructor(
    readonly code: DorisErrorCode,
    readonly retryable: boolean,
    options?: { readonly correlationId?: string },
  ) {
    super(SAFE_MESSAGES[code]);
    this.correlationId = options?.correlationId;
  }

  readonly correlationId?: string;
}

export function toDorisError(error: unknown): DorisError {
  if (error instanceof DorisError) return error;
  const code =
    error instanceof Error && /timeout|timed out|ETIMEDOUT/i.test(error.message)
      ? "ANALYTICS_TIMEOUT"
      : "ANALYTICS_UNAVAILABLE";
  return new DorisError(code, true);
}
