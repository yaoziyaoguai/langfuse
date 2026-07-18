export type AnalyticsPersistenceErrorCode =
  | "ANALYTICS_UNAVAILABLE"
  | "ANALYTICS_TIMEOUT"
  | "ANALYTICS_RESOURCE_EXHAUSTED"
  | "ANALYTICS_VALIDATION_ERROR"
  | "ANALYTICS_CONFLICT"
  | "ANALYTICS_QUARANTINED"
  | "ANALYTICS_UNRECOVERABLE"
  | "ANALYTICS_INVALID_TIME_RANGE"
  | "ANALYTICS_UNSUPPORTED_FEATURE"
  | "ANALYTICS_NOT_FOUND";

const SAFE_MESSAGES: Record<AnalyticsPersistenceErrorCode, string> = {
  ANALYTICS_UNAVAILABLE: "Analytics persistence is unavailable",
  ANALYTICS_TIMEOUT: "Analytics persistence timed out",
  ANALYTICS_RESOURCE_EXHAUSTED: "Analytics persistence capacity is exhausted",
  ANALYTICS_VALIDATION_ERROR: "Analytics persistence input is invalid",
  ANALYTICS_CONFLICT:
    "Analytics persistence input conflicts with durable state",
  ANALYTICS_QUARANTINED: "Analytics persistence input is quarantined",
  ANALYTICS_UNRECOVERABLE: "Analytics persistence input is unrecoverable",
  ANALYTICS_INVALID_TIME_RANGE: "Analytics persistence time range is invalid",
  ANALYTICS_UNSUPPORTED_FEATURE: "Analytics persistence feature is unsupported",
  ANALYTICS_NOT_FOUND: "Analytics persistence entity was not found",
};

export type AnalyticsPersistenceErrorTag =
  | "operationId"
  | "projectId"
  | "entityType"
  | "sourceContract"
  | "phase"
  | "reasonCode";

const SAFE_TAGS = new Set<AnalyticsPersistenceErrorTag>([
  "operationId",
  "projectId",
  "entityType",
  "sourceContract",
  "phase",
  "reasonCode",
]);

function safeIdentifier(value: string | undefined): string | undefined {
  return value && /^[A-Za-z0-9._:/-]{1,128}$/.test(value) ? value : undefined;
}

function safeTags(
  tags: Partial<Record<AnalyticsPersistenceErrorTag, string>> | undefined,
): Readonly<Partial<Record<AnalyticsPersistenceErrorTag, string>>> | undefined {
  if (!tags) return undefined;
  const entries = Object.entries(tags).flatMap(([key, value]) => {
    const safeValue = safeIdentifier(value);
    return SAFE_TAGS.has(key as AnalyticsPersistenceErrorTag) && safeValue
      ? [[key, safeValue] as const]
      : [];
  });
  return entries.length > 0 ? Object.fromEntries(entries) : undefined;
}

export class AnalyticsPersistenceError extends Error {
  readonly name = "AnalyticsPersistenceError";
  readonly correlationId?: string;
  readonly tags?: Readonly<
    Partial<Record<AnalyticsPersistenceErrorTag, string>>
  >;

  constructor(
    readonly code: AnalyticsPersistenceErrorCode,
    readonly retryable: boolean,
    options?: {
      readonly correlationId?: string;
      readonly tags?: Partial<Record<AnalyticsPersistenceErrorTag, string>>;
    },
  ) {
    super(SAFE_MESSAGES[code]);
    this.correlationId = safeIdentifier(options?.correlationId);
    this.tags = safeTags(options?.tags);
  }
}
