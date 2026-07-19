import { z } from "zod";

import { InvalidRequestError, UnauthorizedError } from "../../errors";
import {
  acceptAnalyticsIngestion,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
} from "../analytics-persistence";
import type { AuthHeaderValidVerificationResultIngestion } from "../auth/types";
import {
  getCurrentSpan,
  recordDistribution,
  recordIncrement,
} from "../instrumentation";
import { logger } from "../logger";
import { getS3EventStorageClient } from "../s3";
import { env } from "../../env";
import type { IngestionAttribution } from "./ingestionAttribution";
import {
  createIngestionEventSchema,
  eventTypes,
  type IngestionEventType,
} from "./types";

const UNSUPPORTED_EVENT = {
  error: "UnsupportedFeature",
  code: "R2_LEGACY_INGESTION_UNAVAILABLE",
  message:
    "This ingestion event type is not available in the Doris R1A release.",
  recovery:
    "Use the OTLP traces endpoint for tracing data. Dataset-run analytics requires a separately approved R1B adoption.",
} as const;

type BatchError = {
  id: string;
  status: number;
  message?: string;
  error?: string;
  code?: string;
  recovery?: string;
};

export type AnalyticsEventBatchResult = {
  successes: { id: string; status: number }[];
  errors: BatchError[];
};

type ProcessEventBatchOptions = {
  delay?: number | null;
  source?: "api" | "otel";
  isLangfuseInternal?: boolean;
  forwardToEventsTable?: boolean;
  attribution: IngestionAttribution;
};

function isAuthorized(
  event: IngestionEventType,
  auth: AuthHeaderValidVerificationResultIngestion,
): boolean {
  if (event.type === eventTypes.SDK_LOG) return true;
  if (event.type === eventTypes.SCORE_CREATE) {
    return (
      auth.scope.accessLevel === "scores" ||
      auth.scope.accessLevel === "project"
    );
  }
  return auth.scope.accessLevel === "project";
}

function validationError(id: string, error: z.ZodError): BatchError {
  return {
    id,
    status: 400,
    message: "Invalid request data",
    error: new InvalidRequestError(error.message).message,
  };
}

function unsupportedError(id: string): BatchError {
  return { id, status: 501, ...UNSUPPORTED_EVENT };
}

/**
 * R1A 的事件批入口只接受 score 与非持久化 SDK log。Tracing 统一通过 OTLP，
 * 这样不会重新引入旧 merge queue、v3 表或双写分支。
 */
export async function processEventBatch(
  input: unknown[],
  auth: AuthHeaderValidVerificationResultIngestion,
  options: ProcessEventBatchOptions,
): Promise<AnalyticsEventBatchResult> {
  if (input.length === 0) return { successes: [], errors: [] };
  if (!auth.scope.projectId) throw new UnauthorizedError("Missing project ID");

  const source = options.source ?? "api";
  recordIncrement("langfuse.ingestion.event", input.length, { source });
  recordDistribution("langfuse.ingestion.event_distribution", input.length, {
    source,
  });
  getCurrentSpan()?.setAttribute("langfuse.ingestion.batch_size", input.length);

  const schema = createIngestionEventSchema(
    options.isLangfuseInternal ?? false,
  );
  const scores: IngestionEventType[] = [];
  const successes: AnalyticsEventBatchResult["successes"] = [];
  const errors: AnalyticsEventBatchResult["errors"] = [];

  for (const value of input) {
    const rawId =
      typeof value === "object" && value !== null && "id" in value
        ? String(value.id)
        : "unknown";
    const parsed = schema.safeParse(value);
    if (!parsed.success) {
      errors.push(validationError(rawId, parsed.error));
      continue;
    }
    if (!isAuthorized(parsed.data, auth)) {
      errors.push({
        id: parsed.data.id,
        status: 401,
        message: "Authentication error",
        error: "Access Scope Denied",
      });
      continue;
    }
    if (parsed.data.type === eventTypes.SDK_LOG) {
      logger.info("SDK Log Event", { event: parsed.data });
      successes.push({ id: parsed.data.id, status: 201 });
      continue;
    }
    if (parsed.data.type === eventTypes.SCORE_CREATE) {
      scores.push(parsed.data);
      continue;
    }
    errors.push(unsupportedError(parsed.data.id));
  }

  if (scores.length > 0) {
    await acceptAnalyticsIngestion({
      projectId: auth.scope.projectId,
      envelope: {
        formatVersion: 1,
        source: "score",
        payload: scores,
        attribution: options.attribution,
      },
      canonicalizerVersion: CURRENT_ANALYTICS_CANONICALIZER_VERSION,
      schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
      storageService: getS3EventStorageClient(
        env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
      ),
      rawPrefix: env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX,
    });
    successes.push(...scores.map(({ id }) => ({ id, status: 201 })));
  }

  if (errors.length > 0) {
    logger.warn("Event batch contains rejected events", {
      projectId: auth.scope.projectId,
      rejectedCount: errors.length,
    });
  }
  return { successes, errors };
}
