import { createHash, randomUUID } from "crypto";
import { z } from "zod";

import { env } from "../../env";
import {
  InvalidRequestError,
  LangfuseNotFoundError,
  UnauthorizedError,
} from "../../errors";
import type { AuthHeaderValidVerificationResultIngestion } from "../auth/types";
import {
  acceptAnalyticsIngestion,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
} from "../analytics-persistence";
import { isAnalyticsBackend } from "../analytics-persistence/analyticsBackend";
import {
  getClickhouseEntityType,
  type IngestionEntityTypes,
} from "../clickhouse/schemaUtils";
import {
  getCurrentSpan,
  instrumentAsync,
  recordDistribution,
  recordIncrement,
} from "../instrumentation";
import { logger } from "../logger";
import { QueueJobs } from "../queues";
import { IngestionQueue } from "../redis/ingestionQueue";
import { redis } from "../redis/redis";
import {
  eventTypes,
  createIngestionEventSchema,
  IngestionEventType,
} from "./types";
import type { IngestionAttribution } from "./ingestionAttribution";
import {
  StorageService,
  StorageServiceFactory,
} from "../services/StorageService";
import {
  HASH_HEX_LENGTH,
  safeBlobFilenameStem,
  safeBlobKeySegment,
} from "../services/safeBlobKeySegment";
import { buildEventBucketPrefix } from "./eventBucketPath";
import { isTraceIdInSample } from "./sampling";
import {
  isS3SlowDownError,
  markProjectS3Slowdown,
} from "../redis/s3SlowdownTracking";
import { markProjectIngestFailure } from "../redis/ingestionFailureTracking";
import { getS3EventStorageClient } from "../s3";

export const DORIS_EXPERIMENT_INGESTION_UNAVAILABLE = {
  error: "UnsupportedFeature",
  code: "R1B_EXPERIMENTS_UNAVAILABLE",
  message:
    "Dataset-run ingestion is not available with the Doris analytics backend.",
  recovery:
    "Use the ClickHouse analytics backend until the Doris experiment projection is enabled.",
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

let s3StorageServiceClient: StorageService;

const getS3StorageServiceClient = (bucketName: string): StorageService => {
  if (!s3StorageServiceClient) {
    s3StorageServiceClient = StorageServiceFactory.getInstance({
      bucketName,
      accessKeyId: env.LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID,
      secretAccessKey: env.LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY,
      endpoint: env.LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT,
      region: env.LANGFUSE_S3_EVENT_UPLOAD_REGION,
      forcePathStyle: env.LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE === "true",
      awsSse: env.LANGFUSE_S3_EVENT_UPLOAD_SSE,
      awsSseKmsKeyId: env.LANGFUSE_S3_EVENT_UPLOAD_SSE_KMS_KEY_ID,
    });
  }
  return s3StorageServiceClient;
};

/**
 * Get the delay for the event based on the event type. Uses delay if set, 0 if current UTC timestamp is not between
 * 23:45 and 00:15, and env.LANGFUSE_INGESTION_QUEUE_DELAY_MS otherwise.
 * We need the delay around date boundaries to avoid duplicates for out-of-order processing of events.
 * @param delay - Delay overwrite. Used if non-null.
 */
const getDelay = (delay: number | null, source: "api" | "otel") => {
  if (delay !== null) {
    return delay;
  }
  const now = new Date();
  const hours = now.getUTCHours();
  const minutes = now.getUTCMinutes();

  if ((hours === 23 && minutes >= 45) || (hours === 0 && minutes <= 15)) {
    return env.LANGFUSE_INGESTION_QUEUE_DELAY_MS;
  }

  if (source === "otel") {
    return 0;
  }

  // Use 5s here to avoid duplicate processing on the worker. If the ingestion delay is set to a lower value,
  // we use this instead.
  // Values should be revisited based on a cost/performance trade-off.
  return Math.min(5000, env.LANGFUSE_INGESTION_QUEUE_DELAY_MS);
};

/**
 * Options for event batch processing.
 * @property delay - Delay in ms to wait before processing events in the batch.
 * @property source - Source of the events for metrics tracking (e.g., "otel", "api").
 * @property isLangfuseInternal - Whether the events are being ingested by Langfuse internally (e.g. traces created for prompt experiments).
 * @property forwardToEventsTable - Whether to forward events to the staging events table for batch propagation. If undefined, falls back to environment flags.
 * @property attribution - Request-level ingestion attribution to persist on generated records.
 */
type ProcessEventBatchOptions = {
  delay?: number | null;
  source?: "api" | "otel";
  isLangfuseInternal?: boolean;
  forwardToEventsTable?: boolean;
  attribution: IngestionAttribution;
};

/**
 * Processes a batch of events.
 * @param input - Batch of IngestionEventType. Will validate the types first thing and return errors if they are invalid.
 * @param authCheck - AuthHeaderValidVerificationResultIngestion
 * @param options - (Optional) Options for the event batch processing.
 */
export const processEventBatch = async (
  input: unknown[],
  authCheck: AuthHeaderValidVerificationResultIngestion,
  options: ProcessEventBatchOptions,
): Promise<AnalyticsEventBatchResult> => {
  if (input.length === 0) {
    return { successes: [], errors: [] };
  }
  if (isAnalyticsBackend(env.LANGFUSE_ANALYTICS_BACKEND, "doris")) {
    return processDorisEventBatch(input, authCheck, options);
  }
  const {
    delay = null,
    source = "api",
    isLangfuseInternal = false,
    forwardToEventsTable,
    attribution,
  } = options;

  // add context of api call to the span
  const currentSpan = getCurrentSpan();
  recordIncrement("langfuse.ingestion.event", input.length, { source });
  recordDistribution("langfuse.ingestion.event_distribution", input.length, {
    source,
  });

  currentSpan?.setAttribute("langfuse.ingestion.batch_size", input.length);
  currentSpan?.setAttribute(
    "langfuse.project.id",
    authCheck.scope.projectId ?? "",
  );
  if (authCheck.scope.orgId)
    currentSpan?.setAttribute("langfuse.org.id", authCheck.scope.orgId);
  if (authCheck.scope.plan)
    currentSpan?.setAttribute("langfuse.org.plan", authCheck.scope.plan);

  /**************
   * VALIDATION *
   **************/
  if (!authCheck.scope.projectId) {
    throw new UnauthorizedError("Missing project ID");
  }

  const validationErrors: { id: string; error: unknown }[] = [];
  const authenticationErrors: { id: string; error: unknown }[] = [];

  const ingestionSchema = createIngestionEventSchema(isLangfuseInternal);
  const batch: z.infer<typeof ingestionSchema>[] = input
    .flatMap((event) => {
      const parsed = ingestionSchema.safeParse(event);
      if (!parsed.success) {
        validationErrors.push({
          id:
            typeof event === "object" && event && "id" in event
              ? typeof event.id === "string"
                ? event.id
                : "unknown"
              : "unknown",
          error: new InvalidRequestError(parsed.error.message),
        });
        return [];
      }
      if (!isAuthorized(parsed.data, authCheck)) {
        authenticationErrors.push({
          id: parsed.data.id,
          error: new UnauthorizedError("Access Scope Denied"),
        });
        return [];
      }
      return [parsed.data];
    })
    .flatMap((event) => {
      if (event.type === eventTypes.SDK_LOG) {
        // Log SDK_LOG events, but remove them from further processing
        logger.info("SDK Log Event", { event });
        return [];
      }
      return [event];
    });

  const sortedBatch = sortBatch(batch);

  // We group events by eventBodyId which allows us to store and process them
  // as one which reduces infra interactions per event. Only used in the S3 case.
  //
  // The dedup struct also caches `entityType` and `bucketPrefix` so the two
  // downstream loops (S3 upload + IngestionQueue enqueue) read a single
  // source-of-truth per id instead of recomputing — that makes the
  // producer/consumer-must-agree invariant structural. The sanitization warn
  // log fires once at the time we resolve the prefix.
  //
  // `String(...)` preserves the prior null → "null" coercion of
  // `authCheck.scope.projectId`. The surrounding function legitimately treats
  // projectId as nullable elsewhere (metric labels, span attributes); we
  // don't narrow it in the type system, and a `null` projectId would land
  // events under an isolated `null/...` path rather than throw.
  const sortedBatchByEventBodyId = sortedBatch.reduce(
    (
      acc: Record<
        string,
        {
          data: IngestionEventType[];
          key: string;
          eventBodyId: string;
          type: (typeof eventTypes)[keyof typeof eventTypes];
          entityType: IngestionEntityTypes;
          bucketPrefix: string;
        }
      >,
      event,
    ) => {
      if (!event.body?.id) {
        return acc;
      }
      const entityType = getClickhouseEntityType(event.type);
      const dedupKey = `${entityType}-${event.body.id}`;
      if (!acc[dedupKey]) {
        const eventBodyId = event.body.id;
        const safeEventBodyId = safeBlobKeySegment(eventBodyId);
        if (safeEventBodyId !== eventBodyId) {
          // Do not log the raw or sanitized ID itself: the prefix can carry
          // PII (litellm encodes provider/model/request metadata in the ID).
          // The 16-hex hash is enough to correlate with the stored object
          // during debugging.
          logger.warn("Sanitized oversized/invalid entity ID for S3 key", {
            projectId: authCheck.scope.projectId,
            entityType,
            originalIdByteLength: Buffer.byteLength(eventBodyId, "utf8"),
            originalIdHash16: safeEventBodyId.slice(-HASH_HEX_LENGTH),
          });
        }
        acc[dedupKey] = {
          data: [],
          // `event.id` becomes a single-segment filename (`<id>.json`) at S3
          // write time and rides the queue payload as `fileKey`. Sanitize
          // here so `/`, `\`, control bytes, and over-budget lengths can't
          // reroute the write or overflow NAME_MAX.
          key: safeBlobFilenameStem(event.id, ".json"),
          type: event.type,
          eventBodyId,
          entityType,
          bucketPrefix: buildEventBucketPrefix({
            projectId: String(authCheck.scope.projectId),
            entityType,
            entityId: eventBodyId,
          }),
        };
      }
      acc[dedupKey].data.push(event);
      return acc;
    },
    {},
  );

  /********************
   * ASYNC PROCESSING *
   ********************/
  let s3UploadErrored = false;
  await instrumentAsync({ name: "s3-upload-events" }, async () => {
    // S3 Event Upload is blocking, but non-failing.
    // If a promise rejects, we log it below, but do not throw an error.
    // In this case, we upload the full batch into the Redis queue.
    const results = await Promise.allSettled(
      Object.keys(sortedBatchByEventBodyId).map(async (id) => {
        // We upload the event in an array to the S3 bucket grouped by the eventBodyId.
        // That way we batch updates from the same invocation into a single file and reduce
        // write operations on S3.
        const { data, key, bucketPrefix } = sortedBatchByEventBodyId[id];
        const bucketPath = `${bucketPrefix}${key}.json`;
        return getS3StorageServiceClient(
          env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
        ).uploadJson(bucketPath, data);
      }),
    );
    results.forEach((result) => {
      if (result.status === "rejected") {
        s3UploadErrored = true;

        // Check if this is a SlowDown error and mark the project for secondary queue
        if (isS3SlowDownError(result.reason)) {
          logger.warn(
            "S3 SlowDown error during upload, marking project for secondary queue",
            {
              projectId: authCheck.scope.projectId,
              error: result.reason,
            },
          );
          markProjectS3Slowdown(authCheck.scope.projectId!).catch(() => {});
          markProjectIngestFailure(authCheck.scope.projectId!, {
            source: "process_event_batch",
            reason: "s3_slowdown",
          });
        } else {
          markProjectIngestFailure(authCheck.scope.projectId!, {
            source: "process_event_batch",
            reason: "s3_upload_error",
          });
        }

        logger.error("Failed to upload event to S3", {
          error: result.reason,
        });
      }
    });
  });

  // Send each event individually to IngestionQueue for ClickHouse processing
  if (s3UploadErrored) {
    throw new Error(
      "Failed to upload events to blob storage, aborting event processing",
    );
  }

  if (!redis) {
    throw new Error("Redis not initialized, aborting event processing");
  }

  const projectIdsToSkipS3List =
    env.LANGFUSE_SKIP_S3_LIST_FOR_OBSERVATIONS_PROJECT_IDS?.split(",") ?? [];

  await Promise.all(
    Object.keys(sortedBatchByEventBodyId).map(async (id) => {
      const eventData = sortedBatchByEventBodyId[id];
      const shardingKey = `${authCheck.scope.projectId}-${eventData.eventBodyId}`;
      const queue = IngestionQueue.getInstance({ shardingKey });

      const isDatasetRunItemEvent = eventData.entityType === "dataset_run_item";
      const isObservationEvent = eventData.entityType === "observation";

      const isOtelOrSkipS3Project =
        authCheck.scope.projectId !== null &&
        (source === "otel" ||
          projectIdsToSkipS3List.includes(authCheck.scope.projectId));

      const shouldSkipS3List =
        isDatasetRunItemEvent || (isObservationEvent && isOtelOrSkipS3Project);

      const { isSampled, isSamplingConfigured } = isTraceIdInSample({
        projectId: authCheck.scope.projectId,
        event: eventData.data[0],
      });

      if (!isSampled) {
        recordIncrement("langfuse.ingestion.sampling", eventData.data.length, {
          projectId: authCheck.scope.projectId ?? "<not set>",
          sampling_decision: "out",
        });

        return;
      }

      if (isSamplingConfigured) {
        recordIncrement("langfuse.ingestion.sampling", eventData.data.length, {
          projectId: authCheck.scope.projectId ?? "<not set>",
          sampling_decision: "in",
        });
      }

      return queue
        ? queue.add(
            QueueJobs.IngestionJob,
            {
              id: randomUUID(),
              timestamp: new Date(),
              name: QueueJobs.IngestionJob as const,
              payload: {
                data: {
                  type: eventData.type,
                  eventBodyId: eventData.eventBodyId,
                  fileKey: eventData.key,
                  skipS3List: shouldSkipS3List,
                  forwardToEventsTable,
                  bucketPrefix: eventData.bucketPrefix,
                  ingestionApiKey: attribution.ingestionApiKey,
                  ingestionSdkName: attribution.ingestionSdkName,
                  ingestionSdkVersion: attribution.ingestionSdkVersion,
                },
                authCheck: authCheck as {
                  validKey: true;
                  scope: {
                    projectId: string;
                    accessLevel: "project" | "scores";
                  };
                },
              },
            },
            { delay: getDelay(delay, source) },
          )
        : Promise.reject("Failed to instantiate ingestion queue");
    }),
  );

  return aggregateBatchResult(
    [...validationErrors, ...authenticationErrors],
    sortedBatch.map((event) => ({ id: event.id, result: event })),
    authCheck.scope.projectId,
  );
};

const isAuthorized = (
  event: IngestionEventType,
  authScope: AuthHeaderValidVerificationResultIngestion,
): boolean => {
  if (event.type === eventTypes.SDK_LOG) {
    return true;
  }

  if (event.type === eventTypes.SCORE_CREATE) {
    return (
      authScope.scope.accessLevel === "scores" ||
      authScope.scope.accessLevel === "project"
    );
  }

  return authScope.scope.accessLevel === "project";
};

async function processDorisEventBatch(
  input: unknown[],
  auth: AuthHeaderValidVerificationResultIngestion,
  options: ProcessEventBatchOptions,
): Promise<AnalyticsEventBatchResult> {
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
  const legacyGroups = new Map<string, IngestionEventType[]>();
  const successes: AnalyticsEventBatchResult["successes"] = [];
  const errors: AnalyticsEventBatchResult["errors"] = [];

  for (const value of input) {
    const rawId =
      typeof value === "object" && value !== null && "id" in value
        ? String(value.id)
        : "unknown";
    const parsed = schema.safeParse(value);
    if (!parsed.success) {
      errors.push({
        id: rawId,
        status: 400,
        message: "Invalid request data",
        error: new InvalidRequestError(parsed.error.message).message,
      });
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
    if (parsed.data.type === eventTypes.DATASET_RUN_ITEM_CREATE) {
      errors.push({
        id: parsed.data.id,
        status: 501,
        ...DORIS_EXPERIMENT_INGESTION_UNAVAILABLE,
      });
      continue;
    }
    const entityId = parsed.data.body.id;
    if (!entityId) {
      errors.push({
        id: parsed.data.id,
        status: 400,
        message: "Invalid request data",
        error: "Tracing event body.id is required",
      });
      continue;
    }
    const entityType =
      parsed.data.type === eventTypes.TRACE_CREATE ? "trace" : "observation";
    const groupKey = `${entityType}\0${entityId}`;
    const group = legacyGroups.get(groupKey);
    if (group) group.push(parsed.data);
    else legacyGroups.set(groupKey, [parsed.data]);
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
    for (const { id } of scores) successes.push({ id, status: 201 });
  }

  for (const [groupKey, events] of legacyGroups) {
    const orderedEvents = sortBatch(events);
    const entityLockId = createHash("sha256")
      .update(
        [
          "langfuse-doris-legacy-entity-v1",
          auth.scope.projectId,
          groupKey,
        ].join("\0"),
      )
      .digest("hex");
    const operationHash = createHash("sha256").update(
      ["langfuse-doris-legacy-v1", auth.scope.projectId, groupKey].join("\0"),
    );
    for (const { id } of orderedEvents) operationHash.update("\0").update(id);
    const operationId = operationHash.digest("hex");
    await acceptAnalyticsIngestion({
      projectId: auth.scope.projectId,
      operationId,
      sourceOperationId: `legacy:${entityLockId}:${operationId}`,
      envelope: {
        formatVersion: 1,
        source: "legacy-event",
        payload: orderedEvents,
        attribution: options.attribution,
      },
      canonicalizerVersion: CURRENT_ANALYTICS_CANONICALIZER_VERSION,
      schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
      storageService: getS3EventStorageClient(
        env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
      ),
      rawPrefix: env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX,
    });
    for (const { id } of orderedEvents) successes.push({ id, status: 201 });
  }

  if (errors.length > 0) {
    logger.warn("Event batch contains rejected events", {
      projectId: auth.scope.projectId,
      rejectedCount: errors.length,
    });
  }
  return { successes, errors };
}

/**
 * Sorts a batch of ingestion events. Orders by: updating events last, sorted by timestamp asc.
 */
const sortBatch = (batch: IngestionEventType[]) => {
  const updateEvents: (typeof eventTypes)[keyof typeof eventTypes][] = [
    eventTypes.GENERATION_UPDATE,
    eventTypes.SPAN_UPDATE,
    eventTypes.OBSERVATION_UPDATE, // legacy event type
  ];
  const updates = batch
    .filter((event) => updateEvents.includes(event.type))
    .sort((a, b) => {
      return new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime();
    });
  const others = batch
    .filter((event) => !updateEvents.includes(event.type))
    .sort((a, b) => {
      return new Date(a.timestamp).getTime() - new Date(b.timestamp).getTime();
    });

  // Return the array with non-update events first, followed by update events
  return [...others, ...updates];
};

export const aggregateBatchResult = (
  errors: Array<{ id: string; error: unknown }>,
  results: Array<{ id: string; result: unknown }>,
  projectId?: string,
) => {
  const returnedErrors: {
    id: string;
    status: number;
    message?: string;
    error?: string;
  }[] = [];

  const successes: {
    id: string;
    status: number;
  }[] = [];

  errors.forEach((error) => {
    if (error.error instanceof InvalidRequestError) {
      returnedErrors.push({
        id: error.id,
        status: 400,
        message: "Invalid request data",
        error: error.error.message,
      });
    } else if (error.error instanceof UnauthorizedError) {
      returnedErrors.push({
        id: error.id,
        status: 401,
        message: "Authentication error",
        error: error.error.message,
      });
    } else if (error.error instanceof LangfuseNotFoundError) {
      returnedErrors.push({
        id: error.id,
        status: 404,
        message: "Resource not found",
        error: error.error.message,
      });
    } else {
      returnedErrors.push({
        id: error.id,
        status: 500,
        error: "Internal Server Error",
      });
    }
  });

  if (returnedErrors.length > 0) {
    logger.warn("Error processing events", {
      errors: returnedErrors,
      "langfuse.project.id": projectId,
    });
  }

  results.forEach((result) => {
    successes.push({
      id: result.id,
      status: 201,
    });
  });

  return { successes, errors: returnedErrors };
};
