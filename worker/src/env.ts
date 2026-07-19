import { removeEmptyEnvVariables } from "@langfuse/shared";
import { langfuseS3EventKeyMaxSegmentBytesSchema } from "@langfuse/shared/src/env";
import { z } from "zod";

const EnvSchema = z.object({
  BUILD_ID: z.string().optional(),
  NODE_ENV: z
    .enum(["development", "test", "production"])
    .default("development"),
  DATABASE_URL: z.string(),
  HOSTNAME: z.string().default("0.0.0.0"),
  PORT: z.coerce
    .number() // ".env files convert numbers to strings, therefore we have to enforce them to be numbers"
    .positive()
    .max(65536, `options.port should be >= 0 and < 65536`)
    .default(3030),

  NEXTAUTH_URL: z.string().optional(),
  NEXT_PUBLIC_BASE_PATH: z.string().optional(),

  NEXT_PUBLIC_LANGFUSE_CLOUD_REGION: z
    .enum(["US", "EU", "STAGING", "DEV", "HIPAA", "JP"])
    .optional(),

  STRIPE_SECRET_KEY: z.string().optional(),

  LANGFUSE_CACHE_AUTOMATIONS_ENABLED: z.enum(["true", "false"]).default("true"),
  LANGFUSE_CACHE_AUTOMATIONS_TTL_SECONDS: z.coerce.number().default(60),
  LANGFUSE_S3_BATCH_EXPORT_ENABLED: z.enum(["true", "false"]).default("false"),
  LANGFUSE_S3_BATCH_EXPORT_BUCKET: z.string().optional(),
  LANGFUSE_S3_BATCH_EXPORT_PREFIX: z.string().default(""),
  LANGFUSE_S3_BATCH_EXPORT_REGION: z.string().optional(),
  LANGFUSE_S3_BATCH_EXPORT_ENDPOINT: z.string().optional(),
  LANGFUSE_S3_BATCH_EXPORT_EXTERNAL_ENDPOINT: z.string().optional(),
  LANGFUSE_S3_BATCH_EXPORT_ACCESS_KEY_ID: z.string().optional(),
  LANGFUSE_S3_BATCH_EXPORT_SECRET_ACCESS_KEY: z.string().optional(),
  LANGFUSE_S3_BATCH_EXPORT_FORCE_PATH_STYLE: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_S3_BATCH_EXPORT_SSE: z.enum(["AES256", "aws:kms"]).optional(),
  LANGFUSE_S3_BATCH_EXPORT_SSE_KMS_KEY_ID: z.string().optional(),

  LANGFUSE_S3_EVENT_UPLOAD_BUCKET: z.string({
    error: "Langfuse requires a bucket name for S3 Event Uploads.",
  }),
  LANGFUSE_S3_EVENT_UPLOAD_PREFIX: z.string().default(""),
  LANGFUSE_S3_EVENT_UPLOAD_REGION: z.string().optional(),
  LANGFUSE_S3_EVENT_UPLOAD_ENDPOINT: z.string().optional(),
  LANGFUSE_S3_EVENT_UPLOAD_ACCESS_KEY_ID: z.string().optional(),
  LANGFUSE_S3_EVENT_UPLOAD_SECRET_ACCESS_KEY: z.string().optional(),
  LANGFUSE_S3_EVENT_UPLOAD_FORCE_PATH_STYLE: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_S3_EVENT_UPLOAD_SSE: z.enum(["AES256", "aws:kms"]).optional(),
  LANGFUSE_S3_EVENT_UPLOAD_SSE_KMS_KEY_ID: z.string().optional(),
  // Validation rules live in `@langfuse/shared/src/env` so producer and
  // consumer agree on what values are accepted. Must match the web container's
  // resolved value at deploy time; otherwise web and worker can write/read
  // different S3 keys for the same id.
  LANGFUSE_S3_EVENT_KEY_MAX_SEGMENT_BYTES:
    langfuseS3EventKeyMaxSegmentBytesSchema,

  EMAIL_FROM_ADDRESS: z.string().optional(),
  SMTP_CONNECTION_URL: z.string().optional(),
  CLOUD_CRM_EMAIL: z.string().optional(),
  LANGFUSE_USE_AZURE_BLOB: z.enum(["true", "false"]).default("false"),
  // Doris credentials are injected per workload. Web never receives load auth.
  DORIS_LOCAL_DEV_MODE: z.enum(["true", "false"]).default("false"),
  DORIS_QUERY_URL: z.string().optional(),
  DORIS_QUERY_USER: z.string().optional(),
  DORIS_QUERY_PASSWORD: z.string().optional(),
  DORIS_QUERY_TLS_ENABLED: z.enum(["true", "false"]).default("false"),
  DORIS_QUERY_TLS_CA_PATH: z.string().optional(),
  DORIS_QUERY_MAX_CONNECTIONS: z.coerce.number().int().positive().default(25),
  DORIS_QUERY_CONNECT_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .positive()
    .default(10_000),
  DORIS_QUERY_TIMEOUT_MS: z.coerce.number().int().positive().default(30_000),
  DORIS_STREAM_LOAD_FE_URL: z.string().optional(),
  DORIS_STREAM_LOAD_USER: z.string().optional(),
  DORIS_STREAM_LOAD_PASSWORD: z.string().optional(),
  DORIS_STREAM_LOAD_DATABASE: z.string().default("langfuse"),
  DORIS_STREAM_LOAD_FE_IP_ALLOWLIST: z.string().optional(),
  DORIS_STREAM_LOAD_BE_ALLOWLIST: z.string().optional(),
  DORIS_STREAM_LOAD_BE_IP_ALLOWLIST: z.string().optional(),
  DORIS_STREAM_LOAD_TLS_CA_PATH: z.string().optional(),
  DORIS_STREAM_LOAD_REQUEST_TIMEOUT_MS: z.coerce
    .number()
    .int()
    .positive()
    .default(30_000),
  DORIS_STREAM_LOAD_MAX_BODY_BYTES: z.coerce
    .number()
    .int()
    .min(100 * 1024 * 1024)
    .default(100 * 1024 * 1024),
  LANGFUSE_ANALYTICS_INGESTION_WORKER_CONCURRENCY: z.coerce
    .number()
    .int()
    .positive()
    .default(4),
  LANGFUSE_ANALYTICS_INGESTION_OUTBOX_INTERVAL_MS: z.coerce
    .number()
    .int()
    .min(100)
    .default(500),
  LANGFUSE_ANALYTICS_INGESTION_OUTBOX_BATCH_SIZE: z.coerce
    .number()
    .int()
    .min(1)
    .max(100)
    .default(100),
  LANGFUSE_ANALYTICS_INGESTION_LEGACY_HANDOFF_ENABLED: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_ANALYTICS_DELETION_RECOVERY_INTERVAL_MS: z.coerce
    .number()
    .int()
    .min(1_000)
    .default(30_000),
  LANGFUSE_ANALYTICS_DELETION_RECOVERY_BATCH_SIZE: z.coerce
    .number()
    .int()
    .min(1)
    .max(1_000)
    .default(100),
  LANGFUSE_EVAL_CREATOR_LIMITER_DURATION: z.coerce
    .number()
    .positive()
    .default(500),
  LANGFUSE_EVAL_CREATOR_WORKER_CONCURRENCY: z.coerce
    .number()
    .positive()
    .default(2),
  LANGFUSE_TRACE_UPSERT_WORKER_CONCURRENCY: z.coerce
    .number()
    .positive()
    .default(25),
  LANGFUSE_TRACE_DELETE_CONCURRENCY: z.coerce.number().positive().default(1),
  LANGFUSE_SCORE_DELETE_CONCURRENCY: z.coerce.number().positive().default(1),
  // Delay (ms) inserted after each Mixpanel flush to throttle analytics exports
  // and avoid overwhelming the target instance (see issue #12786).
  LANGFUSE_MIXPANEL_FLUSH_DELAY_MS: z.coerce.number().min(0).default(100),
  LANGFUSE_DATASET_DELETE_CONCURRENCY: z.coerce.number().positive().default(1),
  LANGFUSE_PROJECT_DELETE_CONCURRENCY: z.coerce.number().positive().default(1),
  LANGFUSE_LLM_AS_JUDGE_QUEUE_RETRY_MAX_ATTEMPTS: z.coerce
    .number()
    .int()
    .min(0)
    .default(4),
  LANGFUSE_LLM_AS_JUDGE_QUEUE_RETRY_MAX_AGE_SECONDS: z.coerce
    .number()
    .int()
    .positive()
    .default(120 * 60),

  // Otel
  OTEL_EXPORTER_OTLP_ENDPOINT: z.string().default("http://localhost:4318"),
  OTEL_SERVICE_NAME: z.string().default("worker"),

  LANGFUSE_ENABLE_BACKGROUND_MIGRATIONS: z
    .enum(["true", "false"])
    .default("true"),

  LANGFUSE_ENABLE_REDIS_SEEN_EVENT_CACHE: z
    .enum(["true", "false"])
    .default("false"),

  LANGFUSE_ENABLE_BLOB_STORAGE_FILE_LOG: z
    .enum(["true", "false"])
    .default("true"),

  // Flags to toggle queue consumers on or off.
  QUEUE_CONSUMER_CLOUD_USAGE_METERING_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_CLOUD_SPEND_ALERT_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_FREE_TIER_USAGE_THRESHOLD_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_TRACE_DELETE_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_SCORE_DELETE_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_DATASET_DELETE_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_PROJECT_DELETE_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_POSTHOG_INTEGRATION_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_MIXPANEL_INTEGRATION_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_DEAD_LETTER_RETRY_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("false"),
  QUEUE_CONSUMER_WEBHOOK_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_ENTITY_CHANGE_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  QUEUE_CONSUMER_NOTIFICATION_QUEUE_IS_ENABLED: z
    .enum(["true", "false"])
    .default("true"),

  LANGFUSE_EXPERIMENT_BACKFILL_THROTTLE_MS: z.coerce
    .number()
    .positive()
    .default(5 * 60 * 1000), // 5 minutes

  // Comma-separated list of project IDs to exclude from experiment backfill processing
  LANGFUSE_EXPERIMENT_BACKFILL_EXCLUDE_PROJECT_IDS: z
    .string()
    .optional()
    .transform((s) => (s ? s.split(",").map((id) => id.trim()) : [])),

  // Comma-separated list of project IDs to exclude from event propagation dual-write
  LANGFUSE_EVENT_PROPAGATION_EXCLUDE_PROJECT_IDS: z
    .string()
    .optional()
    .transform((s) => (s ? s.split(",").map((id) => id.trim()) : [])),

  // Core data S3 upload - Langfuse Cloud
  LANGFUSE_S3_CORE_DATA_EXPORT_IS_ENABLED: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_S3_CORE_DATA_UPLOAD_BUCKET: z.string().optional(),
  LANGFUSE_S3_CORE_DATA_UPLOAD_PREFIX: z.string().default(""),
  LANGFUSE_S3_CORE_DATA_UPLOAD_REGION: z.string().optional(),
  LANGFUSE_S3_CORE_DATA_UPLOAD_ENDPOINT: z.string().optional(),
  LANGFUSE_S3_CORE_DATA_UPLOAD_ACCESS_KEY_ID: z.string().optional(),
  LANGFUSE_S3_CORE_DATA_UPLOAD_SECRET_ACCESS_KEY: z.string().optional(),
  LANGFUSE_S3_CORE_DATA_UPLOAD_FORCE_PATH_STYLE: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_S3_CORE_DATA_UPLOAD_SSE: z.enum(["AES256", "aws:kms"]).optional(),
  LANGFUSE_S3_CORE_DATA_UPLOAD_SSE_KMS_KEY_ID: z.string().optional(),

  // Media upload
  LANGFUSE_S3_MEDIA_UPLOAD_BUCKET: z.string().optional(),
  LANGFUSE_S3_MEDIA_UPLOAD_PREFIX: z.string().default(""),
  LANGFUSE_S3_MEDIA_UPLOAD_REGION: z.string().optional(),
  LANGFUSE_S3_MEDIA_UPLOAD_ENDPOINT: z.string().optional(),
  LANGFUSE_S3_MEDIA_UPLOAD_ACCESS_KEY_ID: z.string().optional(),
  LANGFUSE_S3_MEDIA_UPLOAD_SECRET_ACCESS_KEY: z.string().optional(),
  LANGFUSE_S3_MEDIA_UPLOAD_FORCE_PATH_STYLE: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_S3_MEDIA_UPLOAD_SSE: z.enum(["AES256", "aws:kms"]).optional(),
  LANGFUSE_S3_MEDIA_UPLOAD_SSE_KMS_KEY_ID: z.string().optional(),

  // Metering data Postgres export - Langfuse Cloud
  // When disabled: Usage is still tracked in DB but no emails are sent and no orgs are blocked
  // When enabled: Full enforcement (emails + blocking)
  LANGFUSE_FREE_TIER_USAGE_THRESHOLD_ENFORCEMENT_ENABLED: z
    .enum(["true", "false"])
    .default("false"),

  LANGFUSE_S3_CONCURRENT_READS: z.coerce.number().positive().default(50),
  LANGFUSE_ANALYTICS_PROJECT_DELETION_RATE_LIMIT_WINDOW_MS: z.coerce
    .number()
    .positive()
    .default(600_000), // 10 minutes
  LANGFUSE_ANALYTICS_TRACE_DELETION_RATE_LIMIT_WINDOW_MS: z.coerce
    .number()
    .positive()
    .default(120_000), // 2 minutes
  LANGFUSE_ANALYTICS_DATASET_DELETION_RATE_LIMIT_WINDOW_MS: z.coerce
    .number()
    .positive()
    .default(120_000), // 2 minutes

  // Batch Project Cleaner configuration
  LANGFUSE_BATCH_PROJECT_CLEANER_ENABLED: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_BATCH_PROJECT_CLEANER_CHECK_INTERVAL_MS: z.coerce
    .number()
    .positive()
    .default(600_000), // 10 minutes between checks after successful processing
  LANGFUSE_BATCH_PROJECT_CLEANER_SLEEP_ON_EMPTY_MS: z.coerce
    .number()
    .positive()
    .default(3_600_000), // 1 hour sleep when there is no data to process
  LANGFUSE_BATCH_PROJECT_CLEANER_PROJECT_LIMIT: z.coerce
    .number()
    .positive()
    .default(1000), // Max projects per batch
  LANGFUSE_BATCH_PROJECT_CLEANER_DELETE_TIMEOUT_MS: z.coerce
    .number()
    .positive()
    .default(3_600_000), // 1 hour for DELETE operations

  // Batch Project Media Cleaner configuration (S3/PostgreSQL)
  LANGFUSE_BATCH_PROJECT_MEDIA_CLEANER_BATCH_SIZE: z.coerce
    .number()
    .positive()
    .default(5000), // Media items per chunk

  // Media retention cleaner scheduling.
  LANGFUSE_BATCH_DATA_RETENTION_CLEANER_ENABLED: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_MEDIA_RETENTION_CLEANER_INTERVAL_MS: z.coerce
    .number()
    .positive()
    .default(600_000), // 10 minutes between runs
  // Media Retention Cleaner configuration (S3/PostgreSQL)
  LANGFUSE_MEDIA_RETENTION_CLEANER_ITEM_LIMIT: z.coerce
    .number()
    .positive()
    .default(10_000), // Max items (media files) to process per batch

  // Batch Trace Deletion Cleaner configuration
  LANGFUSE_BATCH_TRACE_DELETION_CLEANER_ENABLED: z
    .enum(["true", "false"])
    .default("false"),
  LANGFUSE_BATCH_TRACE_DELETION_CLEANER_INTERVAL_MS: z.coerce
    .number()
    .positive()
    .default(600_000), // 10 minutes between runs
  LANGFUSE_BATCH_TRACE_DELETION_CLEANER_LOCK_TTL_SECONDS: z.coerce
    .number()
    .positive()
    .default(7200), // 2 hours to handle worst-case deletions
  LANGFUSE_TRACE_DELETE_BATCH_ACTION_RUNNER_ENABLED: z
    .enum(["true", "false"])
    .default("true"),
  LANGFUSE_TRACE_DELETE_BATCH_ACTION_RUNNER_INTERVAL_MS: z.coerce
    .number()
    .positive()
    .default(10_000),
  LANGFUSE_TRACE_DELETE_BATCH_ACTION_RUNNER_LOCK_TTL_SECONDS: z.coerce
    .number()
    .positive()
    .default(1_800),
  LANGFUSE_TRACE_DELETE_BATCH_ACTION_RUNNER_MAX_BATCHES_PER_RUN: z.coerce
    .number()
    .positive()
    .default(5),

  LANGFUSE_WEBHOOK_QUEUE_PROCESSING_CONCURRENCY: z.coerce
    .number()
    .positive()
    .default(5),
  LANGFUSE_WEBHOOK_TIMEOUT_MS: z.coerce.number().positive().default(10000),
  LANGFUSE_WEBHOOK_MAX_REDIRECTS: z.coerce.number().positive().default(10),
  LANGFUSE_ENTITY_CHANGE_QUEUE_PROCESSING_CONCURRENCY: z.coerce
    .number()
    .positive()
    .default(2),
  LANGFUSE_MONITOR_QUEUE_PROCESSING_CONCURRENCY: z.coerce
    .number()
    .positive()
    .default(10),
  LANGFUSE_DELETE_BATCH_SIZE: z.coerce.number().positive().default(2000),
  LANGFUSE_TOKEN_COUNT_WORKER_POOL_SIZE: z.coerce
    .number()
    .positive()
    .default(2),
  LANGFUSE_QUEUE_METRICS_SAMPLE_RATE: z.coerce
    .number()
    .min(0)
    .max(1)
    .default(0.3), // Probability for recording sharded queue depth metrics
  LANGFUSE_QUEUE_METRICS_INTERVAL_MS: z.coerce.number().min(100).default(1000),
  LANGFUSE_QUEUE_METRICS_ENABLED: z.enum(["true", "false"]).default("true"),
});

type ParsedEnv = z.infer<typeof EnvSchema>;

// Compatibility export for Enterprise retention code outside the Community
// Doris delivery boundary. Community runtime is always canonical-events only.
export const v4WritesToEventsTable = (_envValue: ParsedEnv): boolean => true;

const parseEnv = (): ParsedEnv => {
  return EnvSchema.parse(removeEmptyEnvVariables(process.env));
};

export const env: ParsedEnv =
  process.env.DOCKER_BUILD === "1" // eslint-disable-line turbo/no-undeclared-env-vars
    ? (process.env as any)
    : parseEnv();
