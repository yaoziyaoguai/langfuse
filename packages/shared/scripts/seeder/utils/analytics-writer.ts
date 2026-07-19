import { createHash } from "node:crypto";

import { env } from "../../../src/env";
import {
  acceptAnalyticsIngestion,
  CURRENT_ANALYTICS_CANONICALIZER_VERSION,
  CURRENT_ANALYTICS_SCHEMA_VERSION,
  eventTypes,
  getAnalyticsIngestionStatusForProject,
  getS3EventStorageClient,
  type EventRecordInsertType,
  type ScoreRecordInsertType,
} from "../../../src/server";
import { SeedError } from "../scenarios/types";

const SUCCESS = new Set(["VISIBLE"]);
const TERMINAL_FAILURE = new Set([
  "PARTIAL_FAILED",
  "QUARANTINED",
  "UNRECOVERABLE",
  "CANCELLED_BY_DELETION",
  "COMPLETED_WITH_CANCELLATIONS",
]);
const WAIT_TIMEOUT_MS = 90_000;

const isoFromMicros = (value: number | null | undefined): string | null => {
  if (value === null || value === undefined) return null;
  const millis = Math.floor(value / 1_000);
  const micros = Math.floor(value % 1_000_000);
  const date = new Date(millis);
  if (!Number.isFinite(date.getTime())) {
    throw new SeedError(`Invalid fixture timestamp: ${value}`);
  }
  return `${date.toISOString().slice(0, 19)}.${String(micros).padStart(6, "0")}Z`;
};

const isoFromMillis = (value: number): string => {
  const date = new Date(value);
  if (!Number.isFinite(date.getTime())) {
    throw new SeedError(`Invalid fixture timestamp: ${value}`);
  }
  return date.toISOString();
};

const stableAcceptanceTime = (): Date => {
  const now = new Date();
  return new Date(
    Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()),
  );
};

const stableOperationId = (
  projectId: string,
  source: string,
  payload: unknown,
  acceptedAt: Date,
): string =>
  `seed-${createHash("sha256")
    .update(
      JSON.stringify({
        projectId,
        source,
        payload,
        acceptedAt: acceptedAt.toISOString(),
      }),
      "utf8",
    )
    .digest("hex")}`;

const waitUntilVisible = async (
  projectId: string,
  operationId: string,
): Promise<void> => {
  const deadline = Date.now() + WAIT_TIMEOUT_MS;
  while (Date.now() < deadline) {
    const operation = await getAnalyticsIngestionStatusForProject({
      projectId,
      operationId,
    });
    if (!operation) {
      throw new SeedError(`Ingestion operation ${operationId} disappeared`);
    }
    if (SUCCESS.has(operation.status)) return;
    if (TERMINAL_FAILURE.has(operation.status)) {
      const errorCode =
        operation.reasonCode ??
        operation.loads.find((load) => load.lastErrorCode)?.lastErrorCode;
      throw new SeedError(
        `Ingestion operation ${operationId} ended in ${operation.status}`,
        errorCode ? `worker reported ${errorCode}` : undefined,
      );
    }
    await new Promise((resolve) => setTimeout(resolve, 250));
  }
  throw new SeedError(
    `Timed out waiting for ingestion operation ${operationId}`,
    "start the worker and confirm the analytics ingestion queue is healthy",
  );
};

const metadataRecord = (event: EventRecordInsertType) =>
  Object.fromEntries(
    event.metadata_names.map((name, index) => [
      name,
      event.metadata_values[index] ?? "",
    ]),
  );

const eventPayload = (event: EventRecordInsertType) => {
  const startTimeISO = isoFromMicros(event.start_time);
  const envelopeTimestamp = isoFromMicros(event.event_ts);
  if (!startTimeISO || !envelopeTimestamp) {
    throw new SeedError(`Event fixture ${event.span_id} has no source time`);
  }
  return {
    envelopeTimestamp,
    eventData: {
      projectId: event.project_id,
      traceId: event.trace_id,
      spanId: event.span_id,
      parentSpanId: event.parent_span_id ?? null,
      startTimeISO,
      endTimeISO: isoFromMicros(event.end_time),
      completionStartTime: isoFromMicros(event.completion_start_time),
      name: event.name,
      type: event.type,
      environment: event.environment,
      version: event.version ?? null,
      release: event.release ?? null,
      traceName: event.trace_name ?? null,
      userId: event.user_id ?? null,
      sessionId: event.session_id ?? null,
      level: event.level,
      statusMessage: event.status_message ?? null,
      isAppRoot: event.is_app_root,
      bookmarked: event.bookmarked ?? false,
      public: event.public ?? false,
      tags: event.tags,
      input: event.input,
      output: event.output,
      metadata: metadataRecord(event),
      modelName: event.provided_model_name ?? null,
      modelParameters: event.model_parameters ?? {},
      providedUsageDetails: event.provided_usage_details,
      usageDetails: event.usage_details,
      providedCostDetails: event.provided_cost_details,
      costDetails: event.cost_details,
      promptName: event.prompt_name ?? null,
      promptVersion: event.prompt_version ?? null,
      toolDefinitions: event.tool_definitions,
      toolCalls: event.tool_calls,
      toolCallNames: event.tool_call_names,
      source: event.source,
      ingestionSdkName: event.ingestion_sdk_name,
      ingestionSdkVersion: event.ingestion_sdk_version,
      serviceName: event.service_name ?? null,
      telemetrySdkLanguage: event.telemetry_sdk_language ?? null,
      eventBytes: event.event_bytes,
    },
  };
};

const scoreValue = (score: ScoreRecordInsertType) => {
  switch (score.data_type) {
    case "BOOLEAN":
      return score.value !== 0;
    case "CATEGORICAL":
    case "TEXT":
      return score.string_value ?? "";
    case "CORRECTION":
      return score.long_string_value;
    default:
      return score.value;
  }
};

const scorePayload = (score: ScoreRecordInsertType) => ({
  id: `seed-event-${score.id}`,
  type: eventTypes.SCORE_CREATE,
  timestamp: isoFromMillis(score.timestamp),
  body: {
    id: score.id,
    name: score.name,
    traceId: score.trace_id ?? null,
    observationId: score.observation_id ?? null,
    sessionId: score.session_id ?? null,
    environment: score.environment,
    value: scoreValue(score),
    dataType: score.data_type,
    source: score.source,
    comment: score.comment ?? null,
    metadata: score.metadata,
    configId: score.config_id ?? null,
    queueId: score.queue_id ?? null,
  },
});

const acceptAndWait = async (input: {
  projectId: string;
  source: "internal-event" | "score";
  payload: unknown;
}): Promise<void> => {
  const acceptedAt = stableAcceptanceTime();
  const operationId = stableOperationId(
    input.projectId,
    input.source,
    input.payload,
    acceptedAt,
  );
  await acceptAnalyticsIngestion({
    projectId: input.projectId,
    operationId,
    sourceOperationId: operationId,
    acceptedAt,
    acceptedAtNanos: BigInt(acceptedAt.getTime()) * 1_000_000n,
    envelope: {
      formatVersion: 1,
      source: input.source,
      payload: input.payload,
      attribution: {
        ingestionApiKey: "internal-seeder",
        ingestionSdkName: "langfuse-seeder",
        ingestionSdkVersion: "doris-r1a",
      },
    },
    canonicalizerVersion: CURRENT_ANALYTICS_CANONICALIZER_VERSION,
    schemaVersion: CURRENT_ANALYTICS_SCHEMA_VERSION,
    storageService: getS3EventStorageClient(
      env.LANGFUSE_S3_EVENT_UPLOAD_BUCKET,
    ),
    rawPrefix: env.LANGFUSE_S3_EVENT_UPLOAD_PREFIX,
  });
  await waitUntilVisible(input.projectId, operationId);
};

export async function seedEventFixtures(
  events: readonly EventRecordInsertType[],
): Promise<void> {
  if (events.length === 0) return;
  const projects = new Set(events.map((event) => event.project_id));
  if (projects.size !== 1) {
    throw new SeedError("A fixture batch must contain exactly one project");
  }
  await acceptAndWait({
    projectId: events[0]!.project_id,
    source: "internal-event",
    payload: events.map(eventPayload),
  });
}

export async function seedScoreFixtures(
  scores: readonly ScoreRecordInsertType[],
): Promise<void> {
  if (scores.length === 0) return;
  const projects = new Set(scores.map((score) => score.project_id));
  if (projects.size !== 1) {
    throw new SeedError("A fixture batch must contain exactly one project");
  }
  await acceptAndWait({
    projectId: scores[0]!.project_id,
    source: "score",
    payload: scores.map(scorePayload),
  });
}
