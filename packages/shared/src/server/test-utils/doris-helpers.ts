import { randomUUID } from "node:crypto";

import { DorisStreamLoadClient } from "../doris";
import type {
  EventRecordInsertType,
  DatasetRunItemRecordInsertType,
  ObservationRecordInsertType,
  ScoreRecordInsertType,
  TraceRecordInsertType,
} from "../repositories/definitions";

type DorisFixtureRow = Readonly<Record<string, unknown>>;

function assertFixtureEnvironment(): void {
  if (process.env.NODE_ENV === "production") {
    throw new Error("Doris test fixtures cannot run in production");
  }
}

function testStreamLoadClient(): DorisStreamLoadClient {
  assertFixtureEnvironment();
  const origin =
    process.env.DORIS_TEST_STREAM_LOAD_BE_URL ?? "http://127.0.0.1:8041";
  return new DorisStreamLoadClient({
    feOrigin: origin,
    database: process.env.DORIS_STREAM_LOAD_DATABASE ?? "langfuse",
    user: process.env.DORIS_STREAM_LOAD_USER ?? "root",
    password: process.env.DORIS_STREAM_LOAD_PASSWORD ?? "",
    requireTls: origin.startsWith("https://"),
    allowedRedirectOrigins: [],
  });
}

function label(table: string): string {
  return `test_${table}_${randomUUID().replaceAll("-", "")}`;
}

async function load(table: string, rows: readonly DorisFixtureRow[]) {
  const activeRows = rows.flatMap(({ is_deleted: isDeleted, ...row }) =>
    isDeleted === 1 ? [] : [row],
  );
  if (activeRows.length === 0) return;
  const result = await testStreamLoadClient().load({
    table,
    label: label(table),
    ndjsonBody: `${activeRows.map((row) => JSON.stringify(row)).join("\n")}\n`,
  });
  if (
    !result.committed ||
    result.numberTotalRows !== activeRows.length ||
    result.numberFilteredRows !== 0
  ) {
    throw new Error("Doris test fixture Stream Load was not fully committed");
  }
}

function dateTimeFromMilliseconds(milliseconds: number): string {
  if (!Number.isFinite(milliseconds)) {
    throw new TypeError("Invalid millisecond timestamp in Doris test fixture");
  }
  return new Date(milliseconds)
    .toISOString()
    .replace("T", " ")
    .replace("Z", "000");
}

function dateTimeFromMicroseconds(microseconds: number): string {
  if (!Number.isSafeInteger(microseconds)) {
    throw new TypeError("Invalid microsecond timestamp in Doris test fixture");
  }
  const wholeMilliseconds = Math.floor(microseconds / 1_000);
  const remainingMicros = microseconds - wholeMilliseconds * 1_000;
  const prefix = new Date(wholeMilliseconds)
    .toISOString()
    .slice(0, 23)
    .replace("T", " ");
  return `${prefix}${String(remainingMicros).padStart(3, "0")}`;
}

function dateFromMilliseconds(milliseconds: number): string {
  return new Date(milliseconds).toISOString().slice(0, 10);
}

function dateFromMicroseconds(microseconds: number): string {
  return new Date(Math.floor(microseconds / 1_000)).toISOString().slice(0, 10);
}

function versionFromMilliseconds(milliseconds: number): string {
  return (BigInt(Math.trunc(milliseconds)) * 1_000_000n).toString();
}

function versionFromMicroseconds(microseconds: number): string {
  return (BigInt(microseconds) * 1_000n).toString();
}

function parseObject(
  value: string | Record<string, unknown> | null | undefined,
) {
  if (value == null) return {};
  if (typeof value !== "string") return value;
  try {
    const parsed = JSON.parse(value) as unknown;
    return typeof parsed === "object" &&
      parsed !== null &&
      !Array.isArray(parsed)
      ? parsed
      : {};
  } catch {
    return {};
  }
}

function eventMetadata(event: EventRecordInsertType): Record<string, string> {
  return Object.fromEntries(
    event.metadata_names.flatMap((name, index) => {
      const value = event.metadata_values[index];
      return value == null ? [] : [[name, value]];
    }),
  );
}

function sharedEventColumns(input: {
  projectId: string;
  traceId: string;
  spanId: string;
  parentSpanId: string | null | undefined;
  type: string;
  name: string | null | undefined;
  environment: string;
  userId: string | null | undefined;
  sessionId: string | null | undefined;
  level: string | null | undefined;
  isAppRoot: boolean;
  bookmarked: boolean | undefined;
  public: boolean | undefined;
  release: string | null | undefined;
  version: string | null | undefined;
  traceName: string | null | undefined;
  startTime: number;
  endTime: number | null | undefined;
  completionStartTime: number | null | undefined;
  createdAt: number;
  updatedAt: number;
  microseconds: boolean;
  providedModelName: string | null | undefined;
  internalModelId: string | null | undefined;
  promptId: string | null | undefined;
  promptName: string | null | undefined;
  promptVersion: number | null | undefined;
  tags: readonly string[];
  metadata: Readonly<Record<string, unknown>>;
  providedUsageDetails: Readonly<Record<string, number>>;
  usageDetails: Readonly<Record<string, number>>;
  providedCostDetails: Readonly<Record<string, number>>;
  costDetails: Readonly<Record<string, number>>;
  totalCost: number | null | undefined;
  modelParameters: string | Record<string, unknown> | null | undefined;
  toolDefinitions: Readonly<Record<string, string>>;
  toolCalls: readonly string[];
  toolCallNames: readonly string[];
  input: string | null | undefined;
  output: string | null | undefined;
  source: string;
  ingestionSdkName: string;
  ingestionSdkVersion: string;
  serviceName: string | null | undefined;
  telemetrySdkLanguage: string | null | undefined;
  eventBytes: number | null | undefined;
  isDeleted: number;
}): DorisFixtureRow {
  const dateTime = input.microseconds
    ? dateTimeFromMicroseconds
    : dateTimeFromMilliseconds;
  const version = input.microseconds
    ? versionFromMicroseconds
    : versionFromMilliseconds;
  const partitionDate = input.microseconds
    ? dateFromMicroseconds(input.startTime)
    : dateFromMilliseconds(input.startTime);
  return {
    project_id: input.projectId,
    partition_date: partitionDate,
    trace_id: input.traceId,
    span_id: input.spanId,
    version_token: version(input.updatedAt),
    parent_span_id: input.parentSpanId ?? null,
    type: input.type,
    name: input.name ?? null,
    environment: input.environment,
    user_id: input.userId ?? null,
    session_id: input.sessionId ?? null,
    level: input.level ?? "DEFAULT",
    status_message: null,
    is_app_root: input.isAppRoot,
    bookmarked: input.bookmarked ?? false,
    public: input.public ?? false,
    release: input.release ?? null,
    version: input.version ?? null,
    trace_name: input.traceName ?? null,
    start_time: dateTime(input.startTime),
    end_time: input.endTime == null ? null : dateTime(input.endTime),
    completion_start_time:
      input.completionStartTime == null
        ? null
        : dateTime(input.completionStartTime),
    created_at: dateTime(input.createdAt),
    updated_at: dateTime(input.updatedAt),
    provided_model_name: input.providedModelName ?? null,
    internal_model_id: input.internalModelId ?? null,
    prompt_id: input.promptId ?? null,
    prompt_name: input.promptName ?? null,
    prompt_version: input.promptVersion ?? null,
    total_input_tokens: input.usageDetails.input ?? null,
    total_output_tokens: input.usageDetails.output ?? null,
    total_cost: input.totalCost ?? null,
    tags: [...input.tags],
    metadata: input.metadata,
    usage_details: input.usageDetails,
    cost_details: input.costDetails,
    provided_usage_details: input.providedUsageDetails,
    provided_cost_details: input.providedCostDetails,
    model_parameters: parseObject(input.modelParameters),
    tool_definitions: input.toolDefinitions,
    tool_calls: [...input.toolCalls],
    tool_call_names: [...input.toolCallNames],
    input: input.input ?? null,
    output: input.output ?? null,
    input_preview: input.input?.slice(0, 200) ?? null,
    output_preview: input.output?.slice(0, 200) ?? null,
    source: input.source,
    ingestion_sdk_name: input.ingestionSdkName || "unknown",
    ingestion_sdk_version: input.ingestionSdkVersion || "unknown",
    service_name: input.serviceName ?? null,
    telemetry_sdk_language: input.telemetrySdkLanguage ?? null,
    blob_storage_file_path: null,
    event_bytes: input.eventBytes ?? null,
    is_deleted: input.isDeleted,
  };
}

export async function createTracesDoris(
  traces: readonly TraceRecordInsertType[],
) {
  return load(
    "events_current",
    traces.map((trace) =>
      sharedEventColumns({
        projectId: trace.project_id,
        traceId: trace.id,
        spanId: `trace-${trace.id}`,
        parentSpanId: null,
        type: "SPAN",
        name: trace.name,
        environment: trace.environment,
        userId: trace.user_id,
        sessionId: trace.session_id,
        level: "DEFAULT",
        isAppRoot: true,
        bookmarked: trace.bookmarked,
        public: trace.public,
        release: trace.release,
        version: trace.version,
        traceName: trace.name,
        startTime: trace.timestamp,
        endTime: null,
        completionStartTime: null,
        createdAt: trace.created_at,
        updatedAt: trace.updated_at,
        microseconds: false,
        providedModelName: null,
        internalModelId: null,
        promptId: null,
        promptName: null,
        promptVersion: null,
        tags: trace.tags,
        metadata: trace.metadata,
        providedUsageDetails: {},
        usageDetails: {},
        providedCostDetails: {},
        costDetails: {},
        totalCost: null,
        modelParameters: null,
        toolDefinitions: {},
        toolCalls: [],
        toolCallNames: [],
        input: trace.input,
        output: trace.output,
        source: "test-fixture",
        ingestionSdkName: "test",
        ingestionSdkVersion: "test",
        serviceName: null,
        telemetrySdkLanguage: null,
        eventBytes: 0,
        isDeleted: trace.is_deleted,
      }),
    ),
  );
}

export async function createObservationsDoris(
  observations: readonly ObservationRecordInsertType[],
) {
  return load(
    "events_current",
    observations.map((observation) =>
      sharedEventColumns({
        projectId: observation.project_id,
        traceId: observation.trace_id ?? observation.id,
        spanId: observation.id,
        parentSpanId: observation.parent_observation_id,
        type: observation.type,
        name: observation.name,
        environment: observation.environment,
        userId: null,
        sessionId: null,
        level: observation.level,
        isAppRoot: false,
        bookmarked: false,
        public: false,
        release: null,
        version: observation.version,
        traceName: null,
        startTime: observation.start_time,
        endTime: observation.end_time,
        completionStartTime: observation.completion_start_time,
        createdAt: observation.created_at,
        updatedAt: observation.updated_at,
        microseconds: false,
        providedModelName: observation.provided_model_name,
        internalModelId: observation.internal_model_id,
        promptId: observation.prompt_id,
        promptName: observation.prompt_name,
        promptVersion: observation.prompt_version,
        tags: [],
        metadata: observation.metadata,
        providedUsageDetails: observation.provided_usage_details,
        usageDetails: observation.usage_details,
        providedCostDetails: observation.provided_cost_details,
        costDetails: observation.cost_details,
        totalCost: observation.total_cost,
        modelParameters: observation.model_parameters,
        toolDefinitions: observation.tool_definitions ?? {},
        toolCalls: observation.tool_calls ?? [],
        toolCallNames: observation.tool_call_names ?? [],
        input: observation.input,
        output: observation.output,
        source: "test-fixture",
        ingestionSdkName: "test",
        ingestionSdkVersion: "test",
        serviceName: null,
        telemetrySdkLanguage: null,
        eventBytes: 0,
        isDeleted: observation.is_deleted,
      }),
    ),
  );
}

export async function createEventsDoris(
  events: readonly EventRecordInsertType[],
) {
  return load(
    "events_current",
    events.map((event) =>
      sharedEventColumns({
        projectId: event.project_id,
        traceId: event.trace_id,
        spanId: event.span_id,
        parentSpanId: event.parent_span_id,
        type: event.type,
        name: event.name,
        environment: event.environment,
        userId: event.user_id,
        sessionId: event.session_id,
        level: event.level,
        isAppRoot: event.is_app_root,
        bookmarked: event.bookmarked,
        public: event.public,
        release: event.release,
        version: event.version,
        traceName: event.trace_name,
        startTime: event.start_time,
        endTime: event.end_time,
        completionStartTime: event.completion_start_time,
        createdAt: event.created_at,
        updatedAt: event.updated_at,
        microseconds: true,
        providedModelName: event.provided_model_name,
        internalModelId: event.model_id,
        promptId: event.prompt_id,
        promptName: event.prompt_name,
        promptVersion: event.prompt_version,
        tags: event.tags,
        metadata: eventMetadata(event),
        providedUsageDetails: event.provided_usage_details,
        usageDetails: event.usage_details,
        providedCostDetails: event.provided_cost_details,
        costDetails: event.cost_details,
        totalCost:
          event.cost_details.total ?? event.provided_cost_details.total ?? null,
        modelParameters: event.model_parameters,
        toolDefinitions: event.tool_definitions,
        toolCalls: event.tool_calls,
        toolCallNames: event.tool_call_names,
        input: event.input,
        output: event.output,
        source: event.source,
        ingestionSdkName: event.ingestion_sdk_name,
        ingestionSdkVersion: event.ingestion_sdk_version,
        serviceName: event.service_name,
        telemetrySdkLanguage: event.telemetry_sdk_language,
        eventBytes: event.event_bytes,
        isDeleted: event.is_deleted,
      }),
    ),
  );
}

export async function createScoresDoris(
  scores: readonly ScoreRecordInsertType[],
) {
  return load(
    "scores_current",
    scores.map((score) => ({
      project_id: score.project_id,
      score_date: dateFromMilliseconds(score.timestamp),
      score_id: score.id,
      version_token: versionFromMilliseconds(score.updated_at),
      trace_id: score.trace_id ?? null,
      observation_id: score.observation_id ?? null,
      session_id: score.session_id ?? null,
      name: score.name,
      source: score.source,
      data_type: score.data_type,
      value:
        score.data_type === "NUMERIC" || score.data_type === "BOOLEAN"
          ? score.value
          : null,
      string_value: score.string_value ?? null,
      long_string_value: score.long_string_value || null,
      boolean_value: score.data_type === "BOOLEAN" ? score.value === 1 : null,
      comment: score.comment ?? null,
      author_user_id: score.author_user_id ?? null,
      config_id: score.config_id ?? null,
      queue_id: score.queue_id ?? null,
      environment: score.environment,
      metadata: score.metadata,
      timestamp: dateTimeFromMilliseconds(score.timestamp),
      created_at: dateTimeFromMilliseconds(score.created_at),
      updated_at: dateTimeFromMilliseconds(score.updated_at),
      is_deleted: score.is_deleted,
    })),
  );
}

/** Dataset-run analytics is intentionally unavailable in the R1A Doris cut. */
export async function createDatasetRunItemsDoris(
  _items: readonly DatasetRunItemRecordInsertType[],
): Promise<never> {
  throw new Error("Dataset-run analytics requires the R1B Doris schema");
}
