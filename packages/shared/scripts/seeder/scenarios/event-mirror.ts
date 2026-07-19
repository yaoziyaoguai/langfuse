import {
  createEvent,
  EventRecordInsertType,
  ObservationRecordInsertType,
  TraceRecordInsertType,
} from "../../../src/server";

const toMicros = (ms: number | null | undefined): number | null =>
  ms === null || ms === undefined ? null : ms * 1000;

const utf8Bytes = (value: unknown): number =>
  typeof value === "string" ? Buffer.byteLength(value, "utf8") : 0;

const sortedMetadata = (metadata: Record<string, string>) => {
  const names = Object.keys(metadata).sort();
  return { names, values: names.map((name) => metadata[name]) };
};

export const observationToEvent = (
  observation: ObservationRecordInsertType,
  trace: TraceRecordInsertType,
): EventRecordInsertType => {
  const isRoot = !observation.parent_observation_id;
  // The backfill merges trace metadata into observation events
  // (mapConcat(o.metadata, t.metadata)); mirror that with the observation
  // taking precedence on key collisions.
  const { names, values } = sortedMetadata({
    ...((trace.metadata ?? {}) as Record<string, string>),
    ...((observation.metadata ?? {}) as Record<string, string>),
  });

  return createEvent({
    project_id: observation.project_id,
    trace_id: observation.trace_id ?? trace.id,
    span_id: observation.id,
    id: observation.id,
    parent_span_id: observation.parent_observation_id ?? null,
    is_app_root: isRoot,
    name: observation.name ?? "",
    trace_name: trace.name ?? "",
    public: trace.public ?? false,
    bookmarked: (trace.bookmarked ?? false) && isRoot,
    type: observation.type,
    environment: observation.environment ?? "default",
    level: observation.level ?? "DEFAULT",
    status_message: observation.status_message ?? null,
    // backfill: coalesce(o.version, t.version) — keeps the trace-level
    // release !== version shape visible on observation events
    version: observation.version ?? trace.version ?? null,
    release: trace.release ?? null,
    tags: trace.tags ?? [],
    user_id: trace.user_id ?? null,
    session_id: trace.session_id ?? null,
    input:
      typeof observation.input === "string"
        ? observation.input
        : isRoot
          ? trace.input
          : "",
    output:
      typeof observation.output === "string"
        ? observation.output
        : isRoot
          ? trace.output
          : "",
    provided_model_name: observation.provided_model_name ?? null,
    model_id: observation.internal_model_id ?? null,
    model_parameters: observation.model_parameters ?? "{}",
    provided_usage_details: observation.provided_usage_details ?? {},
    usage_details: observation.usage_details ?? {},
    provided_cost_details: observation.provided_cost_details ?? {},
    cost_details: observation.cost_details ?? {},
    prompt_id: observation.prompt_id ?? null,
    prompt_name: observation.prompt_name ?? null,
    prompt_version: observation.prompt_version ?? null,
    metadata_names: names,
    metadata_values: values,
    start_time: toMicros(observation.start_time) ?? Date.now() * 1000,
    end_time: toMicros(observation.end_time),
    completion_start_time: toMicros(observation.completion_start_time),
    created_at: toMicros(observation.created_at) ?? Date.now() * 1000,
    updated_at: toMicros(observation.updated_at) ?? Date.now() * 1000,
    event_ts: toMicros(observation.event_ts) ?? Date.now() * 1000,
    event_bytes: utf8Bytes(observation.input) + utf8Bytes(observation.output),
    source: "API",
  });
};
