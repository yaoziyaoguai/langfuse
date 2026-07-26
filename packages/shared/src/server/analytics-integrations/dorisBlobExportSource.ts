import { prisma } from "../../db";
import type { ObservationFieldGroupFull } from "../../domain/observation-field-groups";
import type { AnalyticsIntegrationExecutionManifestItem } from "../repositories/analyticsIntegrationDeliveries";
import {
  getDorisTelemetryRepositories,
  type DorisObservation,
  type DorisScoreWithTraceContext,
  type DorisTrace,
} from "../repositories/telemetry/doris";

export type DorisBlobExportTable =
  | "traces"
  | "observations"
  | "observations_v2"
  | "scores";

export type DorisBlobExportRecord = {
  readonly item: AnalyticsIntegrationExecutionManifestItem;
  readonly table: DorisBlobExportTable;
  readonly row: Readonly<Record<string, unknown>>;
};

export type DorisBlobExactReadResult = {
  readonly records: readonly DorisBlobExportRecord[];
  readonly missing: readonly AnalyticsIntegrationExecutionManifestItem[];
};

type DorisBlobRepositories = {
  readonly traces: {
    getMany(input: {
      readonly projectId: string;
      readonly traceIds: readonly string[];
    }): Promise<readonly DorisTrace[]>;
  };
  readonly observations: {
    get(input: {
      readonly projectId: string;
      readonly observationId: string;
    }): Promise<DorisObservation | null>;
  };
  readonly scores: {
    get(input: {
      readonly projectId: string;
      readonly scoreId: string;
    }): Promise<DorisScoreWithTraceContext | null>;
  };
};

const OBSERVATION_GROUP_FIELDS = {
  core: [
    "id",
    "trace_id",
    "project_id",
    "type",
    "parent_observation_id",
    "start_time",
    "end_time",
  ],
  basic: [
    "environment",
    "name",
    "level",
    "status_message",
    "version",
    "bookmarked",
    "public",
    "user_id",
    "session_id",
  ],
  time: ["completion_start_time", "created_at", "updated_at"],
  io: ["input", "output"],
  metadata: ["metadata"],
  model: [
    "provided_model_name",
    "model_id",
    "model_parameters",
    "input_price",
    "output_price",
    "total_price",
  ],
  usage: [
    "provided_usage_details",
    "usage_details",
    "provided_cost_details",
    "cost_details",
    "total_cost",
    "usage_pricing_tier_name",
  ],
  prompt: ["prompt_id", "prompt_name", "prompt_version"],
  metrics: ["latency", "time_to_first_token"],
  tools: ["tool_definitions", "tool_calls", "tool_call_names"],
  trace_context: ["trace_name", "release", "tags"],
} as const satisfies Record<ObservationFieldGroupFull, readonly string[]>;

function projectObservation(
  observation: DorisObservation,
  fieldGroups: readonly ObservationFieldGroupFull[],
): Readonly<Record<string, unknown>> {
  const row: Readonly<Record<string, unknown>> = {
    id: observation.id,
    trace_id: observation.traceId,
    project_id: observation.projectId,
    environment: observation.environment,
    type: observation.type,
    parent_observation_id: observation.parentObservationId,
    start_time: observation.startTime,
    end_time: observation.endTime,
    name: observation.name,
    metadata: observation.metadata ?? {},
    level: observation.level,
    status_message: observation.statusMessage,
    version: observation.version,
    bookmarked: observation.bookmarked,
    public: observation.public,
    user_id: observation.userId,
    session_id: observation.sessionId,
    input: observation.input ?? null,
    output: observation.output ?? null,
    provided_model_name: observation.providedModelName,
    model_id: observation.internalModelId,
    model_parameters: observation.modelParameters ?? {},
    provided_usage_details: observation.providedUsageDetails,
    usage_details: observation.usageDetails,
    provided_cost_details: observation.providedCostDetails,
    cost_details: observation.costDetails,
    completion_start_time: observation.completionStartTime,
    prompt_id: observation.promptId,
    prompt_name: observation.promptName,
    prompt_version: observation.promptVersion,
    total_cost: observation.totalCost,
    latency: observation.latency,
    time_to_first_token: observation.timeToFirstToken,
    created_at: observation.createdAt,
    updated_at: observation.updatedAt,
    tool_calls: observation.toolCalls ?? [],
    tool_call_names: observation.toolCallNames ?? [],
    tool_definitions: observation.toolDefinitions ?? {},
    usage_pricing_tier_name: null,
    input_price: null,
    output_price: null,
    total_price: null,
    trace_name: observation.traceName,
    release: observation.release,
    tags: observation.tags,
  };
  const selected = new Set<string>([
    ...OBSERVATION_GROUP_FIELDS.core,
    ...fieldGroups.flatMap((group) => OBSERVATION_GROUP_FIELDS[group]),
  ]);
  return Object.fromEntries(
    Object.entries(row).filter(([field]) => selected.has(field)),
  );
}

function traceRow(
  trace: DorisTrace,
  control:
    | { readonly bookmarked: boolean; readonly public: boolean }
    | undefined,
): Readonly<Record<string, unknown>> {
  return {
    id: trace.id,
    timestamp: trace.timestamp,
    name: trace.name,
    environment: trace.environment,
    project_id: trace.projectId,
    metadata: trace.metadata ?? {},
    user_id: trace.userId,
    session_id: trace.sessionId,
    release: trace.release,
    version: trace.version,
    public: control?.public ?? false,
    bookmarked: control?.bookmarked ?? false,
    tags: trace.tags,
    input: trace.input ?? null,
    output: trace.output ?? null,
    created_at: trace.timestamp,
    updated_at: trace.endTime,
  };
}

function scoreRow(
  score: DorisScoreWithTraceContext,
): Readonly<Record<string, unknown>> {
  return {
    id: score.id,
    timestamp: score.timestamp,
    project_id: score.projectId,
    environment: score.environment,
    trace_id: score.traceId,
    observation_id: score.observationId,
    session_id: score.sessionId,
    dataset_run_id: score.datasetRunId,
    name: score.name,
    value: score.value,
    source: score.source,
    comment: score.comment,
    data_type: score.dataType,
    string_value: score.stringValue,
    created_at: score.createdAt,
    updated_at: score.updatedAt,
  };
}

export class DorisBlobAnalyticsExportSource {
  constructor(
    private readonly dependencies: {
      readonly repositories: DorisBlobRepositories;
      readonly findTraceControls: (input: {
        readonly projectId: string;
        readonly traceIds: readonly string[];
      }) => Promise<
        readonly {
          readonly traceId: string;
          readonly bookmarked: boolean;
          readonly public: boolean;
        }[]
      >;
    } = {
      repositories: getDorisTelemetryRepositories(),
      findTraceControls: ({ projectId, traceIds }) =>
        prisma.traceControlState.findMany({
          where: { projectId, traceId: { in: [...traceIds] } },
          select: { traceId: true, bookmarked: true, public: true },
        }),
    },
  ) {}

  async readExact(input: {
    readonly projectId: string;
    readonly items: readonly AnalyticsIntegrationExecutionManifestItem[];
    readonly observationTable: "observations" | "observations_v2";
    readonly observationFieldGroups: readonly ObservationFieldGroupFull[];
  }): Promise<DorisBlobExactReadResult> {
    const traceItems = input.items.filter(
      ({ deliveryKind }) => deliveryKind === "TRACE",
    );
    const traces = await this.dependencies.repositories.traces.getMany({
      projectId: input.projectId,
      traceIds: traceItems.map(({ entityKey }) => entityKey),
    });
    const controls = await this.dependencies.findTraceControls({
      projectId: input.projectId,
      traceIds: traces.map(({ id }) => id),
    });
    const controlsById = new Map(
      controls.map((control) => [control.traceId, control]),
    );
    const tracesById = new Map(traces.map((trace) => [trace.id, trace]));

    const observations = new Map<string, DorisObservation>();
    const scores = new Map<string, DorisScoreWithTraceContext>();
    for (const item of input.items) {
      if (item.deliveryKind === "OBSERVATION") {
        const observation =
          await this.dependencies.repositories.observations.get({
            projectId: input.projectId,
            observationId: item.entityKey,
          });
        if (observation) observations.set(item.entityKey, observation);
      } else if (item.deliveryKind === "SCORE") {
        const score = await this.dependencies.repositories.scores.get({
          projectId: input.projectId,
          scoreId: item.entityKey,
        });
        if (score) scores.set(item.entityKey, score);
      }
    }

    const records: DorisBlobExportRecord[] = [];
    const missing: AnalyticsIntegrationExecutionManifestItem[] = [];
    for (const item of input.items) {
      if (item.deliveryKind === "GENERATION") {
        // OBSERVATION carries the same canonical row for Blob export.
        continue;
      }
      if (item.deliveryKind === "TRACE") {
        const trace = tracesById.get(item.entityKey);
        if (trace) {
          records.push({
            item,
            table: "traces",
            row: traceRow(trace, controlsById.get(trace.id)),
          });
        } else {
          missing.push(item);
        }
      } else if (item.deliveryKind === "OBSERVATION") {
        const observation = observations.get(item.entityKey);
        if (observation) {
          records.push({
            item,
            table: input.observationTable,
            row: projectObservation(observation, input.observationFieldGroups),
          });
        } else {
          missing.push(item);
        }
      } else {
        const score = scores.get(item.entityKey);
        if (score) {
          records.push({ item, table: "scores", row: scoreRow(score) });
        } else {
          missing.push(item);
        }
      }
    }
    return { records, missing };
  }
}
