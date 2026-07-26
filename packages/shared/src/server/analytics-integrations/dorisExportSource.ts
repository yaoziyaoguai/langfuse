import { env } from "../../env";
import type {
  AnalyticsGenerationEvent,
  AnalyticsObservationEvent,
  AnalyticsScoreEvent,
  AnalyticsTraceEvent,
} from "./types";
import type {
  AnalyticsIntegrationBootstrapIdentity,
  AnalyticsIntegrationExecutionManifestItem,
} from "../repositories/analyticsIntegrationDeliveries";
import {
  getDorisTelemetryRepositories,
  type DorisObservation,
  type DorisScoreWithTraceContext,
  type DorisTrace,
} from "../repositories/telemetry/doris";

export type AnalyticsIntegrationSemanticRecord =
  | {
      readonly deliveryKind: "TRACE";
      readonly entityKey: string;
      readonly event: AnalyticsTraceEvent;
    }
  | {
      readonly deliveryKind: "GENERATION";
      readonly entityKey: string;
      readonly event: AnalyticsGenerationEvent;
    }
  | {
      readonly deliveryKind: "OBSERVATION";
      readonly entityKey: string;
      readonly event: AnalyticsObservationEvent;
    }
  | {
      readonly deliveryKind: "SCORE";
      readonly entityKey: string;
      readonly event: AnalyticsScoreEvent;
    };

export type AnalyticsIntegrationExactReadResult = {
  readonly records: readonly AnalyticsIntegrationSemanticRecord[];
  readonly missing: readonly AnalyticsIntegrationExecutionManifestItem[];
};

type DorisIntegrationRepositories = {
  readonly traces: {
    getMany(input: {
      readonly projectId: string;
      readonly traceIds: readonly string[];
    }): Promise<readonly DorisTrace[]>;
    scanIdentities(input: {
      readonly projectId: string;
      readonly range: { readonly from: Date; readonly to: Date };
      readonly filters: [];
      readonly limit: number;
    }): AsyncIterable<{ readonly id: string }>;
  };
  readonly observations: {
    get(input: {
      readonly projectId: string;
      readonly observationId: string;
    }): Promise<DorisObservation | null>;
    scanIdentities(input: {
      readonly projectId: string;
      readonly range: { readonly from: Date; readonly to: Date };
      readonly filters: [];
      readonly limit: number;
    }): AsyncIterable<{ readonly id: string; readonly traceId: string }>;
  };
  readonly scores: {
    get(input: {
      readonly projectId: string;
      readonly scoreId: string;
    }): Promise<DorisScoreWithTraceContext | null>;
    scanIdentities(input: {
      readonly projectId: string;
      readonly range: { readonly from: Date; readonly to: Date };
      readonly filters: [];
      readonly limit: number;
    }): AsyncIterable<{ readonly id: string }>;
  };
};

function metadataSessionId(
  metadata: Readonly<Record<string, unknown>> | undefined,
  key: "$posthog_session_id" | "$mixpanel_session_id",
): string | null {
  const value = metadata?.[key];
  return typeof value === "string" && value ? value : null;
}

function baseUrl(): string {
  return env.NEXTAUTH_URL?.replace("/api/auth", "") ?? "";
}

function traceEvent(
  trace: DorisTrace,
  projectName: string,
): AnalyticsTraceEvent {
  return {
    timestamp: trace.timestamp,
    langfuse_id: trace.id,
    langfuse_trace_name: trace.name,
    langfuse_url: `${baseUrl()}/project/${trace.projectId}/traces/${encodeURIComponent(trace.id)}`,
    langfuse_user_url: trace.userId
      ? `${baseUrl()}/project/${trace.projectId}/users/${encodeURIComponent(trace.userId)}`
      : undefined,
    langfuse_cost_usd: trace.totalCost,
    langfuse_count_observations: trace.observationCount,
    langfuse_session_id: trace.sessionId,
    langfuse_project_id: trace.projectId,
    langfuse_project_name: projectName,
    langfuse_user_id: trace.userId,
    langfuse_latency: trace.latency,
    langfuse_release: trace.release,
    langfuse_version: trace.version,
    langfuse_tags: trace.tags,
    langfuse_environment: trace.environment,
    langfuse_event_version: "1.0.0",
    posthog_session_id: metadataSessionId(
      trace.metadata,
      "$posthog_session_id",
    ),
    mixpanel_session_id: metadataSessionId(
      trace.metadata,
      "$mixpanel_session_id",
    ),
  };
}

function observationEvent(
  observation: DorisObservation,
  projectName: string,
): AnalyticsObservationEvent {
  return {
    timestamp: observation.startTime,
    langfuse_observation_name: observation.name,
    langfuse_trace_name: observation.traceName,
    langfuse_trace_id: observation.traceId,
    langfuse_url: `${baseUrl()}/project/${observation.projectId}/traces/${encodeURIComponent(observation.traceId)}?observation=${encodeURIComponent(observation.id)}`,
    langfuse_user_url: observation.userId
      ? `${baseUrl()}/project/${observation.projectId}/users/${encodeURIComponent(observation.userId)}`
      : undefined,
    langfuse_id: observation.id,
    langfuse_cost_usd: observation.totalCost,
    langfuse_input_units: observation.totalInputTokens,
    langfuse_output_units: observation.totalOutputTokens,
    langfuse_total_units: observation.totalUsage,
    langfuse_session_id: observation.sessionId,
    langfuse_project_id: observation.projectId,
    langfuse_project_name: projectName,
    langfuse_user_id: observation.userId,
    langfuse_latency: observation.latency,
    langfuse_time_to_first_token: observation.timeToFirstToken,
    langfuse_release: observation.release,
    langfuse_version: observation.version,
    langfuse_model: observation.providedModelName,
    langfuse_level: observation.level,
    langfuse_type: observation.type,
    langfuse_tags: observation.tags,
    langfuse_environment: observation.environment,
    langfuse_event_version: "1.0.0",
    posthog_session_id: metadataSessionId(
      observation.metadata,
      "$posthog_session_id",
    ),
    mixpanel_session_id: metadataSessionId(
      observation.metadata,
      "$mixpanel_session_id",
    ),
  };
}

function generationEvent(
  observation: DorisObservation,
  trace: DorisTrace | undefined,
  projectName: string,
): AnalyticsGenerationEvent {
  return {
    timestamp: observation.startTime,
    langfuse_generation_name: observation.name,
    langfuse_trace_name: trace?.name ?? observation.traceName,
    langfuse_trace_id: observation.traceId,
    langfuse_url: `${baseUrl()}/project/${observation.projectId}/traces/${encodeURIComponent(observation.traceId)}?observation=${encodeURIComponent(observation.id)}`,
    langfuse_user_url: observation.userId
      ? `${baseUrl()}/project/${observation.projectId}/users/${encodeURIComponent(observation.userId)}`
      : undefined,
    langfuse_id: observation.id,
    langfuse_cost_usd: observation.totalCost,
    langfuse_input_units: observation.totalInputTokens,
    langfuse_output_units: observation.totalOutputTokens,
    langfuse_total_units: observation.totalUsage,
    langfuse_session_id: trace?.sessionId ?? observation.sessionId,
    langfuse_project_id: observation.projectId,
    langfuse_project_name: projectName,
    langfuse_user_id: trace?.userId ?? observation.userId,
    langfuse_latency: observation.latency,
    langfuse_time_to_first_token: observation.timeToFirstToken,
    langfuse_release: trace?.release ?? observation.release,
    langfuse_version: observation.version,
    langfuse_model: observation.providedModelName,
    langfuse_level: observation.level,
    langfuse_tags: trace?.tags ?? observation.tags,
    langfuse_environment: observation.environment,
    langfuse_event_version: "1.0.0",
    posthog_session_id: metadataSessionId(
      trace?.metadata,
      "$posthog_session_id",
    ),
    mixpanel_session_id: metadataSessionId(
      trace?.metadata,
      "$mixpanel_session_id",
    ),
  };
}

function scoreEvent(
  score: DorisScoreWithTraceContext,
  trace: DorisTrace | undefined,
  projectName: string,
): AnalyticsScoreEvent {
  const effectiveSessionId = score.sessionId ?? trace?.sessionId ?? null;
  return {
    timestamp: score.timestamp,
    langfuse_score_name: score.name,
    langfuse_score_value: score.value,
    langfuse_score_comment: score.comment,
    langfuse_score_metadata: score.metadata,
    langfuse_score_string_value: score.stringValue,
    langfuse_score_data_type: score.dataType,
    langfuse_trace_name: trace?.name,
    langfuse_trace_id: score.traceId,
    langfuse_user_url: trace?.userId
      ? `${baseUrl()}/project/${score.projectId}/users/${encodeURIComponent(trace.userId)}`
      : undefined,
    langfuse_id: score.id,
    langfuse_session_id: effectiveSessionId,
    langfuse_project_id: score.projectId,
    langfuse_project_name: projectName,
    langfuse_user_id: trace?.userId ?? null,
    langfuse_release: trace?.release,
    langfuse_tags: trace?.tags,
    langfuse_environment: score.environment,
    langfuse_event_version: "1.0.0",
    langfuse_score_entity_type: score.traceId
      ? "trace"
      : score.sessionId
        ? "session"
        : score.datasetRunId
          ? "dataset_run"
          : "unknown",
    langfuse_dataset_run_id: score.datasetRunId,
    posthog_session_id: metadataSessionId(
      trace?.metadata,
      "$posthog_session_id",
    ),
    mixpanel_session_id: metadataSessionId(
      trace?.metadata,
      "$mixpanel_session_id",
    ),
  };
}

export class DorisAnalyticsIntegrationExportSource {
  constructor(
    private readonly repositories: DorisIntegrationRepositories = getDorisTelemetryRepositories(),
  ) {}

  async scanBootstrapIdentities(input: {
    readonly projectId: string;
    readonly limit: number;
    readonly now?: Date;
  }): Promise<readonly AnalyticsIntegrationBootstrapIdentity[]> {
    if (
      !input.projectId ||
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > 100_000
    ) {
      throw new TypeError("Invalid Doris analytics integration bootstrap scan");
    }
    const identities: AnalyticsIntegrationBootstrapIdentity[] = [];
    const now = input.now ?? new Date();
    if (!Number.isFinite(now.getTime())) {
      throw new TypeError(
        "Invalid Doris analytics integration bootstrap timestamp",
      );
    }
    const fullHistoryRange = {
      from: new Date("1970-01-01T00:00:00.000Z"),
      to: new Date(now.getTime() + 1),
    };
    const append = (
      deliveryKind: AnalyticsIntegrationBootstrapIdentity["deliveryKind"],
      entityKey: string,
    ) => {
      if (identities.length >= input.limit) {
        throw new RangeError(
          "Analytics integration bootstrap exceeds its durable manifest budget",
        );
      }
      identities.push({ deliveryKind, entityKey });
    };
    for await (const trace of this.repositories.traces.scanIdentities({
      projectId: input.projectId,
      range: fullHistoryRange,
      filters: [],
      limit: input.limit + 1,
    })) {
      append("TRACE", trace.id);
    }
    for await (const observation of this.repositories.observations.scanIdentities(
      {
        projectId: input.projectId,
        range: fullHistoryRange,
        filters: [],
        limit: input.limit - identities.length + 1,
      },
    )) {
      append("OBSERVATION", observation.id);
    }
    for await (const score of this.repositories.scores.scanIdentities({
      projectId: input.projectId,
      range: fullHistoryRange,
      filters: [],
      limit: input.limit - identities.length + 1,
    })) {
      append("SCORE", score.id);
    }
    return identities;
  }

  async readExact(input: {
    readonly projectId: string;
    readonly projectName: string;
    readonly items: readonly AnalyticsIntegrationExecutionManifestItem[];
  }): Promise<AnalyticsIntegrationExactReadResult> {
    const tracesById = new Map(
      (
        await this.repositories.traces.getMany({
          projectId: input.projectId,
          traceIds: input.items
            .filter(({ deliveryKind }) => deliveryKind === "TRACE")
            .map(({ entityKey }) => entityKey),
        })
      ).map((trace) => [trace.id, trace]),
    );
    const observations = new Map<string, DorisObservation>();
    const scores = new Map<string, DorisScoreWithTraceContext>();
    for (const item of input.items) {
      if (
        item.deliveryKind === "OBSERVATION" ||
        item.deliveryKind === "GENERATION"
      ) {
        const observation = await this.repositories.observations.get({
          projectId: input.projectId,
          observationId: item.entityKey,
        });
        if (observation) observations.set(item.entityKey, observation);
      } else if (item.deliveryKind === "SCORE") {
        const score = await this.repositories.scores.get({
          projectId: input.projectId,
          scoreId: item.entityKey,
        });
        if (score) scores.set(item.entityKey, score);
      }
    }
    const contextTraceIds = [
      ...new Set([
        ...[...observations.values()].map(({ traceId }) => traceId),
        ...[...scores.values()].flatMap(({ traceId }) =>
          traceId ? [traceId] : [],
        ),
      ]),
    ].filter((traceId) => !tracesById.has(traceId));
    if (contextTraceIds.length > 0) {
      const traces = await this.repositories.traces.getMany({
        projectId: input.projectId,
        traceIds: contextTraceIds,
      });
      traces.forEach((trace) => tracesById.set(trace.id, trace));
    }

    const records: AnalyticsIntegrationSemanticRecord[] = [];
    const missing: AnalyticsIntegrationExecutionManifestItem[] = [];
    const explicitGenerationIds = new Set(
      input.items
        .filter(({ deliveryKind }) => deliveryKind === "GENERATION")
        .map(({ entityKey }) => entityKey),
    );
    for (const item of input.items) {
      if (item.deliveryKind === "TRACE") {
        const trace = tracesById.get(item.entityKey);
        if (trace) {
          records.push({
            deliveryKind: "TRACE",
            entityKey: item.entityKey,
            event: traceEvent(trace, input.projectName),
          });
        } else {
          missing.push(item);
        }
      } else if (item.deliveryKind === "SCORE") {
        const score = scores.get(item.entityKey);
        if (score) {
          records.push({
            deliveryKind: "SCORE",
            entityKey: item.entityKey,
            event: scoreEvent(
              score,
              score.traceId ? tracesById.get(score.traceId) : undefined,
              input.projectName,
            ),
          });
        } else {
          missing.push(item);
        }
      } else {
        const observation = observations.get(item.entityKey);
        if (
          !observation ||
          (item.deliveryKind === "GENERATION" &&
            observation.type !== "GENERATION")
        ) {
          missing.push(item);
        } else if (item.deliveryKind === "GENERATION") {
          records.push({
            deliveryKind: "GENERATION",
            entityKey: item.entityKey,
            event: generationEvent(
              observation,
              tracesById.get(observation.traceId),
              input.projectName,
            ),
          });
        } else {
          records.push({
            deliveryKind: "OBSERVATION",
            entityKey: item.entityKey,
            event: observationEvent(observation, input.projectName),
          });
          if (
            observation.type === "GENERATION" &&
            !explicitGenerationIds.has(item.entityKey)
          ) {
            records.push({
              deliveryKind: "GENERATION",
              entityKey: item.entityKey,
              event: generationEvent(
                observation,
                tracesById.get(observation.traceId),
                input.projectName,
              ),
            });
          }
        }
      }
    }
    return { records, missing };
  }
}
