import {
  AnalyticsPersistenceError,
  createIngestionEventSchema,
  eventTypes,
  normalizeToolsForObservation,
  type IngestionAttribution,
  type IngestionEventType,
  type InternalTraceEventInput,
} from "@langfuse/shared/src/server";

export type LegacyEventData = Omit<
  InternalTraceEventInput,
  "input" | "output" | "endTimeISO"
> & {
  readonly input?: unknown;
  readonly output?: unknown;
  readonly endTimeISO?: string;
};

export type CurrentLegacyEvent = {
  readonly sourceVersion: bigint;
  readonly eventData: LegacyEventData;
};

export type LoadCurrentLegacyEvent = (input: {
  readonly projectId: string;
  readonly traceId?: string;
  readonly spanId: string;
}) => Promise<CurrentLegacyEvent | null>;

type LegacyCanonicalizationResult = {
  readonly eventData: LegacyEventData;
  readonly envelopeTimestamp: string;
  readonly expectedSourceVersion: bigint | null;
};

function metadataRecord(value: unknown): Record<string, unknown> {
  if (value === null || value === undefined) return {};
  if (typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return {
    metadata:
      typeof value === "string" || typeof value === "number"
        ? String(value)
        : JSON.stringify(value),
  };
}

function observationType(event: IngestionEventType): string {
  switch (event.type) {
    case eventTypes.EVENT_CREATE:
      return "EVENT";
    case eventTypes.SPAN_CREATE:
    case eventTypes.SPAN_UPDATE:
      return "SPAN";
    case eventTypes.GENERATION_CREATE:
    case eventTypes.GENERATION_UPDATE:
      return "GENERATION";
    case eventTypes.AGENT_CREATE:
      return "AGENT";
    case eventTypes.TOOL_CREATE:
      return "TOOL";
    case eventTypes.CHAIN_CREATE:
      return "CHAIN";
    case eventTypes.RETRIEVER_CREATE:
      return "RETRIEVER";
    case eventTypes.EVALUATOR_CREATE:
      return "EVALUATOR";
    case eventTypes.EMBEDDING_CREATE:
      return "EMBEDDING";
    case eventTypes.GUARDRAIL_CREATE:
      return "GUARDRAIL";
    case eventTypes.OBSERVATION_CREATE:
    case eventTypes.OBSERVATION_UPDATE:
      return event.body.type;
    default:
      throw new AnalyticsPersistenceError(
        "ANALYTICS_UNSUPPORTED_FEATURE",
        false,
        { tags: { sourceContract: "legacy" } },
      );
  }
}

function isUpdate(event: IngestionEventType): boolean {
  return (
    event.type === eventTypes.SPAN_UPDATE ||
    event.type === eventTypes.GENERATION_UPDATE ||
    event.type === eventTypes.OBSERVATION_UPDATE
  );
}

function mergeNumberRecord(
  current: Record<string, number> | undefined,
  incoming: unknown,
): Record<string, number> | undefined {
  if (!incoming || typeof incoming !== "object" || Array.isArray(incoming)) {
    return current;
  }
  const values = Object.fromEntries(
    Object.entries(incoming).filter(
      (entry): entry is [string, number] =>
        typeof entry[1] === "number" && Number.isFinite(entry[1]),
    ),
  );
  return Object.keys(values).length > 0
    ? { ...(current ?? {}), ...values }
    : current;
}

function providedUsage(
  current: Record<string, number> | undefined,
  body: Record<string, unknown>,
): Record<string, number> | undefined {
  let merged = mergeNumberRecord(current, body.usageDetails);
  const usage =
    body.usage && typeof body.usage === "object" && !Array.isArray(body.usage)
      ? (body.usage as Record<string, unknown>)
      : null;
  if (!usage) return merged;
  merged = mergeNumberRecord(merged, {
    input: usage.input,
    output: usage.output,
    total: usage.total,
  });
  return merged;
}

function providedCost(
  current: Record<string, number> | undefined,
  body: Record<string, unknown>,
): Record<string, number> | undefined {
  let merged = mergeNumberRecord(current, body.costDetails);
  const usage =
    body.usage && typeof body.usage === "object" && !Array.isArray(body.usage)
      ? (body.usage as Record<string, unknown>)
      : null;
  if (!usage) return merged;
  merged = mergeNumberRecord(merged, {
    input: usage.inputCost,
    output: usage.outputCost,
    total: usage.totalCost,
  });
  return merged;
}

function ordered(events: IngestionEventType[]): IngestionEventType[] {
  return events.slice().sort((left, right) => {
    const delta =
      new Date(left.timestamp).getTime() - new Date(right.timestamp).getTime();
    if (delta !== 0) return delta;
    const leftUpdate = isUpdate(left);
    const rightUpdate = isUpdate(right);
    return leftUpdate === rightUpdate ? 0 : leftUpdate ? 1 : -1;
  });
}

function eventBody(event: IngestionEventType): Record<string, unknown> {
  if (typeof event.body !== "object" || event.body === null) {
    throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
  }
  return event.body as Record<string, unknown>;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value ? value : undefined;
}

function applyTrace(
  current: LegacyEventData | null,
  event: IngestionEventType,
  input: {
    readonly projectId: string;
    readonly traceId: string;
    readonly attribution: IngestionAttribution;
  },
): LegacyEventData {
  const body = eventBody(event);
  const startTimeISO =
    stringValue(body.timestamp) ?? current?.startTimeISO ?? event.timestamp;
  const incomingMetadata = metadataRecord(body.metadata);
  const name = stringValue(body.name) ?? current?.name ?? "";
  const inputValue = body.input ?? current?.input;
  const outputValue = body.output ?? current?.output;
  return {
    ...(current ?? {}),
    projectId: input.projectId,
    traceId: input.traceId,
    spanId: `t-${input.traceId}`,
    parentSpanId: "",
    type: "SPAN",
    name,
    traceName: name,
    environment:
      stringValue(body.environment) ?? current?.environment ?? "default",
    version: stringValue(body.version) ?? current?.version,
    release: stringValue(body.release) ?? current?.release,
    userId: stringValue(body.userId) ?? current?.userId,
    sessionId: stringValue(body.sessionId) ?? current?.sessionId,
    public:
      typeof body.public === "boolean"
        ? body.public
        : (current?.public ?? false),
    bookmarked: current?.bookmarked ?? false,
    tags: Array.from(
      new Set((current?.tags ?? []).concat((body.tags as string[]) ?? [])),
    ).sort(),
    startTimeISO,
    endTimeISO: startTimeISO,
    level: current?.level ?? "DEFAULT",
    input: inputValue,
    output: outputValue,
    metadata: { ...(current?.metadata ?? {}), ...incomingMetadata },
    source: "ingestion-api-legacy",
    ingestionApiKey: input.attribution.ingestionApiKey,
    ingestionSdkName: input.attribution.ingestionSdkName,
    ingestionSdkVersion: input.attribution.ingestionSdkVersion,
  };
}

function applyObservation(
  current: LegacyEventData | null,
  event: IngestionEventType,
  input: {
    readonly projectId: string;
    readonly traceId: string;
    readonly spanId: string;
    readonly attribution: IngestionAttribution;
  },
): LegacyEventData {
  const body = eventBody(event);
  const startTimeISO =
    stringValue(body.startTime) ?? current?.startTimeISO ?? event.timestamp;
  let endTimeISO = stringValue(body.endTime) ?? current?.endTimeISO;
  if (endTimeISO && new Date(endTimeISO) < new Date(startTimeISO)) {
    endTimeISO = startTimeISO;
  }
  const rawInput = body.input ?? current?.input;
  const rawOutput = body.output ?? current?.output;
  const normalizedTools = normalizeToolsForObservation(rawInput, rawOutput, {
    ...(current?.metadata ?? {}),
    ...metadataRecord(body.metadata),
  });
  return {
    ...(current ?? {}),
    projectId: input.projectId,
    traceId: input.traceId,
    spanId: input.spanId,
    parentSpanId:
      stringValue(body.parentObservationId) ??
      current?.parentSpanId ??
      `t-${input.traceId}`,
    type: observationType(event),
    name: stringValue(body.name) ?? current?.name ?? "",
    environment:
      stringValue(body.environment) ?? current?.environment ?? "default",
    version: stringValue(body.version) ?? current?.version,
    startTimeISO,
    ...(endTimeISO ? { endTimeISO } : {}),
    completionStartTime:
      stringValue(body.completionStartTime) ?? current?.completionStartTime,
    level: stringValue(body.level) ?? current?.level ?? "DEFAULT",
    statusMessage: stringValue(body.statusMessage) ?? current?.statusMessage,
    promptName: stringValue(body.promptName) ?? current?.promptName,
    promptVersion:
      typeof body.promptVersion === "number"
        ? String(body.promptVersion)
        : current?.promptVersion,
    modelName: stringValue(body.model) ?? current?.modelName,
    modelParameters:
      body.modelParameters && typeof body.modelParameters === "object"
        ? (body.modelParameters as Record<string, unknown>)
        : current?.modelParameters,
    providedUsageDetails: providedUsage(current?.providedUsageDetails, body),
    providedCostDetails: providedCost(current?.providedCostDetails, body),
    toolDefinitions:
      Object.keys(normalizedTools.toolDefinitions).length > 0
        ? normalizedTools.toolDefinitions
        : current?.toolDefinitions,
    toolCalls:
      normalizedTools.toolCalls.length > 0
        ? normalizedTools.toolCalls
        : current?.toolCalls,
    toolCallNames:
      normalizedTools.toolCallNames.length > 0
        ? normalizedTools.toolCallNames
        : current?.toolCallNames,
    input: normalizedTools.input,
    output: normalizedTools.output,
    metadata: metadataRecord(normalizedTools.metadata),
    source: "ingestion-api-legacy",
    ingestionApiKey: input.attribution.ingestionApiKey,
    ingestionSdkName: input.attribution.ingestionSdkName,
    ingestionSdkVersion: input.attribution.ingestionSdkVersion,
  } as LegacyEventData;
}

export class LegacyEventCanonicalizer {
  constructor(
    private readonly dependencies: {
      readonly loadCurrentEvent: LoadCurrentLegacyEvent;
    },
  ) {}

  async canonicalize(input: {
    readonly projectId: string;
    readonly payload: unknown;
    readonly attribution: IngestionAttribution;
    readonly isLangfuseInternal: boolean;
  }): Promise<LegacyCanonicalizationResult> {
    if (!Array.isArray(input.payload) || input.payload.length === 0) {
      throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
    }
    const schema = createIngestionEventSchema(input.isLangfuseInternal);
    const events = input.payload.map((value) => {
      const parsed = schema.safeParse(value);
      if (!parsed.success) {
        throw new AnalyticsPersistenceError(
          "ANALYTICS_VALIDATION_ERROR",
          false,
        );
      }
      if (
        parsed.data.type === eventTypes.SDK_LOG ||
        parsed.data.type === eventTypes.SCORE_CREATE ||
        parsed.data.type === eventTypes.DATASET_RUN_ITEM_CREATE
      ) {
        throw new AnalyticsPersistenceError(
          "ANALYTICS_UNSUPPORTED_FEATURE",
          false,
          { tags: { sourceContract: "legacy" } },
        );
      }
      return parsed.data;
    });
    const first = events[0]!;
    const firstBody = eventBody(first);
    const entityId = stringValue(firstBody.id);
    if (!entityId) {
      throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
    }
    const traceEvent = first.type === eventTypes.TRACE_CREATE;
    if (
      events.some(
        (event) =>
          (event.type === eventTypes.TRACE_CREATE) !== traceEvent ||
          stringValue(eventBody(event).id) !== entityId,
      )
    ) {
      throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
    }
    const declaredTraceIds = Array.from(
      new Set(
        events.flatMap((event) => {
          const traceId = stringValue(eventBody(event).traceId);
          return traceId ? [traceId] : [];
        }),
      ),
    );
    if (declaredTraceIds.length > 1) {
      throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
    }
    const requestedTraceId = traceEvent ? entityId : declaredTraceIds[0];
    const spanId = traceEvent ? `t-${entityId}` : entityId;
    const current = await this.dependencies.loadCurrentEvent({
      projectId: input.projectId,
      traceId: requestedTraceId,
      spanId,
    });
    const traceId = requestedTraceId ?? current?.eventData.traceId ?? entityId;
    if (
      !current &&
      events.some(isUpdate) &&
      !events.some((event) => !isUpdate(event))
    ) {
      throw new AnalyticsPersistenceError("ANALYTICS_NOT_FOUND", false, {
        tags: { sourceContract: "legacy" },
      });
    }
    let eventData = current?.eventData ?? null;
    const sorted = ordered(events);
    for (const event of sorted) {
      eventData = traceEvent
        ? applyTrace(eventData, event, {
            projectId: input.projectId,
            traceId,
            attribution: input.attribution,
          })
        : applyObservation(eventData, event, {
            projectId: input.projectId,
            traceId,
            spanId,
            attribution: input.attribution,
          });
    }
    if (!eventData) {
      throw new AnalyticsPersistenceError("ANALYTICS_VALIDATION_ERROR", false);
    }
    return {
      eventData,
      envelopeTimestamp: sorted[sorted.length - 1]!.timestamp,
      expectedSourceVersion: current?.sourceVersion ?? null,
    };
  }
}
