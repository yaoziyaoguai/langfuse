import type Decimal from "decimal.js";

import type { ObservationFieldGroupPublicApi } from "../../../../domain/observation-field-groups";
import type { EventsTableFilterState } from "../../../../types";
import { prisma } from "../../../../db";
import type { EventsObservationPublic } from "../../../queries/createGenerationsQuery";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";
import { projectDorisObservation } from "./adapters";
import type { DorisObservation } from "./observations";
import { getDorisTelemetryRepositories } from "./runtime";

export type DorisPublicApiObservationsQuery = {
  readonly projectId: string;
  readonly page: number;
  readonly limit: number;
  readonly traceId?: string;
  readonly userId?: string;
  readonly name?: string;
  readonly type?: string;
  readonly level?: string;
  readonly parentObservationId?: string;
  readonly fromStartTime?: string;
  readonly toStartTime?: string;
  readonly version?: string;
  readonly environment?: string | readonly string[];
  readonly advancedFilters?: EventsTableFilterState;
  readonly cursor?: {
    readonly lastStartTimeTo: Date;
    readonly lastTraceId: string;
    readonly lastId: string;
  };
  readonly fields?: readonly ObservationFieldGroupPublicApi[] | null;
};

type ObservationRepository = Pick<
  ReturnType<typeof getDorisTelemetryRepositories>["observations"],
  "list" | "listForTrace" | "get" | "count" | "countForTrace"
>;

type DorisObservationReadDependencies = {
  readonly repository: ObservationRepository;
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
  readonly findModels: (input: {
    readonly projectId: string;
    readonly modelIds: readonly string[];
  }) => Promise<
    readonly {
      readonly id: string;
      readonly Price: readonly {
        readonly usageType: string;
        readonly price: Decimal;
      }[];
    }[]
  >;
};

function defaultDependencies(): DorisObservationReadDependencies {
  return {
    repository: getDorisTelemetryRepositories().observations,
    findTraceControls: ({ projectId, traceIds }) =>
      prisma.traceControlState.findMany({
        where: { projectId, traceId: { in: [...traceIds] } },
        select: { traceId: true, bookmarked: true, public: true },
      }),
    findModels: ({ projectId, modelIds }) =>
      prisma.model.findMany({
        where: {
          id: { in: [...modelIds] },
          OR: [{ projectId }, { projectId: null }],
        },
        select: { id: true, Price: true },
      }),
  };
}

function optionalStringFilter(
  filters: EventsTableFilterState,
  column: string,
  value: string | undefined,
): void {
  if (!value) return;
  filters.push({ type: "string", column, operator: "=", value });
}

function buildFilters(
  input: DorisPublicApiObservationsQuery,
): EventsTableFilterState {
  const filters: EventsTableFilterState = [...(input.advancedFilters ?? [])];
  optionalStringFilter(filters, "traceId", input.traceId);
  optionalStringFilter(filters, "userId", input.userId);
  optionalStringFilter(filters, "name", input.name);
  optionalStringFilter(filters, "type", input.type);
  optionalStringFilter(filters, "level", input.level);
  optionalStringFilter(
    filters,
    "parentObservationId",
    input.parentObservationId,
  );
  optionalStringFilter(filters, "version", input.version);
  const environments = Array.isArray(input.environment)
    ? input.environment
    : input.environment
      ? [input.environment]
      : [];
  if (environments.length > 0) {
    filters.push({
      type: "stringOptions",
      column: "environment",
      operator: "any of",
      value: [...environments],
    });
  }
  return filters;
}

function validDate(value: string | undefined): Date | undefined {
  if (!value) return undefined;
  return new Date(value);
}

function buildRange(
  input: DorisPublicApiObservationsQuery,
): AnalyticsTimeRange | null {
  const from = validDate(input.fromStartTime);
  const to = validDate(input.toStartTime);
  return from ? { from, to: to ?? new Date() } : null;
}

function exactFilterValue(
  filters: EventsTableFilterState,
  column: string,
): string | undefined {
  for (const filter of filters) {
    if (filter.column !== column) continue;
    if (filter.type === "string" && filter.operator === "=") {
      return filter.value;
    }
    if (
      filter.type === "stringOptions" &&
      filter.operator === "any of" &&
      filter.value.length === 1
    ) {
      return filter.value[0];
    }
  }
  return undefined;
}

function encodeCursor(
  cursor: DorisPublicApiObservationsQuery["cursor"],
): string | undefined {
  return cursor
    ? Buffer.from(
        JSON.stringify({
          version: 1,
          startTime: cursor.lastStartTimeTo.toISOString(),
          traceId: cursor.lastTraceId,
          spanId: cursor.lastId,
        }),
        "utf8",
      ).toString("base64url")
    : undefined;
}

async function applyTraceControls(
  observations: readonly DorisObservation[],
  input: DorisPublicApiObservationsQuery,
  dependencies: DorisObservationReadDependencies,
): Promise<readonly DorisObservation[]> {
  const traceIds = [...new Set(observations.map(({ traceId }) => traceId))];
  const controls = await dependencies.findTraceControls({
    projectId: input.projectId,
    traceIds,
  });
  const byTraceId = new Map(
    controls.map((control) => [control.traceId, control]),
  );
  return observations.map((observation) => ({
    ...observation,
    bookmarked: byTraceId.get(observation.traceId)?.bookmarked ?? false,
    public: byTraceId.get(observation.traceId)?.public ?? false,
  }));
}

async function enrichModels(
  observations: readonly DorisObservation[],
  projected: readonly EventsObservationPublic[],
  fields: readonly ObservationFieldGroupPublicApi[],
  input: DorisPublicApiObservationsQuery,
  dependencies: DorisObservationReadDependencies,
): Promise<EventsObservationPublic[]> {
  if (!fields.includes("model")) return [...projected];
  const modelIds = [
    ...new Set(
      observations.flatMap(({ internalModelId }) =>
        internalModelId ? [internalModelId] : [],
      ),
    ),
  ];
  const models = await dependencies.findModels({
    projectId: input.projectId,
    modelIds,
  });
  const byId = new Map(models.map((model) => [model.id, model]));
  return projected.map((item, index) => {
    const modelId = observations[index]?.internalModelId;
    const model = modelId ? byId.get(modelId) : undefined;
    return {
      ...item,
      modelId: model?.id ?? null,
      inputPrice:
        model?.Price.find(({ usageType }) => usageType === "input")?.price ??
        null,
      outputPrice:
        model?.Price.find(({ usageType }) => usageType === "output")?.price ??
        null,
      totalPrice:
        model?.Price.find(({ usageType }) => usageType === "total")?.price ??
        null,
    };
  });
}

export async function getDorisObservationsForPublicApi(
  input: DorisPublicApiObservationsQuery,
  dependencies: DorisObservationReadDependencies = defaultDependencies(),
): Promise<EventsObservationPublic[]> {
  const filters = buildFilters(input);
  const range = buildRange(input);
  const observationId = exactFilterValue(filters, "id");
  const traceId = input.traceId ?? exactFilterValue(filters, "traceId");
  const fields = input.fields ?? ["core", "basic"];
  const includeFullContent = fields.some((field) =>
    ["io", "metadata", "model"].includes(field),
  );
  let observations: readonly DorisObservation[];
  if (!range && observationId) {
    const observation = await dependencies.repository.get({
      projectId: input.projectId,
      observationId,
      traceId,
    });
    observations = observation ? [observation] : [];
  } else if (!range && traceId) {
    observations = (
      await dependencies.repository.listForTrace({
        projectId: input.projectId,
        traceId,
        filters,
        cursor: encodeCursor(input.cursor),
        limit: input.limit,
        includeFullContent,
      })
    ).items;
  } else {
    const offset = input.cursor ? 0 : Math.max(0, input.page - 1) * input.limit;
    const pageSize = offset + input.limit;
    observations = (
      await dependencies.repository.list({
        projectId: input.projectId,
        range,
        filters,
        cursor: encodeCursor(input.cursor),
        limit: pageSize,
        includeFullContent,
      })
    ).items.slice(offset);
  }
  const controlled = await applyTraceControls(
    observations,
    input,
    dependencies,
  );
  return enrichModels(
    controlled,
    controlled.map((observation) =>
      projectDorisObservation(observation, fields),
    ),
    fields,
    input,
    dependencies,
  );
}

export async function getDorisObservationsCountForPublicApi(
  input: DorisPublicApiObservationsQuery,
  dependencies: DorisObservationReadDependencies = defaultDependencies(),
): Promise<number> {
  const filters = buildFilters(input);
  const range = buildRange(input);
  const traceId = input.traceId ?? exactFilterValue(filters, "traceId");
  return !range && traceId
    ? dependencies.repository.countForTrace({
        projectId: input.projectId,
        traceId,
        filters,
      })
    : dependencies.repository.count({
        projectId: input.projectId,
        range,
        filters,
      });
}
