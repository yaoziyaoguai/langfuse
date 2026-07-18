import type { TracingSearchType } from "../../../../interfaces/search";
import type { EventsTableFilterState } from "../../../../types";
import { InvalidRequestError, LangfuseConflictError } from "../../../../errors";
import { parseJsonIfString } from "../../../../utils/json";
import type { DorisQueryExecutor } from "../../../doris/client";
import {
  compileDorisVisibleEventsQuery,
  type DorisEventCursor,
} from "../../../queries/doris-sql/eventQueryCompiler";
import type { AnalyticsTimeRange } from "../../../queries/logical/searchPlan";
import {
  findObservationHeadLocators,
  type EventHeadLocator,
} from "./entityHeadLocator";

const MAX_PAGE_SIZE = 999;

type LocateObservation = (input: {
  readonly projectId: string;
  readonly observationId: string;
  readonly traceId?: string;
}) => Promise<readonly EventHeadLocator[]>;

type DorisEventRow = Record<string, unknown> & {
  readonly project_id: string;
  readonly partition_date: string | Date;
  readonly trace_id: string;
  readonly span_id: string;
  readonly start_time: string | Date;
};

export type DorisObservation = {
  readonly id: string;
  readonly traceId: string;
  readonly projectId: string;
  readonly partitionDate: string;
  readonly parentObservationId: string | null;
  readonly type: string;
  readonly name: string | null;
  readonly environment: string;
  readonly userId: string | null;
  readonly sessionId: string | null;
  readonly traceName: string | null;
  readonly release: string | null;
  readonly version: string | null;
  readonly level: string | null;
  readonly statusMessage: string | null;
  readonly isAppRoot: boolean;
  readonly bookmarked: boolean;
  readonly public: boolean;
  readonly startTime: Date;
  readonly endTime: Date | null;
  readonly completionStartTime: Date | null;
  readonly createdAt: Date;
  readonly updatedAt: Date;
  readonly providedModelName: string | null;
  readonly internalModelId: string | null;
  readonly promptId: string | null;
  readonly promptName: string | null;
  readonly promptVersion: number | null;
  readonly totalInputTokens: number | null;
  readonly totalOutputTokens: number | null;
  readonly totalUsage: number;
  readonly totalCost: number | null;
  readonly latency: number | null;
  readonly timeToFirstToken: number | null;
  readonly tags: readonly string[];
  readonly usageDetails: Readonly<Record<string, number>>;
  readonly costDetails: Readonly<Record<string, number>>;
  readonly providedUsageDetails: Readonly<Record<string, number>>;
  readonly providedCostDetails: Readonly<Record<string, number>>;
  readonly toolDefinitionsCount: number | null;
  readonly toolCallsCount: number | null;
  readonly inputPreview: string | null;
  readonly outputPreview: string | null;
  readonly input?: unknown;
  readonly output?: unknown;
  readonly metadata?: Readonly<Record<string, unknown>>;
  readonly modelParameters?: Readonly<Record<string, unknown>>;
  readonly toolDefinitions?: Readonly<Record<string, unknown>>;
  readonly toolCalls?: readonly string[];
  readonly toolCallNames?: readonly string[];
};

export type DorisObservationsPage = {
  readonly items: readonly DorisObservation[];
  readonly nextCursor: string | null;
};

function dateOnly(value: unknown): string {
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  if (typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value)) {
    return value.slice(0, 10);
  }
  throw new TypeError("Doris returned an invalid partition date");
}

function dateTime(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value !== "string") {
    throw new TypeError("Doris returned an invalid timestamp");
  }
  const parsed = new Date(
    value.includes("T")
      ? value.replace(/Z?$/, "Z")
      : `${value.replace(" ", "T")}Z`,
  );
  if (Number.isNaN(parsed.getTime())) {
    throw new TypeError("Doris returned an invalid timestamp");
  }
  return parsed;
}

function nullableDateTime(value: unknown): Date | null {
  return value === null || value === undefined || value === ""
    ? null
    : dateTime(value);
}

function nullableString(value: unknown): string | null {
  return value === null || value === undefined ? null : String(value);
}

function nullableNumber(value: unknown): number | null {
  if (value === null || value === undefined || value === "") return null;
  const parsed = Number(value);
  if (!Number.isFinite(parsed)) {
    throw new TypeError("Doris returned an invalid number");
  }
  return parsed;
}

function booleanValue(value: unknown): boolean {
  return value === true || value === 1 || value === "1";
}

function objectValue(value: unknown): Readonly<Record<string, unknown>> {
  const parsed = parseJsonIfString(value);
  return typeof parsed === "object" && parsed !== null && !Array.isArray(parsed)
    ? (parsed as Readonly<Record<string, unknown>>)
    : {};
}

function numericRecord(value: unknown): Readonly<Record<string, number>> {
  return Object.fromEntries(
    Object.entries(objectValue(value)).flatMap(([key, item]) => {
      const parsed = nullableNumber(item);
      return parsed === null ? [] : [[key, parsed]];
    }),
  );
}

function stringArray(value: unknown): readonly string[] {
  const parsed = parseJsonIfString(value);
  return Array.isArray(parsed) ? parsed.map(String) : [];
}

function secondsBetween(from: Date, to: Date | null): number | null {
  return to ? (to.getTime() - from.getTime()) / 1_000 : null;
}

function decodeObservation(row: DorisEventRow): DorisObservation {
  const startTime = dateTime(row.start_time);
  const endTime = nullableDateTime(row.end_time);
  const completionStartTime = nullableDateTime(row.completion_start_time);
  const usageDetails = numericRecord(row.usage_details);
  const inputTokens = nullableNumber(row.total_input_tokens);
  const outputTokens = nullableNumber(row.total_output_tokens);
  const observation: DorisObservation = {
    id: row.span_id,
    traceId: row.trace_id,
    projectId: row.project_id,
    partitionDate: dateOnly(row.partition_date),
    parentObservationId: nullableString(row.parent_span_id),
    type: String(row.type),
    name: nullableString(row.name),
    environment: String(row.environment),
    userId: nullableString(row.user_id),
    sessionId: nullableString(row.session_id),
    traceName: nullableString(row.trace_name),
    release: nullableString(row.release),
    version: nullableString(row.version),
    level: nullableString(row.level),
    statusMessage: nullableString(row.status_message),
    isAppRoot: booleanValue(row.is_app_root),
    bookmarked: booleanValue(row.bookmarked),
    public: booleanValue(row.public),
    startTime,
    endTime,
    completionStartTime,
    createdAt: dateTime(row.created_at),
    updatedAt: dateTime(row.updated_at),
    providedModelName: nullableString(row.provided_model_name),
    internalModelId: nullableString(row.internal_model_id),
    promptId: nullableString(row.prompt_id),
    promptName: nullableString(row.prompt_name),
    promptVersion: nullableNumber(row.prompt_version),
    totalInputTokens: inputTokens,
    totalOutputTokens: outputTokens,
    totalUsage:
      nullableNumber(usageDetails.total) ??
      (inputTokens ?? 0) + (outputTokens ?? 0),
    totalCost: nullableNumber(row.total_cost),
    latency: secondsBetween(startTime, endTime),
    timeToFirstToken: secondsBetween(startTime, completionStartTime),
    tags: stringArray(row.tags),
    usageDetails,
    costDetails: numericRecord(row.cost_details),
    providedUsageDetails: numericRecord(row.provided_usage_details),
    providedCostDetails: numericRecord(row.provided_cost_details),
    toolDefinitionsCount: nullableNumber(row.tool_definitions_count),
    toolCallsCount: nullableNumber(row.tool_calls_count),
    inputPreview: nullableString(row.input_preview),
    outputPreview: nullableString(row.output_preview),
    ...(Object.hasOwn(row, "input") && { input: parseJsonIfString(row.input) }),
    ...(Object.hasOwn(row, "output") && {
      output: parseJsonIfString(row.output),
    }),
    ...(Object.hasOwn(row, "metadata") && {
      metadata: objectValue(row.metadata),
    }),
    ...(Object.hasOwn(row, "model_parameters") && {
      modelParameters: objectValue(row.model_parameters),
    }),
    ...(Object.hasOwn(row, "tool_definitions") && {
      toolDefinitions: objectValue(row.tool_definitions),
    }),
    ...(Object.hasOwn(row, "tool_calls") && {
      toolCalls: stringArray(row.tool_calls),
    }),
    ...(Object.hasOwn(row, "tool_call_names") && {
      toolCallNames: stringArray(row.tool_call_names),
    }),
  };
  return observation;
}

function encodeCursor(observation: DorisObservation): string {
  return Buffer.from(
    JSON.stringify({
      version: 1,
      startTime: observation.startTime.toISOString(),
      traceId: observation.traceId,
      spanId: observation.id,
    }),
    "utf8",
  ).toString("base64url");
}

function decodeCursor(
  cursor: string | undefined,
): DorisEventCursor | undefined {
  if (!cursor) return undefined;
  try {
    const decoded = Buffer.from(cursor, "base64url");
    if (decoded.toString("base64url") !== cursor) throw new Error();
    const value = JSON.parse(decoded.toString("utf8")) as Record<
      string,
      unknown
    >;
    if (
      value.version !== 1 ||
      typeof value.startTime !== "string" ||
      typeof value.traceId !== "string" ||
      !value.traceId ||
      typeof value.spanId !== "string" ||
      !value.spanId
    ) {
      throw new Error();
    }
    return {
      startTime: dateTime(value.startTime),
      traceId: value.traceId,
      spanId: value.spanId,
    };
  } catch {
    throw new InvalidRequestError("Invalid Doris observation cursor");
  }
}

function nextUtcDay(partitionDate: string): Date {
  const start = new Date(`${partitionDate}T00:00:00.000Z`);
  start.setUTCDate(start.getUTCDate() + 1);
  return start;
}

export class DorisObservationsRepository {
  private readonly locateObservation: LocateObservation;

  constructor(
    private readonly dependencies: {
      readonly query: DorisQueryExecutor["query"];
      readonly locateObservation?: LocateObservation;
    },
  ) {
    this.locateObservation =
      dependencies.locateObservation ?? findObservationHeadLocators;
  }

  async list(input: {
    readonly projectId: string;
    readonly range: AnalyticsTimeRange | null;
    readonly filters: EventsTableFilterState;
    readonly search?: {
      readonly query: string;
      readonly searchType?: readonly TracingSearchType[];
    };
    readonly cursor?: string;
    readonly limit: number;
  }): Promise<DorisObservationsPage> {
    if (
      !Number.isSafeInteger(input.limit) ||
      input.limit < 1 ||
      input.limit > MAX_PAGE_SIZE
    ) {
      throw new RangeError("Doris observation page size is invalid");
    }
    const compiled = compileDorisVisibleEventsQuery({
      projectId: input.projectId,
      range: input.range,
      projection: "list",
      filters: input.filters,
      search: input.search,
      cursor: decodeCursor(input.cursor),
      limit: input.limit + 1,
    });
    const rows = await this.dependencies.query<DorisEventRow>(
      compiled.sql,
      compiled.params,
    );
    const items = rows.slice(0, input.limit).map(decodeObservation);
    return {
      items,
      nextCursor:
        rows.length > input.limit && items.length > 0
          ? encodeCursor(items[items.length - 1]!)
          : null,
    };
  }

  async get(input: {
    readonly projectId: string;
    readonly observationId: string;
    readonly traceId?: string;
  }): Promise<DorisObservation | null> {
    const locators = await this.locateObservation({
      projectId: input.projectId,
      observationId: input.observationId,
      traceId: input.traceId,
    });
    if (locators.length === 0) return null;
    if (locators.length > 1) {
      throw new LangfuseConflictError(
        "Observation ID is ambiguous without its trace ID",
      );
    }
    const locator = locators[0]!;
    const from = new Date(`${locator.partitionDate}T00:00:00.000Z`);
    const compiled = compileDorisVisibleEventsQuery({
      projectId: input.projectId,
      range: { from, to: nextUtcDay(locator.partitionDate) },
      projection: "detail",
      filters: [
        {
          type: "stringOptions",
          column: "id",
          operator: "any of",
          value: [input.observationId],
        },
        {
          type: "string",
          column: "traceId",
          operator: "=",
          value: locator.traceId,
        },
      ],
      limit: 2,
    });
    const rows = await this.dependencies.query<DorisEventRow>(
      compiled.sql,
      compiled.params,
    );
    if (rows.length > 1) {
      throw new LangfuseConflictError(
        "Observation locator returned duplicates",
      );
    }
    return rows[0] ? decodeObservation(rows[0]) : null;
  }
}
