import { FTS_MATCH_OPERATOR } from "../../../interfaces/filters";
import type {
  LogicalEventFilter,
  LogicalEventScoreFilter,
} from "../logical/filterPlan";

type BoundParameters = {
  readonly params: unknown[];
  bind(value: unknown): string;
};

function compileString(
  expression: string,
  operator: string,
  value: string,
  bound: BoundParameters,
): string {
  switch (operator) {
    case "=":
      return `${expression} = ${bound.bind(value)}`;
    case "contains":
    case FTS_MATCH_OPERATOR:
      return `LOCATE(${bound.bind(value)}, COALESCE(${expression}, '')) > 0`;
    case "does not contain":
      return `LOCATE(${bound.bind(value)}, COALESCE(${expression}, '')) = 0`;
    case "starts with":
      return `LOCATE(${bound.bind(value)}, COALESCE(${expression}, '')) = 1`;
    case "ends with": {
      const lengthValue = bound.bind(value);
      const comparisonValue = bound.bind(value);
      return `RIGHT(COALESCE(${expression}, ''), CHAR_LENGTH(${lengthValue})) = ${comparisonValue}`;
    }
    default:
      throw new TypeError("Unsupported Doris string filter operator");
  }
}

export function compileDorisEventFilters(
  plans: readonly LogicalEventFilter[],
  bound: BoundParameters,
): readonly string[] {
  return plans.map(({ filter, expression, objectKey }) => {
    if (objectKey !== undefined) bound.bind(objectKey);
    switch (filter.type) {
      case "string":
      case "stringObject":
        return compileString(expression, filter.operator, filter.value, bound);
      case "datetime":
      case "number":
        return `${expression} ${filter.operator} ${bound.bind(filter.value)}`;
      case "numberObject":
        return `${expression} ${filter.operator} ${bound.bind(filter.value)}`;
      case "stringOptions": {
        const values = filter.value
          .map((value) => bound.bind(value))
          .join(", ");
        return `${expression} ${filter.operator === "any of" ? "IN" : "NOT IN"} (${values})`;
      }
      case "arrayOptions": {
        const matches = filter.value.map(
          (value) => `ARRAY_CONTAINS(${expression}, ${bound.bind(value)})`,
        );
        if (matches.length === 0) {
          return filter.operator === "all of" || filter.operator === "none of"
            ? "TRUE"
            : "FALSE";
        }
        if (filter.operator === "all of") return `(${matches.join(" AND ")})`;
        if (filter.operator === "any of") return `(${matches.join(" OR ")})`;
        return `NOT (${matches.join(" OR ")})`;
      }
      case "boolean":
      case "booleanObject":
        return `(${expression}) ${filter.operator} ${bound.bind(filter.value)}`;
      case "null":
        return `${expression} ${filter.operator === "is null" ? "IS NULL" : "IS NOT NULL"}`;
      case "categoryOptions": {
        if (filter.value.length === 0) {
          return filter.operator === "none of" ? "TRUE" : "FALSE";
        }
        const values = filter.value
          .map((value) => bound.bind(value))
          .join(", ");
        return `${expression} ${filter.operator === "any of" ? "IN" : "NOT IN"} (${values})`;
      }
      case "positionInTrace":
        throw new TypeError("Unsupported Doris event filter plan");
    }
  });
}

function scoreLookbackStart(
  from: Date,
  level: LogicalEventScoreFilter["level"],
): Date {
  const lookbackHours = level === "trace" ? 49 : 1;
  return new Date(from.getTime() - lookbackHours * 60 * 60 * 1_000);
}

function utcDate(value: Date): string {
  return value.toISOString().slice(0, 10);
}

/** 将 score-map 过滤器编译为相关半连接或反半连接。 */
export function compileDorisEventScoreFilters(input: {
  readonly plans: readonly LogicalEventScoreFilter[];
  readonly eventAlias: string;
  readonly rangeFrom: Date;
  readonly bound: BoundParameters;
}): readonly string[] {
  return input.plans.map(({ filter, level }) => {
    const lowerBound = scoreLookbackStart(input.rangeFrom, level);
    const predicates = [
      `score_filter.project_id = ${input.eventAlias}.project_id`,
      `score_filter.trace_id = ${input.eventAlias}.trace_id`,
      level === "trace"
        ? "score_filter.observation_id IS NULL"
        : `score_filter.observation_id = ${input.eventAlias}.span_id`,
      `score_filter.score_date >= ${input.bound.bind(utcDate(lowerBound))}`,
      `score_filter.\`timestamp\` >= ${input.bound.bind(lowerBound)}`,
      `score_filter.\`name\` = ${input.bound.bind(filter.key)}`,
    ];

    if (filter.type === "numberObject") {
      const value = input.bound.bind(filter.value);
      return `EXISTS (
  SELECT 1
  FROM scores_current score_filter
  WHERE ${predicates.join("\n    AND ")}
    AND score_filter.data_type IN ('NUMERIC', 'BOOLEAN')
  GROUP BY score_filter.project_id, score_filter.trace_id, score_filter.observation_id, score_filter.\`name\`
  HAVING AVG(score_filter.\`value\`) ${filter.operator} ${value}
)`;
    }

    if (filter.type === "categoryOptions") {
      if (filter.value.length === 0) {
        return filter.operator === "none of" ? "TRUE" : "FALSE";
      }
      const values = filter.value
        .map((value) => input.bound.bind(value))
        .join(", ");
      const exists = `EXISTS (
  SELECT 1
  FROM scores_current score_filter
  WHERE ${predicates.join("\n    AND ")}
    AND score_filter.data_type IN ('CATEGORICAL', 'TEXT')
    AND score_filter.string_value IN (${values})
)`;
      return filter.operator === "none of" ? `NOT ${exists}` : exists;
    }

    const expected = input.bound.bind(filter.value);
    const exists = `EXISTS (
  SELECT 1
  FROM scores_current score_filter
  WHERE ${predicates.join("\n    AND ")}
    AND score_filter.data_type = 'BOOLEAN'
    AND score_filter.boolean_value = ${expected}
)`;
    return filter.operator === "<>" ? `NOT ${exists}` : exists;
  });
}
