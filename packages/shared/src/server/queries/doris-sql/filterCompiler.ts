import { FTS_MATCH_OPERATOR } from "../../../interfaces/filters";
import type { LogicalEventFilter } from "../logical/filterPlan";

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
    if (objectKey) bound.bind(objectKey);
    switch (filter.type) {
      case "string":
      case "stringObject":
        return compileString(expression, filter.operator, filter.value, bound);
      case "datetime":
      case "number":
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
        return `(${expression}) ${filter.operator} ${bound.bind(filter.value)}`;
      case "null":
        return `${expression} ${filter.operator === "is null" ? "IS NULL" : "IS NOT NULL"}`;
      case "categoryOptions":
      case "numberObject":
      case "booleanObject":
      case "positionInTrace":
        throw new TypeError("Unsupported Doris event filter plan");
    }
  });
}
