import type { LogicalSearchPlan } from "../logical/searchPlan";

type BoundParameters = {
  bind(value: unknown): string;
};

function contains(
  expression: string,
  query: string,
  bound: BoundParameters,
): string {
  const pattern = query
    .toLowerCase()
    .replaceAll("\\", "\\\\")
    .replaceAll("%", "\\%")
    .replaceAll("_", "\\_");
  return `LOWER(COALESCE(${expression}, '')) LIKE ${bound.bind(`%${pattern}%`)} ESCAPE '\\\\'`;
}

function toJsonUnicodeEscaped(value: string): string {
  let escaped = "";
  for (const character of value) {
    const codePoint = character.codePointAt(0)!;
    if (codePoint < 0x80) {
      escaped += character;
    } else if (codePoint <= 0xffff) {
      escaped += `\\u${codePoint.toString(16).padStart(4, "0")}`;
    } else {
      const surrogate = codePoint - 0x10000;
      escaped += `\\u${(0xd800 + (surrogate >> 10)).toString(16).padStart(4, "0")}`;
      escaped += `\\u${(0xdc00 + (surrogate & 0x3ff)).toString(16).padStart(4, "0")}`;
    }
  }
  return escaped;
}

function containsIo(
  expression: string,
  query: string,
  bound: BoundParameters,
): string {
  const escaped = toJsonUnicodeEscaped(query);
  const variants = escaped === query ? [query] : [query, escaped];
  return `(${variants
    .map((variant) => contains(expression, variant, bound))
    .join(" OR ")})`;
}

export function compileDorisSearch(
  plan: LogicalSearchPlan | null,
  bound: BoundParameters,
  eventAlias = "e",
): string | null {
  if (!plan) return null;
  const clauses: string[] = [];
  if (plan.searchType.includes("id")) {
    clauses.push(
      [
        `${eventAlias}.span_id`,
        `${eventAlias}.trace_id`,
        `${eventAlias}.user_id`,
        `${eventAlias}.\`name\``,
      ]
        .map((column) => contains(column, plan.query, bound))
        .join(" OR "),
    );
  }
  if (plan.searchType.includes("content")) {
    clauses.push(
      [`${eventAlias}.input`, `${eventAlias}.output`]
        .map((column) => containsIo(column, plan.query, bound))
        .join(" OR "),
    );
  }
  if (plan.searchType.includes("input")) {
    clauses.push(containsIo(`${eventAlias}.input`, plan.query, bound));
  }
  if (plan.searchType.includes("output")) {
    clauses.push(containsIo(`${eventAlias}.output`, plan.query, bound));
  }
  return clauses.length > 0 ? `(${clauses.join(" OR ")})` : null;
}
