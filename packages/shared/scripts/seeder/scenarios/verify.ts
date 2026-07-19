import { getDorisQueryExecutor } from "../../../src/server";
import { ScenarioContext } from "./types";

/**
 * Cheap post-write readback. The logical table names are retained so existing
 * scenarios stay readable while all physical reads target Doris R1A tables.
 */
export const countRows = async (
  table: string,
  whereSql: string,
  params: Record<string, string | number | string[]>,
  countExpr = "count()",
): Promise<number> => {
  const physicalTable =
    table === "scores" ? "scores_current" : "events_current";
  const idColumn =
    table === "traces"
      ? "trace_id"
      : table === "scores"
        ? "score_id"
        : "span_id";
  const parameterValues: unknown[] = [];
  const replaceParameters = (sql: string) =>
    sql.replace(/\{([A-Za-z0-9_]+): [^}]+\}/g, (_match, name: string) => {
      parameterValues.push(params[name]!);
      return "?";
    });
  const metadataExpression = /metadata\[\{([A-Za-z0-9_]+): String\}\]/g;
  const normalizeExpression = (sql: string) =>
    sql
      .replace(metadataExpression, (_match, name: string) => {
        parameterValues.push(params[name]!);
        return "JSON_UNQUOTE(CAST(ELEMENT_AT(metadata, ?) AS STRING))";
      })
      .replace(/\bid\b/g, idColumn);
  const normalizedCount = countExpr
    .replace(/^count\(\)$/i, "COUNT(*)")
    .replace(/^uniqExact\((.+)\)$/i, "COUNT(DISTINCT $1)");
  const select = replaceParameters(normalizeExpression(normalizedCount));
  const where = replaceParameters(normalizeExpression(whereSql));
  const rows = await getDorisQueryExecutor().query<{ c: string | number }>(
    `SELECT ${select} AS c FROM ${physicalTable} WHERE ${where}`,
    parameterValues,
  );
  return Number(rows[0]?.c ?? 0);
};

/** Escapes LIKE-special characters so id prefixes match literally. */
export const escapeLike = (value: string): string =>
  value.replace(/\\/g, "\\\\").replace(/[%_]/g, (match) => `\\${match}`);

export const traceLink = (
  ctx: ScenarioContext,
  traceId: string,
  timestampMs: number,
): string =>
  `${ctx.baseUrl}/project/${ctx.projectId}/traces/${encodeURIComponent(traceId)}?timestamp=${encodeURIComponent(new Date(timestampMs).toISOString())}`;

export const sessionLink = (ctx: ScenarioContext, sessionId: string): string =>
  `${ctx.baseUrl}/project/${ctx.projectId}/sessions/${encodeURIComponent(sessionId)}`;

export const tracesListLink = (ctx: ScenarioContext): string =>
  `${ctx.baseUrl}/project/${ctx.projectId}/traces`;
