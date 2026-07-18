import { describe, expect, it } from "vitest";

import { AnalyticsQueryValidationError } from "../logical/searchPlan";
import {
  compileDorisVisibleEventScope,
  compileDorisVisibleEventsQuery,
} from "./eventQueryCompiler";

const range = {
  from: new Date("2026-07-01T00:00:00.000Z"),
  to: new Date("2026-07-08T00:00:00.000Z"),
};

describe("Doris event query compiler", () => {
  it("exposes a reusable bounded scope without projection, order, or limit", () => {
    const scope = compileDorisVisibleEventScope({
      projectId: "project-1",
      range,
      filters: [
        {
          type: "stringOptions",
          column: "environment",
          operator: "any of",
          value: ["production"],
        },
      ],
      search: { query: "trace", searchType: ["id"] },
    });

    expect(scope.fromSql).toContain("LEFT JOIN trace_tombstones");
    expect(scope.whereSql).toContain("e.project_id = ?");
    expect(scope.whereSql).toContain("e.environment IN (?)");
    expect(scope.whereSql).not.toContain("ORDER BY");
    expect(scope.whereSql).not.toContain("LIMIT");
    expect(scope.params).toEqual(
      expect.arrayContaining(["project-1", "production", "%trace%"]),
    );
  });

  it("binds tenant, range, filter, search, and pagination values", () => {
    const injection = "x' OR 1=1 --";
    const compiled = compileDorisVisibleEventsQuery({
      projectId: "project-1",
      range,
      projection: "list",
      filters: [
        {
          type: "string",
          column: "name",
          operator: "contains",
          value: injection,
        },
        {
          type: "stringObject",
          column: "metadata",
          key: 'region."quoted"',
          operator: "=",
          value: "eu",
        },
      ],
      search: { query: "退款", searchType: ["id", "content"] },
      cursor: {
        startTime: new Date("2026-07-07T12:00:00.000Z"),
        traceId: "trace-cursor",
        spanId: "span-cursor",
      },
      limit: 51,
    });

    expect(compiled.sql).toContain("e.project_id = ?");
    expect(compiled.sql).toContain("e.partition_date >= ?");
    expect(compiled.sql).toContain("e.start_time >= ?");
    expect(compiled.sql).toContain("LEFT JOIN trace_tombstones");
    expect(compiled.sql).toContain("LEFT JOIN project_tombstones");
    expect(compiled.sql).toContain(
      "JSON_UNQUOTE(CAST(ELEMENT_AT(e.metadata, ?) AS STRING))",
    );
    expect(compiled.sql).toContain(
      "ORDER BY e.start_time DESC, e.trace_id DESC, e.span_id DESC",
    );
    expect(compiled.sql).not.toContain(injection);
    expect(compiled.params).toContain(injection);
    expect(compiled.params).toContain('region."quoted"');
    expect(compiled.params).toContain("%退款%");
    expect(compiled.params).toContain("%\\\\u9000\\\\u6b3e%");
    expect(compiled.params.at(-1)).toBe(51);
    expect(compiled.selectsFullContent).toBe(true);
    expect(compiled.sql).not.toContain("e.input AS input,");
    expect(compiled.sql).toContain("e.input_preview AS input_preview");
  });

  it("keeps a list projection narrow when no content predicate is requested", () => {
    const compiled = compileDorisVisibleEventsQuery({
      projectId: "project-1",
      range,
      projection: "list",
      filters: [],
      limit: 10,
    });

    expect(compiled.selectsFullContent).toBe(false);
    expect(compiled.sql).not.toContain("e.input AS input");
    expect(compiled.sql).not.toContain("e.output AS output");
  });

  it("compiles every storage-local filter family with bound values", () => {
    const compiled = compileDorisVisibleEventsQuery({
      projectId: "project-1",
      range,
      projection: "list",
      filters: [
        {
          type: "datetime",
          column: "startTime",
          operator: ">=",
          value: range.from,
        },
        {
          type: "number",
          column: "latency",
          operator: ">",
          value: 1,
        },
        {
          type: "number",
          column: "toolDefinitions",
          operator: ">=",
          value: 2,
        },
        {
          type: "stringOptions",
          column: "type",
          operator: "any of",
          value: ["GENERATION", "SPAN"],
        },
        {
          type: "arrayOptions",
          column: "toolNames",
          operator: "none of",
          value: ["dangerous"],
        },
        {
          type: "boolean",
          column: "hasInput",
          operator: "=",
          value: true,
        },
        {
          type: "null",
          column: "endTime",
          operator: "is null",
          value: "",
        },
      ],
      limit: 10,
    });

    expect(compiled.sql).toContain(
      "CARDINALITY(JSON_KEYS(e.tool_definitions))",
    );
    expect(compiled.sql).toContain("NOT (ARRAY_CONTAINS(");
    expect(compiled.sql).toContain(
      "(e.input IS NOT NULL AND e.input != '') = ?",
    );
    expect(compiled.sql).toContain("e.end_time IS NULL");
    expect(compiled.params).toEqual(
      expect.arrayContaining([1, 2, "GENERATION", "SPAN", "dangerous", true]),
    );
  });

  it.each([
    ["root", "ASC", 1],
    ["first", "ASC", 1],
    ["last", "DESC", 1],
    ["nthFromStart", "ASC", 3],
    ["nthFromEnd", "DESC", 3],
  ] as const)(
    "ranks the %s position after the other bounded filters",
    (key, direction, position) => {
      const compiled = compileDorisVisibleEventsQuery({
        projectId: "project-1",
        range,
        projection: "list",
        filters: [
          {
            type: "stringOptions",
            column: "environment",
            operator: "any of",
            value: ["production"],
          },
          {
            type: "positionInTrace",
            column: "startTime",
            operator: "=",
            key,
            ...(key === "nthFromStart" || key === "nthFromEnd"
              ? { value: position }
              : {}),
          },
        ],
        limit: 10,
      });

      expect(compiled.sql).toContain(
        "PARTITION BY position_event.project_id, position_event.trace_id",
      );
      expect(compiled.sql).toContain(
        `ORDER BY position_event.start_time ${direction}`,
      );
      expect(compiled.sql).toContain("WHERE _position_rank = ?");
      expect(compiled.sql.match(/environment IN \(\?\)/g)).toHaveLength(2);
      expect(compiled.params).toContain(position);
      expect(compiled.sql).not.toContain("position_event.input AS input");
    },
  );

  it("rejects unbounded and over-30-day full-content predicates consistently", () => {
    expect(() =>
      compileDorisVisibleEventsQuery({
        projectId: "project-1",
        range: {
          from: new Date("2026-01-01T00:00:00.000Z"),
          to: new Date("2026-02-01T00:00:00.001Z"),
        },
        projection: "list",
        filters: [],
        search: { query: "needle", searchType: ["content"] },
        limit: 10,
      }),
    ).toThrow(
      expect.objectContaining({
        code: "InvalidTimeRange",
        maxDays: 30,
      }) as AnalyticsQueryValidationError,
    );

    expect(() =>
      compileDorisVisibleEventsQuery({
        projectId: "project-1",
        range: null,
        projection: "list",
        filters: [],
        limit: 10,
      }),
    ).toThrow(expect.objectContaining({ code: "InvalidTimeRange" }));
  });
});
