import { describe, expect, it } from "vitest";

import type { EventsTableFilterState } from "../../../types";
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
    expect(compiled.sql).toContain(
      "COALESCE(e.usage_details_json, CAST(e.usage_details AS STRING)) AS usage_details",
    );
  });

  it("reads exact dynamic JSON for detail responses with a legacy fallback", () => {
    const compiled = compileDorisVisibleEventsQuery({
      projectId: "project-1",
      range,
      projection: "detail",
      filters: [],
      limit: 10,
    });

    expect(compiled.sql).toContain(
      "COALESCE(e.metadata_json, CAST(e.metadata AS STRING)) AS metadata",
    );
    expect(compiled.sql).toContain(
      "COALESCE(e.tool_definitions_json, CAST(e.tool_definitions AS STRING)) AS tool_definitions",
    );
  });

  it("compiles an allowlisted order with stable tie-breakers and offset", () => {
    const compiled = compileDorisVisibleEventsQuery({
      projectId: "project-1",
      range,
      projection: "list",
      filters: [],
      orderBy: { column: "totalCost", order: "ASC" },
      offset: 40,
      limit: 20,
    });

    expect(compiled.sql).toContain(
      "ORDER BY e.total_cost ASC, e.start_time ASC, e.trace_id ASC, e.span_id ASC",
    );
    expect(compiled.sql).toContain("LIMIT ? OFFSET ?");
    expect(compiled.params.slice(-2)).toEqual([20, 40]);
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
    expect(compiled.sql).toContain(
      "MICROSECONDS_DIFF(e.end_time, e.start_time) / 1000000.0",
    );
    expect(compiled.sql).not.toContain("TIMESTAMPDIFF(MICROSECOND");
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
    ["stringObject", "CAST(ELEMENT_AT(e.metadata, ?) AS STRING)", "eu"],
    ["numberObject", "CAST(ELEMENT_AT(e.metadata, ?) AS DOUBLE)", 0.75],
    ["booleanObject", "CAST(ELEMENT_AT(e.metadata, ?) AS BOOLEAN)", true],
    ["categoryOptions", "CAST(ELEMENT_AT(e.metadata, ?) AS STRING)", ["a"]],
  ] as const)(
    "compiles typed %s event metadata filters with a bound key",
    (type, expectedExpression, value) => {
      const filter =
        type === "categoryOptions"
          ? {
              type,
              column: "metadata",
              key: 'nested."quoted"',
              operator: "any of" as const,
              value: [...value],
            }
          : {
              type,
              column: "metadata",
              key: 'nested."quoted"',
              operator: "=" as const,
              value,
            };
      const compiled = compileDorisVisibleEventsQuery({
        projectId: "project-1",
        range,
        projection: "list",
        filters: [filter as EventsTableFilterState[number]],
        limit: 10,
      });

      expect(compiled.sql).toContain(expectedExpression);
      expect(compiled.sql).not.toContain('nested."quoted"');
      expect(compiled.params).toContain('nested."quoted"');
    },
  );

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

  it("pushes typed observation and trace score filters into correlated Doris subqueries", () => {
    const compiled = compileDorisVisibleEventsQuery({
      projectId: "project-1",
      range,
      projection: "list",
      filters: [
        {
          type: "numberObject",
          column: "scores_avg",
          key: "quality",
          operator: ">",
          value: 0.5,
        },
        {
          type: "categoryOptions",
          column: "trace_score_categories",
          key: "topic",
          operator: "none of",
          value: ["unsafe", "unknown"],
        },
        {
          type: "booleanObject",
          column: "score_booleans",
          key: "approved",
          operator: "<>",
          value: true,
        },
      ],
      limit: 10,
    });

    expect(compiled.sql).toContain("FROM scores_current score_filter");
    expect(compiled.sql).toContain("score_filter.observation_id = e.span_id");
    expect(compiled.sql).toContain("score_filter.observation_id IS NULL");
    expect(compiled.sql).toContain("AVG(score_filter.`value`) > ?");
    expect(compiled.sql).toContain("NOT EXISTS (");
    expect(compiled.sql).not.toContain("quality");
    expect(compiled.params).toEqual(
      expect.arrayContaining([
        "project-1",
        "quality",
        0.5,
        "topic",
        "unsafe",
        "unknown",
        "approved",
        true,
      ]),
    );
  });

  it("rejects event filters that exceed the shared Doris resource budget", () => {
    expect(() =>
      compileDorisVisibleEventsQuery({
        projectId: "project-1",
        range,
        projection: "list",
        filters: Array.from({ length: 101 }, (_, index) => ({
          type: "string" as const,
          column: "name",
          operator: "=" as const,
          value: `name-${index}`,
        })),
        limit: 10,
      }),
    ).toThrow("too many filters");

    expect(() =>
      compileDorisVisibleEventsQuery({
        projectId: "project-1",
        range,
        projection: "list",
        filters: [
          {
            type: "stringOptions",
            column: "name",
            operator: "any of",
            value: Array.from({ length: 1_001 }, (_, index) => `name-${index}`),
          },
        ],
        limit: 10,
      }),
    ).toThrow("too many filter values");

    expect(() =>
      compileDorisVisibleEventsQuery({
        projectId: "project-1",
        range,
        projection: "list",
        filters: [
          {
            type: "stringObject",
            column: "metadata",
            key: Array.from({ length: 17 }, () => "nested").join("."),
            operator: "=",
            value: "eu",
          },
        ],
        limit: 10,
      }),
    ).toThrow("object key");
  });

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
