import { describe, expect, it } from "vitest";

import {
  buildDorisDerivedQuery,
  toDorisSessionEventsRow,
  toDorisSessionMetricsRow,
  toDorisUserMetricsRow,
} from "./derivedUi";

const now = new Date("2026-07-18T00:00:00.000Z");

describe("Doris derived UI query adapter", () => {
  it("maps session columns and extracts an explicit range", () => {
    const result = buildDorisDerivedQuery(
      [
        {
          type: "datetime",
          column: "createdAt",
          operator: ">=",
          value: new Date("2026-07-01T00:00:00.000Z"),
        },
        {
          type: "stringOptions",
          column: "id",
          operator: "any of",
          value: ["session-1"],
        },
        {
          type: "arrayOptions",
          column: "traceTags",
          operator: "all of",
          value: ["prod"],
        },
      ],
      "session",
      now,
    );

    expect(result.range).toEqual({
      from: new Date("2026-07-01T00:00:00.000Z"),
      to: now,
    });
    expect(result.filters).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ column: "startTime" }),
        expect.objectContaining({ column: "sessionId" }),
        expect.objectContaining({ column: "traceTags" }),
      ]),
    );
  });

  it("maps user filters and supplies the bounded retention window", () => {
    const result = buildDorisDerivedQuery(
      [
        {
          type: "stringOptions",
          column: "userId",
          operator: "any of",
          value: ["user-1"],
        },
      ],
      "user",
      now,
    );

    expect(result.range).toEqual({
      from: new Date("2025-07-18T00:00:00.000Z"),
      to: now,
    });
    expect(result.filters).toEqual([
      expect.objectContaining({ column: "userId" }),
    ]);
  });

  it("rejects aggregate filters until their Doris aggregate plan is active", () => {
    expect(() =>
      buildDorisDerivedQuery(
        [
          {
            type: "number",
            column: "totalCost",
            operator: ">",
            value: 1,
          },
        ],
        "session",
        now,
      ),
    ).toThrow("Unsupported Doris session aggregate filter: totalCost");
  });

  it("preserves the existing session rows and metrics contracts", () => {
    const session = {
      id: "session-1",
      projectId: "project-1",
      minTimestamp: new Date("2026-07-17T10:00:00.000Z"),
      maxTimestamp: new Date("2026-07-17T10:00:03.000Z"),
      traceIds: ["trace-1"],
      userIds: ["user-1"],
      environments: ["production"],
      tags: ["api"],
      traceCount: 1,
      observationCount: 2,
      totalInputTokens: 12,
      totalOutputTokens: 6,
      totalUsage: 18,
      totalCost: 0.5,
      duration: 3,
    };

    expect(toDorisSessionEventsRow(session)).toEqual({
      session_id: "session-1",
      min_timestamp: "2026-07-17T10:00:00.000Z",
      max_timestamp: "2026-07-17T10:00:03.000Z",
      trace_ids: ["trace-1"],
      user_ids: ["user-1"],
      trace_count: 1,
      trace_tags: ["api"],
      environment: "production",
    });
    expect(toDorisSessionMetricsRow(session)).toEqual(
      expect.objectContaining({
        session_id: "session-1",
        total_observations: 2,
        duration: 3,
        session_input_usage: "12",
        session_output_usage: "6",
        session_total_usage: "18",
        session_total_cost: "0.5",
      }),
    );
  });

  it("preserves the existing user metrics contract", () => {
    expect(
      toDorisUserMetricsRow({
        id: "user-1",
        projectId: "project-1",
        minTimestamp: new Date("2026-07-17T10:00:00.000Z"),
        maxTimestamp: new Date("2026-07-17T10:00:03.000Z"),
        sessionIds: ["session-1"],
        environments: ["production"],
        traceCount: 2,
        sessionCount: 1,
        observationCount: 3,
        totalInputTokens: 12,
        totalOutputTokens: 6,
        totalUsage: 18,
        totalCost: null,
      }),
    ).toEqual({
      userId: "user-1",
      environment: "production",
      minTimestamp: new Date("2026-07-17T10:00:00.000Z"),
      maxTimestamp: new Date("2026-07-17T10:00:03.000Z"),
      inputUsage: 12,
      outputUsage: 6,
      totalUsage: 18,
      observationCount: 3,
      traceCount: 2,
      totalCost: 0,
    });
  });
});
