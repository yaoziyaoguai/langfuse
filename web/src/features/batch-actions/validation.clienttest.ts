import { BatchEvalSourceTable, type BatchActionQuery } from "@langfuse/shared";
import { describe, expect, it } from "vitest";

import { scopeBatchEvaluationQuery } from "./validation";

const query: BatchActionQuery = {
  filter: [],
  orderBy: { column: "startTime", order: "DESC" },
};

describe("scopeBatchEvaluationQuery", () => {
  it("does not change ordinary event evaluation queries", () => {
    expect(scopeBatchEvaluationQuery(query, BatchEvalSourceTable.EVENTS)).toBe(
      query,
    );
  });

  it("forces experiment evaluation onto experiment item root spans", () => {
    expect(
      scopeBatchEvaluationQuery(query, BatchEvalSourceTable.EXPERIMENTS),
    ).toEqual({
      ...query,
      filter: [
        {
          column: "isExperimentItemRootSpan",
          operator: "=",
          value: true,
          type: "boolean",
        },
      ],
    });
  });

  it("keeps an already scoped experiment query unchanged", () => {
    const scopedQuery: BatchActionQuery = {
      ...query,
      filter: [
        {
          column: "isExperimentItemRootSpan",
          operator: "=",
          value: true,
          type: "boolean",
        },
      ],
    };

    expect(
      scopeBatchEvaluationQuery(scopedQuery, BatchEvalSourceTable.EXPERIMENTS),
    ).toBe(scopedQuery);
  });

  it("intersects a conflicting caller filter with the required root scope", () => {
    const conflictingQuery: BatchActionQuery = {
      ...query,
      filter: [
        {
          column: "isExperimentItemRootSpan",
          operator: "=",
          value: false,
          type: "boolean",
        },
      ],
    };

    expect(
      scopeBatchEvaluationQuery(
        conflictingQuery,
        BatchEvalSourceTable.EXPERIMENT_ITEMS,
      ).filter,
    ).toEqual([
      conflictingQuery.filter?.[0],
      {
        column: "isExperimentItemRootSpan",
        operator: "=",
        value: true,
        type: "boolean",
      },
    ]);
  });
});
