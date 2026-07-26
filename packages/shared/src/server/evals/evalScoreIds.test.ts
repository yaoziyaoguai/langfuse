import { describe, expect, it } from "vitest";
import {
  buildDeterministicEvalScoreIds,
  createDeterministicEvalScoreId,
} from "./evalScoreIds";

describe("eval score ids", () => {
  it("preserves the UUIDv5 IDs produced by the original implementation", () => {
    expect(
      createDeterministicEvalScoreId({
        jobExecutionId: "job-123",
        scoreName: "accuracy",
        occurrenceIndex: 0,
      }),
    ).toBe("cac6815d-61dc-54e5-b520-8a1ff3f93eb4");

    expect(
      createDeterministicEvalScoreId({
        jobExecutionId: "job-123",
        scoreName: "accuracy",
        occurrenceIndex: 1,
      }),
    ).toBe("1652475b-f8d7-5509-a9e6-ffb8706af455");
  });

  it("increments occurrences independently for each score name", () => {
    const scoreIds = buildDeterministicEvalScoreIds({
      jobExecutionId: "job-123",
      scores: [
        { name: "accuracy", value: 1 },
        { name: "relevance", value: 0.8 },
        { name: "accuracy", value: 0.9 },
      ],
    });

    expect(scoreIds).toEqual([
      "cac6815d-61dc-54e5-b520-8a1ff3f93eb4",
      "80487174-825d-5e08-aa05-2a13e7409220",
      "1652475b-f8d7-5509-a9e6-ffb8706af455",
    ]);
  });
});
