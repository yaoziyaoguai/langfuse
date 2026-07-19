import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  deleteHeads: vi.fn(),
  findMany: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    analyticsEntityHead: { findMany: mocks.findMany },
  },
}));

vi.mock("../../services/dorisAnalyticsLifecycle", () => ({
  getDorisAnalyticsLifecycleRuntime: () => ({
    materializedDeletion: { deleteHeads: mocks.deleteHeads },
  }),
}));

import { processAnalyticsScoreDelete } from "./processAnalyticsScoreDelete";

describe("processAnalyticsScoreDelete", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("loads terminal deletes for the deduplicated Doris entity heads", async () => {
    const heads = [{ id: "head-1" }, { id: "head-2" }];
    mocks.findMany.mockResolvedValue(heads);

    await processAnalyticsScoreDelete("project-1", [
      "score-2",
      "score-1",
      "score-2",
    ]);

    expect(mocks.findMany).toHaveBeenCalledWith({
      where: {
        projectId: "project-1",
        entityType: "SCORE",
        lookupId: { in: ["score-1", "score-2"] },
      },
    });
    expect(mocks.deleteHeads).toHaveBeenCalledWith(
      expect.stringMatching(/^score-delete-[a-f0-9]{64}$/),
      heads,
    );
  });

  it("does not call Doris when no matching head exists", async () => {
    mocks.findMany.mockResolvedValue([]);

    await processAnalyticsScoreDelete("project-1", ["unknown-score"]);

    expect(mocks.deleteHeads).not.toHaveBeenCalled();
  });
});
