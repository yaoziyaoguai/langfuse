import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  processTrace: vi.fn().mockResolvedValue(undefined),
}));

vi.mock("./processAnalyticsTraceDelete", () => ({
  processAnalyticsTraceDelete: mocks.processTrace,
}));

import { processAnalyticsTraceDeletionBatch } from "./processAnalyticsTraceDeletionBatch";

const analyticsProvenance = {
  analyticsBackend: "DORIS" as const,
  deploymentGeneration: "7",
  workloadEpochFingerprint: "a".repeat(64),
  runtimeContractVersion: 3,
  producerRuntimeLeaseId: "runtime-original",
};

describe("processAnalyticsTraceDeletionBatch", () => {
  beforeEach(() => vi.clearAllMocks());

  it("forwards the exact authoritative queue provenance", async () => {
    await processAnalyticsTraceDeletionBatch({
      projectId: "project-1",
      traceIds: ["trace-1"],
      deletionOperations: [
        {
          operationId: "operation-1",
          traceId: "trace-1",
          generation: "1",
          analyticsProvenance,
        },
      ],
    });

    expect(mocks.processTrace).toHaveBeenCalledWith("project-1", {
      operationId: "operation-1",
      traceId: "trace-1",
      generation: 1n,
      analyticsProvenance,
    });
  });

  it("never creates or restamps a missing deletion reference", async () => {
    await expect(
      processAnalyticsTraceDeletionBatch({
        projectId: "project-1",
        traceIds: ["trace-1"],
        deletionOperations: [],
      }),
    ).rejects.toThrow("Managed trace deletion queue references are missing");
    expect(mocks.processTrace).not.toHaveBeenCalled();
  });
});
