import type { AnalyticsDeletionOperation } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import { processAnalyticsDeletionRecoveryOperation } from "./processOperation";

function operation(scope: "TRACE" | "PROJECT"): AnalyticsDeletionOperation {
  const now = new Date("2026-07-18T00:00:00.000Z");
  return {
    id: `operation-${scope.toLowerCase()}`,
    scope,
    organizationId: "org-1",
    projectId: "project-1",
    traceId: scope === "TRACE" ? "trace-1" : null,
    generation: 7n,
    checkpointGeneration: 0n,
    workerFence: 0n,
    leaseOwner: null,
    leaseExpiresAt: null,
    requesterPrincipalType: "system",
    requesterPrincipalId: "test",
    status: "RETRYING",
    phase: "materialized_cleanup",
    logicallyInvisible: true,
    cancellationReasonCode: null,
    statusExpiresAt: new Date("2026-08-18T00:00:00.000Z"),
    completedAt: null,
    createdAt: now,
    updatedAt: now,
  };
}

describe("processAnalyticsDeletionRecoveryOperation", () => {
  it("uses the trace deletion workflow and shared pending-deletion bookkeeping", async () => {
    const processTrace = vi.fn().mockResolvedValue(undefined);
    const markPendingTraceCompleted = vi.fn().mockResolvedValue(1);
    const processProject = vi.fn();

    await processAnalyticsDeletionRecoveryOperation(operation("TRACE"), {
      processTrace,
      processProject,
      markPendingTraceCompleted,
    });

    expect(processTrace).toHaveBeenCalledWith("project-1", {
      operationId: "operation-trace",
      traceId: "trace-1",
      generation: 7n,
    });
    expect(markPendingTraceCompleted).toHaveBeenCalledWith({
      projectId: "project-1",
      traceIds: ["trace-1"],
    });
    expect(processProject).not.toHaveBeenCalled();
  });

  it("uses the project deletion workflow without trace bookkeeping", async () => {
    const processTrace = vi.fn();
    const markPendingTraceCompleted = vi.fn();
    const processProject = vi.fn().mockResolvedValue(undefined);

    await processAnalyticsDeletionRecoveryOperation(operation("PROJECT"), {
      processTrace,
      processProject,
      markPendingTraceCompleted,
    });

    expect(processProject).toHaveBeenCalledWith({
      projectId: "project-1",
      organizationId: "org-1",
      reference: { operationId: "operation-project", generation: 7n },
    });
    expect(processTrace).not.toHaveBeenCalled();
    expect(markPendingTraceCompleted).not.toHaveBeenCalled();
  });
});
