import type { PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import { processDorisProjectRetention } from "./processDorisProjectRetention";

function client(input: {
  retentionDays: number | null;
  activeRunId?: string | null;
  activeCutoff?: Date | null;
  activePhase?: string | null;
}) {
  return {
    project: {
      findUnique: vi.fn(async () => ({
        retentionDays: input.retentionDays,
      })),
    },
    analyticsRetentionState: {
      findUnique: vi.fn(async () => ({
        activeRunId: input.activeRunId ?? null,
        activeCutoff: input.activeCutoff ?? null,
        activeRun: input.activePhase ? { phase: input.activePhase } : null,
      })),
    },
  } as unknown as PrismaClient;
}

describe("processDorisProjectRetention", () => {
  it("does not create purge work when project retention is disabled", async () => {
    const processStep = vi.fn();

    await expect(
      processDorisProjectRetention(
        {
          projectId: "project-1",
          queuedRetentionDays: 7,
          admissionContext: null,
        },
        {
          client: client({ retentionDays: null }),
          processStep,
          scheduleContinuation: vi.fn(),
        },
      ),
    ).resolves.toEqual({ outcome: "idle" });
    expect(processStep).not.toHaveBeenCalled();
  });

  it("uses the current setting and schedules the next bounded step", async () => {
    const processStep = vi.fn(async () => ({
      outcome: "processed" as const,
      runId: "run-1",
      phase: "EVENTS" as const,
      cutoffDate: new Date("2026-07-01T00:00:00.000Z"),
    }));
    const scheduleContinuation = vi.fn(async () => undefined);
    const deleteDorisHeads = vi.fn(async () => undefined);
    const onCutoffPublished = vi.fn(async () => undefined);
    const admissionContext = {
      runtimeLeaseId: "lease-1",
      backend: "doris" as const,
      deploymentGeneration: 7n,
    };

    await processDorisProjectRetention(
      {
        projectId: "project-1",
        queuedRetentionDays: 7,
        admissionContext,
      },
      {
        client: client({ retentionDays: 30 }),
        processStep,
        deleteDorisHeads,
        onCutoffPublished,
        scheduleContinuation,
      },
    );

    expect(processStep).toHaveBeenCalledWith(
      expect.objectContaining({
        retentionDays: 30,
        scope: {
          stateId: "project:project-1",
          projectId: "project-1",
        },
        admissionContext,
        dependencies: expect.objectContaining({ deleteDorisHeads }),
      }),
    );
    expect(scheduleContinuation).toHaveBeenCalledWith({
      projectId: "project-1",
      retentionDays: 30,
      delayMs: 0,
    });
    expect(onCutoffPublished).not.toHaveBeenCalled();
  });

  it("continues a published cutoff after retention is disabled", async () => {
    const cutoffDate = new Date("2026-07-01T00:00:00.000Z");
    const order: string[] = [];
    const processStep = vi.fn(async () => {
      order.push("process");
      return {
        outcome: "waiting" as const,
        runId: "run-1",
        phase: "DRAIN" as const,
        cutoffDate,
      };
    });
    const scheduleContinuation = vi.fn(async () => undefined);
    const onCutoffPublished = vi.fn(async () => {
      order.push("media");
    });

    await processDorisProjectRetention(
      {
        projectId: "project-1",
        queuedRetentionDays: 7,
        admissionContext: null,
      },
      {
        client: client({
          retentionDays: null,
          activeRunId: "run-1",
          activeCutoff: cutoffDate,
          activePhase: "DRAIN",
        }),
        processStep,
        onCutoffPublished,
        scheduleContinuation,
      },
    );

    expect(processStep).toHaveBeenCalledWith(
      expect.objectContaining({ retentionDays: 7 }),
    );
    expect(scheduleContinuation).toHaveBeenCalledWith(
      expect.objectContaining({ delayMs: 60_000 }),
    );
    expect(onCutoffPublished).toHaveBeenCalledWith({
      projectId: "project-1",
      cutoffDate,
    });
    expect(order).toEqual(["media", "process"]);
  });

  it("does not schedule another job after the durable run completes", async () => {
    const processStep = vi.fn(async () => ({
      outcome: "completed" as const,
      runId: "run-1",
      phase: "COMPLETE" as const,
      cutoffDate: new Date("2026-07-01T00:00:00.000Z"),
    }));
    const scheduleContinuation = vi.fn(async () => undefined);

    await processDorisProjectRetention(
      {
        projectId: "project-1",
        queuedRetentionDays: 7,
        admissionContext: null,
      },
      {
        client: client({ retentionDays: 7, activeRunId: "run-1" }),
        processStep,
        scheduleContinuation,
      },
    );

    expect(scheduleContinuation).not.toHaveBeenCalled();
  });
});
