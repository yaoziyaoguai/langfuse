import type { AnalyticsRetentionRun, PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import { processDorisGlobalRetentionStep } from ".";

const cutoff = new Date("2026-06-20T00:00:00.000Z");

function run(phase: string, startedAt = new Date("2026-07-20T00:00:00.000Z")) {
  return {
    id: "run-1",
    cutoffDate: cutoff,
    phase,
    status: "RUNNING",
    lastErrorCode: null,
    startedAt,
    completedAt: null,
    createdAt: startedAt,
    updatedAt: startedAt,
  } as AnalyticsRetentionRun;
}

describe("processDorisGlobalRetentionStep", () => {
  it("publishes the cutoff and waits for the drain window before deleting", async () => {
    const deleteDorisHeads = vi.fn();
    const advance = vi.fn();

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        now: new Date("2026-07-20T00:01:00.000Z"),
        dependencies: {
          client: {} as PrismaClient,
          startOrResume: vi.fn(async () => run("DRAIN")) as never,
          deleteDorisHeads,
          advance: advance as never,
        },
      }),
    ).resolves.toMatchObject({ outcome: "waiting", phase: "DRAIN" });
    expect(deleteDorisHeads).not.toHaveBeenCalled();
    expect(advance).not.toHaveBeenCalled();
  });

  it("deletes one bounded head batch without advancing its durable phase", async () => {
    const heads = [{ id: "head-1" }] as never;
    const deleteDorisHeads = vi.fn(async () => undefined);
    const deleteHeads = vi.fn(async () => 1);
    const advance = vi.fn();

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        now: new Date("2026-07-20T00:03:00.000Z"),
        dependencies: {
          client: {} as PrismaClient,
          startOrResume: vi.fn(async () => run("EVENTS")) as never,
          findHeads: vi.fn(async () => heads) as never,
          deleteDorisHeads,
          deleteHeads: deleteHeads as never,
          advance: advance as never,
        },
      }),
    ).resolves.toMatchObject({ outcome: "processed", phase: "EVENTS" });
    expect(deleteDorisHeads).toHaveBeenCalledWith("run-1-EVENTS", heads);
    expect(deleteHeads).toHaveBeenCalledWith(
      expect.objectContaining({
        cutoffDate: cutoff,
        entityType: "EVENT",
        headIds: ["head-1"],
      }),
    );
    expect(advance).not.toHaveBeenCalled();
  });

  it("advances a projection phase after its old heads are exhausted", async () => {
    const advance = vi.fn(async () => true);

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        now: new Date("2026-07-20T00:03:00.000Z"),
        dependencies: {
          client: {} as PrismaClient,
          startOrResume: vi.fn(async () => run("EVENTS")) as never,
          findHeads: vi.fn(async () => []) as never,
          advance: advance as never,
        },
      }),
    ).resolves.toMatchObject({ outcome: "advanced", phase: "EVENTS" });
    expect(advance).toHaveBeenCalledWith(
      expect.objectContaining({
        runId: "run-1",
        expectedPhase: "EVENTS",
        nextPhase: "SCORES",
      }),
    );
  });

  it("keeps the drain fence active while an old load outcome is unresolved", async () => {
    const advance = vi.fn();
    const countUnresolvedLoads = vi.fn(async () => 1);

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        now: new Date("2026-07-20T00:03:00.000Z"),
        dependencies: {
          client: {} as PrismaClient,
          startOrResume: vi.fn(async () => run("DRAIN")) as never,
          countUnresolvedLoads: countUnresolvedLoads as never,
          advance: advance as never,
        },
      }),
    ).resolves.toMatchObject({ outcome: "waiting", phase: "DRAIN" });
    expect(countUnresolvedLoads).toHaveBeenCalledWith({
      client: expect.anything(),
      cutoffDate: cutoff,
    });
    expect(advance).not.toHaveBeenCalled();
  });

  it("advances the drain only after old load outcomes are settled", async () => {
    const advance = vi.fn(async () => true);

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        now: new Date("2026-07-20T00:03:00.000Z"),
        dependencies: {
          client: {} as PrismaClient,
          startOrResume: vi.fn(async () => run("DRAIN")) as never,
          countUnresolvedLoads: vi.fn(async () => 0) as never,
          advance: advance as never,
        },
      }),
    ).resolves.toMatchObject({ outcome: "advanced", phase: "DRAIN" });
    expect(advance).toHaveBeenCalledWith(
      expect.objectContaining({
        expectedPhase: "DRAIN",
        nextPhase: "EVENTS",
      }),
    );
  });

  it("records a retryable phase failure without moving the cutoff", async () => {
    const recordFailure = vi.fn(async () => undefined);

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        now: new Date("2026-07-20T00:03:00.000Z"),
        dependencies: {
          client: {} as PrismaClient,
          startOrResume: vi.fn(async () => run("SCORES")) as never,
          findHeads: vi.fn(async () => [{ id: "head-1" }]) as never,
          deleteDorisHeads: vi.fn(async () => {
            throw new Error("Doris unavailable");
          }),
          recordFailure: recordFailure as never,
        },
      }),
    ).rejects.toThrow("Doris unavailable");
    expect(recordFailure).toHaveBeenCalledWith(
      expect.objectContaining({
        runId: "run-1",
        phase: "SCORES",
        reasonCode: "RETENTION_STEP_FAILED",
      }),
    );
  });
});
