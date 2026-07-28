import type { AnalyticsRetentionRun, PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import { processDorisGlobalRetentionStep } from ".";

const cutoff = new Date("2026-06-20T00:00:00.000Z");
const withWorkFence = async <T>(input: {
  execute: (client: PrismaClient) => Promise<T>;
}) => input.execute({} as PrismaClient);
const databaseClock = (value: string) => vi.fn(async () => new Date(value));

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
  it("carries the current runtime admission through the durable work fence", async () => {
    const admissionContext = {
      runtimeLeaseId: "worker-lease",
      backend: "doris" as const,
      deploymentGeneration: 7n,
    };
    const fencedClient = {} as PrismaClient;
    const advance = vi.fn(async () => true);
    const startOrResume = vi.fn(async () => run("DRAIN"));
    const fence = vi.fn(async (input) => {
      expect(input.admissionContext).toEqual(admissionContext);
      return input.execute(fencedClient);
    });

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        admissionContext,
        dependencies: {
          getDatabaseNow: databaseClock("2026-07-20T00:03:00.000Z"),
          client: {} as PrismaClient,
          startOrResume: startOrResume as never,
          countUnresolvedLoads: vi.fn(async () => 0) as never,
          advance: advance as never,
          withWorkFence: fence as never,
        },
      }),
    ).resolves.toMatchObject({ outcome: "advanced", phase: "DRAIN" });
    expect(startOrResume).toHaveBeenCalledWith(
      expect.objectContaining({ admissionContext, retentionDays: 30 }),
    );
    expect(startOrResume).not.toHaveBeenCalledWith(
      expect.objectContaining({ cutoffDate: expect.any(Date) }),
    );
    expect(fence).toHaveBeenCalledTimes(1);
    expect(advance).toHaveBeenCalledWith(
      expect.objectContaining({ client: fencedClient }),
    );
  });

  it("scopes project retention state, drains, and head selection to one project", async () => {
    const startOrResume = vi.fn(async () => run("EVENTS"));
    const findHeads = vi.fn(async () => []);
    const advance = vi.fn(async () => true);

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        scope: {
          stateId: "project:project-1",
          projectId: "project-1",
        },
        dependencies: {
          getDatabaseNow: databaseClock("2026-07-20T00:03:00.000Z"),
          withWorkFence,
          client: {} as PrismaClient,
          startOrResume: startOrResume as never,
          findHeads: findHeads as never,
          advance: advance as never,
        },
      }),
    ).resolves.toMatchObject({ outcome: "advanced", phase: "EVENTS" });
    expect(startOrResume).toHaveBeenCalledWith(
      expect.objectContaining({
        stateId: "project:project-1",
      }),
    );
    expect(findHeads).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "project-1",
      }),
    );
  });

  it("publishes the cutoff and waits for the drain window before deleting", async () => {
    const deleteDorisHeads = vi.fn();
    const advance = vi.fn();

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        dependencies: {
          getDatabaseNow: databaseClock("2026-07-20T00:01:00.000Z"),
          withWorkFence,
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
        dependencies: {
          getDatabaseNow: databaseClock("2026-07-20T00:03:00.000Z"),
          withWorkFence,
          client: {} as PrismaClient,
          startOrResume: vi.fn(async () => run("EVENTS")) as never,
          findHeads: vi.fn(async () => heads) as never,
          deleteDorisHeads,
          deleteHeads: deleteHeads as never,
          advance: advance as never,
        },
      }),
    ).resolves.toMatchObject({ outcome: "processed", phase: "EVENTS" });
    expect(deleteDorisHeads).toHaveBeenCalledWith("run-1-EVENTS", heads, {
      cutoffDate: cutoff,
    });
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
        dependencies: {
          getDatabaseNow: databaseClock("2026-07-20T00:03:00.000Z"),
          withWorkFence,
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
        dependencies: {
          getDatabaseNow: databaseClock("2026-07-20T00:03:00.000Z"),
          withWorkFence,
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
        dependencies: {
          getDatabaseNow: databaseClock("2026-07-20T00:03:00.000Z"),
          withWorkFence,
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
    const client = {} as PrismaClient;
    const fencedClient = {} as PrismaClient;

    await expect(
      processDorisGlobalRetentionStep({
        retentionDays: 30,
        drainMs: 120_000,
        batchSize: 1_000,
        dependencies: {
          getDatabaseNow: databaseClock("2026-07-20T00:03:00.000Z"),
          withWorkFence: async (input) => input.execute(fencedClient),
          client,
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
        client,
        runId: "run-1",
        phase: "SCORES",
        reasonCode: "RETENTION_STEP_FAILED",
      }),
    );
    expect(recordFailure.mock.calls[0]?.[0].client).toBe(client);
  });
});
