import type { AnalyticsEntityHead, PrismaClient } from "@prisma/client";
import { describe, expect, it, vi } from "vitest";

import {
  deleteDorisRetentionBatch,
  processDorisProjectRetention,
} from "./dorisProjectRetention";

describe("Doris community project retention", () => {
  function client(retentionDays: number | null, activeRunId: string | null) {
    return {
      project: { findUnique: vi.fn(async () => ({ retentionDays })) },
      analyticsRetentionState: {
        findUnique: vi.fn(async () => ({ activeRunId })),
      },
    } as unknown as PrismaClient;
  }

  it("leaves a disabled project idle without starting a deletion", async () => {
    const processStep = vi.fn();
    await expect(
      processDorisProjectRetention(
        { projectId: "p1", queuedRetentionDays: 7 },
        { client: client(null, null), processStep },
      ),
    ).resolves.toEqual({ outcome: "idle" });
    expect(processStep).not.toHaveBeenCalled();
  });

  it("resumes a published cutoff after disable and schedules a bounded continuation", async () => {
    const cutoffDate = new Date("2026-09-01T00:00:00Z");
    const processStep = vi.fn(async () => ({
      outcome: "waiting" as const,
      runId: "run-1",
      phase: "DRAIN" as const,
      cutoffDate,
    }));
    const scheduleContinuation = vi.fn(async () => undefined);
    await processDorisProjectRetention(
      { projectId: "p1", queuedRetentionDays: 7 },
      {
        client: client(0, "run-1"),
        processStep,
        scheduleContinuation,
      },
    );
    expect(processStep).toHaveBeenCalledWith(
      expect.objectContaining({
        projectId: "p1",
        stateId: "project:p1",
        retentionDays: 7,
      }),
    );
    expect(scheduleContinuation).toHaveBeenCalledWith({
      retentionDays: 7,
      delayMs: 60_000,
    });
  });

  const heads = [
    { id: "h1", projectId: "p1", operationId: "op1" },
    { id: "h2", projectId: "p1", operationId: "op2" },
  ] as AnalyticsEntityHead[];

  it("preserves artifacts shared by retained heads and deletes objects before references", async () => {
    const order: string[] = [];
    const findOperations = vi.fn(async () => [
      { rawObjectKey: "raw/op2", canonicalObjectKey: "canonical/op2" },
    ]);
    const db = {
      analyticsEntityHead: {
        findMany: vi.fn(async () => [{ operationId: "op1" }]),
      },
      analyticsIngestionOperation: { findMany: findOperations },
    } as unknown as PrismaClient;
    const deleteObjects = vi.fn(async () => {
      order.push("objects");
    });
    const deleteMaterialized = vi.fn(async () => {
      order.push("doris");
    });
    await deleteDorisRetentionBatch("run-1", heads, {
      client: db,
      deleteObjects,
      deleteMaterialized,
    });
    expect(findOperations).toHaveBeenCalledWith(
      expect.objectContaining({
        where: { projectId: "p1", id: { in: ["op2"] }, status: "VISIBLE" },
      }),
    );
    expect(deleteObjects).toHaveBeenCalledWith(["raw/op2", "canonical/op2"]);
    expect(order).toEqual(["objects", "doris"]);
  });

  it("does not erase references when object deletion fails", async () => {
    const db = {
      analyticsEntityHead: { findMany: vi.fn(async () => []) },
      analyticsIngestionOperation: {
        findMany: vi.fn(async () => [
          { rawObjectKey: "raw", canonicalObjectKey: null },
        ]),
      },
    } as unknown as PrismaClient;
    const deleteMaterialized = vi.fn();
    await expect(
      deleteDorisRetentionBatch("run-1", heads, {
        client: db,
        deleteObjects: async () => {
          throw new Error("storage unavailable");
        },
        deleteMaterialized,
      }),
    ).rejects.toThrow("storage unavailable");
    expect(deleteMaterialized).not.toHaveBeenCalled();
  });

  it("rejects mixed-project batches before any external deletion", async () => {
    await expect(
      deleteDorisRetentionBatch("run-1", [
        heads[0]!,
        { ...heads[1]!, projectId: "p2" },
      ]),
    ).rejects.toThrow("multiple projects");
  });
});
