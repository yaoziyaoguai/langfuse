import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  addBulk: vi.fn(),
  findProjects: vi.fn(),
  findRetentionStates: vi.fn(),
  isDoris: vi.fn(),
}));

vi.mock("@langfuse/shared/src/db", () => ({
  prisma: {
    project: { findMany: mocks.findProjects },
    analyticsRetentionState: { findMany: mocks.findRetentionStates },
  },
}));

vi.mock("@langfuse/shared/src/server", () => ({
  analyticsProjectIdFromRetentionStateId: (stateId: string) =>
    stateId.startsWith("project:") ? stateId.slice("project:".length) : null,
  DataRetentionProcessingQueue: {
    getInstance: () => ({ addBulk: mocks.addBulk }),
  },
  isDorisAnalyticsBackend: mocks.isDoris,
  QueueJobs: {
    DataRetentionProcessingJob: "data-retention-processing-job",
  },
}));

import { handleDataRetentionSchedule } from "./handleDataRetentionSchedule";

describe("handleDataRetentionSchedule", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.addBulk.mockResolvedValue([]);
  });

  it("continues a Doris run after the project setting is disabled", async () => {
    mocks.isDoris.mockReturnValue(true);
    mocks.findRetentionStates.mockResolvedValue([{ id: "project:project-1" }]);
    mocks.findProjects.mockResolvedValue([
      { id: "project-1", retentionDays: 0 },
    ]);

    await handleDataRetentionSchedule();

    expect(mocks.findProjects).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          OR: [{ retentionDays: { gt: 0 } }, { id: { in: ["project-1"] } }],
        },
      }),
    );
    expect(mocks.addBulk).toHaveBeenCalledWith([
      expect.objectContaining({
        data: expect.objectContaining({
          payload: {
            projectId: "project-1",
            retention: 3,
          },
        }),
      }),
    ]);
  });

  it("keeps the existing ClickHouse project selection and retention value", async () => {
    mocks.isDoris.mockReturnValue(false);
    mocks.findRetentionStates.mockResolvedValue([]);
    mocks.findProjects.mockResolvedValue([
      { id: "project-1", retentionDays: 7 },
    ]);

    await handleDataRetentionSchedule();

    expect(mocks.findRetentionStates).not.toHaveBeenCalled();
    expect(mocks.findProjects).toHaveBeenCalledWith(
      expect.objectContaining({
        where: {
          OR: [{ retentionDays: { gt: 0 } }],
        },
      }),
    );
    expect(mocks.addBulk).toHaveBeenCalledWith([
      expect.objectContaining({
        data: expect.objectContaining({
          payload: {
            projectId: "project-1",
            retention: 7,
          },
        }),
      }),
    ]);
  });
});
