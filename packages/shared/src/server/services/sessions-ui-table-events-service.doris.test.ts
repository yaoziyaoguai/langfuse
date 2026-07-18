import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  sessions: {
    list: vi.fn(),
    count: vi.fn(),
  },
  traces: {
    list: vi.fn(),
  },
}));

vi.mock("../../db", () => ({ prisma: {} }));

vi.mock("../repositories/telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => mocks,
}));

import {
  getSessionTracesFromEvents,
  getSessionsTableCountFromEvents,
  getSessionsTableFromEvents,
  getSessionsWithMetricsFromEvents,
} from "./sessions-ui-table-events-service";

const session = {
  id: "session-1",
  projectId: "project-1",
  minTimestamp: new Date("2026-07-17T10:00:00.000Z"),
  maxTimestamp: new Date("2026-07-17T10:00:03.000Z"),
  traceIds: ["trace-1"],
  userIds: ["user-1"],
  environments: ["production"],
  tags: ["api"],
  traceCount: 1,
  observationCount: 2,
  totalInputTokens: 12,
  totalOutputTokens: 6,
  totalUsage: 18,
  totalCost: 0.5,
  duration: 3,
};

describe("Doris sessions UI service", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.sessions.list.mockResolvedValue({
      items: [session],
      nextCursor: null,
    });
  });

  it("routes list and count through the Doris repositories", async () => {
    mocks.sessions.count.mockResolvedValue(1);

    await expect(
      getSessionsTableFromEvents({
        projectId: "project-1",
        filter: [],
        page: 0,
        limit: 50,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        session_id: "session-1",
        trace_count: 1,
      }),
    ]);
    await expect(
      getSessionsTableCountFromEvents({
        projectId: "project-1",
        filter: [],
      }),
    ).resolves.toBe(1);
    expect(mocks.sessions.list).toHaveBeenCalledWith(
      expect.objectContaining({ projectId: "project-1", limit: 50 }),
    );
    expect(mocks.sessions.count).toHaveBeenCalledWith(
      expect.objectContaining({ projectId: "project-1" }),
    );
  });

  it("preserves the batch-export metrics row contract", async () => {
    await expect(
      getSessionsWithMetricsFromEvents({
        projectId: "project-1",
        filter: [],
        page: 0,
        limit: 50,
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        session_id: "session-1",
        total_observations: 2,
        session_total_usage: "18",
        session_total_cost: "0.5",
      }),
    ]);
  });

  it("routes session trace cards through the Doris trace repository", async () => {
    mocks.traces.list.mockResolvedValue({
      items: [
        {
          id: "trace-1",
          name: "trace",
          timestamp: new Date("2026-07-17T10:00:00.000Z"),
          environment: "production",
          userId: "user-1",
        },
      ],
      nextCursor: null,
    });

    await expect(
      getSessionTracesFromEvents({
        projectId: "project-1",
        sessionId: "session-1",
      }),
    ).resolves.toEqual([
      {
        id: "trace-1",
        name: "trace",
        timestamp: new Date("2026-07-17T10:00:00.000Z"),
        environment: "production",
        userId: "user-1",
      },
    ]);
  });
});
