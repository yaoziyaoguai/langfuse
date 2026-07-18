import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  users: {
    list: vi.fn(),
    count: vi.fn(),
  },
  sessions: {
    list: vi.fn(),
    count: vi.fn(),
  },
  traces: {
    list: vi.fn(),
  },
  publicObservations: vi.fn(),
  publicObservationCount: vi.fn(),
  publicTraces: vi.fn(),
  publicTraceCount: vi.fn(),
}));

vi.mock("./telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: () => true,
  getDorisTelemetryRepositories: () => mocks,
}));

vi.mock("./telemetry/doris/publicApi", () => ({
  getDorisObservationsForPublicApi: mocks.publicObservations,
  getDorisObservationsCountForPublicApi: mocks.publicObservationCount,
}));

vi.mock("./telemetry/doris/publicTraces", () => ({
  getDorisTracesForPublicApi: mocks.publicTraces,
  getDorisTracesCountForPublicApi: mocks.publicTraceCount,
}));

import {
  getSessionMetricsFromEvents,
  getObservationsCountFromEventsTableForPublicApi,
  getObservationsV2FromEventsTableForPublicApi,
  getTracesCountFromEventsTableForPublicApi,
  getTracesFromEventsTableForPublicApi,
  getTracesIdentifierForSessionFromEvents,
  getUserMetricsFromEventsTable,
  getUsersCountFromEventsTable,
  getUsersFromEventsTable,
  hasAnySessionFromEventsTable,
  hasAnyUserFromEventsTable,
} from "./events";

const user = (id: string, traceCount: number) => ({
  id,
  projectId: "project-1",
  minTimestamp: new Date("2026-07-17T10:00:00.000Z"),
  maxTimestamp: new Date("2026-07-17T10:00:03.000Z"),
  sessionIds: ["session-1"],
  environments: ["production"],
  traceCount,
  sessionCount: 1,
  observationCount: 3,
  totalInputTokens: 12,
  totalOutputTokens: 6,
  totalUsage: 18,
  totalCost: 0.5,
});

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

describe("events repository Doris routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("paginates user rows and preserves their existing shape", async () => {
    mocks.users.list
      .mockResolvedValueOnce({
        items: [user("user-3", 3), user("user-2", 2)],
        nextCursor: "next",
      })
      .mockResolvedValueOnce({
        items: [user("user-1", 1)],
        nextCursor: null,
      });

    await expect(
      getUsersFromEventsTable("project-1", [], undefined, 2, 1),
    ).resolves.toEqual([
      { user: "user-2", count: "2" },
      { user: "user-1", count: "1" },
    ]);
  });

  it("routes user counts, existence, and metrics to Doris", async () => {
    mocks.users.count.mockResolvedValue(2);
    mocks.users.list.mockResolvedValue({
      items: [user("user-1", 2)],
      nextCursor: null,
    });

    await expect(
      getUsersCountFromEventsTable("project-1", []),
    ).resolves.toEqual([{ totalCount: "2" }]);
    await expect(hasAnyUserFromEventsTable("project-1")).resolves.toBe(true);
    await expect(
      getUserMetricsFromEventsTable("project-1", ["user-1"], []),
    ).resolves.toEqual([
      expect.objectContaining({
        userId: "user-1",
        traceCount: 2,
        totalUsage: 18,
      }),
    ]);
  });

  it("routes session existence and metrics to Doris", async () => {
    mocks.sessions.count.mockResolvedValue(1);
    mocks.sessions.list.mockResolvedValue({
      items: [session],
      nextCursor: null,
    });

    await expect(hasAnySessionFromEventsTable("project-1")).resolves.toBe(true);
    await expect(
      getSessionMetricsFromEvents({
        projectId: "project-1",
        sessionIds: ["session-1"],
      }),
    ).resolves.toEqual([
      expect.objectContaining({
        session_id: "session-1",
        total_observations: 2,
      }),
    ]);
  });

  it("routes session trace identifiers to Doris with stable ordering", async () => {
    mocks.traces.list.mockResolvedValue({
      items: [
        {
          id: "trace-2",
          userId: "user-1",
          name: "second",
          timestamp: new Date("2026-07-17T10:00:02.000Z"),
          environment: "production",
        },
        {
          id: "trace-1",
          userId: "user-1",
          name: "first",
          timestamp: new Date("2026-07-17T10:00:01.000Z"),
          environment: "production",
        },
      ],
      nextCursor: null,
    });

    await expect(
      getTracesIdentifierForSessionFromEvents("project-1", "session-1"),
    ).resolves.toEqual([
      expect.objectContaining({ id: "trace-1" }),
      expect.objectContaining({ id: "trace-2" }),
    ]);
  });

  it("loads the Doris public services without introducing a module cycle", async () => {
    mocks.publicObservations.mockResolvedValue([{ id: "observation-1" }]);
    mocks.publicObservationCount.mockResolvedValue(1);
    mocks.publicTraces.mockResolvedValue([{ id: "trace-1" }]);
    mocks.publicTraceCount.mockResolvedValue(1);

    await expect(
      getObservationsV2FromEventsTableForPublicApi({
        projectId: "project-1",
        page: 1,
        limit: 50,
        fields: ["core"],
      }),
    ).resolves.toEqual([{ id: "observation-1" }]);
    await expect(
      getObservationsCountFromEventsTableForPublicApi({
        projectId: "project-1",
        page: 1,
        limit: 50,
      }),
    ).resolves.toBe(1);
    await expect(
      getTracesFromEventsTableForPublicApi({
        projectId: "project-1",
        page: 1,
        limit: 50,
      }),
    ).resolves.toEqual([{ id: "trace-1" }]);
    await expect(
      getTracesCountFromEventsTableForPublicApi({
        projectId: "project-1",
        page: 1,
        limit: 50,
      }),
    ).resolves.toBe(1);
  });
});
