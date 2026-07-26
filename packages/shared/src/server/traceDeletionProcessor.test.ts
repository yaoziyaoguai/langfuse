import { beforeEach, describe, expect, it, vi } from "vitest";

const mocks = vi.hoisted(() => ({
  add: vi.fn(),
  createPending: vi.fn(),
  findProject: vi.fn(),
  isDoris: vi.fn().mockReturnValue(false),
  schedule: vi.fn(),
}));

vi.mock("../db", () => ({
  prisma: {
    pendingDeletion: { createMany: mocks.createPending },
    project: { findUniqueOrThrow: mocks.findProject },
  },
}));
vi.mock("../env", () => ({
  env: { LANGFUSE_TRACE_DELETE_DELAY_MS: 0 },
}));
vi.mock("./deletionGuard", () => ({
  shouldSkipDeletionFor: vi.fn().mockResolvedValue(false),
}));
vi.mock("./logger", () => ({
  logger: { debug: vi.fn(), error: vi.fn(), info: vi.fn(), warn: vi.fn() },
}));
vi.mock("./redis/traceDelete", () => ({
  TraceDeleteQueue: { getInstance: () => ({ add: mocks.add }) },
}));
vi.mock("./repositories/analyticsDeletionOperations", () => ({
  scheduleTraceDeletionOperations: mocks.schedule,
}));
vi.mock("./repositories/telemetry/doris/runtime", () => ({
  isDorisAnalyticsBackend: mocks.isDoris,
}));

import { traceDeletionProcessor } from "./traceDeletionProcessor";

describe("traceDeletionProcessor backend routing", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mocks.isDoris.mockReturnValue(false);
    mocks.createPending.mockResolvedValue({ count: 1 });
    mocks.findProject.mockResolvedValue({ orgId: "org-1" });
    mocks.add.mockResolvedValue(undefined);
    mocks.schedule.mockResolvedValue([
      {
        traceId: "trace-legacy",
        generation: 1n,
        operation: {
          id: "operation-legacy",
          status: "RETRYING",
          logicallyInvisible: false,
          analyticsBackend: null,
          deploymentGeneration: null,
          workloadEpochFingerprint: null,
          runtimeContractVersion: null,
          producerRuntimeLeaseId: null,
        },
      },
    ]);
  });

  it("keeps a managed ClickHouse job on the legacy queue contract", async () => {
    await traceDeletionProcessor("project-1", ["trace-1"], {
      organizationId: "org-1",
      analyticsAdmissionContext: {
        runtimeLeaseId: "runtime-current",
        backend: "clickhouse",
        deploymentGeneration: 7n,
      },
    });

    expect(mocks.schedule).not.toHaveBeenCalled();
    expect(mocks.findProject).not.toHaveBeenCalled();
    expect(mocks.add.mock.calls[0]?.[1].payload).toEqual({
      projectId: "project-1",
      traceIds: ["trace-1"],
    });
  });

  it("keeps a marker-absent ClickHouse job unstamped", async () => {
    await traceDeletionProcessor("project-legacy", ["trace-legacy"]);

    expect(mocks.schedule).not.toHaveBeenCalled();
    expect(mocks.add.mock.calls[0]?.[1].payload).toEqual({
      projectId: "project-legacy",
      traceIds: ["trace-legacy"],
    });
  });

  it("creates a durable operation only for managed Doris", async () => {
    mocks.isDoris.mockReturnValue(true);
    mocks.schedule.mockResolvedValue([
      {
        traceId: "trace-1",
        generation: 1n,
        operation: {
          id: "operation-1",
          status: "RETRYING",
          logicallyInvisible: false,
          analyticsBackend: "DORIS",
          deploymentGeneration: 7n,
          workloadEpochFingerprint: "a".repeat(64),
          runtimeContractVersion: 3,
          producerRuntimeLeaseId: "runtime-original",
        },
      },
    ]);

    await traceDeletionProcessor("project-1", ["trace-1"], {
      organizationId: "org-1",
      analyticsAdmissionContext: {
        runtimeLeaseId: "runtime-current",
        backend: "doris",
        deploymentGeneration: 7n,
      },
    });

    expect(mocks.schedule).toHaveBeenCalledOnce();
    expect(mocks.add.mock.calls[0]?.[1].payload.deletionOperations).toEqual([
      expect.objectContaining({
        operationId: "operation-1",
        analyticsProvenance: expect.objectContaining({
          analyticsBackend: "DORIS",
        }),
      }),
    ]);
  });
});
