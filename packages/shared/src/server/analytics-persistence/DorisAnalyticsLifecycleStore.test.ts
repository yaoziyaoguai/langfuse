import { describe, expect, it, vi } from "vitest";

import { DorisAnalyticsLifecycleStore } from "./DorisAnalyticsLifecycleStore";

describe("DorisAnalyticsLifecycleStore", () => {
  it("publishes and verifies a trace visibility barrier", async () => {
    const load = vi.fn().mockResolvedValue({
      committed: true,
      requiresReconciliation: false,
    });
    const query = vi.fn().mockResolvedValue([{ deletion_generation: "7" }]);
    const store = new DorisAnalyticsLifecycleStore({
      streamLoad: { load, reconcile: vi.fn() },
      query,
      getDeletionProgress: vi.fn(),
    });

    await expect(
      store.publishTraceTombstone({
        operationId: "operation-1",
        projectId: "project-1",
        traceId: "trace-1",
        generation: 7n,
        createdAt: new Date("2026-07-18T00:00:00.000Z"),
      }),
    ).resolves.toEqual({
      projectId: "project-1",
      traceId: "trace-1",
      generation: 7n,
      visible: true,
      barrierLabel: expect.stringMatching(/^lf_trace_delete_/),
    });
    expect(load).toHaveBeenCalledWith(
      expect.objectContaining({
        table: "trace_tombstones",
        columns: [
          "project_id",
          "trace_id",
          "deletion_generation",
          "created_at",
        ],
        ndjsonBody:
          '{"project_id":"project-1","trace_id":"trace-1","deletion_generation":"7","created_at":"2026-07-18 00:00:00.000000"}\n',
      }),
    );
    expect(query).toHaveBeenCalledWith(
      expect.stringContaining("FROM trace_tombstones"),
      ["project-1", "trace-1", "7"],
    );
  });

  it("does not call an unknown load visible until label reconciliation passes", async () => {
    const reconcile = vi.fn().mockResolvedValue({
      status: "COMMITTED",
      visible: false,
    });
    const query = vi.fn();
    const store = new DorisAnalyticsLifecycleStore({
      streamLoad: {
        load: vi.fn().mockResolvedValue({
          committed: false,
          requiresReconciliation: true,
        }),
        reconcile,
      },
      query,
      getDeletionProgress: vi.fn(),
    });

    await expect(
      store.publishProjectTombstone({
        operationId: "operation-2",
        projectId: "project-2",
        generation: 3n,
        createdAt: new Date("2026-07-18T01:00:00.000Z"),
      }),
    ).resolves.toMatchObject({ visible: false });
    expect(reconcile).toHaveBeenCalledOnce();
    expect(query).not.toHaveBeenCalled();
  });
});
